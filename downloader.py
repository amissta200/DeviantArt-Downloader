import os
import time
import json
import logging
import sqlite3
import requests
import urllib.parse
import threading
from queue import Queue
from http.server import BaseHTTPRequestHandler, HTTPServer
from concurrent.futures import ThreadPoolExecutor, as_completed

# --------------------------
# ENV CONFIG
# --------------------------
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
USERNAME = os.getenv("USERNAME")

SAVE_DIR = os.getenv("SAVE_DIR", "./downloads")
DB_PATH = os.getenv("DB_PATH", "./downloads/deviantart.db")

SLEEP_TIME = float(os.getenv("SLEEP_TIME", 0.5))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 6))
RATE_LIMIT_SLEEP = int(os.getenv("RATE_LIMIT_SLEEP", 30))
FORCE_RECHECK = os.getenv("FORCE_RECHECK", "false").lower() == "true"

AUTOTAGGER_URL = os.getenv("AUTOTAGGER_URL", "")
ENABLE_AUTOTAGGER = os.getenv("ENABLE_AUTOTAGGER", "false").lower() == "true"

TOKEN_FILE = os.path.join(SAVE_DIR, "token.json")
PROGRESS_FILE = os.path.join(SAVE_DIR, "progress.json")

REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:8080/callback")

os.makedirs(SAVE_DIR, exist_ok=True)

# --------------------------
# LOGGING
# --------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def log_event(level, event, **kwargs):
    msg = f"{event} | " + " | ".join(f"{k}={v}" for k, v in kwargs.items())
    getattr(logging, level)(msg)

# --------------------------
# QUEUES
# --------------------------
db_queue = Queue()
tag_queue = Queue()

# --------------------------
# DB INIT
# --------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("PRAGMA journal_mode=WAL;")

    c.execute("""
        CREATE TABLE IF NOT EXISTS downloads (
            deviationid TEXT PRIMARY KEY,
            artist TEXT,
            title TEXT,
            url TEXT,
            tags TEXT,
            downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()

init_db()

# --------------------------
# DB WORKER (SAFE)
# --------------------------
def db_worker():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    while True:
        item = db_queue.get()
        if item is None:
            break

        dev_id, artist, title, url, tags = item

        try:
            c.execute("""
                INSERT OR IGNORE INTO downloads (deviationid, artist, title, url, tags)
                VALUES (?, ?, ?, ?, ?)
            """, (dev_id, artist, title, url, "\n".join(tags)))
            conn.commit()
        except Exception as e:
            log_event("error", "DB_WRITE_FAIL", id=dev_id, error=str(e))

        db_queue.task_done()

def is_downloaded(dev_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT 1 FROM downloads WHERE deviationid = ?", (dev_id,))
    result = c.fetchone()
    conn.close()
    return result is not None

# --------------------------
# TAG WORKER
# --------------------------
def tag_worker():
    while True:
        item = tag_queue.get()
        if item is None:
            break

        dev_id, artist, title, url, img_path = item
        tags = []

        if ENABLE_AUTOTAGGER:
            try:
                with open(img_path, "rb") as f:
                    res = requests.post(
                        AUTOTAGGER_URL,
                        files={"file": f},
                        data={"format": "json"},
                        timeout=60
                    )

                res.raise_for_status()

                # 🔍 Debug (optional, remove later)
                if not res.text.strip():
                    raise Exception("Empty response from autotagger")

                data = res.json()

                # ✅ Match your old working format
                if isinstance(data, list) and data:
                    tags_dict = data[0].get("tags", {})
                    tags = list(tags_dict.keys())

                log_event("info", "TAGGED", id=dev_id, tags=len(tags))

            except Exception as e:
                log_event("error", "TAG_FAIL", id=dev_id, error=str(e))

        db_queue.put((dev_id, artist, title, url, tags))
        tag_queue.task_done()

# --------------------------
# OAUTH
# --------------------------
AUTH_CODE = None

class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global AUTH_CODE
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if "code" in params:
            AUTH_CODE = params["code"][0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Auth success. You can close this.")
        else:
            self.send_response(400)
            self.end_headers()

def get_auth_code():
    global AUTH_CODE

    auth_url = (
        "https://www.deviantart.com/oauth2/authorize"
        f"?response_type=code"
        f"&client_id={CLIENT_ID}"
        f"&redirect_uri={urllib.parse.quote(REDIRECT_URI, safe='')}"
        f"&scope=browse%20user"
    )

    print("\n=== OPEN THIS URL ===")
    print(auth_url)
    print("====================\n")

    logging.warning(f"AUTH URL: {auth_url}")

    server = HTTPServer(("0.0.0.0", 8080), CallbackHandler)

    while AUTH_CODE is None:
        server.handle_request()

    return AUTH_CODE

# --------------------------
# TOKEN
# --------------------------
def save_token(token):
    token["expires_at"] = time.time() + token["expires_in"]
    with open(TOKEN_FILE, "w") as f:
        json.dump(token, f)

def load_token():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE) as f:
            return json.load(f)
    return None

def request_token(data):
    r = requests.post("https://www.deviantart.com/oauth2/token", data=data)
    r.raise_for_status()
    return r.json()

def get_access_token():
    token = load_token()

    if not token:
        code = get_auth_code()
        token = request_token({
            "grant_type": "authorization_code",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "redirect_uri": REDIRECT_URI,
            "code": code
        })
        save_token(token)
        return token["access_token"]

    if time.time() > token["expires_at"]:
        token = request_token({
            "grant_type": "refresh_token",
            "refresh_token": token["refresh_token"],
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET
        })
        save_token(token)

    return token["access_token"]

# --------------------------
# REQUEST
# --------------------------
def deviantart_get(url, token, params=None):
    headers = {"Authorization": f"Bearer {token}"}

    while True:
        r = requests.get(url, headers=headers, params=params)

        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", RATE_LIMIT_SLEEP))
            log_event("warning", "RATE_LIMIT", wait=wait)
            time.sleep(wait)
            continue

        if r.status_code == 401:
            token = get_access_token()
            headers["Authorization"] = f"Bearer {token}"
            continue

        r.raise_for_status()
        return r.json()

# --------------------------
# HELPERS
# --------------------------
def is_blurred(url):
    return any(x in url for x in ["/v1/fit/", "/preview/"])

# --------------------------
# DOWNLOAD
# --------------------------
def save_deviation(token, artist, dev):
    dev_id = dev["deviationid"]
    title = dev.get("title", "untitled")
    url = dev.get("url")

    if is_downloaded(dev_id):
        return

    content = dev.get("content")
    download = dev.get("download")

    img_url = None

    # prefer content
    if content and content.get("src"):
        img_url = content["src"]

    # upgrade to download if better
    if download and download.get("src"):
        if not img_url or download.get("filesize", 0) > content.get("filesize", 0):
            img_url = download["src"]

    if not img_url:
        log_event("warning", "SKIP_NO_MEDIA", id=dev_id)
        return

    if is_blurred(img_url):
        log_event("warning", "SKIP_BLUR", id=dev_id)
        return

    artist_dir = os.path.join(SAVE_DIR, artist)
    os.makedirs(artist_dir, exist_ok=True)

    img_path = os.path.join(artist_dir, f"{dev_id}.jpg")

    try:
        res = requests.get(img_url, timeout=30)
        res.raise_for_status()

        with open(img_path, "wb") as f:
            f.write(res.content)

        tag_queue.put((dev_id, artist, title, url, img_path))

        log_event("info", "DOWNLOADED", id=dev_id, artist=artist)

    except Exception as e:
        log_event("error", "DOWNLOAD_FAIL", id=dev_id, error=str(e))

# --------------------------
# MAIN
# --------------------------
def main():
    token = get_access_token()

    # workers
    threading.Thread(target=db_worker, daemon=True).start()

    for _ in range(3):
        threading.Thread(target=tag_worker, daemon=True).start()

    progress = load_progress()

    friends_url = f"https://www.deviantart.com/api/v1/oauth2/user/friends/{USERNAME}"
    data = deviantart_get(friends_url, token)
    artists = [f['user']['username'] for f in data.get("results", [])]

    for artist in artists:
        state = progress.get(artist, {"offset": 0, "done": False})

        if state["done"] and not FORCE_RECHECK:
            continue

        offset = state["offset"]

        while True:
            data = deviantart_get(
                "https://www.deviantart.com/api/v1/oauth2/gallery/all",
                token,
                {
                    "username": artist,
                    "offset": offset,
                    "limit": 24,
                    "mature_content": "true",
                    "expand": "deviation.download"
                }
            )

            results = data.get("results", [])
            if not results:
                break

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
                futures = [exe.submit(save_deviation, token, artist, d) for d in results]
                for f in as_completed(futures):
                    f.result()

            offset = data.get("next_offset", 0)
            progress[artist] = {"offset": offset, "done": False}
            save_progress(progress)

            if not data.get("has_more"):
                break

        progress[artist] = {"offset": 0, "done": True}
        save_progress(progress)

    # shutdown
    tag_queue.join()
    for _ in range(3):
        tag_queue.put(None)

    db_queue.join()
    db_queue.put(None)

# --------------------------
# PROGRESS
# --------------------------
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {}

def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)

# --------------------------
# RUN
# --------------------------
if __name__ == "__main__":
    main()