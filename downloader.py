import os
import time
import json
import logging
import sqlite3
import requests
import urllib.parse
import threading
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
# DB (THREAD SAFE)
# --------------------------
thread_local = threading.local()

def get_db():
    if not hasattr(thread_local, "conn"):
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        thread_local.conn = conn
    return thread_local.conn

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("PRAGMA journal_mode=WAL;")  # ✅ ONLY HERE

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

def is_downloaded(deviation_id):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT 1 FROM downloads WHERE deviationid = ?", (deviation_id,))
    return c.fetchone() is not None

def mark_downloaded(dev_id, artist, title, url, tags):
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        INSERT OR IGNORE INTO downloads (deviationid, artist, title, url, tags)
        VALUES (?, ?, ?, ?, ?)
    """, (dev_id, artist, title, url, "\n".join(tags)))
    conn.commit()

# --------------------------
# OAUTH SERVER
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

    logging.warning("\n" + "="*60)
    logging.warning("🔐 AUTHORIZE THIS APP (OPEN IN YOUR BROWSER):")
    logging.warning(auth_url)
    logging.warning("="*60 + "\n")

    print("\n=== COPY THIS URL ===")
    print(auth_url)
    print("====================\n")

    server = HTTPServer(("0.0.0.0", 8080), CallbackHandler)
    logging.info("🌐 Waiting for OAuth callback on port 8080...")

    while AUTH_CODE is None:
        server.handle_request()

    logging.info("✅ Received OAuth code")
    return AUTH_CODE

# --------------------------
# TOKEN HANDLING
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
    url = "https://www.deviantart.com/oauth2/token"
    r = requests.post(url, data=data)
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
        log_event("info", "TOKEN_REFRESH")
        token = request_token({
            "grant_type": "refresh_token",
            "refresh_token": token["refresh_token"],
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET
        })
        save_token(token)

    return token["access_token"]

# --------------------------
# REQUEST WRAPPER
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
# HELPERS
# --------------------------
def is_blurred(url):
    return any(x in url for x in [
        "/v1/fit/",      # scaled preview
        "/preview/"      # explicit preview
    ])

# --------------------------
# DOWNLOAD
# --------------------------
def save_deviation(token, artist, dev):
    dev_id = dev["deviationid"]
    title = dev.get("title", "untitled")
    url = dev.get("url")

    if is_downloaded(dev_id):
        return

    img_url = None
    source_type = None

    # 1. Best: original download
    content = dev.get("content")
    download = dev.get("download")

    print("CONTENT:", content)
    print("DOWNLOAD:", download.get("src") if download else None)

    img_url = None
    source = None

    # 1. Best: original file
    if download and download.get("src"):
        img_url = download["src"]
        source = "download"

    # 2. Fallback: content
    elif content and content.get("src"):
        img_url = content["src"]
        source = "content"

    # 3. Nothing usable
    if not img_url:
        log_event("warning", "SKIP_NO_MEDIA", id=dev_id)
        return

    # Blur filter
    if is_blurred(img_url):
        log_event("warning", "SKIP_BLUR", id=dev_id, source=source)
        return

    artist_dir = os.path.join(SAVE_DIR, artist)
    os.makedirs(artist_dir, exist_ok=True)

    img_path = os.path.join(artist_dir, f"{dev_id}.jpg")

    try:
        res = requests.get(img_url, timeout=30)
        res.raise_for_status()

        with open(img_path, "wb") as f:
            f.write(res.content)

        mark_downloaded(dev_id, artist, title, url, [])
        log_event("info", "DOWNLOADED", id=dev_id, artist=artist)

    except Exception as e:
        log_event("error", "DOWNLOAD_FAIL", id=dev_id, error=str(e))

# --------------------------
# MAIN
# --------------------------
def main():
    token = get_access_token()
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

if __name__ == "__main__":
    main()