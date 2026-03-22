import os
import time
import json
import sqlite3
import requests
import urllib.parse
import threading
import shutil
import sys
from datetime import datetime
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
TEMP_DIR = os.getenv("TEMP_DIR", "./temporary")
DB_PATH = os.getenv("DB_PATH", "./downloads/deviantart.db")

SLEEP_TIME = float(os.getenv("SLEEP_TIME", 0.5))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 6))
RATE_LIMIT_SLEEP = int(os.getenv("RATE_LIMIT_SLEEP", 30))
FORCE_RECHECK = os.getenv("FORCE_RECHECK", "false").lower() == "true"

TOKEN_FILE = os.path.join(SAVE_DIR, "token.json")
PROGRESS_FILE = os.path.join(SAVE_DIR, "progress.json")

REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:8080/callback")

os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(SAVE_DIR, exist_ok=True)

# --------------------------
# LOGGING (COLOR + THREAD)
# --------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG")

COLORS = {
    "DEBUG": "\033[90m",
    "INFO": "\033[94m",
    "WARNING": "\033[93m",
    "ERROR": "\033[91m",
    "CRITICAL": "\033[95m",
    "END": "\033[0m",
}

def log(level, msg, **kwargs):
    if level == "DEBUG" and LOG_LEVEL != "DEBUG":
        return

    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    thread = threading.current_thread().name
    context = " ".join(f"{k}={v}" for k, v in kwargs.items())

    color = COLORS.get(level, "")
    end = COLORS["END"]

    print(f"{color}[{ts}] [{level}] [{thread}] {msg} {context}{end}", file=sys.stderr)

def dump_json(label, data, max_len=2000):
    try:
        txt = json.dumps(data, indent=2)[:max_len]
        log("DEBUG", label, dump=txt)
    except Exception as e:
        log("ERROR", "JSON_DUMP_FAIL", error=str(e))

# --------------------------
# SAFETY WRAPPER
# --------------------------
def safe_run(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        log("CRITICAL", "THREAD_CRASH", fn=fn.__name__, error=str(e))
        raise

# --------------------------
# QUEUES
# --------------------------
db_queue = Queue()
tag_queue = Queue()

# --------------------------
# DB
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

def db_worker():
    try:
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
                log("DEBUG", "DB_WRITE", id=dev_id)
            except Exception as e:
                log("ERROR", "DB_WRITE_FAIL", id=dev_id, error=str(e))

            db_queue.task_done()
    except Exception as e:
        log("CRITICAL", "DB_WORKER_CRASH", error=str(e))

def is_downloaded(dev_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT 1 FROM downloads WHERE deviationid = ?", (dev_id,))
    result = c.fetchone()
    conn.close()
    return result is not None

def safe_move(src, dst):
    try:
        os.replace(src, dst)
    except OSError:
        shutil.copy2(src, dst)
        os.remove(src)

# --------------------------
# TAG WORKER (simplified)
# --------------------------
def tag_worker():
    try:
        while True:
            item = tag_queue.get()
            if item is None:
                break

            dev_id, artist, title, url, img_path = item

            txt_path = os.path.splitext(img_path)[0] + ".txt"

            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(f"title:{title}\nurl:{url}\n")

            final_dir = os.path.join(SAVE_DIR, artist)
            os.makedirs(final_dir, exist_ok=True)

            final_img = os.path.join(final_dir, os.path.basename(img_path))
            final_txt = os.path.splitext(final_img)[0] + ".txt"

            safe_move(img_path, final_img)
            safe_move(txt_path, final_txt)

            db_queue.put((dev_id, artist, title, url, []))

            log("INFO", "FINALIZED", id=dev_id)

            tag_queue.task_done()
    except Exception as e:
        log("CRITICAL", "TAG_WORKER_CRASH", error=str(e))

# --------------------------
# AUTH
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
            self.wfile.write(b"Auth success.")
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

    print(auth_url)

    server = HTTPServer(("0.0.0.0", 8080), CallbackHandler)

    while AUTH_CODE is None:
        server.handle_request()

    return AUTH_CODE

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
# REQUEST (VERBOSE)
# --------------------------
def deviantart_get(url, token, params=None):
    headers = {"Authorization": f"Bearer {token}"}

    while True:
        log("DEBUG", "API_REQUEST", url=url, params=params)

        r = requests.get(url, headers=headers, params=params)

        log("DEBUG", "API_RESPONSE", status=r.status_code, url=r.url)

        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", RATE_LIMIT_SLEEP))
            log("WARNING", "RATE_LIMIT", wait=wait)
            time.sleep(wait)
            continue

        if r.status_code == 401:
            log("WARNING", "TOKEN_EXPIRED")
            token = get_access_token()
            headers["Authorization"] = f"Bearer {token}"
            continue

        data = r.json()
        dump_json("API_DATA", data)

        r.raise_for_status()
        return data

# --------------------------
# DOWNLOAD LOGIC
# --------------------------
def save_deviation(token, artist, dev):
    dev_id = dev["deviationid"]
    title = dev.get("title", "untitled")

    log("DEBUG", "PROCESS_DEV", id=dev_id)
    dump_json("DEV_FULL", dev)

    if is_downloaded(dev_id):
        return

    content = dev.get("content")
    download = dev.get("download")
    is_mature = dev.get("is_mature", False)

    log("DEBUG", "MEDIA_CHECK",
        has_content=bool(content),
        has_download=bool(download),
        is_mature=is_mature
    )

    img_url = None

    if is_mature:
        if download and download.get("src"):
            img_url = download["src"]
        else:
            log("WARNING", "SKIP_MATURE_NO_DOWNLOAD", id=dev_id)
            return
    else:
        if download and download.get("src"):
            img_url = download["src"]
        elif content and content.get("src"):
            img_url = content["src"]

    if not img_url:
        log("WARNING", "SKIP_NO_MEDIA", id=dev_id)
        return

    if "token=" in img_url:
        log("WARNING", "SKIP_TOKENIZED", id=dev_id)
        return

    path = os.path.join(TEMP_DIR, artist)
    os.makedirs(path, exist_ok=True)

    img_path = os.path.join(path, f"{dev_id}.jpg")

    res = requests.get(img_url)
    with open(img_path, "wb") as f:
        f.write(res.content)

    tag_queue.put((dev_id, artist, title, dev.get("url"), img_path))

    log("INFO", "DOWNLOADED", id=dev_id)

# --------------------------
# MAIN
# --------------------------
def main():
    token = get_access_token()

    threading.Thread(target=db_worker, daemon=True, name="DB").start()

    for i in range(3):
        threading.Thread(target=tag_worker, daemon=True, name=f"TAG-{i}").start()

    data = deviantart_get(
        f"https://www.deviantart.com/api/v1/oauth2/user/friends/{USERNAME}",
        token
    )

    artists = [f['user']['username'] for f in data.get("results", [])]

    for artist in artists:
        log("INFO", "ARTIST", name=artist)

        data = deviantart_get(
            "https://www.deviantart.com/api/v1/oauth2/gallery/all",
            token,
            {
                "username": artist,
                "limit": 24,
                "mature_content": "true",
                "expand": "deviation.download,deviation.content,deviation.flags"
            }
        )

        results = data.get("results", [])

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
            futures = [exe.submit(safe_run, save_deviation, token, artist, d) for d in results]
            for f in as_completed(futures):
                f.result()

    tag_queue.join()
    db_queue.join()

# --------------------------
# RUN
# --------------------------
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log("CRITICAL", "MAIN_CRASH", error=str(e))
        raise