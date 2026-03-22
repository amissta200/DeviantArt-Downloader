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

MAX_WORKERS = int(os.getenv("MAX_WORKERS", 6))
RATE_LIMIT_SLEEP = int(os.getenv("RATE_LIMIT_SLEEP", 30))

MIN_FILE_SIZE = int(os.getenv("MIN_FILE_SIZE", 15000)
)
ONLY_FULL_QUALITY = os.getenv("ONLY_FULL_QUALITY", "false").lower() == "true"

ENABLE_AUTOTAGGER = os.getenv("ENABLE_AUTOTAGGER", "false").lower() == "true"
AUTOTAGGER_URL = os.getenv("AUTOTAGGER_URL", "")

TOKEN_FILE = os.path.join(SAVE_DIR, "token.json")
PROGRESS_FILE = os.path.join(SAVE_DIR, "progress.json")

REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:8080/callback")

os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(SAVE_DIR, exist_ok=True)

# --------------------------
# LOGGING
# --------------------------
def log(level, msg, **kwargs):
    ts = datetime.now().strftime("%H:%M:%S")
    thread = threading.current_thread().name
    context = " ".join(f"{k}={v}" for k, v in kwargs.items())
    print(f"[{ts}] [{level}] [{thread}] {msg} {context}", flush=True)

# --------------------------
# SAFE JSON DUMP
# --------------------------
def dump_json(label, data, max_len=1500):
    try:
        txt = json.dumps(data, indent=2)
        if len(txt) > max_len:
            txt = txt[:max_len] + " ...TRUNCATED"
        log("DEBUG", label, dump=txt)
    except Exception:
        log("DEBUG", label, keys=list(data.keys()) if isinstance(data, dict) else "n/a")

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
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    while True:
        item = db_queue.get()
        if item is None:
            break

        try:
            dev_id, artist, title, url, tags = item

            c.execute("""
                INSERT OR IGNORE INTO downloads
                (deviationid, artist, title, url, tags)
                VALUES (?, ?, ?, ?, ?)
            """, (dev_id, artist, title, url, "\n".join(tags)))

            conn.commit()
            log("DEBUG", "DB_WRITE", id=dev_id)

        except Exception as e:
            log("ERROR", "DB_FAIL", error=str(e))

        db_queue.task_done()

def is_downloaded(dev_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT 1 FROM downloads WHERE deviationid=?", (dev_id,))
    r = c.fetchone()
    conn.close()
    return r is not None

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

                log("DEBUG", "TAGGER_STATUS", code=res.status_code)

                # ❌ HTTP failure
                if res.status_code != 200:
                    log("ERROR", "TAG_HTTP_FAIL",
                        code=res.status_code,
                        body=res.text[:300])
                    raise Exception("Bad HTTP status")

                # ❌ Empty response
                if not res.text or not res.text.strip():
                    log("ERROR", "TAG_EMPTY_RESPONSE")
                    raise Exception("Empty response")

                # ❌ Try parse JSON safely
                try:
                    data = res.json()
                except Exception:
                    log("ERROR", "TAG_INVALID_JSON",
                        preview=res.text[:300])
                    raise

                # ❌ Unexpected format
                if not isinstance(data, list) or not data:
                    log("ERROR", "TAG_BAD_FORMAT", data=data)
                    raise Exception("Unexpected JSON format")

                tags_dict = data[0].get("tags", {})
                tags = list(tags_dict.keys())

                log("INFO", "TAGGED", id=dev_id, count=len(tags))

            except Exception as e:
                log("ERROR", "TAG_FAIL", id=dev_id, error=str(e))
        try:
            txt_path = img_path.replace(".jpg", ".txt")
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(f"title:{title}\nurl:{url}\n\n")
                if tags:
                    f.write("\n".join(tags))
        except Exception as e:
            log("ERROR", "TXT_FAIL", error=str(e))

        try:
            final_dir = os.path.join(SAVE_DIR, artist)
            os.makedirs(final_dir, exist_ok=True)

            final_img = os.path.join(final_dir, os.path.basename(img_path))
            final_txt = final_img.replace(".jpg", ".txt")

            shutil.move(img_path, final_img)
            shutil.move(txt_path, final_txt)

            db_queue.put((dev_id, artist, title, url, tags))
            log("INFO", "FINALIZED", id=dev_id)

        except Exception as e:
            log("ERROR", "MOVE_FAIL", error=str(e))

        tag_queue.task_done()

# --------------------------
# AUTH (BULLETPROOF)
# --------------------------
AUTH_CODE = None

SCOPES = [
    "browse user",   # preferred
    "browse"         # fallback
]

class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global AUTH_CODE
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        AUTH_CODE = params.get("code", [None])[0]

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Auth success. You can close this.")

def get_auth_code(scope):
    global AUTH_CODE
    AUTH_CODE = None

    auth_url = (
        "https://www.deviantart.com/oauth2/authorize"
        f"?response_type=code"
        f"&client_id={CLIENT_ID}"
        f"&redirect_uri={urllib.parse.quote(REDIRECT_URI, safe='')}"
        f"&scope={urllib.parse.quote(scope)}"
    )

    print("\n=== AUTHORIZE ===")
    print(auth_url)
    print("=================\n")

    server = HTTPServer(("0.0.0.0", 8080), CallbackHandler)

    while AUTH_CODE is None:
        server.handle_request()

    return AUTH_CODE

# --------------------------
# TOKEN HELPERS
# --------------------------
def safe_json(res):
    try:
        return res.json()
    except Exception:
        return {"error": "invalid_json", "raw": res.text[:500]}

def validate_token(token):
    """Make a small API call to confirm token is actually usable"""
    try:
        r = requests.get(
            "https://www.deviantart.com/api/v1/oauth2/user/whoami",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10
        )
        return r.status_code == 200
    except Exception:
        return False

# --------------------------
# REQUEST TOKEN (robust)
# --------------------------
def request_token_with_code(code, scope):
    res = requests.post(
        "https://www.deviantart.com/oauth2/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "redirect_uri": REDIRECT_URI
        }
    )

    data = safe_json(res)
    log("DEBUG", "TOKEN_RESPONSE", data=data)

    if "access_token" not in data:
        raise Exception(f"Token exchange failed ({scope}): {data}")

    return data

def refresh_token(old_token):
    res = requests.post(
        "https://www.deviantart.com/oauth2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": old_token.get("refresh_token"),
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET
        }
    )

    data = safe_json(res)
    log("DEBUG", "REFRESH_RESPONSE", data=data)

    if "access_token" not in data:
        raise Exception(f"Refresh failed: {data}")

    return data

# --------------------------
# MAIN TOKEN ENTRY
# --------------------------
def get_token():
    token = None

    # --------------------------
    # LOAD TOKEN
    # --------------------------
    if os.path.exists(TOKEN_FILE):
        try:
            token = json.load(open(TOKEN_FILE))
            log("INFO", "TOKEN_LOADED")
        except Exception:
            log("WARNING", "TOKEN_CORRUPT")
            token = None

    # --------------------------
    # VALID TOKEN?
    # --------------------------
    if token:
        if time.time() < token.get("expires_at", 0):
            if validate_token(token["access_token"]):
                log("INFO", "TOKEN_VALID")
                return token["access_token"]
            else:
                log("WARNING", "TOKEN_INVALID")

        # try refresh
        try:
            log("INFO", "REFRESHING_TOKEN")
            token = refresh_token(token)
        except Exception as e:
            log("WARNING", "REFRESH_FAILED", error=str(e))
            token = None

    # --------------------------
    # FULL AUTH FLOW (WITH FALLBACK SCOPES)
    # --------------------------
    if not token:
        for scope in SCOPES:
            try:
                log("INFO", "TRY_SCOPE", scope=scope)

                code = get_auth_code(scope)
                token = request_token_with_code(code, scope)

                if validate_token(token["access_token"]):
                    log("INFO", "TOKEN_VALIDATED", scope=scope)
                    break
                else:
                    raise Exception("Token failed validation")

            except Exception as e:
                log("ERROR", "AUTH_ATTEMPT_FAILED", scope=scope, error=str(e))
                token = None

        if not token:
            raise Exception("All auth attempts failed")

    # --------------------------
    # FINALIZE TOKEN
    # --------------------------
    expires_in = token.get("expires_in", 3600)
    token["expires_at"] = time.time() + expires_in

    try:
        json.dump(token, open(TOKEN_FILE, "w"))
        log("INFO", "TOKEN_SAVED", expires_in=expires_in)
    except Exception as e:
        log("WARNING", "TOKEN_SAVE_FAIL", error=str(e))

    return token["access_token"]

# --------------------------
# REQUEST
# --------------------------
def api_get(url, token, params=None):
    while True:
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params=params)

        if r.status_code == 429:
            log("WARN", "RATE_LIMIT")
            time.sleep(RATE_LIMIT_SLEEP)
            continue

        if r.status_code == 401:
            log("WARN", "TOKEN_REFRESH")
            token = get_token()
            continue

        r.raise_for_status()
        return r.json()

# --------------------------
# DOWNLOAD
# --------------------------
def download_file(url, path):
    for i in range(3):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()

            if len(r.content) < MIN_FILE_SIZE:
                log("WARN", "TOO_SMALL", size=len(r.content))
                return False

            with open(path, "wb") as f:
                f.write(r.content)

            return True
        except Exception as e:
            log("ERROR", "RETRY", attempt=i, error=str(e))
            time.sleep(2)

    return False

# --------------------------
# CORE
# --------------------------
def process_dev(token, artist, dev):
    dev_id = dev["deviationid"]

    if is_downloaded(dev_id):
        return

    content = dev.get("content")
    download = dev.get("download")
    is_mature = dev.get("is_mature", False)

    img_url = None
    source = None

    if download and download.get("src"):
        img_url = download["src"]
        source = "download"
    elif content and content.get("src") and not is_mature:
        img_url = content["src"]
        source = "content"
    else:
        return

    if any(x in img_url for x in ["/v1/fit/", "/preview/"]):
        return

    if ONLY_FULL_QUALITY and source != "download":
        return

    temp_dir = os.path.join(TEMP_DIR, artist)
    os.makedirs(temp_dir, exist_ok=True)

    img_path = os.path.join(temp_dir, f"{dev_id}.jpg")

    if not download_file(img_url, img_path):
        return

    tag_queue.put((dev_id, artist, dev.get("title"), dev.get("url"), img_path))

    log("INFO", "DOWNLOADED", id=dev_id, src=source)

# --------------------------
# MAIN
# --------------------------
def main():
    token = get_token()

    threading.Thread(target=db_worker, daemon=True, name="DB").start()
    for i in range(3):
        threading.Thread(target=tag_worker, daemon=True, name=f"TAG-{i}").start()

    progress = load_progress()

    friends = api_get(
        f"https://www.deviantart.com/api/v1/oauth2/user/friends/{USERNAME}",
        token
    )["results"]

    for f in friends:
        artist = f["user"]["username"]
        offset = progress.get(artist, 0)

        while True:
            data = api_get(
                "https://www.deviantart.com/api/v1/oauth2/gallery/all",
                token,
                {
                    "username": artist,
                    "offset": offset,
                    "limit": 24,
                    "mature_content": "true",
                    "expand": "deviation.download,deviation.content"
                }
            )

            results = data.get("results", [])
            if not results:
                break

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
                futures = [exe.submit(process_dev, token, artist, d) for d in results]
                for f in as_completed(futures):
                    try:
                        f.result()
                    except Exception as e:
                        log("CRIT", "THREAD_FAIL", error=str(e))

            offset = data.get("next_offset", 0)
            progress[artist] = offset
            save_progress(progress)

            if not data.get("has_more"):
                break

    tag_queue.join()
    db_queue.join()

# --------------------------
# PROGRESS
# --------------------------
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        return json.load(open(PROGRESS_FILE))
    return {}

def save_progress(p):
    json.dump(p, open(PROGRESS_FILE, "w"), indent=2)

# --------------------------
# RUN
# --------------------------
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log("CRITICAL", "MAIN_CRASH", error=str(e))
        raise