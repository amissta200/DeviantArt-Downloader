import os
import time
import logging
import sqlite3
import json
import requests
import pprint
import webbrowser
import hashlib
import secrets
import base64
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, urlencode
from requests.exceptions import HTTPError, RequestException

# --------------------------
# Configuration
# --------------------------
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
USERNAME = os.getenv("USERNAME")
SAVE_DIR = os.getenv("SAVE_DIR", "./downloads")
DB_PATH = os.getenv("DB_PATH", "./downloads/deviantart.db")
LOG_PATH = os.getenv("LOG_PATH", "./downloads/downloader.log")
SLEEP_TIME = float(os.getenv("SLEEP_TIME", 1.0))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))
RATE_LIMIT_SLEEP = int(os.getenv("RATE_LIMIT_SLEEP", 30))
FORCE_RECHECK = os.getenv("FORCE_RECHECK", "false").lower() == "true"
PROGRESS_FILE = os.path.join(SAVE_DIR, "progress.json")
DOWNLOAD_SUBSCRIPTIONS = os.getenv("DOWNLOAD_SUBSCRIPTIONS", "false").lower() == "true"
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
TOKEN_FILE = os.path.join(SAVE_DIR, "tokens.json")

# OAuth redirect — must match what you registered in your DeviantArt app settings
REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:8080/callback")
OAUTH_SCOPE = "basic browse"

if not all([CLIENT_ID, CLIENT_SECRET, USERNAME]):
    raise ValueError("Missing CLIENT_ID, CLIENT_SECRET, or USERNAME in environment variables.")

os.makedirs(SAVE_DIR, exist_ok=True)

# --------------------------
# Logging setup
# --------------------------
logging.basicConfig(
    level=logging.DEBUG if DEBUG_MODE else logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# --------------------------
# Database setup
# --------------------------
def add_is_premium_column_if_missing(conn):
    c = conn.cursor()
    try:
        c.execute("ALTER TABLE downloads ADD COLUMN is_premium INTEGER DEFAULT 0")
        conn.commit()
        logging.info("✅ Added 'is_premium' column to downloads table.")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            logging.debug("'is_premium' column already exists, skipping ALTER TABLE.")
        else:
            logging.error(f"❌ Unexpected error altering table: {e}")
            raise

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS downloads (
            deviationid TEXT PRIMARY KEY,
            artist TEXT,
            title TEXT,
            url TEXT,
            tags TEXT,
            is_premium INTEGER DEFAULT 0,
            downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    add_is_premium_column_if_missing(conn)
    conn.commit()
    return conn

conn = init_db()

def is_downloaded(deviation_id):
    c = conn.cursor()
    c.execute("SELECT 1 FROM downloads WHERE deviationid = ?", (deviation_id,))
    return c.fetchone() is not None

def mark_subscription(deviation_id, artist, title, url):
    c = conn.cursor()
    c.execute("""
        INSERT OR IGNORE INTO downloads
        (deviationid, artist, title, url, tags, is_premium)
        VALUES (?, ?, ?, ?, ?, 1)
    """, (deviation_id, artist, title, url, ""))
    conn.commit()

def is_subscription(deviation_id):
    c = conn.cursor()
    c.execute(
        "SELECT is_premium FROM downloads WHERE deviationid = ?",
        (deviation_id,)
    )
    row = c.fetchone()
    return row and row[0] == 1

def mark_downloaded(deviation_id, artist, title, url, tags):
    c = conn.cursor()
    c.execute("""
        INSERT OR IGNORE INTO downloads (deviationid, artist, title, url, tags)
        VALUES (?, ?, ?, ?, ?)
    """, (deviation_id, artist, title, url, "\n".join(tags)))
    conn.commit()

# --------------------------
# Token persistence
# --------------------------
def save_tokens(access_token, refresh_token):
    try:
        with open(TOKEN_FILE, "w") as f:
            json.dump({"access_token": access_token, "refresh_token": refresh_token}, f)
        logging.debug("💾 Tokens saved.")
    except Exception as e:
        logging.warning(f"⚠️ Could not save tokens: {e}")

def load_tokens():
    if not os.path.exists(TOKEN_FILE):
        return None, None
    try:
        with open(TOKEN_FILE, "r") as f:
            data = json.load(f)
        return data.get("access_token"), data.get("refresh_token")
    except Exception as e:
        logging.warning(f"⚠️ Could not load tokens: {e}")
        return None, None

# --------------------------
# OAuth2 Authorization Code Flow
# --------------------------

def generate_pkce():
    code_verifier = secrets.token_urlsafe(64)
    code_challenge = base64.urlsafe_b64encode(
        hashlib.sha256(code_verifier.encode()).digest()
    ).rstrip(b"=").decode()
    return code_verifier, code_challenge

# Store verifier globally so exchange function can access it
_code_verifier = None

# Simple one-shot HTTP server to capture the OAuth callback
_auth_code = None

class _CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global _auth_code
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        _auth_code = params.get("code", [None])[0]
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"<h1>Authorization complete. You can close this tab.</h1>")
        logging.info("✅ Authorization code received.")

    def log_message(self, format, *args):
        pass  # suppress default HTTP server logs


def get_auth_code():
    global _code_verifier
    _code_verifier, code_challenge = generate_pkce()

    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "scope": OAUTH_SCOPE,
        "state": "deviantart_downloader",
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    auth_url = "https://www.deviantart.com/oauth2/authorize?" + urlencode(params)
    logging.info(f"🌐 Opening browser for authorization:\n{auth_url}")
    webbrowser.open(auth_url)

    parsed = urlparse(REDIRECT_URI)
    port = parsed.port or 8080
    server = HTTPServer(("0.0.0.0", port), _CallbackHandler)
    server.handle_request()
    return _auth_code


def exchange_code_for_tokens(code):
    r = requests.post("https://www.deviantart.com/oauth2/token", data={
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
        "code": code,
        "code_verifier": _code_verifier,   # ← PKCE verifier
    })
    r.raise_for_status()
    data = r.json()
    access_token = data["access_token"]
    refresh_token = data["refresh_token"]
    save_tokens(access_token, refresh_token)
    logging.info("✅ Tokens obtained via authorization code.")
    return access_token, refresh_token


def refresh_access_token(refresh_token):
    """Use the refresh token to get a new access token."""
    r = requests.post("https://www.deviantart.com/oauth2/token", data={
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": refresh_token,
    })
    r.raise_for_status()
    data = r.json()
    new_access = data["access_token"]
    new_refresh = data.get("refresh_token", refresh_token)  # DA may or may not rotate it
    save_tokens(new_access, new_refresh)
    logging.info("🔄 Access token refreshed successfully.")
    return new_access, new_refresh


def authenticate():
    """
    Full auth flow:
    1. Try saved tokens → attempt a refresh.
    2. If no saved tokens, do the browser-based authorization code flow.
    Returns (access_token, refresh_token).
    """
    access_token, refresh_token = load_tokens()

    if refresh_token:
        logging.info("🔑 Found saved refresh token — refreshing access token...")
        try:
            access_token, refresh_token = refresh_access_token(refresh_token)
            return access_token, refresh_token
        except Exception as e:
            logging.warning(f"⚠️ Refresh failed ({e}), re-authorizing via browser...")

    # No valid tokens — do the full browser flow
    code = get_auth_code()
    if not code:
        raise RuntimeError("❌ Failed to obtain authorization code from DeviantArt.")
    return exchange_code_for_tokens(code)


# --------------------------
# Rate-limited GET with auto token refresh
# --------------------------
_refresh_token_global = None  # module-level so deviantart_get can refresh inline

def deviantart_get(url, token, params=None):
    global _refresh_token_global
    retries = 0
    while retries < MAX_RETRIES:
        headers = {"Authorization": f"Bearer {token}"}
        try:
            r = requests.get(url, headers=headers, params=params, timeout=20)

            if r.status_code == 429:
                wait_time = RATE_LIMIT_SLEEP * (retries + 1)
                logging.warning(f"⚠️ Rate limited: sleeping {wait_time}s")
                time.sleep(wait_time)
                retries += 1
                continue

            if r.status_code == 401:
                logging.warning("🔄 Token expired — refreshing...")
                if _refresh_token_global:
                    try:
                        token, _refresh_token_global = refresh_access_token(_refresh_token_global)
                        retries += 1
                        continue
                    except Exception as e:
                        logging.error(f"❌ Could not refresh token: {e}")
                        raise RuntimeError("Token refresh failed, re-run the script to re-authorize.")
                else:
                    raise RuntimeError("No refresh token available — re-run the script to re-authorize.")

            if not r.ok:
                logging.error(f"❌ Request failed {r.status_code}: {r.text[:200]}")
                retries += 1
                time.sleep(SLEEP_TIME)
                continue

            return r.json()

        except (HTTPError, RequestException) as e:
            logging.error(f"Request exception: {e}")
            retries += 1
            time.sleep(SLEEP_TIME)

    raise RuntimeError(f"Failed after {MAX_RETRIES} retries: {url}")


# --------------------------
# Load/save progress checkpoint
# --------------------------
def load_progress():
    if FORCE_RECHECK or not os.path.exists(PROGRESS_FILE):
        return {"last_artist_index": 0, "last_offset": 0}
    try:
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        logging.warning(f"⚠️ Failed to load progress file: {e}")
        return {"last_artist_index": 0, "last_offset": 0}

def save_progress(last_artist_index, last_offset):
    data = {"last_artist_index": last_artist_index, "last_offset": last_offset}
    try:
        with open(PROGRESS_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        logging.warning(f"⚠️ Failed to save progress file: {e}")

# --------------------------
# Fetch followed artists
# --------------------------
def get_followed_artists(token):
    url = f"https://www.deviantart.com/api/v1/oauth2/user/friends/{USERNAME}"
    offset = 0
    artists = []

    logging.info(f"📜 Fetching followed artists for {USERNAME}...")
    while True:
        params = {"offset": offset, "limit": 24, "mature_content": "true"}
        data = deviantart_get(url, token, params)
        batch = [f['user']['username'] for f in data.get('results', [])]
        artists.extend(batch)
        logging.info(f"Fetched {len(batch)} artists (total {len(artists)})")
        if not data.get('has_more'):
            break
        offset = data.get('next_offset', 0)
        time.sleep(SLEEP_TIME)
    return artists

# --------------------------
# Save deviation
# --------------------------
def save_deviation(token, artist, deviation):
    deviation_id = deviation["deviationid"]
    title = deviation.get("title", "untitled")
    content = deviation.get("content", {})
    url = deviation.get("url")

    # Known subscription → always skip first
    if is_subscription(deviation_id):
        logging.debug(f"⏩ Known subscription content skipped: {title} ({deviation_id})")
        return

    # Skip already downloaded normal content
    if is_downloaded(deviation_id):
        logging.debug(f"⏩ Skipping already downloaded {deviation_id}")
        return

    def is_subscription_content(deviation: dict) -> bool:
        premium_data = deviation.get("premium_folder_data")
        if premium_data is not None and premium_data.get("type") == "paid":
            return True
        if "tier_access" in deviation:
            return True
        if "primary_tier" in deviation:
            return True
        return False

    is_premium = is_subscription_content(deviation)

    if is_premium:
        logging.info(f"💰 Detected subscription content: {title} ({deviation_id})")
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(pprint.pformat(deviation))
        mark_subscription(deviation_id, artist, title, url)
        if not DOWNLOAD_SUBSCRIPTIONS:
            return
        logging.warning(f"⚠️ DOWNLOAD_SUBSCRIPTIONS enabled — attempting download anyway")

    # Fetch metadata for tags
    meta_url = "https://www.deviantart.com/api/v1/oauth2/deviation/metadata"
    params = {
        "deviationids[]": deviation_id,
        "mature_content": "true"
    }

    try:
        metadata = deviantart_get(meta_url, token, params)
    except Exception as e:
        logging.error(f"❌ Failed to get metadata for {deviation_id}: {e}")
        metadata = {}

    tags = []
    if "metadata" in metadata and len(metadata["metadata"]) > 0:
        tags = [t["tag_name"] for t in metadata["metadata"][0].get("tags", [])]

    artist_dir = os.path.join(SAVE_DIR, artist)
    os.makedirs(artist_dir, exist_ok=True)

    txt_path = os.path.join(artist_dir, f"{deviation_id}.txt")
    img_path = os.path.join(artist_dir, f"{deviation_id}.jpg")

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(f"title: {title}\n")
        f.write(f"artist: {artist}\n")
        f.write(f"url: {url}\n\n")
        f.write("\n".join(tags) + "\n")

    # Save image
    try:
        is_mature = deviation.get("is_mature", False)

        if is_mature:
            logging.debug(f"🔞 Mature content — resolving download URL for {deviation_id}")

            download_api = f"https://www.deviantart.com/api/v1/oauth2/deviation/download/{deviation_id}"
            params = {"mature_content": "true"}

            download_data = deviantart_get(download_api, token, params)

            if not download_data or "src" not in download_data:
                error_msg = download_data.get("error_description", "Unknown error") if download_data else "No response"
                logging.warning(f"⚠️ Download API failed for {deviation_id}: {error_msg}")
                return

            real_url = download_data["src"]
            img = requests.get(real_url, timeout=20)

        else:
            logging.debug(f"🖼️ Normal content — using content[src] for {deviation_id}")

            if not content or "src" not in content:
                logging.warning(f"⚠️ No content[src] for {deviation_id}")
                return

            img = requests.get(content["src"], timeout=20)

        img.raise_for_status()

        with open(img_path, "wb") as f:
            f.write(img.content)

    except Exception as e:
        logging.warning(f"⚠️ Failed to download image {title}: {e}")

    # Auto-tagger
    try:
        with open(img_path, "rb") as img_file:
            response = requests.post(
                "http://autotagger-deviantart:5000/evaluate",
                files={"file": img_file},
                data={"format": "json"},
                timeout=60
            )

        response.raise_for_status()
        tagger_output = response.json()

        ai_tags = []
        if isinstance(tagger_output, list) and tagger_output:
            ai_tags = list(tagger_output[0].get("tags", {}).keys())

        if ai_tags:
            with open(txt_path, "a", encoding="utf-8") as f:
                f.write("\n# AI tags\n")
                f.write("\n".join(ai_tags) + "\n")

    except Exception as e:
        logging.warning(f"⚠️ Tagger failed for {img_path}: {e}")

    mark_downloaded(deviation_id, artist, title, url, tags)
    logging.info(f"✅ Saved {deviation_id} ({title}) for {artist} ({len(tags)} tags)")


# --------------------------
# Main
# --------------------------
def main():
    global _refresh_token_global

    token, _refresh_token_global = authenticate()

    artists = get_followed_artists(token)
    logging.info(f"Found {len(artists)} artists")

    progress = load_progress()
    start_artist_idx = progress.get("last_artist_index", 0)
    start_offset = progress.get("last_offset", 0)

    for idx, artist in enumerate(artists[start_artist_idx:], start=start_artist_idx):
        logging.info(f"🎨 Processing artist ({idx + 1}/{len(artists)}): {artist}")

        try:
            offset_to_use = start_offset if idx == start_artist_idx else 0
            url = "https://www.deviantart.com/api/v1/oauth2/gallery/all"
            has_more = True
            current_offset = offset_to_use

            while has_more:
                params = {
                    "username": artist,
                    "offset": current_offset,
                    "limit": 24,
                    "mature_content": "true",   # ← key addition
                }
                data = deviantart_get(url, token, params)
                results = data.get("results", [])
                if not results:
                    logging.info(f"⚠️ No gallery results for {artist} at offset {current_offset}.")
                    break

                for deviation in results:
                    save_deviation(token, artist, deviation)
                    time.sleep(SLEEP_TIME)

                has_more = data.get("has_more", False)
                current_offset = data.get("next_offset", 0)
                save_progress(idx, current_offset)

            start_offset = 0

        except Exception as e:
            logging.error(f"❌ Error with {artist}: {e}")

    save_progress(0, 0)

if __name__ == "__main__":
    main()
