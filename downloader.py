import os
import time
import logging
import sqlite3
import json
import requests
import pprint
from requests.exceptions import HTTPError, RequestException

# --------------------------
# Configuration
# --------------------------
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
USERNAME = os.getenv("USERNAME")
SAVE_DIR = os.getenv("SAVE_DIR", "./downloads")
DB_PATH = os.getenv("DB_PATH", "./downloads/deviantart.db")
LOG_PATH = os.path.join(SAVE_DIR, "downloader.log")
SLEEP_TIME = float(os.getenv("SLEEP_TIME", 1.0))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))
RATE_LIMIT_SLEEP = int(os.getenv("RATE_LIMIT_SLEEP", 30))
FORCE_RECHECK = os.getenv("FORCE_RECHECK", "false").lower() == "true"
PROGRESS_FILE = os.path.join(SAVE_DIR, "progress.json")
DOWNLOAD_SUBSCRIPTIONS = os.getenv("DOWNLOAD_SUBSCRIPTIONS", "false").lower() == "true"
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

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
    try:
        c.execute("ALTER TABLE downloads ADD COLUMN is_premium INTEGER DEFAULT 0")
        conn.commit()
    except sqlite3.OperationalError:
        pass # Column already exists
    return conn

conn = init_db()

def is_downloaded(deviation_id):
    c = conn.cursor()
    c.execute("SELECT 1 FROM downloads WHERE deviationid = ?", (deviation_id,))
    return c.fetchone() is not None

def mark_downloaded(deviation_id, artist, title, url, tags, is_premium=0):
    c = conn.cursor()
    c.execute("""
        INSERT OR IGNORE INTO downloads (deviationid, artist, title, url, tags, is_premium)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (deviation_id, artist, title, url, "\n".join(tags), is_premium))
    conn.commit()

# --------------------------
# Authentication & GET
# --------------------------
def get_access_token():
    url = "https://www.deviantart.com/oauth2/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    token = r.json().get("access_token")
    logging.info("✅ Authenticated successfully.")
    return token

def deviantart_get(url, token, params=None):
    retries = 0
    while retries < MAX_RETRIES:
        headers = {"Authorization": f"Bearer {token}"}
        try:
            r = requests.get(url, headers=headers, params=params, timeout=20)
            if r.status_code == 429:
                wait = RATE_LIMIT_SLEEP * (retries + 1)
                logging.warning(f"⚠️ Rate limited: sleeping {wait}s")
                time.sleep(wait)
                retries += 1
                continue
            if r.status_code == 401:
                logging.warning("🔄 Token expired — refreshing...")
                token = get_access_token()
                retries += 1
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logging.error(f"❌ Request failed: {e}")
            retries += 1
            time.sleep(SLEEP_TIME)
    raise RuntimeError(f"Failed after {MAX_RETRIES} retries: {url}")

# --------------------------
# Save deviation with Auto-Tagging
# --------------------------
def save_deviation(token, artist, deviation):
    deviation_id = deviation["deviationid"]
    title = deviation.get("title", "untitled")
    url = deviation.get("url")

    if is_downloaded(deviation_id):
        logging.debug(f"⏩ Skipping already downloaded {deviation_id}")
        return

    # Premium Check
    is_premium = any([
        deviation.get("premium_folder_data"),
        "tier_access" in deviation,
        "primary_tier" in deviation
    ])

    if is_premium:
        logging.info(f"💰 Detected subscription content: {title}")
        if not DOWNLOAD_SUBSCRIPTIONS:
            return
        logging.warning(f"⚠️ DOWNLOAD_SUBSCRIPTIONS enabled — attempting anyway")

    # --- IMAGE SOURCE LOGIC (ANTI-BLUR) ---
    image_source = None
    if "download" in deviation:
        image_source = deviation["download"].get("src") # Best quality
    elif "content" in deviation:
        image_source = deviation["content"].get("src")
    elif deviation.get("thumbs"):
        image_source = deviation["thumbs"][-1].get("src")

    if not image_source:
        logging.warning(f"⚠️ No download source for {deviation_id}")
        return

    # Metadata Fetch
    meta_url = "https://www.deviantart.com/api/v1/oauth2/deviation/metadata"
    params = {"deviationids[]": deviation_id, "mature_content": "true"}
    try:
        metadata = deviantart_get(meta_url, token, params)
        tags = [t["tag_name"] for t in metadata["metadata"][0].get("tags", [])] if metadata.get("metadata") else []
    except Exception as e:
        logging.error(f"❌ Metadata fail for {deviation_id}: {e}")
        tags = []

    # File Preparation
    artist_dir = os.path.join(SAVE_DIR, artist)
    os.makedirs(artist_dir, exist_ok=True)
    img_path = os.path.join(artist_dir, f"{deviation_id}.jpg")
    txt_path = os.path.join(artist_dir, f"{deviation_id}.txt")

    # Save Text Metadata
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(f"title: {title}\nartist: {artist}\nurl: {url}\n\n" + "\n".join(tags) + "\n")

    # Save Image
    try:
        img_res = requests.get(image_source, timeout=30)
        img_res.raise_for_status()
        with open(img_path, "wb") as f:
            f.write(img_res.content)
        
        # --- AUTO TAGGING ---
        try:
            with open(img_path, "rb") as img_file:
                tag_res = requests.post(
                    "http://autotagger-deviantart:5000/evaluate",
                    files={"file": img_file},
                    data={"format": "json"},
                    timeout=60
                )
                tag_res.raise_for_status()
                tagger_output = tag_res.json()
                if isinstance(tagger_output, list) and tagger_output:
                    ai_tags = list(tagger_output[0].get("tags", {}).keys())
                    if ai_tags:
                        with open(txt_path, "a", encoding="utf-8") as f:
                            f.write("\n# AI tags\n" + "\n".join(ai_tags) + "\n")
        except Exception as e:
            logging.warning(f"⚠️ Tagger failed for {deviation_id}: {e}")

        mark_downloaded(deviation_id, artist, title, url, tags, 1 if is_premium else 0)
        logging.info(f"✅ Saved {deviation_id} ({title})")

    except Exception as e:
        logging.error(f"❌ Failed image download {deviation_id}: {e}")

# --------------------------
# Main Execution
# --------------------------
def main():
    token = get_access_token()
    
    # Fetch Artists
    friends_url = f"https://www.deviantart.com/api/v1/oauth2/user/friends/{USERNAME}"
    artists = []
    offset = 0
    logging.info(f"📜 Fetching artists for {USERNAME}...")
    while True:
        data = deviantart_get(friends_url, token, {"offset": offset, "limit": 24})
        batch = [f['user']['username'] for f in data.get('results', [])]
        artists.extend(batch)
        if not data.get('has_more'): break
        offset = data.get('next_offset')
    
    logging.info(f"Found {len(artists)} artists.")

    for artist in artists:
        logging.info(f"🎨 Processing: {artist}")
        gallery_url = "https://www.deviantart.com/api/v1/oauth2/gallery/all"
        g_offset = 0
        while True:
            # FIX: mature_content and expand=deviation.download added to prevent blur
            params = {
                "username": artist,
                "offset": g_offset,
                "limit": 24,
                "mature_content": "true",
                "expand": "deviation.download"
            }
            data = deviantart_get(gallery_url, token, params)
            results = data.get("results", [])
            if not results: break

            for dev in results:
                save_deviation(token, artist, dev)
                time.sleep(SLEEP_TIME)

            if not data.get("has_more"): break
            g_offset = data.get("next_offset")

if __name__ == "__main__":
    main()