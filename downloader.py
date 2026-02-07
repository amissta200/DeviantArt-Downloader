import os
import time
import logging
import sqlite3
import json
import requests
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


if not all([CLIENT_ID, CLIENT_SECRET, USERNAME]):
    raise ValueError("Missing CLIENT_ID, CLIENT_SECRET, or USERNAME in environment variables.")

os.makedirs(SAVE_DIR, exist_ok=True)

# --------------------------
# Logging setup
# --------------------------
logging.basicConfig(
    level=logging.INFO,
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
        # This error usually means the column already exists, so safe to ignore
        if "duplicate column name" in str(e):
            logging.info("'is_premium' column already exists, skipping ALTER TABLE.")
        else:
            logging.error(f"❌ Unexpected error altering table: {e}")
            raise

def init_db():
    conn = sqlite3.connect(DB_PATH)
    add_is_premium_column_if_missing(conn)
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
# Authentication
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

# --------------------------
# Rate-limited GET with refresh
# --------------------------
def deviantart_get(url, token, params=None):
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
                token = get_access_token()   # refresh
                retries += 1
                continue                     # retry with new token

            if not r.ok:
                logging.error(f"❌ Request failed {r.status_code}: {r.text[:200]}")
                retries += 1
                time.sleep(SLEEP_TIME)
                continue

            return r.json()

        except (HTTPError, RequestException) as e:
            logging.error(f"Request exception {e}")
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
        params = {"access_token": token, "offset": offset, "limit": 24}
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

    is_premium = (
        deviation.get("premium_content")
        or deviation.get("premium_folder_data")
        or deviation.get("is_downloadable") is False
        or not content
        or not content.get("src")
    )

    if is_premium:
        logging.info(f"💰 Detected subscription content: {title} ({deviation_id})")

        # Always flag in DB
        mark_subscription(deviation_id, artist, title, url)

        # Only download if explicitly enabled
        if not DOWNLOAD_SUBSCRIPTIONS:
            return

        logging.warning(f"⚠️ DOWNLOAD_SUBSCRIPTIONS enabled — attempting download anyway")


    # Fetch metadata for tags
    meta_url = "https://www.deviantart.com/api/v1/oauth2/deviation/metadata"
    params = {
        "access_token": token,
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
        img = requests.get(content["src"])
        img.raise_for_status()
        with open(img_path, "wb") as f:
            f.write(img.content)
    except Exception as e:
        logging.warning(f"⚠️ Failed to download image {title}: {e}")

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

        # Extract tag names (ignore confidence values)
        ai_tags = []
        if isinstance(tagger_output, list) and tagger_output:
            ai_tags = list(tagger_output[0].get("tags", {}).keys())

        # Append tags to existing txt
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
    token = get_access_token()
    artists = get_followed_artists(token)
    logging.info(f"Found {len(artists)} artists")

    progress = load_progress()
    start_artist_idx = progress.get("last_artist_index", 0)
    start_offset = progress.get("last_offset", 0)

    for idx, artist in enumerate(artists[start_artist_idx:], start=start_artist_idx):
        logging.info(f"🎨 Processing artist ({idx + 1}/{len(artists)}): {artist}")

        try:
            # Use start_offset only for first artist after resuming
            offset_to_use = start_offset if idx == start_artist_idx else 0
            url = "https://www.deviantart.com/api/v1/oauth2/gallery/all"
            has_more = True
            current_offset = offset_to_use

            while has_more:
                params = {"username": artist, "access_token": token, "offset": current_offset, "limit": 24}
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

                # Save progress after each page of deviations for this artist
                save_progress(idx, current_offset)

            # Reset offset for next artist
            start_offset = 0

        except Exception as e:
            logging.error(f"❌ Error with {artist}: {e}")

    # Finished all artists, reset progress
    save_progress(0, 0)

if __name__ == "__main__":
    main()
