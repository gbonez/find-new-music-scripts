import os
import json
import random
import time
from datetime import datetime, timezone, timedelta
from random import choices
import requests
from spotipy import Spotify
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotipy.exceptions import SpotifyException

# Selenium for scraping
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

import psycopg2
import psycopg2.extras
from urllib.parse import urlparse

# add DB helpers import (new file db_helpers.py)
from db_helpers import (
    is_artist_blacklisted,
    add_blacklisted_artist,
    is_playlist_blacklisted,
    add_or_update_user_playlist,
    mark_playlist_blacklisted,
    is_track_blacklisted,
    blacklisted_artist_count,
    add_blacklisted_song,
    get_random_whitelisted_profile,
)

# ==== CONFIG ====
ARTISTS_FILE = "artists.json"
OUTPUT_PLAYLIST_ID = os.environ.get("PLAYLIST_ID")  # Spotify playlist to add tracks
OUTPUT_FILE = "rolled_tracks.json"

LASTFM_API_KEY = os.environ.get("LASTFM_API_KEY")
LASTFM_USERNAME = os.environ.get("LASTFM_USERNAME")

SPOTIFY_CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")
SPOTIFY_REDIRECT_URI = (os.environ.get("BASE_URL") or "http://localhost:5000") + "/callback"
SPOTIFY_REFRESH_TOKEN = os.environ.get("SPOTIFY_REFRESH_TOKEN")

MY_PHONE = os.environ.get("MY_PHONE_NUMBER")
SELFPING_API_KEY = os.environ.get("SELFPING_API_KEY")
SELFPING_ENDPOINT = "https://www.selfping.com/api/sms"

scope = "playlist-modify-public playlist-modify-private user-library-read"

# ==== SPOTIFY AUTH ====
auth_manager = SpotifyOAuth(
    client_id=SPOTIFY_CLIENT_ID,
    client_secret=SPOTIFY_CLIENT_SECRET,
    redirect_uri=SPOTIFY_REDIRECT_URI,
    scope=scope,
    cache_path=None
)
auth_manager.refresh_access_token(SPOTIFY_REFRESH_TOKEN)
sp = Spotify(auth_manager=auth_manager)

# ==== GLOBAL DRIVER FOR SCRAPING ====
global_driver = None
def get_global_driver():
    global global_driver
    if global_driver is None:
        chrome_bin = os.environ.get("CHROME_BIN")
        chromedriver_path = os.environ.get("CHROMEDRIVER_PATH")

        options = webdriver.ChromeOptions()
        options.binary_location = chrome_bin
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        service = Service(chromedriver_path)
        global_driver = webdriver.Chrome(service=service, options=options)
    return global_driver

def close_global_driver():
    global global_driver
    if global_driver:
        global_driver.quit()
        global_driver = None

# ==== HELPER FUNCTIONS ====
def safe_spotify_call(func, *args, **kwargs):
    """Spotify call wrapper with retries, 404 skip, and None fallback."""
    retries = 3
    for attempt in range(retries):
        try:
            time.sleep(0.3)
            return func(*args, **kwargs)
        except spotipy.exceptions.SpotifyException as e:
            if e.http_status == 404:
                print(f"[WARN] Spotify 404 for {func.__name__}: Resource not found")
                return None
            elif e.http_status == 429:
                retry_after = int(e.headers.get("Retry-After", 30))
                print(f"[RATE LIMIT] Waiting {retry_after}s before retrying {func.__name__}...")
                time.sleep(retry_after + 2)
            elif 500 <= e.http_status < 600:
                print(f"[WARN] Spotify server error ({e.http_status}) on {func.__name__}, retrying...")
                time.sleep(2)
            else:
                print(f"[ERROR] Spotify error ({e.http_status}) in {func.__name__}: {e}")
                return None
        except Exception as e:
            print(f"[WARN] Unexpected error in {func.__name__}: {e}")
            time.sleep(2)
    print(f"[FAIL] {func.__name__} failed after {retries} retries")
    return None

def get_random_track_from_playlist(playlist_id, excluded_artist=None, max_followers=None, source_desc="", artists_data=None, existing_artist_ids=None):
    consecutive_invalid = 0
    for attempt in range(1, 21):
        try:
            playlist = safe_spotify_call(
                sp.playlist_items,
                playlist_id,
                fields="items(track(name,id,artists(id,name)))"
            )
            if not playlist or "items" not in playlist:
                print(f"[WARN] Playlist {playlist_id} is empty or inaccessible, skipping")
                return None
        except SpotifyException as e:
            if e.http_status == 404:
                print(f"[WARN] Playlist {playlist_id} not found or inaccessible, skipping...")
                return None
            else:
                raise

        if not playlist["items"]:
            print(f"[WARN] Playlist {playlist_id} is empty, skipping...")
            return None

        item = random.choice(playlist["items"])
        track = item.get("track")
        if not track or "id" not in track:
            print(f"[WARN] Skipping track without ID in playlist '{source_desc}'")
            continue

        if "artists" not in track or not track["artists"]:
            print(f"[WARN] Skipping track '{track.get('name','<unknown>')}' without artists in playlist '{source_desc}'")
            continue

        track_artist = track["artists"][0]
        is_valid, reason = validate_track(track, artists_data, existing_artist_ids, max_followers=max_followers)

        print(f"[ATTEMPT {attempt}] Playlist '{source_desc}' | Song '{track.get('name','<unknown>')}' by '{track_artist.get('name','<unknown>')}' | Valid? {is_valid}")
        if is_valid:
            return track
        else:
            print(f"         Re-rolling because: {reason}")
            consecutive_invalid += 1
            if consecutive_invalid >= 5:
                print(f"[INFO] 5 consecutive invalid tracks found in playlist '{source_desc}', breaking out")
                return None

def scrape_artist_playlists(artist_id_or_url):
    driver = get_global_driver()
    playlists = []
    try:
        if "open.spotify.com/artist/" in artist_id_or_url:
            url = f"{artist_id_or_url}/playlists"
        else:
            url = f"https://open.spotify.com/artist/{artist_id_or_url}/playlists"
        driver.get(url)

        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a[href*='/playlist/']"))
        )
        time.sleep(2)

        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        soup = BeautifulSoup(driver.page_source, "html.parser")
        playlist_elements = soup.select("a[href*='/playlist/']")
        seen = set()
        for pl in playlist_elements:
            href = pl.get("href")
            name = pl.text.strip()
            if href and name and href not in seen:
                playlists.append({"name": name, "url": "https://open.spotify.com" + href})
                seen.add(href)
        return playlists
    except Exception as e:
        print(f"[WARN] Error scraping artist playlists: {e}")
        return playlists

def select_track_for_artist(artist_name, artists_data, existing_artist_ids):
    track = None
    seen_playlists = set()
    playlist_attempts = 0

    # defensive: check search result before indexing
    search_res = safe_spotify_call(sp.search, artist_name, type="artist", limit=1)
    if not search_res or "artists" not in search_res or not search_res["artists"].get("items"):
        print(f"[WARN] No Spotify artist found for '{artist_name}'")
        return None
    artist_results = search_res["artists"]["items"]
    artist_id = artist_results[0]["id"]

    # If artist is in blacklisted_artists_playlists, skip scraping step
    if is_artist_blacklisted(artist_id):
        print(f"[INFO] Artist {artist_name} ({artist_id}) is blacklisted for artist playlists; skipping Step 1")
        scraped_artist_playlists = []
    else:
        # Step 1: Scraped artist playlists
        scraped_artist_playlists = scrape_artist_playlists(artist_id)
    for pl in scraped_artist_playlists:
        playlist_id = pl["url"].split("/")[-1].split("?")[0]
        if playlist_id in seen_playlists:
            continue
        seen_playlists.add(playlist_id)

        try:
            playlist_items = safe_spotify_call(
                sp.playlist_items,
                playlist_id,
                limit=100,
                offset=0,
                fields="items(track(artists(id,name)))"
            )
            if not playlist_items or "items" not in playlist_items:
                print(f"[WARN] Spotify 404 or empty playlist_items: {playlist_id}, skipping")
                # mark artist as problematic (irretrievable artist playlist)
                try:
                    add_blacklisted_artist(artist_id, name=artist_name)
                    print(f"[DB] Marked artist {artist_id} as blacklisted (irretrievable artist playlist)")
                except Exception as _:
                    pass
                break

        except spotipy.exceptions.SpotifyException as e:
            print(f"[WARN] Skipping playlist {playlist_id} due to Spotify error: {e}")
            continue


        artist_track_count = 0
        if playlist_items and isinstance(playlist_items, dict) and "items" in playlist_items:
            artist_track_count = sum(
                1
                for item in playlist_items["items"]
                if item.get("track")
                and artist_name.lower() in [a["name"].lower() for a in item["track"]["artists"]]
            )

        if artist_track_count > 5:
            continue

        playlist_attempts += 1
        if playlist_attempts > 2:
            break

        track = get_random_track_from_playlist(
            playlist_id,
            excluded_artist=artist_name,
            max_followers=80000,
            source_desc=f"{pl['name']} (artist-made playlist scraped)",
            artists_data=artists_data,
            existing_artist_ids=existing_artist_ids
        )
        
        if track:
            return track

    # Step 2: User playlists via API
    print(f"[INFO] No valid tracks found in artist playlists for '{artist_name}'. Trying user made playlists...")

    search_res = safe_spotify_call(sp.search, artist_name, type="playlist", limit=20)
    if not search_res or "playlists" not in search_res or not search_res["playlists"].get("items"):
        user_playlists = []
    else:
        user_playlists = search_res["playlists"]["items"]

    for pl in user_playlists[:10]:
        if not pl or "id" not in pl:
            continue
        playlist_id = pl["id"]
        # skip playlists known to be blacklisted in DB
        if is_playlist_blacklisted(playlist_id):
            print(f"[INFO] Skipping user playlist {playlist_id} because it's blacklisted in DB")
            continue
        if playlist_id in seen_playlists:
            continue
        seen_playlists.add(playlist_id)

        playlist_data = safe_spotify_call(
            sp.playlist_items, 
            playlist_id, 
            fields="items(track(artists(id,name)))",
            limit=100,
            offset=0
        )
        if not playlist_data or "items" not in playlist_data:
            print(f"[WARN] Playlist {playlist_id} is empty or inaccessible, marking blacklisted and skipping")
            try:
                add_or_update_user_playlist(playlist_id, name=pl.get("name"), blacklisted=True)
            except Exception:
                pass
            continue
        else:
            # record access to this user playlist (not blacklisted)
            try:
                add_or_update_user_playlist(playlist_id, name=pl.get("name"), blacklisted=False)
            except Exception:
                pass
        playlist_items = playlist_data["items"]


        artist_track_count = sum(
            1 for item in playlist_items
            if item.get("track") and artist_name.lower() in [a["name"].lower() for a in item["track"]["artists"]]
        )
        if artist_track_count > 10:
            continue

        track = get_random_track_from_playlist(
            playlist_id,
            excluded_artist=artist_name,
            max_followers=50000,
            source_desc=f"{pl['name']} (user-made playlist via API)",
            artists_data=artists_data,
            existing_artist_ids=existing_artist_ids
        )

        if track:
            return track

    # Step 3: Last.fm similar artists
    print(f"[INFO] No valid tracks found in scraped/user playlists for '{artist_name}'. Trying Last.fm similar artists...")
    similar_artists = []
    url = "http://ws.audioscrobbler.com/2.0/"
    params = {"method": "artist.getsimilar", "artist": artist_name, "api_key": LASTFM_API_KEY, "format": "json", "limit": 10}
    try:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        similar_artists = [a["name"] for a in data.get("similarartists", {}).get("artist", [])]
    except Exception as e:
        print(f"[WARN] Failed fetching Last.fm similar artists for {artist_name}: {e}")
        similar_artists = []
    random.shuffle(similar_artists)
    for sim_artist in similar_artists[:10]:
        artist_results = safe_spotify_call(sp.search, sim_artist, type="artist", limit=1)["artists"]["items"]
        if not artist_results:
            continue
        sim_artist_data = artist_results[0]
        if sim_artist_data["followers"]["total"] >= 50000:
            continue
        top_tracks_resp = safe_spotify_call(sp.artist_top_tracks, sim_artist_data["id"], country="US")
        top_tracks = top_tracks_resp["tracks"] if top_tracks_resp and "tracks" in top_tracks_resp else []
        if top_tracks:
            track = random.choice(top_tracks)
            is_valid, reason = validate_track(track, artists_data, existing_artist_ids, max_followers=50000)
            if is_valid:
                print(f"[INFO] Selected valid track '{track['name']}' by '{track['artists'][0]['name']}' from Last.fm similar artists")
                return track
            else:
                print(f"[VALIDATION] Track '{track['name']}' by '{track['artists'][0]['name']}' failed: {reason}")


    # Step 4: Spotify similar artists
    print(f"[INFO] No valid tracks found via Last.fm for '{artist_name}'. Trying Spotify similar artists...")
    similar_artists_data = safe_spotify_call(sp.artist_related_artists, artist_id)
    if not similar_artists_data or "artists" not in similar_artists_data:
        print(f"[WARN] Spotify 404 for artist_related_artists: {artist_id}")
        return None 

    artists_list = similar_artists_data["artists"]
    random.shuffle(artists_list)
    for sim_artist_data in artists_list[:10]:
        if sim_artist_data["followers"]["total"] >= 50000 or sim_artist_data["name"].lower() == artist_name.lower():
            continue
        top_tracks_resp = safe_spotify_call(sp.artist_top_tracks, sim_artist_data["id"], country="US")
        top_tracks = top_tracks_resp["tracks"] if top_tracks_resp and "tracks" in top_tracks_resp else []
        if top_tracks:
            track = random.choice(top_tracks)
            is_valid, reason = validate_track(track, artists_data, existing_artist_ids, max_followers=50000)
            if is_valid:
                print(f"[INFO] Selected valid track '{track['name']}' by '{track['artists'][0]['name']}' from Spotify similar artists")
                return track
            else:
                print(f"[VALIDATION] Track '{track['name']}' by '{track['artists'][0]['name']}' failed: {reason}")


    return None

# ==== LAST.FM TRACKS ====
def fetch_all_recent_tracks(username=LASTFM_USERNAME, api_key=LASTFM_API_KEY):
    recent_tracks = []
    page = 1
    while True:
        params = {"method": "user.getrecenttracks", "user": username, "api_key": api_key, "format": "json", "limit": 200, "page": page}
        time.sleep(0.25)
        resp = requests.get("http://ws.audioscrobbler.com/2.0/", params=params)
        resp.raise_for_status()
        data = resp.json()
        tracks = data.get("recenttracks", {}).get("track", [])
        if not tracks:
            break
        for t in tracks:
            if "@attr" in t and t["@attr"].get("nowplaying") == "true":
                continue
            if "date" in t and "uts" in t["date"]:
                ts = int(t["date"]["uts"])
                recent_tracks.append({"artist": t["artist"]["#text"].lower(), "track": t["name"], "played_at": datetime.fromtimestamp(ts, tz=timezone.utc)})
        total_pages = int(data.get("recenttracks", {}).get("@attr", {}).get("totalPages", 1))
        if page >= total_pages:
            break
        page += 1
    return recent_tracks

def build_artist_play_map(recent_tracks, days_limit=365):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_limit)
    artist_play_map = {}
    for t in recent_tracks:
        if t["played_at"] < cutoff:
            continue
        artist = t["artist"]
        artist_play_map.setdefault(artist, []).append(t["played_at"])
    return artist_play_map

def validate_track(track, artists_data, existing_artist_ids=None, max_followers=None):
    """
    Returns True if track is valid, False otherwise, with reason.
    """
    if not track or "artists" not in track or not track["artists"]:
        return False, "Track has no artists"

    artist = track["artists"][0]
    aid = artist["id"]
    name_lower = artist["name"].lower()

    # 1. Blocked by artists.json
    artist_entry = artists_data.get(aid)
    if not artist_entry:
        for k, v in artists_data.items():
            if v["name"].lower() == name_lower:
                artist_entry = v
                break
    if artist_entry and artist_entry.get("total_liked", 0) >= 3:
        return False, f"Artist '{artist['name']}' blocked by artists.json (total_liked >= 3)"

    # 2. Already in playlist
    if existing_artist_ids and (aid in existing_artist_ids or name_lower in existing_artist_ids):
        return False, f"Artist '{artist['name']}' already has a track in playlist"

    # 3. Max followers
    if max_followers:
        full_artist = safe_spotify_call(sp.artist, aid)
        if full_artist and full_artist["followers"]["total"] > max_followers:
            return False, f"Artist '{artist['name']}' has {full_artist['followers']['total']} followers, exceeds max {max_followers}"

    return True, ""


# ---- DB helpers for artist cache (script-level) ----
def get_db_conn():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("[DB] DATABASE_URL not set; DB operations disabled for artist cache")
        return None
    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"[DB] Failed to connect to DB for artist cache: {e}")
        return None

def load_artists_from_db():
    """
    Load artist cache from user_artists table (artist_id -> {name, total_liked})
    Falls back to reading ARTISTS_FILE if DB is unavailable.
    """
    conn = get_db_conn()
    if not conn:
        # fallback to file if present
        if os.path.exists(ARTISTS_FILE):
            try:
                with open(ARTISTS_FILE, "r") as f:
                    return json.load(f).get("artists", {})
            except Exception as e:
                print(f"[WARN] Failed to load {ARTISTS_FILE}: {e}")
                return {}
        return {}

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT artist_id, artist_name, total_liked FROM user_artists")
            rows = cur.fetchall()
            artists = {}
            for r in rows:
                aid = r.get("artist_id")
                name = r.get("artist_name") or ""
                total = r.get("total_liked") or 0
                if aid:
                    artists[aid] = {"name": name, "total_liked": total}
            return artists
    except Exception as e:
        print(f"[DB] Failed to query user_artists: {e}")
        # fallback to file
        if os.path.exists(ARTISTS_FILE):
            try:
                with open(ARTISTS_FILE, "r") as f:
                    return json.load(f).get("artists", {})
            except Exception:
                pass
        return {}

def update_artists_from_likes():
    print("[INFO] Starting to update liked artist cache")
    
    # load existing cache only for informational purposes (we won't overwrite DB here)
    if os.path.exists(ARTISTS_FILE):
        try:
            with open(ARTISTS_FILE, "r") as f:
                artist_cache = json.load(f).get("artists", {})
        except Exception:
            artist_cache = {}
    else:
        artist_cache = {}

    scan_limit = None if len(artist_cache) < 100 else 100
    offset = 0
    limit = 50
    total_processed = 0
    new_artists = {}
    all_liked_songs = []

    print(f"[INFO] Existing artist cache contains {len(artist_cache)} artists")
    batch_number = 1

    while True:
        batch_limit = limit
        if scan_limit:
            remaining = scan_limit - total_processed
            if remaining <= 0:
                print(f"[INFO] Reached scan limit of {scan_limit} tracks")
                break
            batch_limit = min(batch_limit, remaining)

        results = safe_spotify_call(sp.current_user_saved_tracks, limit=batch_limit, offset=offset)
        if not results or "items" not in results:
            print("[INFO] No more liked tracks returned from Spotify or call failed")
            break
        items = results["items"]
        if not items:
            print("[INFO] No more liked tracks returned from Spotify")
            break

        new_artists_in_batch = 0
        existing_artists_in_batch = 0

        for item in items:
            track = item.get("track")
            if not track:
                continue
            added_at_str = item.get("added_at")
            try:
                added_at = datetime.strptime(added_at_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc) if added_at_str else None
            except Exception:
                added_at = None

            all_liked_songs.append({"track_id": track.get("id"), "artists": track.get("artists", []), "added_at": added_at})

            # process each artist on the track
            for artist in track.get("artists", []):
                aid = artist.get("id")
                if not aid:
                    continue
                if aid not in artist_cache:
                    artist_cache[aid] = {"name": artist.get("name", ""), "total_liked": 1}
                    new_artists[aid] = {"name": artist.get("name", ""), "total_liked": 1}
                    new_artists_in_batch += 1
                else:
                    artist_cache[aid]["total_liked"] = artist_cache[aid].get("total_liked", 0) + 1
                    existing_artists_in_batch += 1

            total_processed += 1

        print(f"[BATCH {batch_number}] Processed {len(items)} tracks | "
              f"New artists: {new_artists_in_batch} | "
              f"Existing artists updated: {existing_artists_in_batch} | "
              f"Total tracks processed so far: {total_processed}")
        
        batch_number += 1
        offset += limit

        if scan_limit and total_processed >= scan_limit:
            print(f"[INFO] Reached the scan limit of {scan_limit} tracks after batch {batch_number-1}")
            break

    # Do NOT overwrite ARTISTS_FILE here. Persisting to DB is optional and depends on schema.
    # Return newly discovered artists (caller will merge with DB-sourced artists).
    print(f"[INFO] Finished scanning liked tracks: {len(new_artists)} new artists discovered in this run")
    
    return new_artists, all_liked_songs

# ==== CALCULATE LOTTERY WEIGHTS ====
def calculate_weights(all_artists, artist_play_map):
    now = datetime.now(timezone.utc)
    recent_14_cutoff = now - timedelta(days=14)
    recent_60_cutoff = now - timedelta(days=60)
    stats = {}
    max_recent_14 = 0
    max_recent_60 = 0

    for aid, info in all_artists.items():
        artist_name_lower = info["name"].lower()
        scrobbles = artist_play_map.get(artist_name_lower, [])
        if not scrobbles:
            continue

        recent_14 = sum(1 for d in scrobbles if d >= recent_14_cutoff)
        recent_60 = sum(1 for d in scrobbles if d >= recent_60_cutoff)
        total_liked = info.get("total_liked", 0)

        max_recent_14 = max(max_recent_14, recent_14)
        max_recent_60 = max(max_recent_60, recent_60)

        stats[aid] = {"recent_14": recent_14, "recent_60": recent_60, "total_liked": total_liked}

    weights = {}
    for aid, s in stats.items():
        top_ratio_weight = 0  # placeholder, optional
        recent_60_weight = (s["recent_60"] / max(1, max_recent_60)) * 60
        recent_14_weight = (s["recent_14"] / max(1, max_recent_14)) * 10
        bonus = 5 if s["total_liked"] > 6 else 0
        weights[aid] = top_ratio_weight + recent_60_weight + recent_14_weight + bonus

    return weights

def remove_old_tracks_from_playlist(playlist_id, days_old=8):
    print(f"[INFO] Checking for tracks older than {days_old} days in playlist {playlist_id}...")
    existing_tracks = safe_spotify_call(
        sp.playlist_items,
        playlist_id,
        fields="items(track(id,name,artists(id,name)), added_at)",
        limit=100  # adjust if your playlist is bigger
    )

    if not existing_tracks or "items" not in existing_tracks:
        print(f"[WARN] Could not fetch existing tracks for playlist {playlist_id}")
        return 0

    now = datetime.now(timezone.utc)
    tracks_to_remove = []

    for item in existing_tracks["items"]:
        track = item.get("track")
        added_at_str = item.get("added_at")
        if not track or not added_at_str:
            continue
        try:
            added_at = datetime.strptime(added_at_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except Exception:
            continue
        age_days = (now - added_at).days
        if age_days >= days_old:
            track_id = track.get("id")
            if track_id:
                tracks_to_remove.append({"uri": f"spotify:track:{track_id}"})

    removed_count = 0
    if tracks_to_remove:
        sp.playlist_remove_all_occurrences_of_items(playlist_id, [t["uri"] for t in tracks_to_remove])
        removed_count = len(tracks_to_remove)
        print(f"[INFO] Removed {removed_count} track(s) older than {days_old} days")
    else:
        print(f"[INFO] No tracks older than {days_old} days found")

    return removed_count

# add track_allowed_to_add helper to check DB blacklists before adding a track
def track_allowed_to_add(track):
    """
    Returns (True, "") if the track may be added.
    Returns (False, reason) if the track should be skipped.
    Checks:
      - track has an id and artists
      - song is not present in blacklisted_songs
      - artist does not appear >= 3 times in blacklisted_songs
    DB errors are logged and treated conservatively (allow).
    """
    if not track or not isinstance(track, dict):
        return False, "Invalid track payload"
    tid = track.get("id")
    if not tid:
        return False, "Missing track id"

    try:
        # exact track blacklist
        if is_track_blacklisted(tid):
            return False, "Track is blacklisted"

        # artist-level blacklist count
        artists = track.get("artists") or []
        if artists:
            artist_id = artists[0].get("id")
            if artist_id:
                try:
                    cnt = blacklisted_artist_count(artist_id) or 0
                except Exception:
                    cnt = 0
                if cnt >= 3:
                    return False, f"Artist has {cnt} entries in blacklisted_songs"
    except Exception as e:
        # If DB checks fail, log and allow (avoid blocking due to transient DB issues)
        print(f"[WARN] track_allowed_to_add DB check failed: {e}")
        return True, ""

    return True, ""
def send_playlist_update_sms(songs_added, max_songs, removed_count, playlist_id):
    today = datetime.now(timezone.utc).strftime("%m/%d/%Y")
    playlist_link = f"https://open.spotify.com/playlist/{playlist_id}"
    
    # Determine status
    if songs_added >= max_songs:
        status_emoji = "✅"
        status_text = "Playlist successfully updated"
    else:
        status_emoji = "❌"
        status_text = "Playlist not fully updated"

    message_body = (
        f"🎵 Playlist Update Summary ({today})\n\n"
        f"Songs added: {songs_added}/{max_songs}\n"
        f"Old tracks removed (>=8 days old): {removed_count}\n"
        f"{status_text} {status_emoji}\n\n"
        f"Playlist Link: {playlist_link}"
    )

    data = {"to": MY_PHONE, "message": message_body}
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {SELFPING_API_KEY}"
    }

    try:
        response = requests.post(SELFPING_ENDPOINT, headers=headers, json=data)
        if response.status_code == 200:
            print("📱 SMS notification sent via SelfPing!")
        else:
            print(f"⚠️ Failed to send SMS. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"⚠️ Exception occurred while sending SMS: {e}")



# ==== MAIN COMBINED SCRIPT ====
if __name__ == "__main__":
    print("Starting Enhanced Recs Script...")
    time.sleep(1)
    # Update artists cache (scan user's liked songs for new artists)
    new_artists, _ = update_artists_from_likes()

    # Load canonical artist cache from DB (fallback to file if DB absent)
    artists_data = load_artists_from_db()

    # Merge DB-cache with newly discovered artists (new_artists may include names/total_liked increments)
    all_artists = {**artists_data}
    for aid, info in new_artists.items():
        if aid in all_artists:
            all_artists[aid]["total_liked"] = max(all_artists[aid].get("total_liked", 0), info.get("total_liked", 0))
            if not all_artists[aid].get("name"):
                all_artists[aid]["name"] = info.get("name")
        else:
            all_artists[aid] = info

    recent_tracks = fetch_all_recent_tracks()
    artist_play_map = build_artist_play_map(recent_tracks)

    weights = calculate_weights(all_artists, artist_play_map)

    songs_added = 0
    max_songs = 50
    rolled_aids = set()
    # Fetch existing tracks in the playlist
    existing_tracks = safe_spotify_call(
        sp.playlist_items,
        OUTPUT_PLAYLIST_ID,
        fields="items(track(id, artists(id,name)))",
        limit=100  # adjust if your playlist is bigger
    )
    if not existing_tracks or "items" not in existing_tracks:
        existing_artist_ids = set()
        print(f"[WARN] Could not fetch existing playlist items for {OUTPUT_PLAYLIST_ID}, proceeding with empty set")
    else:
        existing_artist_ids = {
            t.get("track", {}).get("artists", [])[0].get("id")
            for t in existing_tracks["items"]
            if t.get("track") and t["track"].get("artists")
        }
    print(f"[INFO] Found {len(existing_artist_ids)} existing artists in playlist")


    try:
        while songs_added < max_songs and len(rolled_aids) < len(weights):
            # Pick artist via lottery
            artist_ids = list(weights.keys())
            weight_values = [weights[aid] for aid in artist_ids]
            chosen_aid = choices(artist_ids, weights=weight_values, k=1)[0]
            if chosen_aid in rolled_aids:
                continue
            rolled_aids.add(chosen_aid)
            artist_name = all_artists[chosen_aid]["name"]
            print(f"[INFO] Lottery picked artist '{artist_name}' (weight {weights[chosen_aid]:.2f})")

            track = select_track_for_artist(artist_name, artists_data, existing_artist_ids)

            if track is None:
                print(f"[INFO] No valid track found for '{artist_name}', rerolling lottery")
                continue

            if track:
                track_id = track.get("id")
                allowed, reason = track_allowed_to_add(track)
                if not track_id:
                    print(f"[WARN] Skipping invalid track with missing ID: {track}")
                elif not allowed:
                    print(f"[INFO] Skipping track '{track.get('name')}' - {reason}")
                else:
                    safe_spotify_call(sp.playlist_add_items, OUTPUT_PLAYLIST_ID, [track_id])
                    first_artist_id = None
                    if isinstance(track.get("artists"), list) and track["artists"]:
                        first_artist_id = track["artists"][0].get("id")
                    if first_artist_id:
                        existing_artist_ids.add(first_artist_id)
                    songs_added += 1
                    print(f"[INFO] Added track '{track.get('name','<unknown>')}' by '{track.get('artists',[{}])[0].get('name','<unknown>')}' | Total songs added: {songs_added}/{max_songs}")
    finally:
        # After main rolling, attempt to add up to 10 tracks sourced from whitelisted user profiles (if we hit quota)
        try:
            if songs_added >= max_songs:
                print("[INFO] Attempting to add up to 10 tracks from whitelisted user profiles")
                import random as _r
                for attempt_i in range(10):
                    profile_id = get_random_whitelisted_profile()
                    if not profile_id:
                        print("[INFO] No whitelisted profiles found in DB")
                        break
                    pls = safe_spotify_call(sp.user_playlists, profile_id, limit=50)
                    if not pls or "items" not in pls or not pls["items"]:
                        continue
                    candidate_pl = _r.choice(pls["items"])
                    pid = candidate_pl.get("id")
                    if not pid or is_playlist_blacklisted(pid):
                        continue
                    items = safe_spotify_call(sp.playlist_items, pid, fields="items(track(id,artists(id,name)))", limit=100)
                    if not items or "items" not in items:
                        try:
                            mark_playlist_blacklisted(pid)
                        except Exception:
                            pass
                        continue
                    tracks = [it.get("track") for it in items["items"] if it.get("track") and it["track"].get("id")]
                    if not tracks:
                        continue
                    picked = _r.choice(tracks)
                    allowed, reason = track_allowed_to_add(picked)
                    if not allowed:
                        continue
                    tid = picked["id"]
                    safe_spotify_call(sp.playlist_add_items, OUTPUT_PLAYLIST_ID, [tid])
                    print(f"[INFO] Added whitelist-sourced track '{picked.get('name')}' from profile {profile_id}")
        except Exception as e:
            print(f"[WARN] Error during whitelist processing: {e}")
        finally:
            close_global_driver()
            removed_count = remove_old_tracks_from_playlist(OUTPUT_PLAYLIST_ID, days_old=8)
            send_playlist_update_sms(songs_added, max_songs, removed_count, OUTPUT_PLAYLIST_ID)

            # Export artists table from DB to ARTISTS_FILE if DB is available
            try:
                conn = get_db_conn()
                if conn:
                    try:
                        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                            cur.execute("SELECT artist_id, artist_name, total_liked FROM user_artists")
                            rows = cur.fetchall() or []
                            artists_out = {}
                            for r in rows:
                                aid = r.get("artist_id")
                                name = r.get("artist_name") or ""
                                total = r.get("total_liked") or 0
                                if aid:
                                    artists_out[aid] = {"name": name, "total_liked": total}
                        try:
                            with open(ARTISTS_FILE, "w", encoding="utf-8") as f:
                                json.dump({"artists": artists_out}, f, indent=2, ensure_ascii=False)
                            print(f"[DB->FILE] Exported {len(artists_out)} artists to {ARTISTS_FILE}")
                        except Exception as e:
                            print(f"[WARN] Failed to write {ARTISTS_FILE}: {e}")
                    except Exception as e:
                        print(f"[WARN] Failed to query user_artists for export: {e}")
            except Exception as e:
                print(f"[WARN] Export artists to file skipped due to DB error: {e}")
