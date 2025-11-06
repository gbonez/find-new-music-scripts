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
        if chrome_bin:
            options.binary_location = chrome_bin
        # fallback to legacy headless flag if new not supported
        try:
            options.add_argument("--headless=new")
        except Exception:
            options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        # try to use chromedriver-binary if no explicit path provided
        if not chromedriver_path:
            try:
                import chromedriver_binary  # installs and exposes binary_path
                chromedriver_path = getattr(chromedriver_binary, "binary_path", None)
            except Exception:
                chromedriver_path = None

        if not chromedriver_path:
            raise RuntimeError("CHROMEDRIVER_PATH (or chromedriver-binary) is required to start the Chrome driver")

        service = Service(chromedriver_path)
        global_driver = webdriver.Chrome(service=service, options=options)
    return global_driver

def close_global_driver():
    global global_driver
    if global_driver:
        try:
            global_driver.quit()
        except Exception:
            pass
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
            if getattr(e, "http_status", None) == 404:
                print(f"[WARN] Spotify 404 for {getattr(func,'__name__',str(func))}: Resource not found")
                return None
            elif getattr(e, "http_status", None) == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 30))
                print(f"[RATE LIMIT] Waiting {retry_after}s before retrying {getattr(func,'__name__',str(func))}...")
                time.sleep(retry_after + 2)
            elif 500 <= getattr(e, "http_status", 0) < 600:
                print(f"[WARN] Spotify server error ({getattr(e,'http_status',None)}) on {getattr(func,'__name__',str(func))}, retrying...")
                time.sleep(2)
            else:
                print(f"[ERROR] Spotify error ({getattr(e,'http_status',None)}) in {getattr(func,'__name__',str(func))}: {e}")
                return None
        except Exception as e:
            print(f"[WARN] Unexpected error in {getattr(func,'__name__',str(func))}: {e}")
            time.sleep(2)
    print(f"[FAIL] {getattr(func,'__name__',str(func))} failed after {retries} retries")
    return None

def get_random_track_from_playlist(playlist_id, excluded_artist=None, max_followers=None, source_desc="", artists_data=None, existing_artist_ids=None):
    consecutive_invalid = 0
    for attempt in range(1, 21):
        playlist = safe_spotify_call(
            sp.playlist_items,
            playlist_id,
            fields="items(track(name,id,artists(id,name)))"
        )
        if not playlist or "items" not in playlist:
            print(f"[WARN] Playlist {playlist_id} is empty or inaccessible, skipping")
            return None

        if not playlist["items"]:
            print(f"[WARN] Playlist {playlist_id} is empty, skipping...")
            return None

        item = random.choice(playlist["items"])
        track = item.get("track")
        if not track or "id" not in track or track.get("id") is None:
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
            except Exception:
                pass
            break

        artist_track_count = 0
        if playlist_items and isinstance(playlist_items, dict) and "items" in playlist_items:
            artist_track_count = sum(
                1
                for item in playlist_items["items"]
                if item.get("track")
                and artist_name.lower() in [(a.get("name") or "").lower() for a in item["track"]["artists"] if a.get("name") is not None]
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

    # Step 2: User playlists via API (improved & randomized)
    print(f"[INFO] No valid tracks found in artist playlists for '{artist_name}'. Trying user made playlists...")

    # Gather a randomized candidate set of playlists (avoid repeatedly using the same top results)
    max_checks = 10
    checked = 0
    search_limit = 50
    max_search_pages = 4  # up to ~200 playlists

    candidate_playlists = []
    seen_candidate_ids = set()
    offset = 0
    for page in range(max_search_pages):
        search_res = safe_spotify_call(sp.search, artist_name, type="playlist", limit=search_limit, offset=offset)
        offset += search_limit
        if not search_res or "playlists" not in search_res or not search_res["playlists"].get("items"):
            break
        for pl in search_res["playlists"]["items"]:
            if not pl or not isinstance(pl, dict):
                print(f"[WARN] Skipping malformed playlist entry: {pl}")
                continue

            pid = pl.get("id")
            if not pid or pid in seen_candidate_ids:
                continue

            seen_candidate_ids.add(pid)
            candidate_playlists.append(pl)
        # small pause to be polite / avoid hitting rate-limits on many pages
        time.sleep(0.15)
        # limit growth if we already gathered enough candidates
        if len(candidate_playlists) >= max_checks * 6:
            break

    if not candidate_playlists:
        print(f"[INFO] No user-playlist search results for '{artist_name}'")
    else:
        random.shuffle(candidate_playlists)
        for pl in candidate_playlists:
            if checked >= max_checks:
                break
            if not pl or "id" not in pl:
                continue
            playlist_id = pl["id"]

            # skip blacklisted playlists (do not count them)
            if is_playlist_blacklisted(playlist_id):
                print(f"[INFO] Skipping blacklisted user playlist {playlist_id} (does not count toward {max_checks} checks)")
                continue
            if playlist_id in seen_playlists:
                continue

            # fetch playlist items and verify the artist is actually present
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
                    print(f"[DB] Marked user playlist {playlist_id} as blacklisted (empty or inaccessible)")
                except Exception:
                    pass
                continue

            # inspect whether this playlist truly contains the artist (prefer id match)
            playlist_items = playlist_data["items"]
            contains_artist = False
            for item in playlist_items:
                tr = item.get("track") or {}
                for a in tr.get("artists", []):
                    if a.get("id") and a.get("id") == artist_id:
                        contains_artist = True
                        break
                    if (a.get("name") or "").strip().lower() == artist_name.strip().lower():
                        contains_artist = True
                        break
                if contains_artist:
                    break

            if not contains_artist:
                # playlist doesn't actually contain the artist; skip and do NOT increment checked
                print(f"[INFO] Playlist {playlist_id} does not contain artist '{artist_name}' — skipping (does not count toward {max_checks})")
                seen_playlists.add(playlist_id)
                try:
                    add_or_update_user_playlist(playlist_id, name=pl.get("name"), blacklisted=False)
                except Exception:
                    pass
                continue

            # playlist confirmed to contain the artist; count as inspected
            seen_playlists.add(playlist_id)
            checked += 1

            # filter playlists overly dominated by the artist
            artist_track_count = sum(
                1 for item in playlist_items
                if item.get("track") and artist_name.lower() in [(a.get("name") or "").lower() for a in item["track"]["artists"] if a.get("name") is not None]
            )
            if artist_track_count > 10:
                continue

            track = get_random_track_from_playlist(
                playlist_id,
                excluded_artist=artist_name,
                max_followers=50000,
                source_desc=f"{pl.get('name','<unknown>')} (user-made playlist via API)",
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
        similar_artists = [a.get("name") for a in data.get("similarartists", {}).get("artist", []) if a.get("name")]
    except Exception as e:
        print(f"[WARN] Failed fetching Last.fm similar artists for {artist_name}: {e}")
        similar_artists = []
    random.shuffle(similar_artists)
    for sim_artist in similar_artists[:10]:
        # defensive Spotify search result handling
        search_res = safe_spotify_call(sp.search, sim_artist, type="artist", limit=1)
        if not search_res or "artists" not in search_res or not search_res["artists"].get("items"):
            continue
        artist_results = search_res["artists"]["items"]
        sim_artist_data = artist_results[0]
        # follower count may be int or string; coerce safely
        try:
            sim_followers = int(sim_artist_data.get("followers", {}).get("total", 0) or 0)
        except Exception:
            sim_followers = 0
        if sim_followers >= 50000:
            continue
        top_tracks_resp = safe_spotify_call(sp.artist_top_tracks, sim_artist_data["id"], country="US")
        top_tracks = top_tracks_resp["tracks"] if top_tracks_resp and "tracks" in top_tracks_resp else []
        if top_tracks:
             track = random.choice(top_tracks)
             is_valid, reason = validate_track(track, artists_data, existing_artist_ids, max_followers=50000)
             if is_valid:
                 print(f"[INFO] Selected valid track '{track.get('name')}' by '{(track.get('artists') or [{}])[0].get('name')}' from Last.fm similar artists")
                 return track
             else:
                 print(f"[VALIDATION] Track '{track.get('name')}' by '{(track.get('artists') or [{}])[0].get('name')}' failed: {reason}")

    # Step 4: Spotify similar artists (improved)
    print(f"[INFO] No valid tracks found via Last.fm for '{artist_name}'. Trying Spotify similar artists...")
    similar_artists_data = safe_spotify_call(sp.artist_related_artists, artist_id)
    if not similar_artists_data or "artists" not in similar_artists_data or not similar_artists_data["artists"]:
        # try to re-resolve artist id via broader search (attempt to handle ambiguous/missed artist ids)
        print(f"[WARN] Spotify returned no related artists for {artist_name} ({artist_id}). Attempting broader artist lookup and retry.")
        alt_search = safe_spotify_call(sp.search, artist_name, type="artist", limit=10)
        if alt_search and "artists" in alt_search and alt_search["artists"].get("items"):
            # try to pick the best matching artist by exact name match first
            candidates = alt_search["artists"]["items"]
            best = None
            for c in candidates:
                if (c.get("name") or "").strip().lower() == artist_name.strip().lower():
                    best = c
                    break
            if not best:
                best = candidates[0]
            if best and best.get("id") and best.get("id") != artist_id:
                similar_artists_data = safe_spotify_call(sp.artist_related_artists, best["id"])

    if not similar_artists_data or "artists" not in similar_artists_data:
        print(f"[WARN] Spotify related-artists not available for '{artist_name}'. Skipping Spotify-similar step.")
        return None 

    artists_list = similar_artists_data["artists"]
    random.shuffle(artists_list)
    for sim_artist_data in artists_list[:10]:
        # defensive follower/name handling
        try:
            sim_followers = int(sim_artist_data.get("followers", {}).get("total", 0) or 0)
        except Exception:
            sim_followers = 0
        sim_name = (sim_artist_data.get("name") or "").lower()
        if sim_followers >= 50000 or sim_name == artist_name.lower():
            continue
        top_tracks_resp = safe_spotify_call(sp.artist_top_tracks, sim_artist_data["id"], country="US")
        top_tracks = top_tracks_resp["tracks"] if top_tracks_resp and "tracks" in top_tracks_resp else []
        if top_tracks:
            track = random.choice(top_tracks)
            is_valid, reason = validate_track(track, artists_data, existing_artist_ids, max_followers=50000)
            if is_valid:
                print(f"[INFO] Selected valid track '{track.get('name')}' by '{(track.get('artists') or [{}])[0].get('name')}' from Spotify similar artists")
                return track
            else:
                print(f"[VALIDATION] Track '{track.get('name')}' by '{(track.get('artists') or [{}])[0].get('name')}' failed: {reason}")

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
    aid = artist.get("id")
    name_lower = (artist.get("name") or "").lower()

    # DB-level blacklist checks: immediate ineligibility if track or artist appears in blacklisted_songs
    try:
        tid = track.get("id")
        if tid and is_track_blacklisted(tid):
            return False, "Track is blacklisted in DB"
        if aid and blacklisted_artist_count(aid) and blacklisted_artist_count(aid) > 0:
            return False, f"Artist '{artist.get('name')}' appears in blacklisted_songs"
    except Exception as e:
        # log and continue with other checks (avoid blocking on DB failures)
        print(f"[WARN] validate_track DB blacklist check failed: {e}")

    # 1. Blocked by artists.json
    artist_entry = artists_data.get(aid)
    if not artist_entry:
        for k, v in artists_data.items():
            if (v.get("name") or "").lower() == name_lower:
                artist_entry = v
                break
    if artist_entry and int(artist_entry.get("total_liked", 0)) >= 3:
        return False, f"Artist '{artist.get('name')}' blocked by artists.json (total_liked >= 3)"

    # 2. Already in playlist
    if existing_artist_ids and (aid in existing_artist_ids):
        return False, f"Artist '{artist.get('name')}' already has a track in playlist"

    # 3. Max followers
    if max_followers:
        full_artist = safe_spotify_call(sp.artist, aid)
        if full_artist and full_artist.get("followers", {}).get("total", 0) > max_followers:
            return False, f"Artist '{artist.get('name')}' has {full_artist.get('followers', {}).get('total', 0)} followers, exceeds max {max_followers}"

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
                # coerce total_liked to int to avoid later type errors
                try:
                    total = int(r.get("total_liked") or 0)
                except Exception:
                    total = 0
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
        artist_name_lower = (info.get("name") or "").lower()
        if not artist_name_lower:
            continue

        scrobbles = artist_play_map.get(artist_name_lower, [])
        if not scrobbles:
            continue

        recent_14 = sum(1 for d in scrobbles if d >= recent_14_cutoff)
        recent_60 = sum(1 for d in scrobbles if d >= recent_60_cutoff)
        try:
            total_liked = int(info.get("total_liked", 0) or 0)
        except Exception:
            total_liked = 0

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
    """
    Scan the entire playlist (paged) and remove any track whose added_at is
    >= days_old. Uses safe_spotify_call and removes in batches.
    Note: spotify.playlist_remove_all_occurrences_of_items removes all occurrences
    of the provided track URIs in the playlist.
    """
    print(f"[INFO] Scanning entire playlist for tracks older than {days_old} days: {playlist_id}")
    limit = 100
    offset = 0
    now = datetime.now(timezone.utc)
    uris_to_remove = []
    while True:
        res = safe_spotify_call(
            sp.playlist_items,
            playlist_id,
            fields="items(track(id,name,artists(id,name)),added_at)",
            limit=limit,
            offset=offset
        )
        if not res or "items" not in res or not res["items"]:
            break

        items = res["items"]
        for item in items:
            track = item.get("track")
            added_at_str = item.get("added_at")
            if not track or not added_at_str:
                continue
            # robust parse: handle "YYYY-MM-DDTHH:MM:SSZ" and fractional seconds
            ts = None
            try:
                s = added_at_str
                if s.endswith("Z"):
                    s = s[:-1]
                # try ISO parse (handles fractional seconds)
                ts = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
            except Exception:
                try:
                    ts = datetime.strptime(added_at_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                except Exception:
                    # fallback: skip item if we can't parse
                    print(f"[WARN] Could not parse added_at '{added_at_str}' for track {track.get('id')}, skipping")
                    continue

            age_days = (now - ts).days
            if age_days >= days_old:
                tid = track.get("id")
                if tid:
                    uri = f"spotify:track:{tid}"
                    uris_to_remove.append(uri)

        # paging advance
        if len(items) < limit:
            break
        offset += limit

    if not uris_to_remove:
        print(f"[INFO] No tracks older than {days_old} days found in playlist {playlist_id}")
        return 0

    # remove in batches to avoid API limits
    removed_total = 0
    batch_size = 50
    for i in range(0, len(uris_to_remove), batch_size):
        batch = uris_to_remove[i:i+batch_size]
        res = safe_spotify_call(sp.playlist_remove_all_occurrences_of_items, playlist_id, batch)
        if res is None:
            print(f"[WARN] Removal batch failed for {len(batch)} URIs")
        else:
            removed_total += len(batch)
            print(f"[INFO] Removed batch of {len(batch)} URIs from playlist {playlist_id}")

    print(f"[INFO] Removed {removed_total} track URIs older than {days_old} days from playlist {playlist_id}")
    return removed_total

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
def send_playlist_update_sms(songs_added, max_songs, removed_count, playlist_id, whitelist_added=0, whitelist_target=10):
    today = datetime.now(timezone.utc).strftime("%m/%d/%Y")
    playlist_link = f"https://open.spotify.com/playlist/{playlist_id}"
    
    # Determine status
    if songs_added >= max_songs and whitelist_added >= whitelist_target:
        status_emoji = "✅"
        status_text = "Playlist successfully updated (including whitelist)"
    elif songs_added >= max_songs:
        status_emoji = "✅"
        status_text = "Playlist successfully updated"
    else:
        status_emoji = "❌"
        status_text = "Playlist not fully updated"

    message_body = (
        f"🎵 Playlist Update Summary ({today})\n\n"
        f"Enhanced Recs added: {songs_added}/{max_songs}\n"
        f"User Recs added: {whitelist_added}/{whitelist_target}\n"
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

# ==== PAGED PLAYLIST HELPERS (improves duplicate detection) ====
def fetch_all_playlist_items(playlist_id, page_limit=100):
    """Return list of track objects (paged) for a playlist_id using safe_spotify_call."""
    offset = 0
    all_items = []
    while True:
        res = safe_spotify_call(
            sp.playlist_items,
            playlist_id,
            fields="items(track(id,name,artists(id,name)), added_at)",
            limit=page_limit,
            offset=offset
        )
        if not res or "items" not in res or not res["items"]:
            break
        # keep the raw items so callers can inspect added_at etc.
        all_items.extend([it.get("track") for it in res["items"] if it.get("track")])
        if len(res["items"]) < page_limit:
            break
        offset += page_limit
    return all_items

# ...existing code...

def add_track_to_blacklist_db(track, fixed=False):
    """
    Insert a track into blacklisted_songs with fixed flag.
    Attempts insertion with created_at column if present, otherwise falls back.
    """
    if not track or not isinstance(track, dict):
        return
    tid = track.get("id")
    if not tid:
        return
    song_name = track.get("name") or ""
    artists = track.get("artists") or []
    artist_id = artists[0].get("id") if artists and artists[0].get("id") else None
    artist_name = artists[0].get("name") if artists and artists[0].get("name") else None

    conn = get_db_conn()
    if not conn:
        print("[DB] No DB connection available to insert blacklisted song")
        return
    try:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO blacklisted_songs (song_id, song_name, artist_id, artist_name, fixed, created_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (song_id) DO NOTHING
                    """,
                    (tid, song_name, artist_id, artist_name, fixed),
                )
            except Exception:
                # fallback if created_at doesn't exist
                try:
                    cur.execute(
                        """
                        INSERT INTO blacklisted_songs (song_id, song_name, artist_id, artist_name, fixed)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (song_id) DO NOTHING
                        """,
                        (tid, song_name, artist_id, artist_name, fixed),
                    )
                except Exception as e2:
                    print(f"[DB] Failed to insert blacklisted song (fallback): {e2}")
    except Exception as e:
        print(f"[DB] Failed to insert blacklisted song: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

def cleanup_old_blacklisted_songs(playlist_id, days=14):
    """
    Remove tracks from the playlist that are in blacklisted_songs with fixed = false
    and older than `days`. After removal, mark them fixed = true.
    Returns number of tracks removed.
    """
    conn = get_db_conn()
    if not conn:
        print("[DB] No DB connection available for cleanup_old_blacklisted_songs")
        return 0
    try:
        with conn.cursor() as cur:
            # try to select using created_at if available
            try:
                cur.execute(
                    """
                    SELECT song_id FROM blacklisted_songs
                    WHERE fixed = false AND created_at <= (NOW() - INTERVAL %s)
                    """,
                    (f"{days} days",),
                )
            except Exception as e:
                # created_at might not exist, fallback: select all fixed=false (can't filter by age)
                print(f"[DB] cleanup query with created_at failed: {e}; attempting fallback select of all fixed=false")
                try:
                    cur.execute(
                        "SELECT song_id, created_at FROM blacklisted_songs WHERE fixed = false"
                    )
                    rows = cur.fetchall()
                    # if created_at not available, don't remove anything conservatively
                    # try to filter by created_at if present in row
                    song_ids = []
                    now = datetime.now(timezone.utc)
                    for r in rows:
                        sid = r[0]
                        created = None
                        if len(r) > 1 and isinstance(r[1], datetime):
                            created = r[1]
                        if created:
                            age_days = (now - created).days
                            if age_days >= days:
                                song_ids.append(sid)
                    if not song_ids:
                        print("[DB] No qualifying old blacklisted songs found in fallback")
                        return 0
                except Exception as e2:
                    print(f"[DB] Fallback cleanup query failed: {e2}")
                    return 0
            else:
                rows = cur.fetchall()
                song_ids = [r[0] for r in rows if r and r[0]]
            if not song_ids:
                print("[DB] No old blacklisted songs to remove")
                return 0

            # build URIs and remove in batches
            uris = [f"spotify:track:{sid}" for sid in sorted(set(song_ids))]
            removed_total = 0
            batch_size = 50
            for i in range(0, len(uris), batch_size):
                batch = uris[i : i + batch_size]
                res = safe_spotify_call(sp.playlist_remove_all_occurrences_of_items, playlist_id, batch)
                if res is None:
                    print(f"[WARN] Removal batch failed for {len(batch)} URIs during blacklist cleanup")
                else:
                    removed_total += len(batch)
                    print(f"[INFO] Removed {len(batch)} blacklisted URIs from playlist {playlist_id}")

            if removed_total > 0:
                # mark removed song_ids as fixed = true
                try:
                    cur.execute(
                        """
                        UPDATE blacklisted_songs SET fixed = true
                        WHERE song_id = ANY(%s)
                        """,
                        (list(set(song_ids)),),
                    )
                    print(f"[DB] Marked {len(set(song_ids))} blacklisted_songs.fixed = true")
                except Exception as e:
                    print(f"[DB] Failed to mark blacklisted_songs fixed: {e}")

            return removed_total
    except Exception as e:
        print(f"[DB] Error during cleanup_old_blacklisted_songs: {e}")
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass

def build_existing_artist_ids(tracks):
    ids = set()
    for t in tracks:
        if not t:
            continue
        artists = t.get("artists") or []
        if artists and artists[0].get("id"):
            ids.add(artists[0].get("id"))
    return ids

def build_artist_first_map(tracks):
    """Map artist key -> first seen track {track_id, track_name, pos} for better duplicate reporting."""
    first_map = {}
    for idx, t in enumerate(tracks):
        if not t:
            continue
        artists = t.get("artists") or []
        if not artists:
            continue
        aid = artists[0].get("id")
        name = artists[0].get("name") or ""
        key = _artist_key_from_track(t)
        if key and key not in first_map:
            first_map[key] = {"track_id": t.get("id"), "track_name": t.get("name") or "<unknown>", "pos": idx}
    return first_map

def _artist_key_from_track(t):
    artists = t.get("artists") or []
    if not artists:
        return None
    aid = artists[0].get("id")
    if aid:
        return f"id:{aid}"
    name = (artists[0].get("name") or "").strip().lower()
    if name:
        return f"name:{name}"
    return None

# ==== MAIN COMBINED SCRIPT ====
if __name__ == "__main__":
    print("Starting Enhanced Recs Script...")
    time.sleep(1)

    new_artists, liked_songs = update_artists_from_likes()

    # Persist detected liked songs into blacklisted_songs with fixed = true so they are excluded
    try:
        inserted = 0
        for ls in liked_songs:
            tid = ls.get("track_id")
            if not tid:
                continue
            # build minimal track dict to reuse insertion helper
            track_stub = {"id": tid, "name": ls.get("track") or "", "artists": ls.get("artists") or []}
            add_track_to_blacklist_db(track_stub, fixed=True)
            inserted += 1
        if inserted:
            print(f"[DB] Inserted {inserted} liked songs into blacklisted_songs with fixed=true")
    except Exception as e:
        print(f"[WARN] Failed to insert liked songs into blacklist DB: {e}")

    # Load canonical artist cache from DB (fallback to file if DB absent)
    artists_data = load_artists_from_db()

    # Merge DB-cache with newly discovered artists (new_artists may include names/total_liked increments)
    all_artists = {**artists_data}

    def _to_int(v):
        try:
            return int(v)
        except Exception:
            return 0

    for aid, info in new_artists.items():
        new_total = _to_int(info.get("total_liked", 0))
        new_name = info.get("name") or ""
        if aid in all_artists:
            existing_total = _to_int(all_artists[aid].get("total_liked", 0))
            all_artists[aid]["total_liked"] = max(existing_total, new_total)
            if not all_artists[aid].get("name"):
                all_artists[aid]["name"] = new_name
        else:
            all_artists[aid] = {"name": new_name, "total_liked": new_total}

    # Ensure validation uses the merged view (DB + newly scanned liked songs)
    artists_data = dict(all_artists)

    recent_tracks = fetch_all_recent_tracks()
    artist_play_map = build_artist_play_map(recent_tracks)

    weights = calculate_weights(all_artists, artist_play_map)

    songs_added = 0
    max_songs = 50
    rolled_aids = set()

    # --- REPLACE single-page fetch with a full paged fetch to build accurate existing_artist_ids & first-occurrence map
    existing_tracks = fetch_all_playlist_items(OUTPUT_PLAYLIST_ID, page_limit=100)
    if not existing_tracks:
        existing_artist_ids = set()
        print(f"[WARN] Could not fetch existing playlist items for {OUTPUT_PLAYLIST_ID}, proceeding with empty set")
    else:
        existing_artist_ids = build_existing_artist_ids(existing_tracks)
    first_artist_map = build_artist_first_map(existing_tracks)
    print(f"[INFO] Found {len(existing_artist_ids)} existing artists in playlist (paged)")

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

            # Final gate: enforce DB and playlist validation AGAIN with full existing_artist_ids
            track_id = track.get("id")
            if not track_id:
                print(f"[WARN] Skipping invalid track with missing ID: {track}")
                continue

            allowed_db, reason_db = track_allowed_to_add(track)
            valid_logic, reason_logic = validate_track(track, artists_data, existing_artist_ids, max_followers=None)

            # If validate fails due to existing artist in playlist, try to include reporting of first occurrence
            if not valid_logic and "already has a track" in (reason_logic or "").lower():
                artist_key = _artist_key_from_track(track)
                first = first_artist_map.get(artist_key) if artist_key else None
                if first:
                    reason_logic = f"{reason_logic}; first occurrence: '{first['track_name']}' (pos {first['pos']})"

            if not allowed_db:
                print(f"[INFO] Skipping track '{track.get('name')}' - DB block: {reason_db}")
                continue
            if not valid_logic:
                print(f"[INFO] Skipping track '{track.get('name')}' - validation block: {reason_logic}")
                continue

            # Passed final gates: add track
            add_res = safe_spotify_call(sp.playlist_add_items, OUTPUT_PLAYLIST_ID, [track_id])
            if add_res is None:
                print(f"[WARN] Failed to add track '{track.get('name')}' (API error).")
                continue

            # insert into blacklisted_songs (fixed = false) so this track is ineligible on future runs
            try:
                add_track_to_blacklist_db(track)
                print(f"[DB] Inserted added track '{track.get('name')}' ({track_id}) into blacklisted_songs (fixed=false)")
            except Exception as e:
                print(f"[DB] Failed to insert added track into blacklisted_songs: {e}")

            # update local caches so further validations are accurate within this run
            first_artist_id = None
            if isinstance(track.get("artists"), list) and track["artists"]:
                first_artist_id = track["artists"][0].get("id")
            if first_artist_id:
                existing_artist_ids.add(first_artist_id)
                # also add to first_artist_map if absent
                artist_key = _artist_key_from_track(track)
                if artist_key and artist_key not in first_artist_map:
                    first_artist_map[artist_key] = {"track_id": track_id, "track_name": track.get("name") or "<unknown>", "pos": None}
            songs_added += 1
            print(f"[INFO] Added track '{track.get('name','<unknown>')}' by '{track.get('artists',[{}])[0].get('name','<unknown>')}' | Total songs added: {songs_added}/{max_songs}")
    finally:
        # After main rolling, attempt to add up to 10 tracks sourced from whitelisted user profiles (if we hit quota)
        whitelist_added = 0
        try:
            if songs_added >= max_songs:
                print("[INFO] Attempting to add up to 10 tracks from whitelisted user profiles")
                import random as _r
                attempts = 0
                # keep trying until we add 10 whitelist tracks or exhaust attempts
                while whitelist_added < 10 and attempts < 200:
                    attempts += 1
                    profile_id = get_random_whitelisted_profile()
                    if not profile_id:
                        print("[INFO] No whitelisted profiles found in DB")
                        break

                    print(f"[WHITELIST] Attempt {attempts} for profile: {profile_id}")
                    pls = safe_spotify_call(sp.user_playlists, profile_id, limit=50)
                    if not pls or "items" not in pls or not pls["items"]:
                        print(f"[WHITELIST] No playlists returned for profile {profile_id} (possibly private or 404). Skipping profile.")
                        continue

                    candidate_pl = _r.choice(pls["items"])
                    pid = candidate_pl.get("id")
                    pl_name = candidate_pl.get("name") or "<unknown playlist>"
                    print(f"[WHITELIST] Selected playlist '{pl_name}' ({pid}) from profile {profile_id}")

                    if not pid:
                        print(f"[WHITELIST] Playlist id missing for selected playlist from {profile_id}, skipping.")
                        continue
                    if is_playlist_blacklisted(pid):
                        print(f"[WHITELIST] Playlist {pid} is blacklisted in DB; skipping.")
                        continue

                    items = safe_spotify_call(sp.playlist_items, pid, fields="items(track(id,name,artists(id,name)))", limit=100)
                    if not items or "items" not in items or not items["items"]:
                        print(f"[WHITELIST] Could not fetch items for playlist '{pl_name}' ({pid}). Marking blacklisted.")
                        try:
                            mark_playlist_blacklisted(pid)
                        except Exception:
                            pass
                        continue

                    # build candidate track list with safe fields
                    tracks = []
                    for it in items["items"]:
                        tr = it.get("track")
                        if not tr or not tr.get("id"):
                            continue
                        # ensure artists exist
                        artists = tr.get("artists") or []
                        if not artists:
                            continue
                        tracks.append(tr)
                    if not tracks:
                        print(f"[WHITELIST] No valid tracks found in playlist '{pl_name}' ({pid})")
                        continue

                    picked = _r.choice(tracks)
                    track_name = picked.get("name") or "<unknown track>"
                    artist_name = (picked.get("artists") or [{}])[0].get("name") or "<unknown artist>"
                    print(f"[WHITELIST] Picked track '{track_name}' by '{artist_name}' from playlist '{pl_name}'")

                    # Run the same DB + validation checks as for main pipeline
                    allowed_db, reason_db = track_allowed_to_add(picked)
                    valid_logic, reason_logic = validate_track(picked, artists_data, existing_artist_ids, max_followers=None)
                    if not allowed_db:
                        print(f"[WHITELIST] Skipping '{track_name}' - DB blacklist: {reason_db}")
                        continue
                    if not valid_logic:
                        print(f"[WHITELIST] Skipping '{track_name}' - validate logic: {reason_logic}")
                        continue

                    # Add the whitelist track
                    add_res = safe_spotify_call(sp.playlist_add_items, OUTPUT_PLAYLIST_ID, [picked.get("id")])
                    if add_res is None:
                        print(f"[WHITELIST] Failed to add '{track_name}' to playlist (API error).")
                        # don't increment whitelist_added; continue attempting
                        continue

                    # insert whitelist-added track into blacklisted_songs (fixed = false)
                    try:
                        add_track_to_blacklist_db(picked)
                        print(f"[DB] Inserted whitelist track '{track_name}' ({picked.get('id')}) into blacklisted_songs (fixed=false)")
                    except Exception as e:
                        print(f"[DB] Failed to insert whitelist track into blacklisted_songs: {e}")

                    whitelist_added += 1
                    print(f"[WHITELIST] Added whitelist-sourced track '{track_name}' by '{artist_name}' from playlist '{pl_name}' [{whitelist_added}/10]")
                    # update local existing artist cache so further checks in this run are accurate
                    try:
                        if isinstance(picked.get("artists"), list) and picked["artists"]:
                            fid = picked["artists"][0].get("id")
                            if fid:
                                existing_artist_ids.add(fid)
                    except Exception:
                        pass
                    # small sleep to be polite to Spotify API
                    time.sleep(0.2)
        except Exception as e:
            print(f"[WARN] Error during whitelist processing: {e}")
        finally:
            # cleanup & reporting
            try:
                close_global_driver()
            except Exception:
                pass
            removed_count = remove_old_tracks_from_playlist(OUTPUT_PLAYLIST_ID, days_old=8)
            # remove any blacklisted_songs older than 14 days with fixed=false
            added_removed = cleanup_old_blacklisted_songs(OUTPUT_PLAYLIST_ID, days=14)
            removed_count += added_removed
            send_playlist_update_sms(songs_added, max_songs, removed_count, OUTPUT_PLAYLIST_ID, whitelist_added, 10)
            print(f"[INFO] Run complete. Enhanced added: {songs_added}/{max_songs} | Whitelist added: {whitelist_added}/10 | Old removed: {removed_count}")

