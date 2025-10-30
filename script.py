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
    time.sleep(.5)
    try:
        return func(*args, **kwargs)
    except spotipy.exceptions.SpotifyException as e:
        # Common transient or not-found cases
        if e.http_status == 404:
            print(f"[WARN] Spotify 404 for {func.__name__}: Resource not found")
        elif e.http_status == 429:
            print(f"[WARN] Rate limited in {func.__name__}; sleeping...")
            time.sleep(120)
        else:
            print(f"[WARN] Spotify error in {func.__name__}: {e}")
        return None
    except Exception as e:
        print(f"[WARN] Unexpected error in {func.__name__}: {e}")
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

    artist_results = safe_spotify_call(sp.search, artist_name, type="artist", limit=1)["artists"]["items"]
    if not artist_results:
        print(f"[WARN] No Spotify artist found for '{artist_name}'")
        return None
    artist_id = artist_results[0]["id"]

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
                fields="items(track(artists(id,name)))",
                market=None,
                additional_types="track,episode"
            )
            if not playlist_items or "items" not in playlist_items:
                print(f"[WARN] Spotify 404 for playlist_items: {playlist_id}, skipping")
                continue

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

    user_playlists = safe_spotify_call(sp.search, artist_name, type="playlist", limit=20)["playlists"]["items"]
    for pl in user_playlists[:10]:
        if not pl or "id" not in pl:
            continue
        playlist_id = pl["id"]
        if playlist_id in seen_playlists:
            continue
        seen_playlists.add(playlist_id)

        playlist_data = safe_spotify_call(
            sp.playlist_items, 
            playlist_id, 
            fields="items(track(artists(id,name)))"
        )
        if not playlist_data or "items" not in playlist_data:
            print(f"[WARN] Playlist {playlist_id} is empty or inaccessible, skipping")
            continue
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


# ==== UPDATE ARTISTS CACHE ====
def update_artists_from_likes():
    print("[INFO] Starting to update liked artist cache")
    
    if os.path.exists(ARTISTS_FILE):
        with open(ARTISTS_FILE, "r") as f:
            artist_cache = json.load(f).get("artists", {})
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
        items = results["items"]
        if not items:
            print("[INFO] No more liked tracks returned from Spotify")
            break

        new_artists_in_batch = 0
        existing_artists_in_batch = 0

        for item in items:
            track = item["track"]
            added_at = datetime.strptime(item["added_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            all_liked_songs.append({"track_id": track["id"], "artists": track["artists"], "added_at": added_at})

            for artist in track["artists"]:
                aid = artist["id"]
            if aid not in artist_cache:
                artist_cache[aid] = {"name": artist["name"], "total_liked": 1}
                new_artists[aid] = {"name": artist["name"], "total_liked": 1}
            else:
                artist_cache[aid]["total_liked"] += 1


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

    with open(ARTISTS_FILE, "w") as f:
        json.dump({"artists": artist_cache}, f, indent=2)

    print(f"[INFO] Finished updating liked artist cache: {len(artist_cache)} total artists cached, "
          f"{len(new_artists)} new artists added in this run")
    
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

    now = datetime.now(timezone.utc)
    tracks_to_remove = []

    for item in existing_tracks["items"]:
        track = item["track"]
        added_at = datetime.strptime(item["added_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        age_days = (now - added_at).days
        if age_days >= days_old:
            tracks_to_remove.append({"uri": track["id"]})

    removed_count = 0
    if tracks_to_remove:
        sp.playlist_remove_all_occurrences_of_items(playlist_id, [t["uri"] for t in tracks_to_remove])
        removed_count = len(tracks_to_remove)
        print(f"[INFO] Removed {removed_count} track(s) older than {days_old} days")
    else:
        print(f"[INFO] No tracks older than {days_old} days found")

    return removed_count

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
    # Update artists cache
    new_artists, _ = update_artists_from_likes()
    with open(ARTISTS_FILE, "r") as f:
        artists_data = json.load(f)["artists"]
    all_artists = {**artists_data, **new_artists}

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
    existing_artist_ids = {t["track"]["artists"][0]["id"] for t in existing_tracks["items"]}
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
                sp.playlist_add_items(OUTPUT_PLAYLIST_ID, [track["id"]])
                existing_artist_ids.add(track["artists"][0]["id"])
                songs_added += 1
                print(f"[INFO] Added track '{track['name']}' by '{track['artists'][0]['name']}' | Total songs added: {songs_added}/{max_songs}")
    finally:
        close_global_driver()
        removed_count = remove_old_tracks_from_playlist(OUTPUT_PLAYLIST_ID, days_old=8)
        send_playlist_update_sms(songs_added, max_songs, removed_count, OUTPUT_PLAYLIST_ID)
