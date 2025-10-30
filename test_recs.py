import os
import json
import random
import time
from datetime import datetime, timezone
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
from spotipy.exceptions import SpotifyException
import requests

# Selenium for scraping
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# ==== CONFIG ====
ARTISTS_FILE = "artists.json"
OUTPUT_PLAYLIST_ID = os.environ.get("PLAYLIST_ID")  # playlist to add selected tracks
LASTFM_API_KEY = os.environ.get("LASTFM_API_KEY")
LASTFM_USERNAME = os.environ.get("LASTFM_USERNAME")

SPOTIFY_CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")
SPOTIFY_REDIRECT_URI = (os.environ.get("BASE_URL") or "http://localhost:5000") + "/callback"
SPOTIFY_REFRESH_TOKEN = os.environ.get("SPOTIFY_REFRESH_TOKEN")

scope = "playlist-modify-public playlist-modify-private"

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

# ==== GLOBAL DRIVER ====
global_driver = None
def get_global_driver():
    global global_driver
    if global_driver:
        return global_driver
    options = Options()
    options.add_argument("--headless") 
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    global_driver = webdriver.Chrome(options=options)
    return global_driver

def close_global_driver():
    global global_driver
    if global_driver:
        global_driver.quit()
        global_driver = None

# ==== HELPERS ====
def safe_spotify_call(func, *args, **kwargs):
    while True:
        try:
            result = func(*args, **kwargs)
            time.sleep(0.5)
            return result
        except SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get("Retry-After", 5))
                print(f"[INFO] Rate limited, retrying after {retry_after}s...")
                time.sleep(retry_after)
            else:
                raise

def track_valid_for_artists_json(track, artists_data):
    """Skip a track if its artist is in artists.json and liked count >= 3"""
    track_artist_name = track["artists"][0]["name"]
    artist_entry = artists_data.get(track_artist_name.lower())
    if artist_entry and artist_entry.get("liked_count", 0) >= 3:
        print(f"[INFO] Skipping '{track['name']}' because '{track_artist_name}' has liked_count >= 3")
        return False
    return True

def get_random_track_from_playlist(playlist_id, excluded_artist=None, max_followers=None, source_desc="", artists_data=None):
    consecutive_invalid = 0
    for attempt in range(1, 21):
        try:
            playlist = safe_spotify_call(
                sp.playlist_items,
                playlist_id,
                fields="items(track(name,id,artists(id,name)))"
            )
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
            continue

        track_artist = track["artists"][0]
        is_valid = True
        reason = ""

        # Skip same artist
        if excluded_artist and track_artist["name"].lower() == excluded_artist.lower():
            is_valid = False
            reason = f"track artist '{track_artist['name']}' matches excluded artist '{excluded_artist}'"
        # Skip artists exceeding follower limit
        elif max_followers:
            full_artist = safe_spotify_call(sp.artist, track_artist["id"])
            if full_artist["followers"]["total"] > max_followers:
                is_valid = False
                reason = f"track artist '{track_artist['name']}' has {full_artist['followers']['total']} followers, exceeds {max_followers}"
        # Skip tracks by artists in artists.json with liked_count >= 3
        elif artists_data and not track_valid_for_artists_json(track, artists_data):
            is_valid = False
            reason = f"track artist '{track_artist['name']}' blocked by artists.json"

        print(f"[ATTEMPT {attempt}] Playlist '{source_desc}' | Song '{track['name']}' by '{track_artist['name']}' | Valid? {is_valid}")

        if is_valid:
            return track
        else:
            print(f"         Re-rolling because: {reason}")
            consecutive_invalid += 1
            if consecutive_invalid >= 5:
                print(f"[INFO] 5 consecutive invalid tracks found in playlist '{source_desc}', breaking out of playlist attempt")
                return None


def fetch_lastfm_similar_artists(artist_name, limit=10):
    url = "http://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "artist.getsimilar",
        "artist": artist_name,
        "api_key": LASTFM_API_KEY,
        "format": "json",
        "limit": limit
    }
    time.sleep(.25)
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    return [a["name"] for a in data.get("similarartists", {}).get("artist", [])]

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

# ==== TRACK SELECTION ====
def select_track_for_artist(artist_name, artists_data):
    track = None
    seen_playlists = set()
    playlist_attempts = 0

    # Get Spotify artist ID
    artist_results = safe_spotify_call(sp.search, artist_name, type="artist", limit=1)["artists"]["items"]
    if not artist_results:
        print(f"[WARN] No Spotify artist found for '{artist_name}'")
        return None
    artist_id = artist_results[0]["id"]

    # Step 1: Scraped artist playlists
    try:
        scraped_artist_playlists = scrape_artist_playlists(artist_id)
    except Exception as e:
        print(f"[WARN] Scraping failed for {artist_name}: {e}")
        scraped_artist_playlists = []

    for pl in scraped_artist_playlists:
        playlist_id = pl["url"].split("/")[-1].split("?")[0]
        if playlist_id in seen_playlists:
            continue
        seen_playlists.add(playlist_id)

        # Count tracks by original artist
        try:
            playlist_items = safe_spotify_call(
                sp.playlist_items, 
                playlist_id, 
                fields="items(track(artists(id,name)))"
            )["items"]
        except SpotifyException as e:
            if e.http_status == 404:
                print(f"[WARN] Playlist '{pl.get('name')}' not found or inaccessible, skipping...")
                continue
            else:
                raise

        # Calculate track count by original artist
        artist_track_count = sum(
            1 for item in playlist_items
            if item.get("track") and artist_name.lower() in [a["name"].lower() for a in item["track"]["artists"]]
        )

        if artist_track_count > 5:
            print(f"[INFO] Playlist '{pl['name']}' has {artist_track_count} tracks by original artist, skipping")
            continue

        playlist_attempts += 1
        if playlist_attempts > 2:
            print(f"[INFO] Tried 2 artist-made playlists for '{artist_name}', moving on")
            break

        track = get_random_track_from_playlist(
            playlist_id,
            excluded_artist=artist_name,
            max_followers=50000,
            source_desc=f"{pl['name']} (artist-made playlist scraped)",
            artists_data=artists_data
        )
        if track:
            print(f"[SUCCESS] Selected '{track['name']}' from scraped artist playlist '{pl['name']}'")
            return track

    # Step 2: User playlists via API
    try:
        user_playlists = safe_spotify_call(sp.search, artist_name, type="playlist", limit=20)["playlists"]["items"]
    except Exception as e:
        print(f"[WARN] Failed to fetch user playlists for {artist_name}: {e}")
        user_playlists = []

    for pl_index, pl in enumerate(user_playlists[:10], start=1):
        if not pl or "id" not in pl:
            print(f"[WARN] Skipping invalid playlist entry for {artist_name}: {pl}")
            continue
        playlist_id = pl["id"]

        if playlist_id in seen_playlists:
            continue
        seen_playlists.add(playlist_id)

        # Count tracks by original artist (same logic as artist-made playlists)
        try:
            playlist_items = safe_spotify_call(
                sp.playlist_items, 
                playlist_id, 
                fields="items(track(artists(id,name)))"
            )["items"]
        except Exception as e:
            print(f"[WARN] Could not fetch items for playlist {pl.get('name')}: {e}")
            continue

        artist_track_count = sum(
            1 for item in playlist_items
            if item.get("track") and artist_name.lower() in [a["name"].lower() for a in item["track"]["artists"]]
        )
        if artist_track_count > 10:
            print(f"[INFO] Playlist '{pl['name']}' has {artist_track_count} tracks by original artist, skipping")
            continue

        # Select a track
        track = get_random_track_from_playlist(
            playlist_id,
            excluded_artist=artist_name,
            max_followers=50000,
            source_desc=f"{pl['name']} (user-made playlist via API)",
            artists_data=artists_data
        )
        if track:
            print(f"[SUCCESS] Selected '{track['name']}' from user playlist '{pl['name']}'")
            return track

    # Step 3: Last.fm similar artists
    similar_artists = fetch_lastfm_similar_artists(artist_name)
    random.shuffle(similar_artists)
    for sim_artist in similar_artists[:10]:
        artist_results = safe_spotify_call(sp.search, sim_artist, type="artist", limit=1)["artists"]["items"]
        if not artist_results:
            continue
        sim_artist_data = artist_results[0]
        if sim_artist_data["followers"]["total"] >= 100000:
            continue
        top_tracks = safe_spotify_call(sp.artist_top_tracks, sim_artist_data["id"], country="US")["tracks"]
        if top_tracks:
            track = random.choice(top_tracks)
            if track_valid_for_artists_json(track, artists_data):
                print(f"[INFO] Picked track '{track['name']}' from Last.fm similar artist '{sim_artist}'")
                return track

    # Step 4: Spotify similar artists
    similar_artists_data = safe_spotify_call(sp.artist_related_artists, artist_id)["artists"]
    random.shuffle(similar_artists_data)
    for sim_artist_data in similar_artists_data[:10]:
        if sim_artist_data["followers"]["total"] >= 50000 or sim_artist_data["name"].lower() == artist_name.lower():
            continue
        top_tracks = safe_spotify_call(sp.artist_top_tracks, sim_artist_data["id"], country="US")["tracks"]
        if top_tracks:
            track = random.choice(top_tracks)
            if track_valid_for_artists_json(track, artists_data):
                print(f"[INFO] Picked track '{track['name']}' from Spotify similar artist '{sim_artist_data['name']}'")
                return track

    print(f"[WARN] No valid track found for artist '{artist_name}' in any source")
    return None

# ==== MAIN SCRIPT ====
if __name__ == "__main__":
    # Load artists.json
    with open(ARTISTS_FILE, "r") as f:
        artists_data = json.load(f)["artists"]  # <-- get the inner dict

    artist_keys = list(artists_data.keys())
    random.shuffle(artist_keys)

    songs_added = 0
    max_songs = 20

    try:
        while songs_added < max_songs:
            # Pick a random artist from the list
            artist_key = random.choice(artist_keys)
            artist_info = artists_data[artist_key]
            artist_name = artist_info["name"]

            print(f"[INFO] Processing artist '{artist_name}'")
            track = select_track_for_artist(artist_name, artists_data)

            if track:
                print(f"[INFO] Adding track '{track['name']}' by '{track['artists'][0]['name']}' to playlist {OUTPUT_PLAYLIST_ID}")
                sp.playlist_add_items(OUTPUT_PLAYLIST_ID, [track["id"]])
                songs_added += 1
                print(f"[INFO] Total songs added so far: {songs_added}/{max_songs}")
            else:
                print(f"[INFO] No valid track found for artist '{artist_name}', picking another artist...")
    finally:
        close_global_driver()
        print(f"[INFO] Finished adding {songs_added} tracks to playlist {OUTPUT_PLAYLIST_ID}")
