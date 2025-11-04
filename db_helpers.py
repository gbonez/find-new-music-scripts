import os
import psycopg2
import psycopg2.extras
import random

DB_CONN = None

def get_db_conn():
    global DB_CONN
    if DB_CONN:
        return DB_CONN
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        # DB operations are effectively disabled when no DATABASE_URL
        return None
    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        DB_CONN = conn
        return DB_CONN
    except Exception as e:
        print(f"[DB] connection failed: {e}")
        return None

def db_query(sql, params=None, fetch=False):
    conn = get_db_conn()
    if not conn:
        return None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql, params or ())
            if fetch:
                return cur.fetchall()
    except Exception as e:
        print(f"[DB] query failed: {e} | sql: {sql} | params: {params}")
        return None

def is_artist_blacklisted(artist_id):
    rows = db_query("SELECT 1 FROM blacklisted_artists_playlists WHERE artist_playlist_id = %s LIMIT 1", (artist_id,), fetch=True)
    return bool(rows)

def add_blacklisted_artist(artist_id, name=None):
    conn = get_db_conn()
    if not conn:
        return
    try:
        db_query("INSERT INTO blacklisted_artists_playlists (artist_playlist_id, name) VALUES (%s, %s)", (artist_id, name))
    except Exception:
        pass

def is_playlist_blacklisted(playlist_id):
    rows = db_query("SELECT blacklisted FROM user_playlists WHERE playlist_id = %s LIMIT 1", (playlist_id,), fetch=True)
    if not rows:
        return False
    return bool(rows[0].get("blacklisted"))

def add_or_update_user_playlist(playlist_id, name=None, blacklisted=False):
    conn = get_db_conn()
    if not conn:
        return
    try:
        # upsert-like behavior
        db_query("""
            INSERT INTO user_playlists (playlist_id, name, blacklisted)
            VALUES (%s, %s, %s)
            """, (playlist_id, name, blacklisted))
    except Exception:
        # attempt update if insert fails (no unique constraint assumed)
        try:
            db_query("UPDATE user_playlists SET name = %s, blacklisted = %s WHERE playlist_id = %s", (name, blacklisted, playlist_id))
        except Exception:
            pass

def mark_playlist_blacklisted(playlist_id):
    db_query("UPDATE user_playlists SET blacklisted = TRUE WHERE playlist_id = %s", (playlist_id,))

def is_track_blacklisted(song_id):
    rows = db_query("SELECT 1 FROM blacklisted_songs WHERE song_id = %s LIMIT 1", (song_id,), fetch=True)
    return bool(rows)

def blacklisted_artist_count(artist_id):
    rows = db_query("SELECT COUNT(*) AS c FROM blacklisted_songs WHERE artist_id = %s", (artist_id,), fetch=True)
    if not rows:
        return 0
    return int(rows[0].get("c", 0) or 0)

def add_blacklisted_song(song_id, song_name=None, artist_id=None, artist_name=None):
    conn = get_db_conn()
    if not conn:
        return
    try:
        db_query("INSERT INTO blacklisted_songs (song_id, song_name, artist_id, artist_name) VALUES (%s,%s,%s,%s)",
                 (song_id, song_name, artist_id, artist_name))
    except Exception:
        pass

def get_random_whitelisted_profile():
    rows = db_query("SELECT profile_id FROM whitelisted_user_profiles", fetch=True)
    if not rows:
        return None
    return random.choice(rows)[0]