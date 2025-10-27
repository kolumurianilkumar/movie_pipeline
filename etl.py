import os
import time
import json
import requests
import sqlite3
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()
OMDB_KEY = os.getenv("OMDB_API_KEY")
MOVIES_CSV = "movies.csv"
RATINGS_CSV = "ratings.csv"
DB_FILE = "movies.db"
OMDB_CACHE = "omdb_cache.json"

tqdm.pandas()

def parse_title_and_year(title):
    
    if isinstance(title, str) and title.strip().endswith(")"):
        parts = title.strip().rsplit("(", 1)
        if len(parts) == 2:
            t = parts[0].strip()
            y = parts[1].rstrip(")").strip()
            if y.isdigit():
                return t, int(y)
    return title, None

def load_csvs():
    if not os.path.exists(MOVIES_CSV) or not os.path.exists(RATINGS_CSV):
        raise FileNotFoundError("Make sure movies.csv and ratings.csv are in the project folder.")
    movies_df = pd.read_csv(MOVIES_CSV)
    ratings_df = pd.read_csv(RATINGS_CSV)
    return movies_df, ratings_df

def load_cache():
    if os.path.exists(OMDB_CACHE):
        with open(OMDB_CACHE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(OMDB_CACHE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)

def fetch_omdb(title, year=None, cache=None):
    """Fetch OMDb data by title+year; uses simple cache dict."""
    if cache is None:
        cache = {}
    key = f"{title}::{year}" if year else f"{title}::"
    if key in cache:
        return cache[key]
    if not OMDB_KEY:
        cache[key] = None
        return None
    params = {"apikey": OMDB_KEY, "t": title}
    if year:
        params["y"] = str(year)
    try:
        r = requests.get("http://www.omdbapi.com/", params=params, timeout=10)
        data = r.json()
        if data.get("Response") == "True":
            cache[key] = data
        else:
            cache[key] = None
    except Exception:
        cache[key] = None
    
    time.sleep(0.2)
    return cache[key]

def ensure_tables(conn):
    
    conn.execute("PRAGMA foreign_keys = ON;")

def upsert_movie(conn, movie_id, title, year, imdb_id, director, plot, box_office):
   
    sql = """
    INSERT OR REPLACE INTO movies (movie_id, title, year, imdb_id, director, plot, box_office)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    conn.execute(sql, (movie_id, title, year, imdb_id, director, plot, box_office))

def insert_genre_and_link(conn, movie_id, genre_name):
   
    conn.execute("INSERT OR IGNORE INTO genres(name) VALUES (?)", (genre_name,))
    conn.execute("""
        INSERT OR IGNORE INTO movie_genres(movie_id, genre_id)
        VALUES (?, (SELECT id FROM genres WHERE name = ?))
    """, (movie_id, genre_name))

def load_ratings(conn, ratings_df):
    
    conn.execute("DELETE FROM ratings;")
    insert_sql = "INSERT INTO ratings(user_id, movie_id, rating, timestamp) VALUES (?, ?, ?, ?)"
    vals = ratings_df[["userId", "movieId", "rating", "timestamp"]].values.tolist()
    conn.executemany(insert_sql, vals)

def main():
    print("Starting ETL...")
    movies_df, ratings_df = load_csvs()
    cache = load_cache()

    
    conn = sqlite3.connect(DB_FILE)
    ensure_tables(conn)
    conn.isolation_level = None 
    try:
       
        for idx, row in tqdm(movies_df.iterrows(), total=len(movies_df), desc="Movies"):
            movie_id = int(row.movieId)
            raw_title = row.title
            title, year_from_title = parse_title_and_year(raw_title)
            genres_str = row.genres if isinstance(row.genres, str) else ""
           
            omdb = fetch_omdb(title, year_from_title, cache)
            imdb = omdb.get("imdbID") if omdb else None
            director = omdb.get("Director") if omdb else None
            plot = omdb.get("Plot") if omdb else None
            box_office = omdb.get("BoxOffice") if omdb else None
           
            upsert_movie(conn, movie_id, title, year_from_title, imdb, director, plot, box_office)
           
            if genres_str and genres_str != "(no genres listed)":
                for g in genres_str.split("|"):
                    g = g.strip()
                    if g:
                        insert_genre_and_link(conn, movie_id, g)

        
        save_cache(cache)

        
        print("Loading ratings (this will replace any existing ratings in the DB)...")
        load_ratings(conn, ratings_df)

        print("Committing and closing DB...")
        conn.commit()
    finally:
        conn.close()

    print("ETL finished successfully. Database:", DB_FILE)

if __name__ == "__main__":
    main()
