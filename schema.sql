PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS movies (
  movie_id INTEGER PRIMARY KEY, 
  title TEXT NOT NULL,
  year INTEGER,
  imdb_id TEXT,
  director TEXT,
  plot TEXT,
  box_office TEXT
);

CREATE TABLE IF NOT EXISTS genres (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS movie_genres (
  movie_id INTEGER NOT NULL,
  genre_id INTEGER NOT NULL,
  PRIMARY KEY(movie_id, genre_id),
  FOREIGN KEY(movie_id) REFERENCES movies(movie_id),
  FOREIGN KEY(genre_id) REFERENCES genres(id)
);

CREATE TABLE IF NOT EXISTS ratings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER,
  movie_id INTEGER,
  rating REAL,
  timestamp INTEGER,
  FOREIGN KEY(movie_id) REFERENCES movies(movie_id)
);
