CREATE TABLE IF NOT EXISTS articles_scraping (
    id              SERIAL PRIMARY KEY,
    url             TEXT UNIQUE NOT NULL,
    title           TEXT,
    author          TEXT,
    published_at    TIMESTAMPTZ,
    reading_time_min INTEGER,
    body_text       TEXT,
    body_clean      TEXT,
    title_clean     TEXT,
    body_text_missing BOOLEAN,
    score           INTEGER,
    num_comments    INTEGER,
    subreddit       TEXT,
    selftext        TEXT,
    bronze_source   TEXT,
    silver_ingested_at TIMESTAMPTZ DEFAULT NOW()
);
