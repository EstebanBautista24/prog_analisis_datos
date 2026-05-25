-- Reddit
CREATE TABLE IF NOT EXISTS reddit_posts (
    url                TEXT PRIMARY KEY,
    title              TEXT,
    author             TEXT,
    published_at       TIMESTAMPTZ,
    body_text          TEXT,
    body_text_clean    TEXT,
    title_clean        TEXT,
    body_text_missing  BOOLEAN,
    score              FLOAT,
    num_comments       FLOAT,
    subreddit          TEXT,
    bronze_source      TEXT,
    source             TEXT
);

-- Scraping
CREATE TABLE IF NOT EXISTS articles_scraping (
    url                TEXT PRIMARY KEY,
    title              TEXT,
    author             TEXT,
    published_at       TIMESTAMPTZ,
    reading_time_min   FLOAT,
    body_text          TEXT,
    body_text_clean    TEXT,
    title_clean        TEXT,
    body_text_missing  BOOLEAN,
    bronze_source      TEXT,
    source             TEXT
);