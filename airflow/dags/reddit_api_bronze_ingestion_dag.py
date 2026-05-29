import os
from datetime import datetime, timedelta
from urllib import response

import pandas as pd
import requests
try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task


BRONZE_PATH = "/opt/airflow/datalake_bronze"
REDDIT_SEARCH_URL = "https://www.reddit.com/r/realmadrid/search.json"
REDDIT_QUERY = "Real Madrid -Castilla"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="reddit_api_bronze_ingestion",
    default_args=default_args,
    description="Extracts Reddit API data to Bronze layer as CSV",
    schedule="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["bronze", "api", "reddit"],
)
def reddit_api_bronze_dag():

    @task()
    def fetch_reddit_posts():
        from langdetect import detect, LangDetectException

        headers = {
            "User-Agent": "python:sentiment.analysis:v1.0 (by /u/test)",
            "Accept": "application/json",
        }

        retrieved_at = datetime.utcnow().isoformat()
        records      = []
        skipped_lang = 0
        urls_vistas  = set()

        # ── Intento 1: search.json ────────────────────────────────────────────
        params = {
            "q":           REDDIT_QUERY,
            "limit":       100,
            "sort":        "new",
            "restrict_sr": True,
            "t":           "week",
        }

        try:
            response = requests.get(
                REDDIT_SEARCH_URL,
                headers=headers,
                params=params,
                timeout=10,
            )
            print(f"Status code: {response.status_code}")
            print(f"Response raw: {response.text[:500]}")
            response.raise_for_status()
            payload = response.json()
            children = payload.get("data", {}).get("children", [])
            print(f"📡 search.json retornó {len(children)} posts")
            
        except Exception as exc:
            print(f"⚠️  search.json falló: {exc}")
            children = []

        # ── Intento 2: new.json como fallback si search trajo poco ───────────
        if len(children) < 5:
            print("📡 Pocos resultados en search.json, usando new.json como fallback...")
            try:
                response = requests.get(
                    "https://www.reddit.com/r/realmadrid/new.json",
                    headers=headers,
                    params={"limit": 100},
                    timeout=10,
                )
                print(f"Status code: {response.status_code}")
                print(f"Response raw: {response.text[:500]}")
                response.raise_for_status()
                payload  = response.json()
                children = payload.get("data", {}).get("children", [])
                print(f"📡 new.json retornó {len(children)} posts")
            except Exception as exc:
                print(f"⚠️  new.json también falló: {exc}")

        # ── Procesar posts ────────────────────────────────────────────────────
        for post in children:
            info      = post.get("data", {})
            title     = info.get("title", "") or ""
            selftext  = info.get("selftext", "") or ""
            permalink = info.get("permalink")
            url       = f"https://www.reddit.com{permalink}" if permalink else info.get("url")

            if url in urls_vistas:
                continue
            urls_vistas.add(url)

            text_to_check = f"{title} {selftext}".strip()
            try:
                lang = detect(text_to_check) if text_to_check else "unknown"
            except LangDetectException:
                lang = "unknown"

            if lang != "en":
                skipped_lang += 1
                print(f"⏭️  Ignorado (idioma={lang}): {title[:60]}")
                continue

            records.append({
                "api_query":    REDDIT_QUERY,
                "url":          url,
                "permalink":    permalink,
                "title":        title,
                "selftext":     selftext,
                "author":       info.get("author"),
                "score":        info.get("score"),
                "num_comments": info.get("num_comments"),
                "created_utc":  info.get("created_utc"),
                "subreddit":    info.get("subreddit"),
                "retrieved_at": retrieved_at,
                "lang":         lang,
            })

        print(f"✅ Posts en inglés: {len(records)}")
        print(f"⏭️  Posts ignorados por idioma: {skipped_lang}")
        return records


    
    @task()
    def save_to_bronze_json(records):
        if not records:
            print("No hay datos para guardar desde la API.")
            return None

        import json
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reddit_api_realmadrid_{timestamp}.json"
        full_path = os.path.join(BRONZE_PATH, filename)

        if not os.path.exists(BRONZE_PATH):
            os.makedirs(BRONZE_PATH)

        with open(full_path, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        print(f"Archivo guardado exitosamente en: {full_path}")
        return full_path

    posts = fetch_reddit_posts()
    save_to_bronze_json(posts)


dag_instance = reddit_api_bronze_dag()