import os
from datetime import datetime, timedelta

import pandas as pd
import requests
try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task


BRONZE_PATH = "/opt/airflow/datalake_bronze"
REDDIT_SEARCH_URL = "https://www.reddit.com/search.json"
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
        headers = {
            "User-Agent": "python:sentiment.analysis:v1.0 (by /u/test)",
            "Accept": "application/json",
        }
        params = {
            "q": REDDIT_QUERY,
            "limit": 25,
            "sort": "new",
            "t": "all",
        }

        try:
            response = requests.get(
                REDDIT_SEARCH_URL,
                headers=headers,
                params=params,
                timeout=10,
            )
            response.raise_for_status()
            payload = response.json()

            records = []
            retrieved_at = datetime.utcnow().isoformat()

            for post in payload.get("data", {}).get("children", []):
                info = post.get("data", {})
                permalink = info.get("permalink")
                url = f"https://www.reddit.com{permalink}" if permalink else info.get("url")

                records.append(
                    {
                        "api_query": REDDIT_QUERY,
                        "url": url,
                        "permalink": permalink,
                        "title": info.get("title"),
                        "selftext": info.get("selftext"),
                        "author": info.get("author"),
                        "score": info.get("score"),
                        "num_comments": info.get("num_comments"),
                        "created_utc": info.get("created_utc"),
                        "subreddit": info.get("subreddit"),
                        "retrieved_at": retrieved_at,
                    }
                )

            return records

        except Exception as exc:
            print(f"Error obteniendo datos de Reddit: {exc}")
            return []

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