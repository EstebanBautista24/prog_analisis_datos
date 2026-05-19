import glob
import os
import re
from datetime import datetime, timedelta

import pandas as pd
try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.sensors.filesystem import FileSensor


BRONZE_PATH = "/opt/airflow/datalake_bronze"
SILVER_PATH = "/opt/airflow/datalake_silver"


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract_ts_from_filename(filepath):
    name = os.path.basename(filepath)
    match = re.search(r"(\d{8}_\d{6})", name)
    return match.group(1) if match else "00000000_000000"


@dag(
    dag_id="reddit_api_silver_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["silver", "cleaning", "api", "reddit"],
)
def reddit_api_silver_dag():

    wait_for_bronze_file = FileSensor(
        task_id="wait_for_reddit_bronze_json",
        filepath=f"{BRONZE_PATH}/reddit_api_realmadrid_*.json",
        fs_conn_id="fs_default",
        poke_interval=60,
        timeout=60 * 60 * 6,
        mode="reschedule",
        soft_fail=True,
    )

    @task()
    def detect_new_file():
        pattern = os.path.join(BRONZE_PATH, "reddit_api_realmadrid_*.json")
        files = glob.glob(pattern)

        if not files:
            raise ValueError("No se encontraron archivos de la API en bronze.")

        latest_file = max(files, key=extract_ts_from_filename)
        latest_ts = extract_ts_from_filename(latest_file)

        last_processed_ts = Variable.get(
            "last_processed_reddit_api_ts",
            default_var="00000000_000000",
        )

        if latest_ts <= last_processed_ts:
            raise ValueError(f"No hay archivo nuevo. Último procesado: {last_processed_ts}")

        print(f"Archivo nuevo detectado: {latest_file}")
        return latest_file

    @task()
    def clean_and_normalize(bronze_filepath: str):
        import string
        import json

        import nltk

        nltk.download('stopwords', quiet=True)
        from nltk.corpus import stopwords

        stop_words = set(stopwords.words('english'))
        curly_quotes = "\u2018\u2019\u201c\u201d\u2013\u2014"

        with open(bronze_filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        print(f"Filas a limpiar: {len(df)}")

        def clean_text_for_nlp(text):
            if not isinstance(text, str) or not text.strip():
                return ""
            text = text.lower()
            text = re.sub(r"http\S+|www\S+", "", text)
            text = text.translate(str.maketrans("", "", string.punctuation + curly_quotes))
            text = re.sub(r"\s+", " ", text).strip()
            tokens = [token for token in text.split() if token not in stop_words]
            return " ".join(tokens)

        df["published_at"] = pd.to_datetime(df["created_utc"], unit="s", utc=True, errors="coerce")
        df["body_text"] = df["selftext"].fillna("")
        df["body_text_missing"] = df["body_text"].str.strip().eq("")
        df["body_clean"] = df["body_text"].apply(clean_text_for_nlp)
        df["title_clean"] = df["title"].apply(clean_text_for_nlp)

        df["score"] = pd.to_numeric(df["score"], errors="coerce").astype("Int64")
        df["num_comments"] = pd.to_numeric(df["num_comments"], errors="coerce").astype("Int64")
        df["author"] = df["author"].astype(str)
        df["subreddit"] = df["subreddit"].astype(str)

        df_clean = df[
            [
                "url",
                "title",
                "author",
                "published_at",
                "body_text",
                "body_clean",
                "title_clean",
                "body_text_missing",
                "score",
                "num_comments",
                "subreddit",
                "selftext",
            ]
        ].copy()

        df_clean["published_at"] = df_clean["published_at"].astype(str)
        df_clean["score"] = df_clean["score"].astype(object).where(df_clean["score"].notna(), None)
        df_clean["num_comments"] = df_clean["num_comments"].astype(object).where(df_clean["num_comments"].notna(), None)

        print(f"Limpieza completada: {len(df_clean)} posts")
        print(f"   Posts sin texto: {df_clean['body_text_missing'].sum()}")

        return df_clean.to_json(orient="records")

    @task()
    def save_to_silver_parquet(df_json: str, bronze_filepath: str):
        df = pd.read_json(df_json, orient="records")
        df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
        df["bronze_source"] = os.path.basename(bronze_filepath)

        # Nomenclatura Silver: reddit_api_YYYYMMDD_HHMMSS.parquet
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reddit_api_{timestamp}.parquet"
        full_path = os.path.join(SILVER_PATH, filename)

        if not os.path.exists(SILVER_PATH):
            os.makedirs(SILVER_PATH)

        df.to_parquet(full_path, index=False, engine="pyarrow")

        Variable.set(
            "last_processed_reddit_api_ts",
            extract_ts_from_filename(bronze_filepath),
        )

        print(f"Archivo Parquet guardado en: {full_path}")
        print(f"Registros procesados: {len(df)}")

        return full_path

    bronze_file = detect_new_file()
    df_clean = clean_and_normalize(bronze_file)
    save_to_silver_parquet(df_clean, bronze_file)
    wait_for_bronze_file >> bronze_file


dag_instance = reddit_api_silver_dag()
