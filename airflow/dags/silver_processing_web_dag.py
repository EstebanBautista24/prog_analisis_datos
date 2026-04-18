from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import glob
import re

BRONZE_PATH = "/opt/airflow/datalake_bronze"
SILVER_PATH = "/opt/airflow/datalake_silver"


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_ts_from_filename(filepath):
    name = os.path.basename(filepath)
    match = re.search(r'(\d{8}_\d{6})', name)
    return match.group(1) if match else "00000000_000000"


@dag(
    dag_id="football_silver_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["silver", "cleaning"]
)
def football_silver_dag():

    # ── 1. FileSensor ─────────────────────────────────────────
    wait_for_bronze_file = FileSensor(
        task_id="wait_for_bronze_json",
        filepath=f"{BRONZE_PATH}/web_scraping_realmadrid_*.json",
        fs_conn_id="fs_default",
        poke_interval=60,
        timeout=60 * 60 * 6,
        mode="reschedule",
        soft_fail=True,
    )

    # ── 2. Detectar archivo nuevo ─────────────────────────────
    @task()
    def detect_new_file():
        pattern = os.path.join(BRONZE_PATH, "web_scraping_realmadrid_*.json")
        files = glob.glob(pattern)

        if not files:
            raise ValueError("No se encontraron archivos en bronze.")

        latest_file = max(files, key=extract_ts_from_filename)
        latest_ts   = extract_ts_from_filename(latest_file)

        last_processed_ts = Variable.get("last_processed_bronze_ts", default_var="00000000_000000")

        if latest_ts <= last_processed_ts:
            raise ValueError(f"No hay archivo nuevo. Último procesado: {last_processed_ts}")

        print(f"✅ Archivo nuevo detectado: {latest_file}")
        return latest_file

   
    # ── 4. Limpieza y NLP ─────────────────────────────────────
    @task()
    def clean_and_normalize(bronze_filepath: str):
        import pandas as pd
        import ast
        import string
        import nltk
        nltk.download('stopwords', quiet=True)
        from nltk.corpus import stopwords

        STOPWORDS = set(stopwords.words('english'))
        CURLY_QUOTES = '\u2018\u2019\u201c\u201d\u2013\u2014'

        df = pd.read_json(bronze_filepath)
        print(f"Filas a limpiar: {len(df)}")

        # ── Helpers ───────────────────────────────────────────
        def get_meta(meta_str, key):
            try:
                d = ast.literal_eval(meta_str)
                return d.get(key)
            except:
                return None

        NOISE_PHRASES = [
            'Welcome to our', 'Your email address', 'Required fields',
            'Comment*', 'Name*', 'Email*', 'Notify me', 'This site uses',
            'Learn how your comment', 'Live Comments', 'Leave a Reply',
            'Cancel reply', 'Posted by'
        ]

        def extract_body(p_str):
            try:
                paragraphs = ast.literal_eval(p_str)
                clean_paragraphs = [
                    p for p in paragraphs
                    if len(p) >= 80 and not any(n in p for n in NOISE_PHRASES)
                ]
                return ' '.join(clean_paragraphs) if clean_paragraphs else None
            except:
                return None

        def parse_reading_time(val):
            if val is None:
                return None
            match = re.search(r'(\d+)', str(val))
            return int(match.group(1)) if match else None

        def clean_text_for_nlp(text):
            if not isinstance(text, str) or not text.strip():
                return ''
            text = text.lower()
            text = re.sub(r'http\S+|www\S+', '', text)
            text = text.translate(str.maketrans('', '', string.punctuation + CURLY_QUOTES))
            text = re.sub(r'\s+', ' ', text).strip()
            tokens = [t for t in text.split() if t not in STOPWORDS]
            return ' '.join(tokens)

        # ── Extraer columnas desde meta tags ──────────────────
        df['title']            = df['all_meta'].apply(lambda x: get_meta(x, 'og:title'))
        df['author']           = df['all_meta'].apply(lambda x: get_meta(x, 'author'))
        df['pub_time_iso']     = df['all_meta'].apply(lambda x: get_meta(x, 'article:published_time'))
        df['reading_time_raw'] = df['all_meta'].apply(lambda x: get_meta(x, 'twitter:data2'))

        # ── Extraer body text ─────────────────────────────────
        df['body_text'] = df['p_all_texts'].apply(extract_body)

        # ── Tipos de datos ────────────────────────────────────
        df['published_at']     = pd.to_datetime(df['pub_time_iso'], utc=True, errors='coerce')
        df['reading_time_min'] = pd.array(df['reading_time_raw'].apply(parse_reading_time), dtype='Int64')

        # ── Nulos ─────────────────────────────────────────────
        df['body_text_missing'] = df['body_text'].isnull() | (df['body_text'].str.strip() == '')

        mask_no_date = df['published_at'].isnull()
        if mask_no_date.any():
            df.loc[mask_no_date, 'published_at'] = pd.to_datetime(
                df.loc[mask_no_date, 'published_time'], dayfirst=True, errors='coerce', utc=True
            )

        median_rt = df['reading_time_min'].median()
        df['reading_time_min'] = df['reading_time_min'].fillna(median_rt)

        # ── NLP cleaning ──────────────────────────────────────
        df['body_clean']  = df['body_text'].apply(clean_text_for_nlp)
        df['title_clean'] = df['title'].apply(clean_text_for_nlp)

        # ── Columnas finales ──────────────────────────────────
        df_clean = df[[
            'url', 'title', 'author', 'published_at',
            'reading_time_min', 'body_text', 'body_clean',
            'title_clean', 'body_text_missing'
        ]].copy()

        # Convertir a string para que sea serializable entre tasks
        df_clean['published_at'] = df_clean['published_at'].astype(str)
        df_clean['author']       = df_clean['author'].astype(str)

        print(f"✅ Limpieza completada: {len(df_clean)} artículos")
        print(f"   Artículos sin cuerpo: {df_clean['body_text_missing'].sum()}")

        return df_clean.to_json(orient='records')

    # ── 5. Guardar Parquet y actualizar registro ───────────────
    @task()
    def save_to_db(df_json: str, bronze_filepath: str):
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        df = pd.read_json(df_json, orient='records')

        # Reconstruir tipos
        df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce')

        # Metadatos
        df['bronze_source'] = os.path.basename(bronze_filepath)

        # Conexión
        hook = PostgresHook(postgres_conn_id="postgres_realmadrid")
        conn = hook.get_conn()
        cursor = conn.cursor()

        inserted = 0

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO articles_scraping (
                    url, title, author, published_at,
                    reading_time_min, body_text, body_clean,
                    title_clean, body_text_missing, bronze_source
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING
            """, (
                row['url'],
                row['title'],
                row['author'],
                row['published_at'],
                row['reading_time_min'],
                row['body_text'],
                row['body_clean'],
                row['title_clean'],
                row['body_text_missing'],
                row['bronze_source']
            ))

            inserted += 1

        conn.commit()

        print(f"✅ Filas procesadas: {len(df)}")
        print(f"✅ Insertadas (sin duplicados): {inserted}")

        return "OK"
    # ── Flujo ─────────────────────────────────────────────────
    bronze_file = detect_new_file()
    df_clean    = clean_and_normalize(bronze_file)
    save_to_db(df_clean, bronze_file)
    wait_for_bronze_file >> bronze_file
    

dag_instance = football_silver_dag()