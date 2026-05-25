from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from datetime import datetime, timedelta

import os
import glob
import re

BRONZE_PATH = "/opt/airflow/datalake_bronze"
SILVER_PATH = "/opt/airflow/datalake_silver"

# ── Schema objetivo para Silver Reddit ───────────────────────────────────────
# Campo              | Tipo Python   | Nullable | Estrategia nulos
# url                | str           | No       | Drop si nulo
# title              | str           | Sí       | Mantener como NaN
# author             | str           | Sí       | Sentinel "Unknown"
# published_at       | datetime[utc] | Sí       | Derivado de created_utc
# body_text          | str           | Sí       | Flag en body_text_missing
# body_text_missing  | bool          | No       | Derivado de body_text
# score              | float         | Sí       | Capping IQR
# num_comments       | float         | Sí       | Capping IQR
# subreddit          | str           | Sí       | Mantener como NaN
# bronze_source      | str           | No       | Archivo origen
# source             | str           | No       | "reddit"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract_ts_from_filename(filepath: str) -> str:
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

    # ── 1. Esperar archivos bronze ────────────────────────────────────────────
    wait_for_bronze_file = FileSensor(
        task_id="wait_for_reddit_bronze_json",
        filepath=BRONZE_PATH,
        fs_conn_id="fs_default",
        poke_interval=60,
        timeout=60 * 60 * 6,
        mode="reschedule",
        soft_fail=True,
    )

    # ── 2. Detectar archivo nuevo ─────────────────────────────────────────────
    @task()
    def detect_new_file() -> str:

        pattern = os.path.join(
            BRONZE_PATH,
            "reddit_api_realmadrid_*.json"
        )

        files = glob.glob(pattern)

        if not files:
            raise ValueError(
                "No se encontraron archivos Reddit en bronze."
            )

        latest_file = max(files, key=extract_ts_from_filename)
        latest_ts = extract_ts_from_filename(latest_file)

        last_processed_ts = Variable.get(
            "last_processed_reddit_api_ts",
            default_var="00000000_000000",
        )

        if latest_ts <= last_processed_ts:
            raise ValueError(
                f"No hay archivo nuevo. "
                f"Último procesado: {last_processed_ts}"
            )

        print(f"✅ Archivo nuevo detectado: {latest_file}")

        return latest_file

    # ── 3. Limpieza y normalización ───────────────────────────────────────────
    @task()
    def clean_and_normalize(bronze_filepath: str) -> str:

        import json
        import string

        import pandas as pd
        import matplotlib.pyplot as plt

        CURLY_QUOTES = "\u2018\u2019\u201c\u201d\u2013\u2014"

        # ── Limpieza básica de texto (sin lematización — Gold layer) ──────────
        def clean_text_basic(text: str) -> str:

            if not isinstance(text, str) or not text.strip():
                return ""

            text = text.lower()

            # URLs
            text = re.sub(r"http\S+|www\S+", "", text)

            # hashtags
            text = re.sub(r"#\w+", "", text)

            # menciones u/usuario
            text = re.sub(r"u/\w+", "", text)

            # subreddits r/nombre
            text = re.sub(r"r/\w+", "", text)

            # puntuación y comillas tipográficas
            text = text.translate(
                str.maketrans("", "", string.punctuation + CURLY_QUOTES)
            )

            # espacios múltiples
            text = re.sub(r"\s+", " ", text).strip()

            return text

        # ── Carga de datos ────────────────────────────────────────────────────
        print(f"Procesando archivo: {bronze_filepath}")

        with open(bronze_filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        df = pd.DataFrame(data)

        print(f"📥 Filas cargadas: {len(df)}")

        # ── Normalización de tipos ────────────────────────────────────────────
        df["published_at"] = pd.to_datetime(
            df["created_utc"],
            unit="s",
            utc=True,
            errors="coerce"
        )

        df["body_text"] = df["selftext"].fillna("")

        df["score"] = pd.to_numeric(
            df["score"], errors="coerce"
        ).astype(float)

        df["num_comments"] = pd.to_numeric(
            df["num_comments"], errors="coerce"
        ).astype(float)

        df["author"] = df["author"].fillna("Unknown").astype(str)

        df["subreddit"] = df["subreddit"].astype(str)

        # ── Deduplicación ─────────────────────────────────────────────────────
        before_dedup = len(df)

        df = df.drop_duplicates(
            subset=["url"], keep="first"
        ).reset_index(drop=True)

        duplicates_removed = before_dedup - len(df)

        print(f"🔁 Duplicados eliminados: {duplicates_removed}")

        # ── Drop filas sin URL ────────────────────────────────────────────────
        df = df.dropna(subset=["url"]).reset_index(drop=True)

        # ── Flag body missing ─────────────────────────────────────────────────
        df["body_text_missing"] = df["body_text"].str.strip().eq("")

        # ── Limpieza básica de texto ──────────────────────────────────────────
        df["body_text_clean"] = df["body_text"].apply(clean_text_basic)
        df["title_clean"] = df["title"].apply(clean_text_basic)

        # ── Reportes ──────────────────────────────────────────────────────────
        report_path = f"{SILVER_PATH}/outlier_reports"
        os.makedirs(report_path, exist_ok=True)

        # ── Outliers IQR ──────────────────────────────────────────────────────
        for col in ["score", "num_comments"]:

            before_values = df[col].copy()

            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1

            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            outliers_mask = (
                (df[col] < lower_bound) | (df[col] > upper_bound)
            )

            n_outliers = outliers_mask.sum()
            pct_outliers = (n_outliers / len(df)) * 100 if len(df) > 0 else 0

            print(f"📊 Outliers en {col}: {n_outliers} ({pct_outliers:.2f}%)")
            print(f"📏 Límites IQR {col}: [{lower_bound:.2f}, {upper_bound:.2f}]")

            if n_outliers > 0:
                df.loc[outliers_mask, col] = df.loc[
                    outliers_mask, col
                ].clip(lower=lower_bound, upper=upper_bound)

            # ── Boxplot ───────────────────────────────────────────────────────
            plt.figure(figsize=(12, 6))

            plt.boxplot(
                [before_values.dropna(), df[col].dropna()],
                labels=["Antes", "Después"]
            )

            plt.title(f"Tratamiento de Outliers - {col}")
            plt.ylabel(col)

            plot_file = os.path.join(
                report_path,
                f"outliers_{col}_{extract_ts_from_filename(bronze_filepath)}.png"
            )

            plt.savefig(plot_file)
            plt.close()

            print(f"📈 Gráfica guardada: {plot_file}")

        # ── Guardar métricas ──────────────────────────────────────────────────
        metrics_file = os.path.join(
            report_path,
            f"metrics_reddit_{extract_ts_from_filename(bronze_filepath)}.txt"
        )

        with open(metrics_file, "w") as f:
            f.write(f"Total registros: {len(df)}\n")
            f.write(f"Duplicados eliminados: {duplicates_removed}\n")
            f.write(f"Posts sin texto: {df['body_text_missing'].sum()}\n")
            f.write(f"Score mediana: {df['score'].median():.2f}\n")
            f.write(f"num_comments mediana: {df['num_comments'].median():.2f}\n")

        # ── Schema enforcement ────────────────────────────────────────────────
        SILVER_SCHEMA = [
            "url",
            "title",
            "author",
            "published_at",
            "body_text",
            "body_text_clean",
            "title_clean",
            "body_text_missing",
            "score",
            "num_comments",
            "subreddit",
        ]

        df_clean = df[SILVER_SCHEMA].copy()

        # Campo requerido por el Gold DAG para comparación entre fuentes
        df_clean["source"] = "reddit"

        # Serialización para XCom (published_at no es serializable como datetime)
        df_clean["published_at"] = df_clean["published_at"].astype(str)

        print(f"✅ Limpieza completada: {len(df_clean)} posts")
        print(f"📭 Posts sin texto: {df_clean['body_text_missing'].sum()}")

        return df_clean.to_json(orient="records")

    # ── 4. Guardar PostgreSQL y Parquet ──────────────────────────────────────
    @task()
    def save_to_db_and_parquet(df_json: str, bronze_filepath: str) -> str:

        from io import StringIO

        import pandas as pd

        from airflow.providers.postgres.hooks.postgres import PostgresHook

        df = pd.read_json(StringIO(df_json), orient="records")

        df["published_at"] = pd.to_datetime(
            df["published_at"], errors="coerce"
        )

        df["bronze_source"] = os.path.basename(bronze_filepath)

        # ── PostgreSQL ────────────────────────────────────────────────────────
        hook = PostgresHook(postgres_conn_id="postgres_realmadrid")
        conn = hook.get_conn()
        cursor = conn.cursor()

        inserted = 0
        skipped = 0

        for _, row in df.iterrows():

            cursor.execute(
                """
                INSERT INTO reddit_posts (
                    url, title, author, published_at,
                    body_text, body_text_clean, title_clean,
                    body_text_missing, score, num_comments,
                    subreddit, bronze_source, source
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (url) DO NOTHING
                """,
                (
                    row["url"],
                    row["title"],
                    row["author"],
                    row["published_at"],
                    row["body_text"],
                    row["body_text_clean"],
                    row["title_clean"],
                    row["body_text_missing"],
                    row["score"],
                    row["num_comments"],
                    row["subreddit"],
                    row["bronze_source"],
                    row["source"],
                )
            )

            if cursor.rowcount == 1:
                inserted += 1
            else:
                skipped += 1

        conn.commit()
        cursor.close()
        conn.close()

        print(f"📋 Filas procesadas: {len(df)}")
        print(f"✅ Insertadas PostgreSQL: {inserted}")
        print(f"⏭️  Duplicadas ignoradas: {skipped}")

        # ── Guardar Parquet ───────────────────────────────────────────────────
        os.makedirs(SILVER_PATH, exist_ok=True)

        run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        parquet_filename = f"reddit_api_realmadrid_{run_ts}.parquet"
        parquet_path = os.path.join(SILVER_PATH, parquet_filename)

        df.to_parquet(parquet_path, index=False, engine="pyarrow")

        print(f"💾 Parquet guardado: {parquet_path}")

        # ── Actualizar Variable ───────────────────────────────────────────────
        Variable.delete("last_processed_reddit_api_ts")
        Variable.set(
            "last_processed_reddit_api_ts",
            extract_ts_from_filename(bronze_filepath),
            )

        print("🔖 Variable actualizada → last_processed_reddit_api_ts")

        return parquet_path

    # ── Flujo DAG ─────────────────────────────────────────────────────────────
    bronze_file = detect_new_file()
    df_clean = clean_and_normalize(bronze_file)
    save_to_db_and_parquet(df_clean, bronze_file)

    wait_for_bronze_file >> bronze_file


dag_instance = reddit_api_silver_dag()