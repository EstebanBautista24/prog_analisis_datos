from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import os
import glob
import re

BRONZE_PATH = "/opt/airflow/datalake_bronze"
SILVER_PATH = "/opt/airflow/datalake_silver"

# ── Schema objetivo para Silver Web Scraping ─────────────────────────────────
# Campo              | Tipo Python   | Nullable | Estrategia nulos
# url                | str           | No       | Drop si nulo
# title              | str           | Sí       | Mantener como NaN
# author             | str           | Sí       | Sentinel "Unknown"
# published_at       | datetime[utc] | Sí       | Fallback a published_time
# reading_time_min   | float         | Sí       | Imputar con mediana
# body_text          | str           | Sí       | Flag en body_text_missing
# body_text_clean    | str           | Sí       | Limpieza básica (sin lematización)
# title_clean        | str           | Sí       | Limpieza básica (sin lematización)
# body_text_missing  | bool          | No       | Derivado de body_text
# bronze_source      | str           | No       | Nombre del archivo bronze
# source             | str           | No       | "scraping"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def extract_ts_from_filename(filepath: str) -> str:
    name = os.path.basename(filepath)
    match = re.search(r"(\d{8}_\d{6})", name)
    return match.group(1) if match else "00000000_000000"


@dag(
    dag_id="football_silver_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["silver", "cleaning", "scraping"],
)
def football_silver_dag():

    # ── 1. Esperar archivos bronze ────────────────────────────────────────────
    wait_for_bronze_file = FileSensor(
        task_id="wait_for_bronze_json",
        filepath=BRONZE_PATH,
        fs_conn_id="fs_default",
        poke_interval=60,
        timeout=60 * 60 * 6,
        mode="reschedule",
        soft_fail=True,
    )

    # ── 2. Detectar archivos nuevos ───────────────────────────────────────────
    @task()
    def detect_new_files() -> list:

        json_files = glob.glob(
            os.path.join(BRONZE_PATH, "web_scraping_realmadrid_*.json")
        )

        csv_files = glob.glob(
            os.path.join(BRONZE_PATH, "web_scraping_realmadrid_*.csv")
        )

        files = sorted(
            json_files + csv_files,
            key=extract_ts_from_filename
        )

        if not files:
            raise ValueError("No se encontraron archivos en bronze.")

        last_processed_ts = Variable.get(
            "last_processed_bronze_ts",
            default_var="00000000_000000"
        )

        pending_files = [
            f for f in files
            if extract_ts_from_filename(f) > last_processed_ts
        ]

        if not pending_files:
            # No es error: nada nuevo que procesar. Skip mantiene limpio el
            # historial y el KPI de frecuencia de ingesta.
            raise AirflowSkipException(
                f"No hay archivos nuevos. Último procesado: {last_processed_ts}"
            )

        print(f"✅ Archivos pendientes: {len(pending_files)}")

        for f in pending_files:
            print(f"  - {f}")

        return pending_files

    # ── 3. Limpieza y normalización ───────────────────────────────────────────
    @task()
    def clean_and_normalize(bronze_filepath: str) -> dict:

        import ast
        import string
        import html
        import unicodedata
        from io import StringIO

        import pandas as pd
        import matplotlib.pyplot as plt

        CURLY_QUOTES = "\u2018\u2019\u201c\u201d\u2013\u2014"

        NOISE_PHRASES = [
            "Welcome to our",
            "Your email address",
            "Required fields",
            "Comment*",
            "Name*",
            "Email*",
            "Notify me",
            "This site uses",
            "Learn how your comment",
            "Live Comments",
            "Leave a Reply",
            "Cancel reply",
            "Posted by",
        ]

        # ── Helpers ───────────────────────────────────────────────────────────
        def get_meta(meta_val, key: str):
            try:
                d = meta_val if isinstance(meta_val, dict) else ast.literal_eval(meta_val)
                return d.get(key)
            except Exception:
                return None

        def extract_body(p_val):
            try:
                paragraphs = p_val if isinstance(p_val, list) else ast.literal_eval(p_val)
                clean = []
                for p in paragraphs:
                    if not isinstance(p, str):
                        continue
                    # decodificar entidades y quitar etiquetas HTML residuales
                    # del scraping, antes de filtrar por longitud / boilerplate
                    p = html.unescape(p)
                    p = re.sub(r"<[^>]+>", " ", p)
                    p = re.sub(r"\s+", " ", p).strip()
                    if len(p) >= 80 and not any(n in p for n in NOISE_PHRASES):
                        clean.append(p)
                return " ".join(clean) if clean else None
            except Exception:
                return None

        def parse_reading_time(val):
            if val is None:
                return None
            match = re.search(r"(\d+)", str(val))
            return int(match.group(1)) if match else None

        # ── Limpieza básica de texto (sin lematización — Gold layer) ──────────
        def clean_text_basic(text: str) -> str:

            if not isinstance(text, str) or not text.strip():
                return ""

            # normalización de codificación (arregla artefactos Unicode)
            text = unicodedata.normalize("NFKC", text)

            # decodificar entidades HTML y quitar etiquetas residuales
            text = html.unescape(text)
            text = re.sub(r"<[^>]+>", " ", text)

            text = text.lower()

            # URLs
            text = re.sub(r"http\S+|www\S+", "", text)

            # puntuación y comillas tipográficas
            text = text.translate(
                str.maketrans("", "", string.punctuation + CURLY_QUOTES)
            )

            # espacios múltiples
            text = re.sub(r"\s+", " ", text).strip()

            return text

        # ── Carga de datos ────────────────────────────────────────────────────
        print(f"Procesando archivo: {bronze_filepath}")

        if bronze_filepath.endswith(".json"):
            df = pd.read_json(bronze_filepath)
        elif bronze_filepath.endswith(".csv"):
            df = pd.read_csv(bronze_filepath)
        else:
            raise ValueError(f"Formato no soportado: {bronze_filepath}")

        print(f"📥 Filas cargadas: {len(df)}")

        # ── Extracción de campos desde metadatos ──────────────────────────────
        df["title"] = df["all_meta"].apply(lambda x: get_meta(x, "og:title"))
        df["author"] = df["all_meta"].apply(lambda x: get_meta(x, "author"))
        df["pub_time_iso"] = df["all_meta"].apply(
            lambda x: get_meta(x, "article:published_time")
        )
        df["reading_time_raw"] = df["all_meta"].apply(
            lambda x: get_meta(x, "twitter:data2")
        )
        df["body_text"] = df["p_all_texts"].apply(extract_body)

        # ── Deduplicación ─────────────────────────────────────────────────────
        before_dedup = len(df)

        df = df.drop_duplicates(
            subset=["url"], keep="first"
        ).reset_index(drop=True)

        duplicates_removed = before_dedup - len(df)

        print(f"🔁 Duplicados eliminados: {duplicates_removed}")

        # ── Drop filas sin URL ────────────────────────────────────────────────
        df = df.dropna(subset=["url"]).reset_index(drop=True)

        # ── Normalización de tipos ────────────────────────────────────────────
        df["published_at"] = pd.to_datetime(
            df["pub_time_iso"], utc=True, errors="coerce"
        )

        df["reading_time_min"] = pd.array(
            df["reading_time_raw"].apply(parse_reading_time),
            dtype="Int64"
        )

        # ── Fallback fechas ───────────────────────────────────────────────────
        mask_no_date = df["published_at"].isnull()

        # Solo intentar el fallback si la columna de respaldo existe en el bronze;
        # de lo contrario un cambio de esquema rompería la tarea con KeyError.
        if mask_no_date.any() and "published_time" in df.columns:
            df.loc[mask_no_date, "published_at"] = pd.to_datetime(
                df.loc[mask_no_date, "published_time"],
                dayfirst=True,
                errors="coerce",
                utc=True,
            )
            print(f"📅 Fechas recuperadas: {mask_no_date.sum()}")
        elif mask_no_date.any():
            print(f"⚠️  {mask_no_date.sum()} fechas nulas sin columna 'published_time' para fallback")

        # ── Author nulls ──────────────────────────────────────────────────────
        df["author"] = df["author"].fillna("Unknown").astype(str)

        # ── Body text missing flag ────────────────────────────────────────────
        df["body_text_missing"] = (
            df["body_text"].isnull()
            | (df["body_text"].str.strip() == "")
        )

        # ── Reading time nulls → imputar con mediana ──────────────────────────
        median_rt = df["reading_time_min"].median()
        df["reading_time_min"] = df["reading_time_min"].fillna(median_rt)
        df["reading_time_min"] = df["reading_time_min"].astype(float)

        print(f"⏱️  Mediana reading_time: {median_rt}")

        # ── Outliers IQR sobre reading_time_min ───────────────────────────────
        before_outlier_values = df["reading_time_min"].copy()

        Q1 = df["reading_time_min"].quantile(0.25)
        Q3 = df["reading_time_min"].quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        outliers_mask = (
            (df["reading_time_min"] < lower_bound)
            | (df["reading_time_min"] > upper_bound)
        )

        n_outliers = outliers_mask.sum()
        pct_outliers = (n_outliers / len(df)) * 100

        print(f"📊 Outliers detectados: {n_outliers}")
        print(f"📏 Porcentaje afectado: {pct_outliers:.2f}%")

        if n_outliers > 0:
            df.loc[outliers_mask, "reading_time_min"] = df.loc[
                outliers_mask, "reading_time_min"
            ].clip(lower=lower_bound, upper=upper_bound)

        # ── Reportes ──────────────────────────────────────────────────────────
        report_path = f"{SILVER_PATH}/outlier_reports"
        os.makedirs(report_path, exist_ok=True)

        plt.figure(figsize=(12, 6))

        plt.boxplot(
            [before_outlier_values, df["reading_time_min"]],
            labels=["Antes", "Después"]
        )

        plt.title("Tratamiento de Outliers - reading_time_min")
        plt.ylabel("Minutos de lectura")

        plot_file = os.path.join(
            report_path,
            f"outliers_{extract_ts_from_filename(bronze_filepath)}.png"
        )

        plt.savefig(plot_file)
        plt.close()

        metrics_file = os.path.join(
            report_path,
            f"metrics_{extract_ts_from_filename(bronze_filepath)}.txt"
        )

        with open(metrics_file, "w") as f:
            f.write(f"Total registros: {len(df)}\n")
            f.write(f"Duplicados eliminados: {duplicates_removed}\n")
            f.write(f"Outliers detectados: {n_outliers}\n")
            f.write(f"Porcentaje outliers: {pct_outliers:.2f}%\n")
            f.write(f"Q1: {Q1:.2f}\n")
            f.write(f"Q3: {Q3:.2f}\n")
            f.write(f"IQR: {IQR:.2f}\n")
            f.write(f"Límite inferior: {lower_bound:.2f}\n")
            f.write(f"Límite superior: {upper_bound:.2f}\n")

        # ── Limpieza básica de texto ──────────────────────────────────────────
        df["body_text_clean"] = df["body_text"].apply(clean_text_basic)
        df["title_clean"] = df["title"].apply(clean_text_basic)

        # ── Schema enforcement ────────────────────────────────────────────────
        SILVER_SCHEMA = [
            "url",
            "title",
            "author",
            "published_at",
            "reading_time_min",
            "body_text",
            "body_text_clean",
            "title_clean",
            "body_text_missing",
        ]

        df_clean = df[SILVER_SCHEMA].copy()

        # Campo requerido por el Gold DAG para comparación entre fuentes
        df_clean["source"] = "scraping"

        # Serialización para XCom (published_at no es serializable como datetime)
        df_clean["published_at"] = df_clean["published_at"].astype(str)

        print(f"✅ Limpieza completada: {len(df_clean)} artículos")

        return {
            "df_json": df_clean.to_json(orient="records"),
            "bronze_filepath": bronze_filepath,
        }

    # ── 4. Guardar PostgreSQL y Parquet ──────────────────────────────────────
    @task()
    def save_to_db(payload: dict) -> str:

        from io import StringIO
        import pandas as pd

        from airflow.providers.postgres.hooks.postgres import PostgresHook

        df_json = payload["df_json"]
        bronze_filepath = payload["bronze_filepath"]

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
                INSERT INTO articles_scraping (
                    url, title, author, published_at,
                    reading_time_min, body_text, body_text_clean,
                    title_clean, body_text_missing, bronze_source, source
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING
                """,
                (
                    row["url"],
                    row["title"],
                    row["author"],
                    row["published_at"],
                    row["reading_time_min"],
                    row["body_text"],
                    row["body_text_clean"],
                    row["title_clean"],
                    row["body_text_missing"],
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
        parquet_filename = f"web_scraping_realmadrid_{run_ts}.parquet"
        parquet_path = os.path.join(SILVER_PATH, parquet_filename)

        df.to_parquet(parquet_path, index=False, engine="pyarrow")

        print(f"💾 Parquet guardado: {parquet_path}")

        return "OK"

    # ── 5. Actualizar variable de control ─────────────────────────────────────
    @task()
    def update_last_processed(bronze_files: list):

        latest_ts = max(
            extract_ts_from_filename(f) for f in bronze_files
        )

        Variable.set("last_processed_bronze_ts", latest_ts)

        print(f"🔖 Variable actualizada: {latest_ts}")

    # ── Flujo DAG ─────────────────────────────────────────────────────────────
    bronze_files = detect_new_files()

    payloads = clean_and_normalize.expand(bronze_filepath=bronze_files)

    saved = save_to_db.expand(payload=payloads)

    update_var = update_last_processed(bronze_files)

    saved >> update_var

    wait_for_bronze_file >> bronze_files


dag_instance = football_silver_dag()