from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import glob
import re

BRONZE_PATH = "/opt/airflow/datalake_bronze"
SILVER_PATH = "/opt/airflow/datalake_silver"

# ── Schema objetivo para Silver ───────────────────────────────────────────────
# Campo              | Tipo Python   | Nullable | Estrategia nulos
# url                | str           | No       | Drop si nulo
# title              | str           | Sí       | Mantener como NaN
# author             | str           | Sí       | Sentinel "Unknown"
# published_at       | datetime[utc] | Sí       | Fallback a published_time
# reading_time_min   | Int64         | Sí       | Imputar con mediana
# body_text          | str           | Sí       | Flag en body_text_missing
# body_clean         | str           | Sí       | Vacío si body_text nulo
# title_clean        | str           | Sí       | Vacío si title nulo
# body_text_missing  | bool          | No       | Derivado de body_text
# bronze_source      | str           | No       | Nombre del archivo bronze

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
    dag_id="football_silver_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["silver", "cleaning", "nlp"],
)
def football_silver_dag():

    # ── 1. FileSensor ─────────────────────────────────────────────────────────
    # poke_interval=60s  → revisa cada minuto si llegó un archivo nuevo
    # timeout=6h         → si en 6 horas no llega nada, el sensor falla suave
    # mode="reschedule"  → libera el worker mientras espera (no bloquea slot)
    # soft_fail=True     → si el sensor vence, marca la tarea como skipped
    #                       en lugar de fallar el DAG completo
    wait_for_bronze_file = FileSensor(
        task_id="wait_for_bronze_json",
        filepath=f"{BRONZE_PATH}/web_scraping_realmadrid_*.json",
        fs_conn_id="fs_default",
        poke_interval=60,
        timeout=60 * 60 * 6,
        mode="reschedule",
        soft_fail=True,
    )

    # ── 2. Detectar archivo nuevo ─────────────────────────────────────────────
    @task()
    def detect_new_files():
        json_files = glob.glob(
            os.path.join(BRONZE_PATH, "web_scraping_realmadrid_*.json")
        )

        csv_files = glob.glob(
            os.path.join(BRONZE_PATH, "web_scraping_realmadrid_*.csv")
        )

        files = json_files + csv_files

        if not files:
            raise ValueError("No se encontraron archivos en bronze.")

        files = sorted(files, key=extract_ts_from_filename)

        last_processed_ts = Variable.get(
            "last_processed_bronze_ts",
            default_var="00000000_000000"
        )

        pending_files = [
            f for f in files
            if extract_ts_from_filename(f) > last_processed_ts
        ]

        if not pending_files:
            raise ValueError(
                f"No hay archivos nuevos. Último procesado: {last_processed_ts}"
            )

        print(f"✅ Archivos pendientes: {len(pending_files)}")

        for f in pending_files:
            print(f" - {f}")

        return pending_files

    # ── 3. Limpieza, NLP con spaCy y detección de outliers ───────────────────
    @task()
    def clean_and_normalize(bronze_filepath: str) -> str:
        import matplotlib.pyplot as plt
        """
        Aplica el pipeline de preprocesamiento Silver completo:
          - Extracción de campos desde meta tags
          - Manejo de nulos con estrategias por campo
          - Deduplicación por URL
          - Normalización de tipos de datos
          - Limpieza de texto con spaCy (tokenización, stop words, lematización)
          - Detección y manejo de outliers en reading_time_min (IQR)
          - Enforcement del schema Silver
        """
        import ast
        import string
        import pandas as pd
        import spacy

        # ── Cargar modelo spaCy ───────────────────────────────────────────────
        # spaCy ofrece ventajas sobre NLTK para este pipeline:
        #  • Lematización → "winning" y "wins" se unifican a "win"
        #  • Stop words integradas y más completas que la lista NLTK
        #  • Pipeline eficiente vectorizado en C (más rápido en datasets grandes)
        #  • Reconocimiento de entidades (útil para Gold layer)
        # Requiere: python -m spacy download en_core_web_sm
        try:
            nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
        except OSError:
            raise RuntimeError(
                "Modelo spaCy no encontrado. "
                "Ejecuta: python -m spacy download en_core_web_sm"
            )

        CURLY_QUOTES = "\u2018\u2019\u201c\u201d\u2013\u2014"
        NOISE_PHRASES = [
            "Welcome to our", "Your email address", "Required fields",
            "Comment*", "Name*", "Email*", "Notify me", "This site uses",
            "Learn how your comment", "Live Comments", "Leave a Reply",
            "Cancel reply", "Posted by",
        ]

        # ── Helpers ───────────────────────────────────────────────────────────
        def get_meta(meta_str: str, key: str):
            try:
                d = ast.literal_eval(meta_str)
                return d.get(key)
            except Exception:
                return None

        def extract_body(p_val):
            try:
                # Si ya es lista, usarla directamente
                if isinstance(p_val, list):
                    paragraphs = p_val
                else:
                    paragraphs = ast.literal_eval(p_val)
                
                clean = [
                    p for p in paragraphs
                    if len(p) >= 80 and not any(n in p for n in NOISE_PHRASES)
                ]
                return " ".join(clean) if clean else None
            except Exception:
                return None

        def parse_reading_time(val):
            if val is None:
                return None
            match = re.search(r"(\d+)", str(val))
            return int(match.group(1)) if match else None

        def clean_text_spacy(text: str) -> str:
            """
            Pipeline NLP con spaCy:
              1. Lowercase + eliminar URLs y puntuación especial
              2. Tokenización automática de spaCy
              3. Filtrar stop words (is_stop), puntuación (is_punct)
                 y tokens muy cortos (< 2 chars)
              4. Lematizar cada token (text → lemma_)
            Resultado: cadena de lemas en minúsculas, sin ruido.
            """
            if not isinstance(text, str) or not text.strip():
                return ""

            # Pre-limpieza: URLs y caracteres especiales
            text = text.lower()
            text = re.sub(r"http\S+|www\S+", "", text)
            text = text.translate(
                str.maketrans("", "", string.punctuation + CURLY_QUOTES)
            )
            text = re.sub(r"\s+", " ", text).strip()

            # spaCy procesa el texto en un único pase vectorizado
            doc = nlp(text)
            tokens = [
                token.lemma_
                for token in doc
                if not token.is_stop          # eliminar stop words
                and not token.is_punct        # eliminar puntuación residual
                and not token.is_space        # eliminar espacios tokenizados
                and len(token.lemma_) >= 2    # descartar tokens de 1 char
            ]
            return " ".join(tokens)
        print(f"Procesando archivo: {bronze_filepath}")
        print(f"Tamaño: {os.path.getsize(bronze_filepath)} bytes")
        # ── Carga de datos ────────────────────────────────────────────────────
        if bronze_filepath.endswith(".json"):
            df = pd.read_json(bronze_filepath)

        elif bronze_filepath.endswith(".csv"):
            df = pd.read_csv(bronze_filepath)

        else:
            raise ValueError(
                f"Formato no soportado: {bronze_filepath}"
            )
        print(f"📥 Filas cargadas desde bronze: {len(df)}")

        # ── Extracción de campos desde meta tags ──────────────────────────────
        df["title"]            = df["all_meta"].apply(lambda x: get_meta(x, "og:title"))
        df["author"]           = df["all_meta"].apply(lambda x: get_meta(x, "author"))
        df["pub_time_iso"]     = df["all_meta"].apply(lambda x: get_meta(x, "article:published_time"))
        df["reading_time_raw"] = df["all_meta"].apply(lambda x: get_meta(x, "twitter:data2"))
        print("Valores originales de reading_time_raw:")
        print(df["reading_time_raw"].dropna().unique()[:20])
        df["body_text"]        = df["p_all_texts"].apply(extract_body)

        # ── Deduplicación por URL ─────────────────────────────────────────────
        # Criterio: URL es identificador único de artículo.
        # Se conserva la primera ocurrencia (orden de scraping).
        before_dedup = len(df)
        df = df.drop_duplicates(subset=["url"], keep="first").reset_index(drop=True)
        print(f"🔁 Duplicados eliminados: {before_dedup - len(df)}")

        # ── Drop filas sin URL (clave primaria) ───────────────────────────────
        df = df.dropna(subset=["url"]).reset_index(drop=True)

        # ── Normalización de tipos ────────────────────────────────────────────
        df["published_at"]     = pd.to_datetime(df["pub_time_iso"], utc=True, errors="coerce")
        df["reading_time_min"] = pd.array(
            df["reading_time_raw"].apply(parse_reading_time), dtype="Int64"
        )

        # ── Manejo de nulos: published_at ─────────────────────────────────────
        # Estrategia: fallback a la columna published_time (formato dayfirst)
        mask_no_date = df["published_at"].isnull()
        if mask_no_date.any():
            df.loc[mask_no_date, "published_at"] = pd.to_datetime(
                df.loc[mask_no_date, "published_time"],
                dayfirst=True,
                errors="coerce",
                utc=True,
            )
            print(f"📅 Fechas recuperadas desde fallback: {mask_no_date.sum()}")

        # ── Manejo de nulos: author ───────────────────────────────────────────
        # Estrategia: sentinel "Unknown" (no tiene sentido imputar un autor)
        df["author"] = df["author"].fillna("Unknown").astype(str)

        # ── Manejo de nulos: body_text ────────────────────────────────────────
        df["body_text_missing"] = df["body_text"].isnull() | (
            df["body_text"].str.strip() == ""
        )

        # ── Manejo de nulos: reading_time_min ─────────────────────────────────
        # Estrategia: imputar con mediana (robusto ante distribución sesgada)
        median_rt = df["reading_time_min"].median()
        df["reading_time_min"] = df["reading_time_min"].fillna(median_rt)
        print(f"⏱  Mediana reading_time usada para imputación: {median_rt} min")
        df["reading_time_min"] = df["reading_time_min"].astype(float)

        # ── Detección y manejo de outliers: reading_time_min (IQR) ───────────

        before_outlier_values = df["reading_time_min"].copy()

        Q1 = df["reading_time_min"].quantile(0.25)
        Q3 = df["reading_time_min"].quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        outliers_mask = (
            (df["reading_time_min"] < lower_bound) |
            (df["reading_time_min"] > upper_bound)
        )

        n_outliers = outliers_mask.sum()
        pct_outliers = (n_outliers / len(df)) * 100

        print(f"📊 Outliers detectados: {n_outliers}")
        print(f"📈 Porcentaje afectado: {pct_outliers:.2f}%")
        print(f"📏 Límites IQR: [{lower_bound:.2f}, {upper_bound:.2f}]")

        if n_outliers > 0:
            df.loc[outliers_mask, "reading_time_min"] = df.loc[
                outliers_mask,
                "reading_time_min"
            ].clip(
                lower=lower_bound,
                upper=upper_bound
            )
        report_path = f"{SILVER_PATH}/outlier_reports"
        os.makedirs(report_path, exist_ok=True)

        fig = plt.figure(figsize=(12, 6))

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

        print(f"🖼️ Gráfica guardada en: {plot_file}")
        # ── NLP con spaCy ─────────────────────────────────────────────────────
        df["body_clean"]  = df["body_text"].apply(clean_text_spacy)
        df["title_clean"] = df["title"].apply(clean_text_spacy)
        metrics_file = os.path.join(
            report_path,
            f"metrics_{extract_ts_from_filename(bronze_filepath)}.txt"
        )

        with open(metrics_file, "w") as f:
            f.write(f"Total registros: {len(df)}\n")
            f.write(f"Outliers detectados: {n_outliers}\n")
            f.write(f"Porcentaje: {pct_outliers:.2f}%\n")
            f.write(f"Q1: {Q1:.2f}\n")
            f.write(f"Q3: {Q3:.2f}\n")
            f.write(f"IQR: {IQR:.2f}\n")
            f.write(f"Límite inferior: {lower_bound:.2f}\n")
            f.write(f"Límite superior: {upper_bound:.2f}\n")

        # ── Schema enforcement Silver ─────────────────────────────────────────
        # Solo se conservan las columnas definidas en el schema objetivo.
        # Orden explícito garantiza consistencia con Gold layer en Workshop 3.
        SILVER_SCHEMA = [
            "url", "title", "author", "published_at",
            "reading_time_min", "body_text", "body_clean",
            "title_clean", "body_text_missing",
        ]
        df_clean = df[SILVER_SCHEMA].copy()

        # Serialización para XCom (Airflow pasa datos entre tasks como JSON)
        df_clean["published_at"] = df_clean["published_at"].astype(str)

        print(f"✅ Limpieza completada: {len(df_clean)} artículos")
        print(f"⚠️  Sin cuerpo de texto: {df_clean['body_text_missing'].sum()}")

        return {
            "df_json": df_clean.to_json(orient="records"),
            "bronze_filepath": bronze_filepath
            }

    # ── 4. Guardar en PostgreSQL y actualizar Variable ────────────────────────
    @task()
    def save_to_db(payload: dict) -> str:
        df_json = payload["df_json"]
        bronze_filepath = payload["bronze_filepath"]
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        df = pd.read_json(df_json, orient="records")

        # Reconstruir tipos correctos tras deserializar desde JSON
        df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")

        # Trazabilidad: nombre del archivo bronze de origen
        df["bronze_source"] = os.path.basename(bronze_filepath)

        hook   = PostgresHook(postgres_conn_id="postgres_realmadrid")
        conn   = hook.get_conn()
        cursor = conn.cursor()

        inserted = 0
        skipped = 0
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
                row["url"],
                row["title"],
                row["author"],
                row["published_at"],
                row["reading_time_min"],
                row["body_text"],
                row["body_clean"],
                row["title_clean"],
                row["body_text_missing"],
                row["bronze_source"],
            ))
            if cursor.rowcount == 1:  # 1 = se insertó, 0 = se ignoró por conflicto
                inserted += 1
            else:
                skipped += 1

        conn.commit()
        cursor.close()
        conn.close()

        print(f"📋 Filas procesadas: {len(df)}")
        print(f"✅ Insertadas (sin duplicados): {inserted}")

        # Actualizar Variable → garantiza idempotencia en detect_new_file
        processed_ts = extract_ts_from_filename(bronze_filepath)
        Variable.set("last_processed_bronze_ts", processed_ts)
        print(f"🔖 Variable actualizada → last_processed_bronze_ts: {processed_ts}")

        return "OK"

    # ── Flujo ─────────────────────────────────────────────────────────────────
    bronze_files = detect_new_files()
    payloads = clean_and_normalize.expand(bronze_filepath=bronze_files)
    save_to_db.expand(payload=payloads)
    wait_for_bronze_file >> bronze_files


dag_instance = football_silver_dag()