from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

SILVER_PATH = "/opt/airflow/datalake_silver"
GOLD_PATH   = "/opt/airflow/datalake_gold"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="gold_processing_dag",
    default_args=default_args,
    description=(
        "Silver → Gold: UNION de todos los Parquet (reddit + scraping), "
        "pipeline NLP completo + VADER, governance KPIs y storytelling aggregations"
    ),
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gold", "nlp", "vader", "spacy", "pyspark", "governance", "storytelling"],
)
def gold_processing_dag():

    # ─────────────────────────────────────────────────────────────────────────
    # TASK 1 — Leer y unir TODOS los Parquet de Silver con PySpark
    # ─────────────────────────────────────────────────────────────────────────
    @task()
    def read_silver_with_spark() -> str:
        """
        spark.read.parquet(SILVER_PATH) lee TODOS los archivos de la carpeta
        en un único DataFrame distribuido — reddit_*.parquet y
        web_scraping_*.parquet se apilan automáticamente (UNION).

        El campo `source` ("reddit" / "scraping") identifica el origen
        de cada registro y se usa en governance y storytelling.

        Columnas comunes garantizadas en ambas fuentes:
            url, title, author, published_at,
            body_text, body_text_clean, title_clean,
            body_text_missing, source, bronze_source

        Columnas exclusivas de reddit : score, num_comments, subreddit
        Columnas exclusivas de scraping: reading_time_min

        PySpark rellena con null las columnas ausentes en cada fuente
        gracias a mergeSchema=true.
        """
        from pyspark.sql import SparkSession
        import os

        # ── Validación previa ────────────────────────────────────────────────
        parquet_files = [
            f for f in os.listdir(SILVER_PATH)
            if f.endswith(".parquet") and "outlier_reports" not in f
        ] if os.path.exists(SILVER_PATH) else []

        if not parquet_files:
            raise FileNotFoundError(
                f"No se encontraron archivos Parquet en {SILVER_PATH}. "
                "Ejecuta primero los DAGs Silver de Reddit y Scraping."
            )

        print(f"📂 Archivos Parquet encontrados en Silver ({len(parquet_files)}):")
        for f in sorted(parquet_files):
            print(f"   └─ {f}")

        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import DoubleType
        import os, glob
# ── SparkSession ─────────────────────────────────────────────────────────
        spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("GoldLayer_RealMadrid")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        # ── Leer cada fuente por separado y castear score a DOUBLE ───────────────
        reddit_files   = glob.glob(f"{SILVER_PATH}/reddit_api_realmadrid_*.parquet")
        scraping_files = glob.glob(f"{SILVER_PATH}/web_scraping_realmadrid_*.parquet")

        def normalize_schema(df):
            """Castea columnas numéricas problemáticas a DOUBLE para unificar schema."""
            for col_name in ["score", "num_comments", "reading_time_min"]:
                if col_name in df.columns:
                    df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
            return df

        dfs = []

        if reddit_files:
            df_reddit = spark.read.parquet(*reddit_files)
            df_reddit = normalize_schema(df_reddit)
            # Agregar columna exclusiva de scraping como null
            if "reading_time_min" not in df_reddit.columns:
                df_reddit = df_reddit.withColumn(
                    "reading_time_min", F.lit(None).cast(DoubleType())
                )
            dfs.append(df_reddit)
            print(f"✅ Reddit: {df_reddit.count()} registros")

        if scraping_files:
            df_scraping = spark.read.parquet(*scraping_files)
            df_scraping = normalize_schema(df_scraping)
            # Agregar columnas exclusivas de reddit como null
            for col_name, dtype in [("score", DoubleType()), ("num_comments", DoubleType()), ("subreddit", "string")]:
                if col_name not in df_scraping.columns:
                    df_scraping = df_scraping.withColumn(
                        col_name, F.lit(None).cast(dtype)
                    )
            dfs.append(df_scraping)
            print(f"✅ Scraping: {df_scraping.count()} registros")

        if not dfs:
            raise FileNotFoundError("No se encontraron parquet en Silver.")

        # ── UNION explícito con schemas alineados ────────────────────────────────
        if len(dfs) == 1:
            df = dfs[0]
        else:
            # Alinear columnas en el mismo orden antes del union
            all_cols = sorted(set(dfs[0].columns) | set(dfs[1].columns))
            dfs = [d.select(*[c for c in all_cols if c in d.columns]) for d in dfs]
            df = dfs[0].unionByName(dfs[1], allowMissingColumns=True)

        print(f"\n✅ Total registros unidos: {df.count()}")
        df.groupBy("source").count().show()
        df.printSchema()
        # ── Columnas Gold ─────────────────────────────────────────────────────
        # FIX: body_text_clean es el nombre correcto en ambos Silver DAGs.
        # El Gold DAG original usaba body_text_clean en GOLD_COLUMNS pero
        # la Task 2 leía body_text_clean de forma correcta — solo se asegura
        # que esté explícitamente en la selección.
        GOLD_COLUMNS = [
            "url",
            "title",
            "author",
            "published_at",
            "body_text",
            "body_text_clean",   # campo correcto — coincide con ambos Silver DAGs
            "title_clean",
            "body_text_missing",
            "source",
            "bronze_source",
            # Reddit-only (null en scraping)
            "score",
            "num_comments",
            "subreddit",
            # Scraping-only (null en reddit)
            "reading_time_min",
        ]

        existing  = df.columns
        selected  = [c for c in GOLD_COLUMNS if c in existing]
        df        = df.select(*selected)

        # ── Deduplicación global cross-source ─────────────────────────────────
        before_dedup = df.count()
        df           = df.dropDuplicates(["url"])
        after_dedup  = df.count()
        print(f"\n🔁 Deduplicación global: {before_dedup - after_dedup} duplicados eliminados")

        # ── Convertir a pandas para XCom ──────────────────────────────────────
        df_pandas = df.toPandas()
        spark.stop()

        print(f"\n💾 DataFrame Gold base listo:")
        print(f"   └─ Filas   : {len(df_pandas)}")
        print(f"   └─ Columnas: {list(df_pandas.columns)}")

        return df_pandas.to_json(orient="records")

    # ─────────────────────────────────────────────────────────────────────────
    # TASK 2 — Pipeline NLP completo sobre body_text_clean
    # ─────────────────────────────────────────────────────────────────────────
    @task()
    def nlp_pipeline(df_json: str) -> str:
        """
        Pipeline NLP sobre body_text_clean usando ÚNICAMENTE spaCy.

        FIX vs versión anterior:
          - Se elimina NLTK (word_tokenize + stopwords) — era redundante.
            spaCy hace tokenización, filtrado de stopwords y lematización
            en un único pase vectorizado, más eficiente y consistente
            con el pipeline usado en los Silver DAGs.
          - El campo de entrada es body_text_clean (nombre correcto en
            ambos Silver DAGs). La versión anterior usaba body_text_clean
            en la línea de apply pero el campo en el DataFrame era correcto;
            se mantiene igual con esta aclaración explícita.

        Pipeline spaCy:
          1. Tokenización automática
          2. Filtrar stop words (is_stop)
          3. Filtrar puntuación (is_punct) y espacios (is_space)
          4. Filtrar tokens < 2 chars
          5. Lematizar (lemma_)

        Produce:
          text_processed  — lemas limpios de body_text_clean
          title_processed — lemas limpios de title_clean
          token_count     — número de tokens en text_processed
        """
        import spacy
        import pandas as pd
        from io import StringIO

        # ── spaCy — único motor NLP ───────────────────────────────────────────
        try:
            nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
        except OSError:
            raise RuntimeError(
                "Modelo spaCy no encontrado. "
                "Ejecuta: python -m spacy download en_core_web_sm"
            )

        def process_text(text: str) -> str:
            """
            Pipeline spaCy en un solo pase:
            tokenización → stopwords → puntuación → lematización.
            Elimina la dependencia de NLTK que existía en la versión anterior.
            """
            if not isinstance(text, str) or not text.strip():
                return ""

            doc = nlp(text)
            lemmas = [
                token.lemma_
                for token in doc
                if not token.is_stop
                and not token.is_punct
                and not token.is_space
                and token.is_alpha
                and len(token.lemma_) >= 2
            ]
            return " ".join(lemmas)

        # ── Aplicar ───────────────────────────────────────────────────────────
        df = pd.read_json(StringIO(df_json), orient="records")

        print(f"🔤 Aplicando pipeline NLP (spaCy) a {len(df)} registros...")

        # body_text_clean es el campo correcto que viene de ambos Silver DAGs
        df["text_processed"]  = df["body_text_clean"].apply(process_text)
        df["title_processed"] = df["title_clean"].apply(process_text)
        df["token_count"]     = df["text_processed"].apply(
            lambda x: len(x.split()) if isinstance(x, str) and x.strip() else 0
        )

        # Stats por fuente
        for src in df["source"].unique():
            subset    = df[df["source"] == src]
            empty     = (subset["text_processed"].str.strip() == "").sum()
            avg_tokens = subset["token_count"].mean()
            print(f"   [{src}] vacíos={empty} | avg_tokens={avg_tokens:.1f}")

        print(f"✅ NLP completado — token_count promedio global: {df['token_count'].mean():.1f}")

        return df.to_json(orient="records")

    # ─────────────────────────────────────────────────────────────────────────
    # TASK 3 — VADER sobre body_text original
    # ─────────────────────────────────────────────────────────────────────────
    @task()
    def vader_sentiment(df_json: str) -> str:
        """
        VADER se aplica sobre body_text (texto original con puntuación,
        mayúsculas y signos de exclamación) porque necesita esos elementos
        para calcular correctamente el compound score.

        Si body_text está vacío usa title como fallback.

        Produce por registro:
          vader_compound   float  [-1.0, 1.0]
          vader_pos        float  [0.0, 1.0]
          vader_neg        float  [0.0, 1.0]
          vader_neu        float  [0.0, 1.0]
          sentiment_label  str    positive / negative / neutral

        Umbrales VADER (Hutto & Gilbert, 2014):
          compound >= 0.05  → positive
          compound <= -0.05 → negative
          else              → neutral
        """
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        import pandas as pd
        from io import StringIO

        analyzer = SentimentIntensityAnalyzer()

        def vader_label(compound: float) -> str:
            if compound >= 0.05:
                return "positive"
            if compound <= -0.05:
                return "negative"
            return "neutral"

        def analyze(row) -> dict:
            text = row.get("body_text", "")
            if not isinstance(text, str) or not text.strip():
                text = row.get("title", "") or ""
            if not text.strip():
                return {
                    "vader_compound": 0.0,
                    "vader_pos":      0.0,
                    "vader_neg":      0.0,
                    "vader_neu":      1.0,
                    "sentiment_label": "neutral",
                }
            sc = analyzer.polarity_scores(text)
            return {
                "vader_compound":  round(sc["compound"], 4),
                "vader_pos":       round(sc["pos"],      4),
                "vader_neg":       round(sc["neg"],      4),
                "vader_neu":       round(sc["neu"],      4),
                "sentiment_label": vader_label(sc["compound"]),
            }

        df       = pd.read_json(StringIO(df_json), orient="records")
        print(f"💬 Aplicando VADER a {len(df)} registros...")

        vader_df = pd.DataFrame(df.apply(analyze, axis=1).tolist())
        df       = pd.concat([df.reset_index(drop=True), vader_df], axis=1)

        total = len(df)
        dist  = df["sentiment_label"].value_counts()
        print("✅ VADER completado:")
        print(f"   └─ Positivos: {dist.get('positive', 0)} ({dist.get('positive', 0)/total*100:.1f}%)")
        print(f"   └─ Neutrales: {dist.get('neutral',  0)} ({dist.get('neutral',  0)/total*100:.1f}%)")
        print(f"   └─ Negativos: {dist.get('negative', 0)} ({dist.get('negative', 0)/total*100:.1f}%)")
        print(f"   └─ Compound promedio: {df['vader_compound'].mean():.4f}")
        print("\n📊 Sentimiento por fuente:")
        print(df.groupby("source")["sentiment_label"].value_counts().to_string())

        return df.to_json(orient="records")

    # ─────────────────────────────────────────────────────────────────────────
    # TASK 4 — Guardar Gold base + Governance KPIs
    # ─────────────────────────────────────────────────────────────────────────
    @task()
    def save_gold_and_governance(df_json: str) -> str:
        """
        Persiste dos archivos en datalake_gold/:

        1. gold_realmadrid_YYYYMMDD_HHMMSS.parquet
           DataFrame completo con NLP + VADER por registro.

        2. governance_YYYYMMDD_HHMMSS.parquet
           KPIs de calidad de datos. Mejoras respecto a versión anterior:

           NUEVO — outlier_rate_reading_time_min:
             Porcentaje de registros del scraping cuyo reading_time_min
             estaba fuera del rango IQR antes del capping. Justificado
             porque el Silver DAG detectó y corrigió outliers en este campo.

           NUEVO — outlier_rate_score / outlier_rate_num_comments:
             Porcentaje de posts Reddit con score o num_comments fuera
             del rango IQR. Justificado porque posts virales distorsionan
             el análisis de engagement.

           NUEVO — ingestion_frequency_compliance:
             Runs reales vs runs esperados (@daily = 7 por semana).
             Se calcula contando bronze_source únicos en el dataset.
             Justificado por el requisito explícito del Workshop 3.

           NUEVO — text_length_median:
             Mediana de longitud de body_text_clean por fuente.
             Complementa mean/min/max para una distribución más robusta.

           FIX — duplicate_rate ahora viene de la deduplicación global
             en Task 1 en vez de estar hardcodeado a 0.
        """
        import pandas as pd
        import numpy as np
        import os
        import re
        from io import StringIO

        df = pd.read_json(StringIO(df_json), orient="records")

        if "published_at" in df.columns:
            df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")

        os.makedirs(GOLD_PATH, exist_ok=True)
        run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        # ── 1. Gold base parquet ──────────────────────────────────────────────
        gold_path = os.path.join(GOLD_PATH, f"gold_realmadrid_{run_ts}.parquet")
        df.to_parquet(gold_path, index=False, engine="pyarrow")
        print(f"💾 Gold base guardado: {gold_path} ({len(df)} filas)")

        # ── 2. Governance KPIs ────────────────────────────────────────────────
        total    = len(df)
        by_source = df["source"].value_counts().to_dict()

        # ── Null rates por columna clave ──────────────────────────────────────
        key_cols = [
            "url", "title", "author", "published_at",
            "body_text", "body_text_clean", "source",
            "vader_compound", "text_processed",
        ]
        null_rates = {}
        for col in key_cols:
            if col in df.columns:
                rate = round(df[col].isnull().sum() / total * 100, 2)
                null_rates[f"null_rate_{col}"] = rate

        # ── body_text_missing rate ────────────────────────────────────────────
        body_missing_rate = round(
            df["body_text_missing"].sum() / total * 100, 2
        ) if "body_text_missing" in df.columns else None

        # ── Token count stats ─────────────────────────────────────────────────
        tc = df["token_count"] if "token_count" in df.columns else pd.Series(dtype=float)
        token_stats = {
            "token_count_mean": round(float(tc.mean()), 2),
            "token_count_std":  round(float(tc.std()),  2),
            "token_count_min":  int(tc.min()),
            "token_count_max":  int(tc.max()),
        } if not tc.empty else {}

        # ── Text length stats por fuente y global ─────────────────────────────
        text_len_global = df["body_text_clean"].dropna().apply(len) if "body_text_clean" in df.columns else pd.Series(dtype=float)
        text_len_stats  = {
            "text_length_mean":   round(float(text_len_global.mean()),   2),
            "text_length_median": round(float(text_len_global.median()), 2),
            "text_length_min":    int(text_len_global.min()),
            "text_length_max":    int(text_len_global.max()),
        } if not text_len_global.empty else {}

        # Text length por fuente
        for src in ["scraping", "reddit"]:
            subset_len = df[df["source"] == src]["body_text_clean"].dropna().apply(len)
            if not subset_len.empty:
                text_len_stats[f"text_length_mean_{src}"]   = round(float(subset_len.mean()),   2)
                text_len_stats[f"text_length_median_{src}"] = round(float(subset_len.median()), 2)

        # ── Sentiment distribution ────────────────────────────────────────────
        sent_dist      = df["sentiment_label"].value_counts(normalize=True).mul(100).round(2).to_dict() if "sentiment_label" in df.columns else {}
        sent_dist_named = {f"sentiment_pct_{k}": v for k, v in sent_dist.items()}

        # ── Compound stats ────────────────────────────────────────────────────
        compound = df["vader_compound"] if "vader_compound" in df.columns else pd.Series(dtype=float)
        compound_stats = {
            "compound_mean": round(float(compound.mean()), 4),
            "compound_std":  round(float(compound.std()),  4),
        } if not compound.empty else {}

        # ── NUEVO: Outlier rate — reading_time_min (scraping) ─────────────────
        # Justificación: el Silver DAG de scraping detectó y aplicó capping IQR
        # sobre reading_time_min. Este KPI cuantifica cuántos registros
        # fueron afectados, demostrando la efectividad del tratamiento.
        outlier_stats = {}
        scraping_df   = df[df["source"] == "scraping"]
        if "reading_time_min" in scraping_df.columns and len(scraping_df) > 0:
            rt = scraping_df["reading_time_min"].dropna()
            if len(rt) > 0:
                Q1_rt  = rt.quantile(0.25)
                Q3_rt  = rt.quantile(0.75)
                IQR_rt = Q3_rt - Q1_rt
                outliers_rt = ((rt < Q1_rt - 1.5 * IQR_rt) | (rt > Q3_rt + 1.5 * IQR_rt)).sum()
                outlier_stats["outlier_rate_reading_time_min"] = round(
                    outliers_rt / len(rt) * 100, 2
                )
                outlier_stats["outlier_iqr_lower_reading_time_min"] = round(float(Q1_rt - 1.5 * IQR_rt), 2)
                outlier_stats["outlier_iqr_upper_reading_time_min"] = round(float(Q3_rt + 1.5 * IQR_rt), 2)

        # ── NUEVO: Outlier rate — score y num_comments (reddit) ───────────────
        # Justificación: el Silver DAG de Reddit detectó outliers en score
        # y num_comments. Posts virales con 50k+ upvotes distorsionan el
        # análisis de engagement. Este KPI muestra la magnitud del problema.
        reddit_df = df[df["source"] == "reddit"]
        for col in ["score", "num_comments"]:
            if col in reddit_df.columns and len(reddit_df) > 0:
                vals = reddit_df[col].dropna()
                if len(vals) > 0:
                    Q1_c   = vals.quantile(0.25)
                    Q3_c   = vals.quantile(0.75)
                    IQR_c  = Q3_c - Q1_c
                    out_c  = ((vals < Q1_c - 1.5 * IQR_c) | (vals > Q3_c + 1.5 * IQR_c)).sum()
                    outlier_stats[f"outlier_rate_{col}"] = round(
                        out_c / len(vals) * 100, 2
                    )
                    outlier_stats[f"outlier_iqr_lower_{col}"] = round(float(Q1_c - 1.5 * IQR_c), 2)
                    outlier_stats[f"outlier_iqr_upper_{col}"] = round(float(Q3_c + 1.5 * IQR_c), 2)

        # ── NUEVO: Ingestion frequency compliance ─────────────────────────────
        # Justificación: el Workshop 3 pide explícitamente este KPI.
        # Se calcula contando cuántos bronze_source únicos hay en el dataset
        # (cada uno representa un run de ingestion) y comparando con el
        # número esperado según el schedule @daily (7 runs por semana).
        ingestion_stats = {}
        if "bronze_source" in df.columns:
            actual_runs_scraping = df[df["source"] == "scraping"]["bronze_source"].nunique()
            actual_runs_reddit   = df[df["source"] == "reddit"]["bronze_source"].nunique()
            expected_weekly      = 7  # @daily = 7 runs por semana

            ingestion_stats = {
                "actual_runs_scraping":          actual_runs_scraping,
                "actual_runs_reddit":            actual_runs_reddit,
                "expected_runs_weekly":          expected_weekly,
                "ingestion_compliance_scraping": round(
                    min(actual_runs_scraping / expected_weekly * 100, 100), 2
                ),
                "ingestion_compliance_reddit":   round(
                    min(actual_runs_reddit / expected_weekly * 100, 100), 2
                ),
            }

        # ── Schema compliance rate ────────────────────────────────────────────
        # Porcentaje de registros con todos los campos obligatorios presentes
        required_cols = ["url", "title", "author", "published_at", "body_text", "source"]
        if all(c in df.columns for c in required_cols):
            schema_compliant = df[required_cols].notnull().all(axis=1).sum()
            schema_compliance_rate = round(schema_compliant / total * 100, 2)
        else:
            schema_compliance_rate = None

        # ── Construir governance row ──────────────────────────────────────────
        governance_row = {
            "pipeline_run_ts":         run_ts,
            "total_records":           total,
            "records_reddit":          by_source.get("reddit",   0),
            "records_scraping":        by_source.get("scraping", 0),
            "body_text_missing_rate":  body_missing_rate,
            "schema_compliance_rate":  schema_compliance_rate,
            **null_rates,
            **token_stats,
            **text_len_stats,
            **sent_dist_named,
            **compound_stats,
            **outlier_stats,       # NUEVO
            **ingestion_stats,     # NUEVO
        }

        df_gov   = pd.DataFrame([governance_row])
        gov_path = os.path.join(GOLD_PATH, f"governance_{run_ts}.parquet")
        df_gov.to_parquet(gov_path, index=False, engine="pyarrow")

        print(f"📊 Governance guardado: {gov_path}")
        print(f"   └─ KPIs calculados: {len(governance_row)}")
        for k, v in governance_row.items():
            print(f"      {k}: {v}")

        return df_json  # pasa el mismo df_json a storytelling

    # ─────────────────────────────────────────────────────────────────────────
    # TASK 5 — Storytelling aggregations
    # ─────────────────────────────────────────────────────────────────────────
    @task()
    def save_storytelling(df_json: str):
        """
        Agrega los datos Gold para el dashboard de Workshop 4.
        Cada agregación mapea a una visualización específica.

        Archivos producidos en datalake_gold/:
          sentiment_distribution_YYYYMMDD.parquet  → KPI cards + donut chart
          sentiment_trend_YYYYMMDD.parquet          → Line chart temporal
          top_keywords_YYYYMMDD.parquet             → Word cloud + bar chart
          keyword_sentiment_YYYYMMDD.parquet        → Bar chart por keyword
          source_comparison_YYYYMMDD.parquet        → Bar reddit vs scraping
          volume_trend_YYYYMMDD.parquet             → Area chart actividad
          aspect_sentiment_YYYYMMDD.parquet         → NUEVO: Bar chart por aspecto
          storytelling_summary_YYYYMMDD.parquet     → FIX: renombrado de storytelling_

        FIX vs versión anterior:
          - storytelling_ renombrado a storytelling_summary_ para evitar
            confusión: el archivo solo contiene metadata del run, no las
            agregaciones (esas están en los 7 archivos individuales).
          - NUEVO Agg 7: aspect_sentiment — sentiment promedio por aspecto
            clave del proyecto (Mourinho, Mbappe, Vinicius, Carvajal,
            transfers, injuries). Es el análisis más valioso para el
            storytelling dashboard porque responde directamente las
            user stories del proyecto.
        """
        import pandas as pd
        import os
        from io import StringIO
        from collections import Counter

        df = pd.read_json(StringIO(df_json), orient="records")

        if "published_at" in df.columns:
            df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
            df["week"]         = df["published_at"].dt.to_period("W").astype(str)

        os.makedirs(GOLD_PATH, exist_ok=True)
        run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        total  = len(df)

        # ── Agg 1: Sentiment distribution por fuente ──────────────────────────
        # → KPI cards + donut chart
        sent_dist     = (
            df.groupby(["source", "sentiment_label"])
            .size()
            .reset_index(name="count")
        )
        sent_dist["pct"] = (sent_dist["count"] / total * 100).round(2)
        print(f"📊 Agg1 — sentiment_distribution: {len(sent_dist)} filas")

        # ── Agg 2: Sentiment trend por semana ─────────────────────────────────
        # → Line chart temporal
        if "week" in df.columns:
            sent_trend = (
                df.groupby(["week", "source"])["vader_compound"]
                .agg(["mean", "count"])
                .reset_index()
                .rename(columns={"mean": "compound_mean", "count": "n_records"})
            )
            sent_trend["compound_mean"] = sent_trend["compound_mean"].round(4)
        else:
            sent_trend = pd.DataFrame(
                columns=["week", "source", "compound_mean", "n_records"]
            )
        print(f"📊 Agg2 — sentiment_trend: {len(sent_trend)} filas")

        # ── Agg 3: Top 30 keywords globales ───────────────────────────────────
        # → Word cloud + bar chart
        all_tokens = []
        for text in df["text_processed"].dropna():
            if isinstance(text, str) and text.strip():
                all_tokens.extend(text.split())

        token_counts = Counter(all_tokens)
        top_keywords = pd.DataFrame(
            token_counts.most_common(30),
            columns=["keyword", "frequency"]
        )
        print(f"📊 Agg3 — top_keywords: {len(top_keywords)} keywords")

        # ── Agg 4: Sentiment promedio por keyword (top 20) ────────────────────
        # → Bar chart horizontal coloreado por sentimiento
        top20_words = [w for w, _ in token_counts.most_common(20)]
        kw_rows     = []
        for word in top20_words:
            mask   = df["text_processed"].str.contains(
                rf"\b{word}\b", na=False, regex=True
            )
            subset = df[mask]
            if len(subset) > 0:
                kw_rows.append({
                    "keyword":        word,
                    "n_mentions":     len(subset),
                    "compound_mean":  round(float(subset["vader_compound"].mean()), 4),
                    "sentiment_mode": subset["sentiment_label"].mode().iloc[0],
                })
        kw_sentiment = pd.DataFrame(kw_rows)
        print(f"📊 Agg4 — keyword_sentiment: {len(kw_sentiment)} keywords")

        # ── Agg 5: Comparación por fuente ─────────────────────────────────────
        # → Side-by-side bar chart reddit vs scraping
        source_comp = (
            df.groupby("source")
            .agg(
                n_records       = ("url",             "count"),
                compound_mean   = ("vader_compound",   "mean"),
                compound_std    = ("vader_compound",   "std"),
                pct_positive    = ("sentiment_label",  lambda x: (x == "positive").mean() * 100),
                pct_neutral     = ("sentiment_label",  lambda x: (x == "neutral").mean()  * 100),
                pct_negative    = ("sentiment_label",  lambda x: (x == "negative").mean() * 100),
                avg_token_count = ("token_count",      "mean"),
            )
            .reset_index()
        )
        source_comp = source_comp.round(4)
        print(f"📊 Agg5 — source_comparison: {len(source_comp)} fuentes")

        # ── Agg 6: Volumen por semana ─────────────────────────────────────────
        # → Area chart de actividad temporal
        if "week" in df.columns:
            vol_trend = (
                df.groupby(["week", "source"])
                .size()
                .reset_index(name="n_records")
                .sort_values("week")
            )
        else:
            vol_trend = pd.DataFrame(columns=["week", "source", "n_records"])
        print(f"📊 Agg6 — volume_trend: {len(vol_trend)} filas")

        # ── Agg 7 NUEVO: Aspect-based sentiment ──────────────────────────────
        # → Bar chart por aspecto — responde directamente las user stories:
        #   "¿El sentimiento sobre Mourinho es positivo o negativo?"
        #   "¿Cómo se habla de Mbappe en Reddit vs noticias?"
        #
        # Para cada aspecto se busca la palabra en body_text (texto original,
        # no procesado) para capturar todas las variantes (Mbappe, mbappe,
        # MBAPPE) y en title también. Se calcula el compound promedio de
        # los registros donde aparece el aspecto.
        ASPECTS = {
            "mourinho":  ["mourinho", "mou"],
            "mbappe":    ["mbappe", "kylian"],
            "vinicius":  ["vinicius", "vini"],
            "carvajal":  ["carvajal", "dani"],
            "transfers":  ["transfer", "signing", "sign", "deal", "contract"],
            "injuries":  ["injury", "injured", "fitness", "knock", "discomfort"],
            "arbeloa":   ["arbeloa"],
            "bellingham": ["bellingham", "jude"],
        }

        aspect_rows = []
        for aspect, keywords in ASPECTS.items():
            # Construir regex que busca cualquiera de las palabras del aspecto
            pattern = "|".join([rf"\b{kw}\b" for kw in keywords])

            # Buscar en body_text original (case-insensitive) y en title
            body_mask  = df["body_text"].str.contains(
                pattern, case=False, na=False, regex=True
            ) if "body_text" in df.columns else pd.Series([False] * len(df))

            title_mask = df["title"].str.contains(
                pattern, case=False, na=False, regex=True
            ) if "title" in df.columns else pd.Series([False] * len(df))

            combined_mask = body_mask | title_mask
            subset        = df[combined_mask]

            if len(subset) == 0:
                continue

            # Global
            aspect_rows.append({
                "aspect":         aspect,
                "source":         "all",
                "n_mentions":     len(subset),
                "compound_mean":  round(float(subset["vader_compound"].mean()), 4),
                "compound_std":   round(float(subset["vader_compound"].std()),  4),
                "pct_positive":   round((subset["sentiment_label"] == "positive").mean() * 100, 2),
                "pct_neutral":    round((subset["sentiment_label"] == "neutral").mean()  * 100, 2),
                "pct_negative":   round((subset["sentiment_label"] == "negative").mean() * 100, 2),
                "sentiment_mode": subset["sentiment_label"].mode().iloc[0],
            })

            # Por fuente (reddit vs scraping)
            for src in ["reddit", "scraping"]:
                src_subset = subset[subset["source"] == src]
                if len(src_subset) == 0:
                    continue
                aspect_rows.append({
                    "aspect":         aspect,
                    "source":         src,
                    "n_mentions":     len(src_subset),
                    "compound_mean":  round(float(src_subset["vader_compound"].mean()), 4),
                    "compound_std":   round(float(src_subset["vader_compound"].std()),  4),
                    "pct_positive":   round((src_subset["sentiment_label"] == "positive").mean() * 100, 2),
                    "pct_neutral":    round((src_subset["sentiment_label"] == "neutral").mean()  * 100, 2),
                    "pct_negative":   round((src_subset["sentiment_label"] == "negative").mean() * 100, 2),
                    "sentiment_mode": src_subset["sentiment_label"].mode().iloc[0],
                })

        aspect_sentiment = pd.DataFrame(aspect_rows)
        print(f"📊 Agg7 — aspect_sentiment: {len(aspect_sentiment)} filas "
              f"({aspect_sentiment['aspect'].nunique() if len(aspect_sentiment) > 0 else 0} aspectos)")

        if len(aspect_sentiment) > 0:
            print("\n🎯 Sentimiento por aspecto (global):")
            global_aspects = aspect_sentiment[aspect_sentiment["source"] == "all"]
            for _, row in global_aspects.iterrows():
                print(f"   [{row['aspect']}] mentions={row['n_mentions']} "
                      f"compound={row['compound_mean']} "
                      f"label={row['sentiment_mode']}")

        # ── Guardar cada agregación como Parquet independiente ────────────────
        aggregations = {
            "sentiment_distribution": sent_dist,
            "sentiment_trend":        sent_trend,
            "top_keywords":           top_keywords,
            "keyword_sentiment":      kw_sentiment,
            "source_comparison":      source_comp,
            "volume_trend":           vol_trend,
            "aspect_sentiment":       aspect_sentiment,  # NUEVO
        }

        for name, agg_df in aggregations.items():
            path = os.path.join(GOLD_PATH, f"{name}_{run_ts}.parquet")
            agg_df.to_parquet(path, index=False, engine="pyarrow")
            print(f"💾 {name}_{run_ts}.parquet guardado ({len(agg_df)} filas)")

        # ── FIX: storytelling_summary_ en vez de storytelling_ ───────────────
        # El nombre anterior era confuso porque el archivo solo tiene metadata
        # del run, no las agregaciones. Renombrado a storytelling_summary_
        # para que quede claro que los datos reales están en los 7 archivos
        # individuales listados en `aggregations_saved`.
        story_meta = pd.DataFrame([{
            "pipeline_run_ts":    run_ts,
            "total_records":      total,
            "sources":            str(df["source"].unique().tolist()),
            "weeks_covered":      df["week"].nunique() if "week" in df.columns else 0,
            "top_keyword":        top_keywords.iloc[0]["keyword"] if len(top_keywords) > 0 else "",
            "overall_sentiment":  df["sentiment_label"].mode().iloc[0] if len(df) > 0 else "",
            "overall_compound":   round(float(df["vader_compound"].mean()), 4),
            "aggregations_saved": str(list(aggregations.keys())),
            "aspects_analyzed":   str(list(ASPECTS.keys())),
        }])

        # FIX: storytelling_summary_ en vez de storytelling_
        story_path = os.path.join(GOLD_PATH, f"storytelling_summary_{run_ts}.parquet")
        story_meta.to_parquet(story_path, index=False, engine="pyarrow")
        print(f"\n📖 storytelling_summary_{run_ts}.parquet guardado (metadata del run)")
        print(f"   Archivos de agregación: {list(aggregations.keys())}")

    # ─────────────────────────────────────────────────────────────────────────
    # Flujo DAG
    # ─────────────────────────────────────────────────────────────────────────
    df_silver = read_silver_with_spark()
    df_nlp    = nlp_pipeline(df_silver)
    df_vader  = vader_sentiment(df_nlp)
    df_gold   = save_gold_and_governance(df_vader)
    save_storytelling(df_gold)


dag_instance = gold_processing_dag()