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

        reddit_files   = glob.glob(f"{SILVER_PATH}/reddit_api_realmadrid_*.parquet")
        scraping_files = glob.glob(f"{SILVER_PATH}/web_scraping_realmadrid_*.parquet")

        def normalize_schema(df):
            for col_name in ["score", "num_comments", "reading_time_min"]:
                if col_name in df.columns:
                    df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
            return df

        dfs = []

        if reddit_files:
            df_reddit = spark.read.parquet(*reddit_files)
            df_reddit = normalize_schema(df_reddit)
            if "reading_time_min" not in df_reddit.columns:
                df_reddit = df_reddit.withColumn(
                    "reading_time_min", F.lit(None).cast(DoubleType())
                )
            dfs.append(df_reddit)
            print(f"✅ Reddit: {df_reddit.count()} registros")

        if scraping_files:
            df_scraping = spark.read.parquet(*scraping_files)
            df_scraping = normalize_schema(df_scraping)
            for col_name, dtype in [("score", DoubleType()), ("num_comments", DoubleType()), ("subreddit", "string")]:
                if col_name not in df_scraping.columns:
                    df_scraping = df_scraping.withColumn(
                        col_name, F.lit(None).cast(dtype)
                    )
            dfs.append(df_scraping)
            print(f"✅ Scraping: {df_scraping.count()} registros")

        if not dfs:
            raise FileNotFoundError("No se encontraron parquet en Silver.")

        if len(dfs) == 1:
            df = dfs[0]
        else:
            all_cols = sorted(set(dfs[0].columns) | set(dfs[1].columns))
            dfs = [d.select(*[c for c in all_cols if c in d.columns]) for d in dfs]
            df = dfs[0].unionByName(dfs[1], allowMissingColumns=True)

        print(f"\n✅ Total registros unidos: {df.count()}")
        df.groupBy("source").count().show()
        df.printSchema()

        GOLD_COLUMNS = [
            "url", "title", "author", "published_at",
            "body_text", "body_text_clean", "title_clean",
            "body_text_missing", "source", "bronze_source",
            "score", "num_comments", "subreddit",
            "reading_time_min",
        ]

        existing = df.columns
        selected = [c for c in GOLD_COLUMNS if c in existing]
        df       = df.select(*selected)

        before_dedup = df.count()
        df           = df.dropDuplicates(["url"])
        after_dedup  = df.count()
        print(f"\n🔁 Deduplicación global: {before_dedup - after_dedup} duplicados eliminados")

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
        Produce: text_processed, title_processed, token_count.
        """
        import spacy
        import pandas as pd
        from io import StringIO

        try:
            nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
        except OSError:
            raise RuntimeError(
                "Modelo spaCy no encontrado. "
                "Ejecuta: python -m spacy download en_core_web_sm"
            )

        # ── Stopwords en ESPAÑOL ──────────────────────────────────────────────
        # El modelo spaCy es en inglés (en_core_web_sm) y NO conoce estas
        # palabras, por eso "el", "la", "los"... se colaban en las gráficas.
        # Vienen de "El Clásico", "La Liga", nombres y citas en español.
        SPANISH_STOPWORDS = {
            "el", "la", "los", "las", "un", "una", "unos", "unas", "lo",
            "de", "del", "al", "ante", "con", "contra", "desde", "durante",
            "en", "entre", "hacia", "hasta", "para", "por", "segun", "sin",
            "sobre", "tras", "y", "e", "o", "u", "que", "se", "su", "sus",
            "le", "les", "es", "son", "fue", "ser", "no", "mas", "muy", "ya",
            "si", "como", "esta", "este", "esto", "esa", "ese", "eso", "pero",
            "tambien", "todo", "todos", "toda", "todas", "ni", "ha", "han",
            "hay", "mi", "tu", "nos", "me", "te",
        }

        # ── Vocabulario GENÉRICO que no explica el sentimiento ────────────────
        # Nombres del equipo + ruido temporal/futbolístico ("week", "gol",
        # "time"...). NO se incluyen palabras con carga de sentimiento
        # (good, win, loss, great...) porque ESAS sí explican el porqué.
        DOMAIN_STOPWORDS = {
            "real", "madrid", "club", "team", "player", "players",
            "season", "game", "games", "match", "matches", "football",
            "laliga", "liga", "league", "play", "playing", "played",
            "time", "week", "weeks", "day", "days", "year", "years",
            "month", "today", "yesterday", "tomorrow", "gol", "goal",
            "goals", "minute", "minutes", "half", "side", "thing", "things",
            "way", "ways", "lot", "bit", "guy", "guys", "people", "fan",
            "fans", "head", "like", "just", "going", "get", "got", "really",
            "want", "think", "know", "say", "said", "make", "made", "see",
        }

        ALL_CUSTOM_STOPWORDS = SPANISH_STOPWORDS | DOMAIN_STOPWORDS
        SPACY_STOPWORDS      = nlp.Defaults.stop_words   # lista de spaCy (inglés)

        def process_text(text: str) -> str:
            if not isinstance(text, str) or not text.strip():
                return ""
            doc = nlp(text)
            lemmas = []
            for token in doc:
                if token.is_stop or token.is_punct or token.is_space:
                    continue
                if not token.is_alpha:
                    continue
                lemma = token.lemma_.lower()
                # Longitud mínima 3: elimina "el", "la", "go", "no", iniciales...
                if len(lemma) < 3:
                    continue
                # Re-chequea el LEMA como stopword de spaCy.
                # Esto atrapa fugas tipo "going"->"go", "not", "like" que
                # is_stop no marca sobre la forma flexionada original.
                if lemma in SPACY_STOPWORDS:
                    continue
                if lemma in ALL_CUSTOM_STOPWORDS:
                    continue
                lemmas.append(lemma)
            return " ".join(lemmas)

        df = pd.read_json(StringIO(df_json), orient="records")

        print(f"🔤 Aplicando pipeline NLP (spaCy) a {len(df)} registros...")

        df["text_processed"]  = df["body_text_clean"].apply(process_text)
        df["title_processed"] = df["title_clean"].apply(process_text)
        df["token_count"]     = df["text_processed"].apply(
            lambda x: len(x.split()) if isinstance(x, str) and x.strip() else 0
        )

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
        mayúsculas y signos de exclamación).
        Si body_text está vacío usa title como fallback.
        Produce: vader_compound, vader_pos, vader_neg, vader_neu, sentiment_label.
        """
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        import pandas as pd
        from io import StringIO

        analyzer = SentimentIntensityAnalyzer()

        def analyze(row) -> dict:
            text = row.get("body_text", "")
            if not isinstance(text, str) or not text.strip():
                text = row.get("title", "") or ""

            if not text.strip():
                return {
                    "vader_compound": 0.0,
                    "vader_pos": 0.0,
                    "vader_neg": 0.0,
                    "vader_neu": 1.0,
                    "sentiment_label": "neutral",
                }

            source = row.get("source", "")

            if source == "scraping":
                MAX_CHARS = 1000
                text = text[:MAX_CHARS]
                paragraphs = [p.strip() for p in text.split("\n") if p.strip()]
                if not paragraphs:
                    paragraphs = [text]

                paragraph_scores = [analyzer.polarity_scores(p) for p in paragraphs]

                compound = sum(s["compound"] for s in paragraph_scores) / len(paragraph_scores)
                pos = sum(s["pos"] for s in paragraph_scores) / len(paragraph_scores)
                neg = sum(s["neg"] for s in paragraph_scores) / len(paragraph_scores)
                neu = sum(s["neu"] for s in paragraph_scores) / len(paragraph_scores)

                label = "positive" if compound >= 0.20 else "negative" if compound <= -0.20 else "neutral"

            else:
                sc = analyzer.polarity_scores(text)
                compound = sc["compound"]
                pos = sc["pos"]
                neg = sc["neg"]
                neu = sc["neu"]
                label = "positive" if compound >= 0.05 else "negative" if compound <= -0.05 else "neutral"

            return {
                "vader_compound": round(compound, 4),
                "vader_pos":      round(pos,      4),
                "vader_neg":      round(neg,      4),
                "vader_neu":      round(neu,      4),
                "sentiment_label": label,
            }

        df = pd.read_json(StringIO(df_json), orient="records")

        print(f"💬 Aplicando VADER a {len(df)} registros...")

        vader_df = pd.DataFrame(df.apply(analyze, axis=1).tolist())
        df = pd.concat([df.reset_index(drop=True), vader_df], axis=1)

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
        Persiste gold_realmadrid_*.parquet y governance_*.parquet en datalake_gold/.
        """
        import pandas as pd
        import numpy as np
        import os
        from io import StringIO

        print(type(df_json))
        if df_json is None:
            raise ValueError("df_json es None")

        df = pd.read_json(StringIO(df_json), orient="records")

        if "published_at" in df.columns:
            df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")

        os.makedirs(GOLD_PATH, exist_ok=True)
        run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        gold_path = os.path.join(GOLD_PATH, f"gold_realmadrid_{run_ts}.parquet")
        df.to_parquet(gold_path, index=False, engine="pyarrow")
        print(f"💾 Gold base guardado: {gold_path} ({len(df)} filas)")

        total     = len(df)
        by_source = df["source"].value_counts().to_dict()

        key_cols = [
            "url", "title", "author", "published_at",
            "body_text", "body_text_clean", "source",
            "vader_compound", "text_processed",
        ]
        null_rates = {}
        for col in key_cols:
            if col in df.columns:
                null_rates[f"null_rate_{col}"] = round(df[col].isnull().sum() / total * 100, 2)

        body_missing_rate = round(
            df["body_text_missing"].sum() / total * 100, 2
        ) if "body_text_missing" in df.columns else None

        tc = df["token_count"] if "token_count" in df.columns else pd.Series(dtype=float)
        token_stats = {
            "token_count_mean": round(float(tc.mean()), 2),
            "token_count_std":  round(float(tc.std()),  2),
            "token_count_min":  int(tc.min()),
            "token_count_max":  int(tc.max()),
        } if not tc.empty else {}

        text_len_global = df["body_text_clean"].dropna().apply(len) if "body_text_clean" in df.columns else pd.Series(dtype=float)
        text_len_stats  = {
            "text_length_mean":   round(float(text_len_global.mean()),   2),
            "text_length_median": round(float(text_len_global.median()), 2),
            "text_length_min":    int(text_len_global.min()),
            "text_length_max":    int(text_len_global.max()),
        } if not text_len_global.empty else {}

        for src in ["scraping", "reddit"]:
            subset_len = df[df["source"] == src]["body_text_clean"].dropna().apply(len)
            if not subset_len.empty:
                text_len_stats[f"text_length_mean_{src}"]   = round(float(subset_len.mean()),   2)
                text_len_stats[f"text_length_median_{src}"] = round(float(subset_len.median()), 2)

        sent_dist = df["sentiment_label"].value_counts(normalize=True).mul(100).round(2).to_dict() if "sentiment_label" in df.columns else {}
        sent_dist_named = {f"sentiment_pct_{k}": v for k, v in sent_dist.items()}

        compound = df["vader_compound"] if "vader_compound" in df.columns else pd.Series(dtype=float)
        compound_stats = {
            "compound_mean": round(float(compound.mean()), 4),
            "compound_std":  round(float(compound.std()),  4),
        } if not compound.empty else {}

        outlier_stats = {}
        scraping_df   = df[df["source"] == "scraping"]
        if "reading_time_min" in scraping_df.columns and len(scraping_df) > 0:
            rt = scraping_df["reading_time_min"].dropna()
            if len(rt) > 0:
                Q1_rt  = rt.quantile(0.25)
                Q3_rt  = rt.quantile(0.75)
                IQR_rt = Q3_rt - Q1_rt
                outliers_rt = ((rt < Q1_rt - 1.5 * IQR_rt) | (rt > Q3_rt + 1.5 * IQR_rt)).sum()
                outlier_stats["outlier_rate_reading_time_min"]        = round(outliers_rt / len(rt) * 100, 2)
                outlier_stats["outlier_iqr_lower_reading_time_min"]   = round(float(Q1_rt - 1.5 * IQR_rt), 2)
                outlier_stats["outlier_iqr_upper_reading_time_min"]   = round(float(Q3_rt + 1.5 * IQR_rt), 2)

        reddit_df = df[df["source"] == "reddit"]
        for col in ["score", "num_comments"]:
            if col in reddit_df.columns and len(reddit_df) > 0:
                vals = reddit_df[col].dropna()
                if len(vals) > 0:
                    Q1_c  = vals.quantile(0.25)
                    Q3_c  = vals.quantile(0.75)
                    IQR_c = Q3_c - Q1_c
                    out_c = ((vals < Q1_c - 1.5 * IQR_c) | (vals > Q3_c + 1.5 * IQR_c)).sum()
                    outlier_stats[f"outlier_rate_{col}"]        = round(out_c / len(vals) * 100, 2)
                    outlier_stats[f"outlier_iqr_lower_{col}"]   = round(float(Q1_c - 1.5 * IQR_c), 2)
                    outlier_stats[f"outlier_iqr_upper_{col}"]   = round(float(Q3_c + 1.5 * IQR_c), 2)

        ingestion_stats = {}
        if "bronze_source" in df.columns:
            actual_runs_scraping = df[df["source"] == "scraping"]["bronze_source"].nunique()
            actual_runs_reddit   = df[df["source"] == "reddit"]["bronze_source"].nunique()
            expected_weekly      = 7
            ingestion_stats = {
                "actual_runs_scraping":          actual_runs_scraping,
                "actual_runs_reddit":            actual_runs_reddit,
                "expected_runs_weekly":          expected_weekly,
                "ingestion_compliance_scraping": round(min(actual_runs_scraping / expected_weekly * 100, 100), 2),
                "ingestion_compliance_reddit":   round(min(actual_runs_reddit   / expected_weekly * 100, 100), 2),
            }

        required_cols = ["url", "title", "author", "published_at", "body_text", "source"]
        if all(c in df.columns for c in required_cols):
            schema_compliant       = df[required_cols].notnull().all(axis=1).sum()
            schema_compliance_rate = round(schema_compliant / total * 100, 2)
        else:
            schema_compliance_rate = None

        governance_row = {
            "pipeline_run_ts":        run_ts,
            "total_records":          total,
            "records_reddit":         by_source.get("reddit",   0),
            "records_scraping":       by_source.get("scraping", 0),
            "body_text_missing_rate": body_missing_rate,
            "schema_compliance_rate": schema_compliance_rate,
            **null_rates,
            **token_stats,
            **text_len_stats,
            **sent_dist_named,
            **compound_stats,
            **outlier_stats,
            **ingestion_stats,
        }

        df_gov   = pd.DataFrame([governance_row])
        gov_path = os.path.join(GOLD_PATH, f"governance_{run_ts}.parquet")
        df_gov.to_parquet(gov_path, index=False, engine="pyarrow")

        print(f"📊 Governance guardado: {gov_path}")
        print(f"   └─ KPIs calculados: {len(governance_row)}")
        for k, v in governance_row.items():
            print(f"      {k}: {v}")

        return df_json

    # ─────────────────────────────────────────────────────────────────────────
    # TASK 5 — Storytelling aggregations (incluye Agg 8 y Agg 9 nuevas)
    # ─────────────────────────────────────────────────────────────────────────
    @task()
    def save_storytelling(df_json: str):
        """
        Agrega los datos Gold para el dashboard de Workshop 4.

        Archivos producidos en datalake_gold/:
          sentiment_distribution_*.parquet   → KPI cards + donut chart (global y por fuente)
          sentiment_dist_time_*.parquet      → NUEVO: distribución de sentimiento por periodo
          sentiment_trend_*.parquet          → Line chart temporal
          top_keywords_*.parquet             → Bar chart keywords (unigramas)
          top_bigrams_*.parquet              → NUEVO: bigramas más frecuentes (n-grams)
          keyword_sentiment_*.parquet        → Bar chart por keyword
          source_comparison_*.parquet        → Reddit vs prensa
          volume_trend_*.parquet             → Area chart actividad
          aspect_sentiment_*.parquet         → Bar chart por aspecto
          aspect_cowords_*.parquet           → NUEVO: palabras coocurrentes por aspecto/sentimiento
          aspect_snippets_*.parquet          → NUEVO: textos representativos por aspecto
          storytelling_summary_*.parquet     → Metadata del run
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
        sent_dist     = (
            df.groupby(["source", "sentiment_label"])
            .size()
            .reset_index(name="count")
        )
        sent_dist["pct"] = (sent_dist["count"] / total * 100).round(2)
        print(f"📊 Agg1 — sentiment_distribution: {len(sent_dist)} filas")

        # ── Agg 1b: Sentiment distribution POR PERIODO ────────────────────────
        # Requisito Workshop 3: conteo y % de pos/neg/neu "overall AND per
        # time period". Esto da la mezcla de sentimiento semana a semana.
        if "week" in df.columns:
            sent_dist_time = (
                df.groupby(["week", "sentiment_label"])
                .size()
                .reset_index(name="count")
            )
            week_totals = sent_dist_time.groupby("week")["count"].transform("sum")
            sent_dist_time["pct"] = (sent_dist_time["count"] / week_totals * 100).round(2)
            sent_dist_time = sent_dist_time.sort_values("week")
        else:
            sent_dist_time = pd.DataFrame(columns=["week", "sentiment_label", "count", "pct"])
        print(f"📊 Agg1b — sentiment_dist_time: {len(sent_dist_time)} filas")

        # ── Agg 2: Sentiment trend por semana ─────────────────────────────────
        if "week" in df.columns:
            sent_trend = (
                df.groupby(["week", "source"])["vader_compound"]
                .agg(["mean", "count"])
                .reset_index()
                .rename(columns={"mean": "compound_mean", "count": "n_records"})
            )
            sent_trend["compound_mean"] = sent_trend["compound_mean"].round(4)
        else:
            sent_trend = pd.DataFrame(columns=["week", "source", "compound_mean", "n_records"])
        print(f"📊 Agg2 — sentiment_trend: {len(sent_trend)} filas")

        # ── Agg 3: Top 50 keywords globales ───────────────────────────────────
        all_tokens = []
        for text in df["text_processed"].dropna():
            if isinstance(text, str) and text.strip():
                all_tokens.extend(text.split())

        token_counts = Counter(all_tokens)
        top_keywords = pd.DataFrame(
            token_counts.most_common(50),
            columns=["keyword", "frequency"]
        )
        print(f"📊 Agg3 — top_keywords: {len(top_keywords)} keywords")

        # ── Agg 3b: Top 30 BIGRAMAS (n-grams) ─────────────────────────────────
        # Requisito Workshop 3: "Most frequent terms AND bigrams".
        # Se construyen sobre el texto ya preprocesado (text_processed), por lo
        # que heredan el filtrado de stopwords del pipeline NLP.
        bigram_counter = Counter()
        for text in df["text_processed"].dropna():
            if isinstance(text, str) and text.strip():
                toks = text.split()
                for i in range(len(toks) - 1):
                    bigram_counter[f"{toks[i]} {toks[i + 1]}"] += 1
        top_bigrams = pd.DataFrame(
            bigram_counter.most_common(30),
            columns=["bigram", "frequency"]
        )
        print(f"📊 Agg3b — top_bigrams: {len(top_bigrams)} bigramas")

        # ── Agg 4: Sentiment promedio por keyword (top 20) ────────────────────
        top20_words = [w for w, _ in token_counts.most_common(20)]
        kw_rows     = []
        for word in top20_words:
            mask   = df["text_processed"].str.contains(rf"\b{word}\b", na=False, regex=True)
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
        source_comp = (
            df.groupby("source")
            .agg(
                n_records       = ("url",            "count"),
                compound_mean   = ("vader_compound",  "mean"),
                compound_std    = ("vader_compound",  "std"),
                pct_positive    = ("sentiment_label", lambda x: (x == "positive").mean() * 100),
                pct_neutral     = ("sentiment_label", lambda x: (x == "neutral").mean()  * 100),
                pct_negative    = ("sentiment_label", lambda x: (x == "negative").mean() * 100),
                avg_token_count = ("token_count",     "mean"),
            )
            .reset_index()
        )
        source_comp = source_comp.round(4)
        print(f"📊 Agg5 — source_comparison: {len(source_comp)} fuentes")

        # ── Agg 6: Volumen por semana ─────────────────────────────────────────
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

        # ── Agg 7: Aspect-based sentiment ─────────────────────────────────────
        # Aspectos = entidades curadas manualmente. NO se generan solos a
        # partir de las palabras frecuentes; por eso aquí solo van entidades
        # que de verdad afectan al sentimiento (jugadores, rivales, temas),
        # nunca ruido como "week" o "gol". Para agregar un protagonista nuevo,
        # añádelo aquí Y en el dict LABELS de storytelling_app.py.
        ASPECTS = {
            "mourinho":   ["mourinho", "mou"],
            "mbappe":     ["mbappe", "kylian"],
            "vinicius":   ["vinicius", "vini"],
            "carvajal":   ["carvajal", "dani"],
            "transfers":  ["transfer", "signing", "sign", "deal", "contract"],
            "injuries":   ["injury", "injured", "fitness", "knock", "discomfort"],
            "arbeloa":    ["arbeloa"],
            "bellingham": ["bellingham", "jude"],
            "valverde":   ["valverde", "fede"],
            # ── Entidades agregadas manualmente ──────────────────────────────
            "barcelona":  ["barcelona", "barca", "barça", "barsa"],
            "clasico":    ["clasico", "clásico"],
            "tchouameni": ["tchouameni", "tchoumeni", "tchouaméni"],
            "ancelotti":  ["ancelotti", "carlo", "ancheloti"],
        }

        aspect_rows = []
        for aspect, keywords in ASPECTS.items():
            pattern = "|".join([rf"\b{kw}\b" for kw in keywords])

            body_mask  = df["body_text"].str.contains(pattern, case=False, na=False, regex=True) \
                         if "body_text" in df.columns else pd.Series([False] * len(df))
            title_mask = df["title"].str.contains(pattern, case=False, na=False, regex=True) \
                         if "title" in df.columns else pd.Series([False] * len(df))

            subset = df[body_mask | title_mask]
            if len(subset) == 0:
                continue

            aspect_rows.append({
                "aspect":        aspect,
                "source":        "all",
                "n_mentions":    len(subset),
                "compound_mean": round(float(subset["vader_compound"].mean()), 4),
                "compound_std":  round(float(subset["vader_compound"].std()),  4),
                "pct_positive":  round((subset["sentiment_label"] == "positive").mean() * 100, 2),
                "pct_neutral":   round((subset["sentiment_label"] == "neutral").mean()  * 100, 2),
                "pct_negative":  round((subset["sentiment_label"] == "negative").mean() * 100, 2),
                "sentiment_mode": subset["sentiment_label"].mode().iloc[0],
            })

            for src in ["reddit", "scraping"]:
                src_subset = subset[subset["source"] == src]
                if len(src_subset) == 0:
                    continue
                aspect_rows.append({
                    "aspect":        aspect,
                    "source":        src,
                    "n_mentions":    len(src_subset),
                    "compound_mean": round(float(src_subset["vader_compound"].mean()), 4),
                    "compound_std":  round(float(src_subset["vader_compound"].std()),  4),
                    "pct_positive":  round((src_subset["sentiment_label"] == "positive").mean() * 100, 2),
                    "pct_neutral":   round((src_subset["sentiment_label"] == "neutral").mean()  * 100, 2),
                    "pct_negative":  round((src_subset["sentiment_label"] == "negative").mean() * 100, 2),
                    "sentiment_mode": src_subset["sentiment_label"].mode().iloc[0],
                })

        aspect_sentiment = pd.DataFrame(aspect_rows)
        print(f"📊 Agg7 — aspect_sentiment: {len(aspect_sentiment)} filas")

        # ── Agg 8 NUEVO: Palabras coocurrentes por aspecto y sentimiento ──────
        # Para cada aspecto y cada polo de sentimiento (positive/negative),
        # extrae las top-10 palabras del text_processed de esos registros.
        # Responde: "¿qué palabras hacen que Valverde sea negativo?"
        #
        # Red de seguridad: aunque text_processed ya viene limpio del pipeline
        # NLP, este blocklist garantiza que stopwords en español ("el", "la")
        # o ruido genérico nunca aparezcan en las gráficas, incluso si se corre
        # con Parquet procesados por una versión antigua del pipeline.
        COWORD_BLOCKLIST = {
            "el", "la", "los", "las", "un", "una", "lo", "de", "del", "que",
            "se", "su", "es", "no", "mas", "como", "para", "por", "con", "en",
            "play", "time", "week", "day", "gol", "goal", "head", "like",
            "just", "going", "thing", "way", "people", "really",
        }
        coword_rows = []
        for aspect, keywords in ASPECTS.items():
            pattern   = "|".join([rf"\b{kw}\b" for kw in keywords])
            body_mask  = df["body_text"].str.contains(pattern, case=False, na=False, regex=True) \
                         if "body_text" in df.columns else pd.Series([False] * len(df))
            title_mask = df["title"].str.contains(pattern, case=False, na=False, regex=True) \
                         if "title" in df.columns else pd.Series([False] * len(df))
            subset = df[body_mask | title_mask]

            if len(subset) == 0:
                continue

            for label in ["positive", "negative", "neutral"]:
                sub_label = subset[subset["sentiment_label"] == label]
                if sub_label.empty:
                    continue

                # Recoger todos los tokens de text_processed para ese grupo
                all_toks = []
                for text in sub_label["text_processed"].dropna():
                    if isinstance(text, str) and text.strip():
                        all_toks.extend(text.split())

                # Excluir: las propias keywords del aspecto, el blocklist y
                # tokens muy cortos (≤2 chars) que casi siempre son ruido.
                aspect_kw_set = set(keywords)
                filtered = [
                    (w, c) for w, c in Counter(all_toks).most_common(25)
                    if w.lower() not in aspect_kw_set
                    and w.lower() not in COWORD_BLOCKLIST
                    and len(w) >= 3
                ]

                for word, freq in filtered[:10]:
                    coword_rows.append({
                        "aspect":     aspect,
                        "sentiment":  label,
                        "word":       word,
                        "frequency":  freq,
                        "n_docs":     len(sub_label),
                    })

        aspect_cowords = pd.DataFrame(coword_rows)
        print(f"📊 Agg8 — aspect_cowords: {len(aspect_cowords)} filas "
              f"({aspect_cowords['aspect'].nunique() if len(aspect_cowords) > 0 else 0} aspectos)")

        # ── Agg 9 NUEVO: Snippets representativos por aspecto ─────────────────
        # Para cada aspecto guarda los 3 textos más positivos y 3 más negativos.
        # Permite mostrar al usuario frases reales que explican el score.
        snippet_rows = []
        for aspect, keywords in ASPECTS.items():
            pattern   = "|".join([rf"\b{kw}\b" for kw in keywords])
            body_mask  = df["body_text"].str.contains(pattern, case=False, na=False, regex=True) \
                         if "body_text" in df.columns else pd.Series([False] * len(df))
            title_mask = df["title"].str.contains(pattern, case=False, na=False, regex=True) \
                         if "title" in df.columns else pd.Series([False] * len(df))
            subset = df[body_mask | title_mask].copy()

            if len(subset) == 0:
                continue

            # Top 3 más positivos (compound más alto)
            top_pos = subset.nlargest(3, "vader_compound")
            for _, row in top_pos.iterrows():
                raw_text = str(row.get("body_text", "") or row.get("title", ""))
                snippet_rows.append({
                    "aspect":         aspect,
                    "sentiment_pole": "positive",
                    "text_snippet":   raw_text[:300].strip(),
                    "vader_compound": row["vader_compound"],
                    "sentiment_label": row["sentiment_label"],
                    "source":         row["source"],
                    "title":          str(row.get("title", ""))[:120],
                })

            # Top 3 más negativos (compound más bajo)
            top_neg = subset.nsmallest(3, "vader_compound")
            for _, row in top_neg.iterrows():
                raw_text = str(row.get("body_text", "") or row.get("title", ""))
                snippet_rows.append({
                    "aspect":         aspect,
                    "sentiment_pole": "negative",
                    "text_snippet":   raw_text[:300].strip(),
                    "vader_compound": row["vader_compound"],
                    "sentiment_label": row["sentiment_label"],
                    "source":         row["source"],
                    "title":          str(row.get("title", ""))[:120],
                })

        aspect_snippets = pd.DataFrame(snippet_rows)
        print(f"📊 Agg9 — aspect_snippets: {len(aspect_snippets)} filas "
              f"({aspect_snippets['aspect'].nunique() if len(aspect_snippets) > 0 else 0} aspectos)")

        # ── Guardar cada agregación como Parquet independiente ────────────────
        aggregations = {
            "sentiment_distribution": sent_dist,
            "sentiment_dist_time":    sent_dist_time,   # NUEVO (por periodo)
            "sentiment_trend":        sent_trend,
            "top_keywords":           top_keywords,
            "top_bigrams":            top_bigrams,       # NUEVO (n-grams)
            "keyword_sentiment":      kw_sentiment,
            "source_comparison":      source_comp,
            "volume_trend":           vol_trend,
            "aspect_sentiment":       aspect_sentiment,
            "aspect_cowords":         aspect_cowords,    # NUEVO
            "aspect_snippets":        aspect_snippets,   # NUEVO
        }

        for name, agg_df in aggregations.items():
            path = os.path.join(GOLD_PATH, f"{name}_{run_ts}.parquet")
            agg_df.to_parquet(path, index=False, engine="pyarrow")
            print(f"💾 {name}_{run_ts}.parquet guardado ({len(agg_df)} filas)")

        # ── Metadata del run ──────────────────────────────────────────────────
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

        story_path = os.path.join(GOLD_PATH, f"storytelling_summary_{run_ts}.parquet")
        story_meta.to_parquet(story_path, index=False, engine="pyarrow")
        print(f"\n📖 storytelling_summary_{run_ts}.parquet guardado")
        print(f"   Archivos de agregación: {list(aggregations.keys())}")

        if len(aspect_cowords) > 0:
            print("\n🔍 Palabras coocurrentes por aspecto (muestra):")
            for asp in aspect_cowords["aspect"].unique()[:3]:
                sub = aspect_cowords[aspect_cowords["aspect"] == asp]
                for label in ["positive", "negative"]:
                    words = sub[sub["sentiment"] == label]["word"].tolist()[:5]
                    print(f"   [{asp}][{label}]: {words}")

    # ── Flujo DAG ─────────────────────────────────────────────────────────────
    df_silver = read_silver_with_spark()
    df_nlp    = nlp_pipeline(df_silver)
    df_vader  = vader_sentiment(df_nlp)
    df_gold   = save_gold_and_governance(df_vader)
    save_storytelling(df_gold)


dag_instance = gold_processing_dag()