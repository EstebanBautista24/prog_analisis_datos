from airflow.decorators import dag, task
from datetime import datetime, timedelta

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
            url, title, author, published_at, body_text, body_text_clean,
            title_clean, body_text_missing, source

        Columnas exclusivas de reddit : score, num_comments, subreddit
        Columnas exclusivas de scraping: reading_time_min

        PySpark rellena con null las columnas ausentes en cada fuente,
        por lo que el schema unificado es siempre completo.
        """
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        import os

        # ── Validación previa ────────────────────────────────────────────────
        parquet_files = [
            f for f in os.listdir(SILVER_PATH)
            if f.endswith(".parquet")
        ] if os.path.exists(SILVER_PATH) else []

        if not parquet_files:
            raise FileNotFoundError(
                f"No se encontraron archivos Parquet en {SILVER_PATH}. "
                "Ejecuta primero los DAGs Silver de Reddit y Scraping."
            )

        print(f"📂 Archivos Parquet encontrados en Silver ({len(parquet_files)}):")
        for f in sorted(parquet_files):
            print(f"   └─ {f}")

        # ── SparkSession ─────────────────────────────────────────────────────
        spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("GoldLayer_RealMadrid")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            # Permite leer Parquets con schemas ligeramente distintos
            .config("spark.sql.parquet.mergeSchema", "true")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        # ── UNION automático de todos los Parquet ────────────────────────────
        # reddit_api_realmadrid_*.parquet  ──┐
        #                                    ├─► DataFrame unificado
        # web_scraping_realmadrid_*.parquet ──┘
        #
        # Las columnas exclusivas de cada fuente quedan como null en la otra.
        print(f"\n🔗 Uniendo todos los Parquet de: {SILVER_PATH}")
        df = spark.read.parquet(SILVER_PATH)

        total = df.count()
        print(f"\n✅ Total registros unidos: {total}")

        # Distribución por fuente
        print("\n📊 Registros por fuente (source):")
        df.groupBy("source").count().show()

        # Schema unificado
        print("📋 Schema unificado:")
        df.printSchema()

        # ── Selección de columnas Gold ────────────────────────────────────────
        # Columnas comunes + exclusivas de cada fuente (null donde no aplica)
        GOLD_COLUMNS = [
            "url",
            "title",
            "author",
            "published_at",
            "body_text",
            "body_text_clean",
            "title_clean",
            "body_text_missing",
            "source",
            # Reddit-only (null en scraping)
            "score",
            "num_comments",
            "subreddit",
            # Scraping-only (null en reddit)
            "reading_time_min",
        ]

        existing = df.columns
        selected = [c for c in GOLD_COLUMNS if c in existing]
        df = df.select(*selected)

        # ── Deduplicación global cross-source ─────────────────────────────────
        before_dedup = df.count()
        df = df.dropDuplicates(["url"])
        after_dedup = df.count()
        print(f"\n🔁 Deduplicación global: {before_dedup - after_dedup} duplicados eliminados")

        # ── Convertir a pandas para XCom ─────────────────────────────────────
        df_pandas = df.toPandas()
        spark.stop()

        print(f"\n💾 DataFrame Gold base listo:")
        print(f"   └─ Filas  : {len(df_pandas)}")
        print(f"   └─ Columnas: {list(df_pandas.columns)}")

        return df_pandas.to_json(orient="records")

    # ─────────────────────────────────────────────────────────────────────────
    # TASK 2 — Pipeline NLP completo sobre body_text_clean
    # ─────────────────────────────────────────────────────────────────────────
    @task()
    def nlp_pipeline(df_json: str) -> str:
        """
        Pipeline NLP sobre body_text_clean (ya sin URLs, puntuación y ruido):
          1. Tokenización  — nltk word_tokenize
          2. Lowercase
          3. Remoción de stopwords — nltk english
          4. Lematización — spaCy en_core_web_sm

        Produce:
          text_processed  — tokens limpios y lematizados de body_text_clean
          title_processed — tokens limpios y lematizados de title_clean
          token_count     — número de tokens en text_processed
        """
        import ssl
        import certifi
        import nltk
        import spacy
        import pandas as pd
        from io import StringIO

        # ── NLTK resources ───────────────────────────────────────────────────
        ssl._create_default_https_context = lambda: ssl.create_default_context(
            cafile=certifi.where()
        )
        for resource in ["stopwords", "punkt", "punkt_tab"]:
            nltk.download(resource, quiet=True)

        from nltk.corpus import stopwords
        from nltk.tokenize import word_tokenize

        STOPWORDS = set(stopwords.words("english"))

        # ── spaCy ────────────────────────────────────────────────────────────
        try:
            nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
        except OSError:
            raise RuntimeError(
                "Modelo spaCy no encontrado. "
                "Ejecuta: python -m spacy download en_core_web_sm"
            )

        def process_text(text: str) -> str:
            if not isinstance(text, str) or not text.strip():
                return ""

            # 1 — Tokenizar
            tokens = word_tokenize(text)

            # 2 — Lowercase
            tokens_lower = [t.lower() for t in tokens]

            # 3 — Filtrar stopwords y no-alfabéticos
            tokens_clean = [
                t for t in tokens_lower
                if t.isalpha() and t not in STOPWORDS and len(t) >= 2
            ]

            if not tokens_clean:
                return ""

            # 4 — Lematizar con spaCy
            doc = nlp(" ".join(tokens_clean))
            lemmas = [
                token.lemma_
                for token in doc
                if not token.is_stop
                and not token.is_punct
                and not token.is_space
                and len(token.lemma_) >= 2
            ]

            return " ".join(lemmas)

        # ── Aplicar ──────────────────────────────────────────────────────────
        df = pd.read_json(StringIO(df_json), orient="records")

        print(f"🔤 Aplicando pipeline NLP a {len(df)} registros...")

        df["text_processed"]  = df["body_text_clean"].apply(process_text)
        df["title_processed"] = df["title_clean"].apply(process_text)
        df["token_count"]     = df["text_processed"].apply(
            lambda x: len(x.split()) if isinstance(x, str) and x.strip() else 0
        )

        # Stats por fuente
        for src in df["source"].unique():
            subset = df[df["source"] == src]
            empty = (subset["text_processed"].str.strip() == "").sum()
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
                    "vader_pos": 0.0,
                    "vader_neg": 0.0,
                    "vader_neu": 1.0,
                    "sentiment_label": "neutral",
                }
            sc = analyzer.polarity_scores(text)
            return {
                "vader_compound": round(sc["compound"], 4),
                "vader_pos":      round(sc["pos"],      4),
                "vader_neg":      round(sc["neg"],      4),
                "vader_neu":      round(sc["neu"],      4),
                "sentiment_label": vader_label(sc["compound"]),
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
        Persiste dos archivos en datalake_gold/:

        1. gold_realmadrid_YYYYMMDD_HHMMSS.parquet
           DataFrame completo con NLP + VADER por registro.
           Fuente única de verdad para el dashboard de Workshop 4.

        2. governance_YYYYMMDD_HHMMSS.parquet
           KPIs de calidad de datos calculados sobre el Gold base:
             - total_records          : registros totales
             - records_by_source      : registros por fuente (reddit/scraping)
             - null_rate_*            : tasa de nulos por columna clave
             - body_text_missing_rate : % de registros sin cuerpo de texto
             - duplicate_rate         : % deduplicados en Task 1
             - token_count_mean/std/min/max : estadísticas de tokens NLP
             - text_length_mean/min/max     : largo de body_text_clean
             - sentiment_distribution : % por label (positive/neutral/negative)
             - compound_mean/std       : estadísticas de compound score
             - pipeline_run_ts         : timestamp de este run
        """
        import pandas as pd
        import numpy as np
        import os
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
        total = len(df)

        # Null rates para columnas clave
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

        # Registros por fuente
        by_source = df["source"].value_counts().to_dict()

        # body_text_missing rate
        body_missing_rate = round(
            df["body_text_missing"].sum() / total * 100, 2
        ) if "body_text_missing" in df.columns else None

        # Token count stats
        tc = df["token_count"] if "token_count" in df.columns else pd.Series(dtype=float)
        token_stats = {
            "token_count_mean": round(tc.mean(), 2),
            "token_count_std":  round(tc.std(),  2),
            "token_count_min":  int(tc.min()),
            "token_count_max":  int(tc.max()),
        } if not tc.empty else {}

        # Text length stats (sobre body_text_clean)
        text_len = df["body_text_clean"].dropna().apply(len) if "body_text_clean" in df.columns else pd.Series(dtype=float)
        text_len_stats = {
            "text_length_mean": round(text_len.mean(), 2),
            "text_length_min":  int(text_len.min()),
            "text_length_max":  int(text_len.max()),
        } if not text_len.empty else {}

        # Sentiment distribution
        sent_dist = df["sentiment_label"].value_counts(normalize=True).mul(100).round(2).to_dict() if "sentiment_label" in df.columns else {}
        sent_dist_named = {f"sentiment_pct_{k}": v for k, v in sent_dist.items()}

        # Compound stats
        compound = df["vader_compound"] if "vader_compound" in df.columns else pd.Series(dtype=float)
        compound_stats = {
            "compound_mean": round(compound.mean(), 4),
            "compound_std":  round(compound.std(),  4),
        } if not compound.empty else {}

        # Construir governance row
        governance_row = {
            "pipeline_run_ts":       run_ts,
            "total_records":         total,
            "records_reddit":        by_source.get("reddit", 0),
            "records_scraping":      by_source.get("scraping", 0),
            "body_text_missing_rate": body_missing_rate,
            **null_rates,
            **token_stats,
            **text_len_stats,
            **sent_dist_named,
            **compound_stats,
        }

        df_gov = pd.DataFrame([governance_row])
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

        storytelling_YYYYMMDD_HHMMSS.parquet
          ├── sentiment_distribution.parquet
          │     Conteo y % de positive/neutral/negative por fuente
          │     → KPI cards + donut chart del dashboard
          ├── sentiment_trend.parquet
          │     Compound promedio por semana y por fuente
          │     → Line chart de tendencia temporal
          ├── top_keywords.parquet
          │     Top 30 tokens por frecuencia en text_processed
          │     → Word cloud + bar chart
          ├── keyword_sentiment.parquet
          │     Compound promedio por keyword top-20
          │     → Bar chart horizontal con color por sentimiento
          ├── source_comparison.parquet
          │     Sentimiento y volumen por fuente
          │     → Side-by-side bar chart reddit vs scraping
          └── volume_trend.parquet
                Registros por semana
                → Area chart de actividad temporal

        Todos se guardan también consolidados en storytelling_YYYYMMDD_HHMMSS.parquet
        como un dict serializado para fácil lectura en el dashboard.
        """
        import pandas as pd
        import numpy as np
        import os
        from io import StringIO
        from collections import Counter

        df = pd.read_json(StringIO(df_json), orient="records")

        if "published_at" in df.columns:
            df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
            df["week"] = df["published_at"].dt.to_period("W").astype(str)

        os.makedirs(GOLD_PATH, exist_ok=True)
        run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        total  = len(df)

        # ── Agg 1: Sentiment distribution por fuente ──────────────────────────
        # → KPI cards + donut chart
        sent_dist = (
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
        kw_rows = []
        for word in top20_words:
            mask = df["text_processed"].str.contains(
                rf"\b{word}\b", na=False, regex=True
            )
            subset = df[mask]
            if len(subset) > 0:
                kw_rows.append({
                    "keyword":        word,
                    "n_mentions":     len(subset),
                    "compound_mean":  round(subset["vader_compound"].mean(), 4),
                    "sentiment_mode": subset["sentiment_label"].mode().iloc[0],
                })
        kw_sentiment = pd.DataFrame(kw_rows)
        print(f"📊 Agg4 — keyword_sentiment: {len(kw_sentiment)} keywords")

        # ── Agg 5: Comparación por fuente ─────────────────────────────────────
        # → Side-by-side bar chart reddit vs scraping
        source_comp = (
            df.groupby("source")
            .agg(
                n_records        = ("url",            "count"),
                compound_mean    = ("vader_compound",  "mean"),
                compound_std     = ("vader_compound",  "std"),
                pct_positive     = ("sentiment_label", lambda x: (x == "positive").mean() * 100),
                pct_neutral      = ("sentiment_label", lambda x: (x == "neutral").mean()  * 100),
                pct_negative     = ("sentiment_label", lambda x: (x == "negative").mean() * 100),
                avg_token_count  = ("token_count",     "mean"),
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

        # ── Guardar cada agregación como Parquet independiente ────────────────
        aggregations = {
            "sentiment_distribution": sent_dist,
            "sentiment_trend":        sent_trend,
            "top_keywords":           top_keywords,
            "keyword_sentiment":      kw_sentiment,
            "source_comparison":      source_comp,
            "volume_trend":           vol_trend,
        }

        for name, agg_df in aggregations.items():
            path = os.path.join(GOLD_PATH, f"{name}_{run_ts}.parquet")
            agg_df.to_parquet(path, index=False, engine="pyarrow")
            print(f"💾 {name}_{run_ts}.parquet guardado ({len(agg_df)} filas)")

        # ── Storytelling consolidado ──────────────────────────────────────────
        story_meta = pd.DataFrame([{
            "pipeline_run_ts":   run_ts,
            "total_records":     total,
            "sources":           str(df["source"].unique().tolist()),
            "weeks_covered":     df["week"].nunique() if "week" in df.columns else 0,
            "top_keyword":       top_keywords.iloc[0]["keyword"] if len(top_keywords) > 0 else "",
            "overall_sentiment": df["sentiment_label"].mode().iloc[0] if len(df) > 0 else "",
            "overall_compound":  round(df["vader_compound"].mean(), 4),
            "aggregations_saved": str(list(aggregations.keys())),
        }])
        story_path = os.path.join(GOLD_PATH, f"storytelling_{run_ts}.parquet")
        story_meta.to_parquet(story_path, index=False, engine="pyarrow")
        print(f"\n📖 storytelling_{run_ts}.parquet guardado (metadata consolidada)")

    # ─────────────────────────────────────────────────────────────────────────
    # Flujo DAG
    # ─────────────────────────────────────────────────────────────────────────
    df_silver  = read_silver_with_spark()
    df_nlp     = nlp_pipeline(df_silver)
    df_vader   = vader_sentiment(df_nlp)
    df_gold    = save_gold_and_governance(df_vader)
    save_storytelling(df_gold)


dag_instance = gold_processing_dag()