"""
Real Madrid Pulse — Storytelling Dashboard
Workshop 4 — Plotly Dash
Puerto: localhost:8051

Lee exclusivamente desde datalake_gold/ (Gold Parquet files).
Nunca conecta directamente a Silver, Bronze o PostgreSQL.

Estructura de archivos Gold esperados:
  - sentiment_distribution_*.parquet  → columnas: source, sentiment_label, count, pct
  - sentiment_trend_*.parquet          → columnas: week/day, source, compound_mean, n_records
  - top_keywords_*.parquet             → columnas: keyword, frequency
  - keyword_sentiment_*.parquet        → columnas: keyword, n_mentions, compound_mean, sentiment_mode
  - source_comparison_*.parquet        → columnas: source, n_records, compound_mean, pct_positive, pct_neutral, pct_negative
  - volume_trend_*.parquet             → columnas: week/day, source, n_records
  - aspect_sentiment_*.parquet         → columnas: aspect, source, n_mentions, compound_mean, pct_positive, pct_neutral, pct_negative
  - gold_realmadrid_*.parquet          → DataFrame completo con vader_compound, sentiment_label, published_at, source
"""

import os
import glob
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from dash import Dash, html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────
GOLD_PATH = "/opt/airflow/datalake_gold"

# Paleta de colores — consistente en todo el dashboard
COLORS = {
    "positive":   "#2ecc71",   # verde
    "neutral":    "#95a5a6",   # gris
    "negative":   "#e74c3c",   # rojo
    "reddit":     "#FF4500",   # naranja Reddit
    "scraping":   "#1a5276",   # azul oscuro Football-España
    "bg":         "#0d1117",   # fondo principal
    "card_bg":    "#161b22",   # fondo cards
    "border":     "#30363d",   # bordes
    "text":       "#e6edf3",   # texto principal
    "text_muted": "#8b949e",   # texto secundario
    "accent":     "#f0a500",   # dorado Real Madrid
}

SENTIMENT_COLORS = {
    "positive": COLORS["positive"],
    "neutral":  COLORS["neutral"],
    "negative": COLORS["negative"],
}

# ─────────────────────────────────────────────────────────────────────────────
# FUNCIONES DE CARGA DE DATOS
# ─────────────────────────────────────────────────────────────────────────────
def load_latest_parquet(prefix: str) -> pd.DataFrame:
    pattern = os.path.join(GOLD_PATH, f"{prefix}_*.parquet")

    print("\n" + "=" * 80)
    print(f"BUSCANDO: {pattern}")

    files = glob.glob(pattern)

    if not files:
        print(f"❌ NO SE ENCONTRARON ARCHIVOS PARA {prefix}")
        return pd.DataFrame()

    print("ARCHIVOS ENCONTRADOS:")
    for f in files:
        print("  ", os.path.basename(f))

    latest = max(files, key=os.path.getmtime)

    print(f"✅ USANDO: {os.path.basename(latest)}")

    try:
        df = pd.read_parquet(latest)

        print(f"Shape: {df.shape}")
        print("Columnas:", list(df.columns))

        return df

    except Exception as e:
        print("ERROR LEYENDO PARQUET:")
        print(e)
        return pd.DataFrame()



def get_last_updated() -> str:
    """Retorna el timestamp del Parquet Gold más reciente."""
    files = glob.glob(os.path.join(GOLD_PATH, "gold_realmadrid_*.parquet"))
    if not files:
        return "No data"
    latest = max(files, key=os.path.getmtime)
    ts     = datetime.fromtimestamp(os.path.getmtime(latest))
    return ts.strftime("%B %d, %Y — %H:%M UTC")


def load_full_gold() -> pd.DataFrame:
    """Carga el Gold base completo para cálculos de tendencia día/semana."""
    df = load_latest_parquet("gold_realmadrid")
    if df.empty:
        return df
    if "published_at" in df.columns:
        df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
        df["day"]  = df["published_at"].dt.strftime("%Y-%m-%d")
        df["week"] = df["published_at"].dt.to_period("W").astype(str)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# INICIALIZACIÓN DE LA APP
# ─────────────────────────────────────────────────────────────────────────────
app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],
    title="Real Madrid Pulse — Storytelling",
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
server = app.server

# ─────────────────────────────────────────────────────────────────────────────
# ESTILOS BASE
# ─────────────────────────────────────────────────────────────────────────────
CARD_STYLE = {
    "backgroundColor": COLORS["card_bg"],
    "border":          f"1px solid {COLORS['border']}",
    "borderRadius":    "8px",
    "padding":         "20px",
    "marginBottom":    "16px",
    "height":          "100%",
}

CHART_LAYOUT = dict(
    paper_bgcolor = COLORS["card_bg"],
    plot_bgcolor  = COLORS["card_bg"],
    font          = dict(color=COLORS["text"], family="'Helvetica Neue', Arial, sans-serif"),
    margin        = dict(l=40, r=20, t=40, b=40),
    legend        = dict(
        bgcolor     = "rgba(0,0,0,0)",
        bordercolor = COLORS["border"],
    ),
    xaxis = dict(
        gridcolor  = COLORS["border"],
        linecolor  = COLORS["border"],
        tickcolor  = COLORS["border"],
    ),
    yaxis = dict(
        gridcolor  = COLORS["border"],
        linecolor  = COLORS["border"],
        tickcolor  = COLORS["border"],
    ),
)


def make_kpi_card(value, label, color=None, subtitle=None):
    """Genera una KPI card con valor, label y color opcional."""
    return html.Div(
        style={**CARD_STYLE, "textAlign": "center"},
        children=[
            html.Div(
                str(value),
                style={
                    "fontSize":   "2.4rem",
                    "fontWeight": "700",
                    "color":      color or COLORS["accent"],
                    "lineHeight": "1.1",
                }
            ),
            html.Div(
                label,
                style={
                    "fontSize":  "0.85rem",
                    "color":     COLORS["text_muted"],
                    "marginTop": "6px",
                    "textTransform": "uppercase",
                    "letterSpacing": "0.05em",
                }
            ),
            html.Div(
                subtitle or "",
                style={
                    "fontSize":  "0.75rem",
                    "color":     COLORS["text_muted"],
                    "marginTop": "4px",
                }
            ) if subtitle else html.Div(),
        ]
    )


# ─────────────────────────────────────────────────────────────────────────────
# LAYOUT
# ─────────────────────────────────────────────────────────────────────────────
app.layout = html.Div(
    style={
        "backgroundColor": COLORS["bg"],
        "minHeight":       "100vh",
        "fontFamily":      "'Helvetica Neue', Arial, sans-serif",
        "color":           COLORS["text"],
    },
    children=[

        # ── HEADER ───────────────────────────────────────────────────────────
        html.Div(
            style={
                "borderBottom": f"2px solid {COLORS['accent']}",
                "padding":      "24px 40px 20px",
                "display":      "flex",
                "alignItems":   "center",
                "justifyContent": "space-between",
                "backgroundColor": COLORS["card_bg"],
            },
            children=[
                html.Div([
                    html.Div(
                        "⚽ REAL MADRID PULSE",
                        style={
                            "fontSize":      "1.6rem",
                            "fontWeight":    "800",
                            "color":         COLORS["accent"],
                            "letterSpacing": "0.08em",
                        }
                    ),
                    html.Div(
                        "Public Opinion & Sentiment Analysis Dashboard",
                        style={
                            "fontSize": "0.9rem",
                            "color":    COLORS["text_muted"],
                            "marginTop": "4px",
                        }
                    ),
                ]),
                html.Div([
                    html.Div(
                        "Sources: Reddit r/realmadrid + Football-España",
                        style={"fontSize": "0.8rem", "color": COLORS["text_muted"], "textAlign": "right"}
                    ),
                    html.Div(
                        id="last-updated",
                        style={"fontSize": "0.8rem", "color": COLORS["text_muted"], "textAlign": "right", "marginTop": "4px"}
                    ),
                    html.Div(
                        "Team: Ortiz · Cruz · Bautista — Universidad Distrital",
                        style={"fontSize": "0.75rem", "color": COLORS["text_muted"], "textAlign": "right", "marginTop": "4px"}
                    ),
                ])
            ]
        ),

        # ── MAIN CONTENT ─────────────────────────────────────────────────────
        html.Div(
            style={"padding": "24px 40px"},
            children=[

                # ── FILA 1: KPI CARDS ─────────────────────────────────────────
                dbc.Row(
                    id="kpi-row",
                    className="mb-3",
                    children=[]   # llenado por callback
                ),

                # ── NARRATIVE CARD ────────────────────────────────────────────
                html.Div(
                    id="narrative-card",
                    style={
                        **CARD_STYLE,
                        "borderLeft":    f"4px solid {COLORS['accent']}",
                        "marginBottom":  "24px",
                    }
                ),

                # ── FILA 2: TOGGLE + TENDENCIA TEMPORAL ──────────────────────
                html.Div(
                    style={**CARD_STYLE, "marginBottom": "24px"},
                    children=[
                        html.Div(
                            style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "marginBottom": "16px"},
                            children=[
                                html.H5(
                                    "Sentiment & Activity Over Time",
                                    style={"color": COLORS["text"], "margin": 0, "fontWeight": "600"}
                                ),
                                # Toggle día / semana
                                dbc.RadioItems(
                                    id="time-granularity",
                                    options=[
                                        {"label": " Daily",  "value": "day"},
                                        {"label": " Weekly", "value": "week"},
                                    ],
                                    value="week",
                                    inline=True,
                                    inputStyle={"marginRight": "4px"},
                                    labelStyle={
                                        "marginRight":  "16px",
                                        "color":        COLORS["text"],
                                        "fontSize":     "0.85rem",
                                        "cursor":       "pointer",
                                    },
                                    style={"color": COLORS["text"]},
                                ),
                            ]
                        ),
                        dbc.Row([
                            dbc.Col(
                                dcc.Graph(id="sentiment-trend-chart", config={"displayModeBar": False}),
                                width=7
                            ),
                            dbc.Col(
                                dcc.Graph(id="volume-trend-chart", config={"displayModeBar": False}),
                                width=5
                            ),
                        ]),
                    ]
                ),

                # ── FILA 3: DISTRIBUCIÓN + COMPARACIÓN FUENTES ───────────────
                dbc.Row([
                    dbc.Col(
                        html.Div(
                            style=CARD_STYLE,
                            children=[
                                html.H5("Overall Sentiment Distribution", style={"color": COLORS["text"], "fontWeight": "600", "marginBottom": "12px"}),
                                dcc.Graph(id="sentiment-donut", config={"displayModeBar": False}),
                            ]
                        ),
                        width=5
                    ),
                    dbc.Col(
                        html.Div(
                            style=CARD_STYLE,
                            children=[
                                html.H5("Reddit vs Football-España", style={"color": COLORS["text"], "fontWeight": "600", "marginBottom": "12px"}),
                                dcc.Graph(id="source-comparison-chart", config={"displayModeBar": False}),
                            ]
                        ),
                        width=7
                    ),
                ], className="mb-3"),

                # ── FILA 4: KEYWORDS + ASPECT SENTIMENT ──────────────────────
                dbc.Row([
                    dbc.Col(
                        html.Div(
                            style=CARD_STYLE,
                            children=[
                                html.H5("Top Keywords in Corpus", style={"color": COLORS["text"], "fontWeight": "600", "marginBottom": "12px"}),
                                dcc.Graph(id="keywords-chart", config={"displayModeBar": False}),
                            ]
                        ),
                        width=5
                    ),
                    dbc.Col(
                        html.Div(
                            style=CARD_STYLE,
                            children=[
                                html.H5("Sentiment by Key Topic", style={"color": COLORS["text"], "fontWeight": "600", "marginBottom": "12px"}),
                                html.Div(
                                    "Who drives the conversation and with what tone?",
                                    style={"fontSize": "0.8rem", "color": COLORS["text_muted"], "marginBottom": "12px"}
                                ),
                                dcc.Graph(id="aspect-sentiment-chart", config={"displayModeBar": False}),
                            ]
                        ),
                        width=7
                    ),
                ], className="mb-3"),

            ]
        ),

        # Intervalo para auto-refresh cada 5 minutos
        dcc.Interval(id="interval", interval=5 * 60 * 1000, n_intervals=0),
    ]
)


# ─────────────────────────────────────────────────────────────────────────────
# CALLBACKS
# ─────────────────────────────────────────────────────────────────────────────

@app.callback(
    Output("last-updated", "children"),
    Input("interval", "n_intervals"),
)
def update_timestamp(_):
    return f"Last updated: {get_last_updated()}"

@app.callback(
    Output("kpi-row", "children"),
    Output("narrative-card", "children"),
    Input("interval", "n_intervals")
)

def update_kpis(_):
    """KPI cards + Narrative summary card."""
    df_dist    = load_latest_parquet("sentiment_distribution")
    df_src     = load_latest_parquet("source_comparison")
    df_gold    = load_full_gold()

    # ── KPIs ─────────────────────────────────────────────────────────────────
    total = int(df_dist["count"].sum()) if not df_dist.empty and "count" in df_dist.columns else 0

    # Sentimiento global dominante
    if not df_dist.empty and "count" in df_dist.columns and "sentiment_label" in df_dist.columns:
        global_dist   = df_dist.groupby("sentiment_label")["count"].sum()
        dominant      = global_dist.idxmax() if not global_dist.empty else "neutral"
        dominant_pct  = round(global_dist.max() / global_dist.sum() * 100, 1) if global_dist.sum() > 0 else 0
        dominant_color = COLORS.get(dominant, COLORS["text"])
    else:
        dominant, dominant_pct, dominant_color = "N/A", 0, COLORS["text"]

    # Compound promedio global
    compound_avg = round(df_gold["vader_compound"].mean(), 3) if not df_gold.empty and "vader_compound" in df_gold.columns else 0

    # Fuente más activa
    if not df_src.empty and "n_records" in df_src.columns and "source" in df_src.columns:
        top_source = df_src.loc[df_src["n_records"].idxmax(), "source"] if len(df_src) > 0 else "N/A"
        top_source_label = "Reddit" if top_source == "reddit" else "Football-España"
    else:
        top_source_label = "N/A"

    kpi_cards = [
        dbc.Col(make_kpi_card(f"{total:,}", "Total Mentions Analyzed"), width=3),
        dbc.Col(make_kpi_card(f"{dominant_pct}%", "Dominant Sentiment", color=dominant_color, subtitle=dominant.capitalize()), width=3),
        dbc.Col(make_kpi_card(f"{compound_avg:+.3f}", "Avg Compound Score", color=COLORS["positive"] if compound_avg > 0 else COLORS["negative"]), width=3),
        dbc.Col(make_kpi_card(top_source_label, "Most Active Source", color=COLORS["accent"]), width=3),
    ]

    # ── Narrative card ────────────────────────────────────────────────────────
    if not df_dist.empty and not df_gold.empty:
        pos_pct = round(df_dist[df_dist["sentiment_label"] == "positive"]["count"].sum() / total * 100, 1) if total > 0 else 0
        neg_pct = round(df_dist[df_dist["sentiment_label"] == "negative"]["count"].sum() / total * 100, 1) if total > 0 else 0

        # Aspecto más positivo y más negativo
        df_asp = load_latest_parquet("aspect_sentiment")
        best_aspect  = "N/A"
        worst_aspect = "N/A"
        if not df_asp.empty and "compound_mean" in df_asp.columns:
            global_asp   = df_asp[df_asp["source"] == "all"] if "source" in df_asp.columns else df_asp
            if not global_asp.empty:
                best_aspect  = global_asp.loc[global_asp["compound_mean"].idxmax(), "aspect"].capitalize()
                worst_aspect = global_asp.loc[global_asp["compound_mean"].idxmin(), "aspect"].capitalize()

        narrative = [
            html.Div("📊 Key Insight", style={"fontSize": "0.75rem", "color": COLORS["accent"], "fontWeight": "700", "letterSpacing": "0.1em", "marginBottom": "8px"}),
            html.P(
                f"Analysis of {total:,} mentions from Reddit and Football-España shows that public opinion about "
                f"Real Madrid is {dominant.upper()} ({dominant_pct}% of all content, compound score: {compound_avg:+.3f}). "
                f"Positive sentiment accounts for {pos_pct}% of mentions, while negative sentiment represents {neg_pct}%. "
                f"The topic generating the most positive reactions is {best_aspect}, "
                f"while {worst_aspect} drives the most critical discussion.",
                style={"fontSize": "0.95rem", "color": COLORS["text"], "margin": 0, "lineHeight": "1.6"}
            ),
        ]
    else:
        narrative = [html.P("No data available. Run the Gold DAG first.", style={"color": COLORS["text_muted"]})]

    return kpi_cards, narrative


@app.callback(
    Output("sentiment-trend-chart", "figure"),
    Output("volume-trend-chart", "figure"),
    Input("time-granularity", "value"),
    Input("interval", "n_intervals"),
)
def update_trend_charts(granularity, _):
    """
    Actualiza los charts de tendencia y volumen según el toggle día/semana.
    Lee del Gold base (gold_realmadrid_*.parquet) y calcula la agrupación
    en tiempo real para soportar ambas granularidades.
    """
    df = load_full_gold()

    time_col    = granularity  # "day" o "week"
    xlabel      = "Date" if granularity == "day" else "Week"
    tick_format = "%b %d" if granularity == "day" else "%b %d"

    # ── Sentiment trend ───────────────────────────────────────────────────────
    fig_trend = go.Figure()

    if not df.empty and time_col in df.columns and "vader_compound" in df.columns:
        for src, src_label, color in [
            ("reddit",   "Reddit",          COLORS["reddit"]),
            ("scraping", "Football-España", COLORS["scraping"]),
        ]:
            subset = df[df["source"] == src] if "source" in df.columns else df
            if subset.empty:
                continue
            trend = (
                subset.groupby(time_col)["vader_compound"]
                .mean()
                .reset_index()
                .sort_values(time_col)
            )
            fig_trend.add_trace(go.Scatter(
                x         = trend[time_col],
                y         = trend["vader_compound"],
                name      = src_label,
                mode      = "lines+markers",
                line      = dict(color=color, width=2),
                marker    = dict(size=5),
                hovertemplate = f"<b>{src_label}</b><br>{xlabel}: %{{x}}<br>Avg Compound: %{{y:.3f}}<extra></extra>",
            ))

        # Línea de referencia en 0
        fig_trend.add_hline(
            y=0, line_dash="dash",
            line_color=COLORS["border"],
            annotation_text="Neutral",
            annotation_font_color=COLORS["text_muted"],
        )

    fig_trend.update_layout(**CHART_LAYOUT)

    fig_trend.update_layout(
        title=dict(
            text=f"Average Sentiment Score ({xlabel})",
            font=dict(size=13),
            x=0
        ),
        xaxis_title=xlabel,
        yaxis_title="Compound Score",
        height=300
    )

    fig_trend.update_yaxes(range=[-1, 1])

    fig_trend.update_layout(
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )

    # ── Volume trend ──────────────────────────────────────────────────────────
    fig_vol = go.Figure()

    if not df.empty and time_col in df.columns:
        for src, src_label, color in [
            ("reddit",   "Reddit",          COLORS["reddit"]),
            ("scraping", "Football-España", COLORS["scraping"]),
        ]:
            subset = df[df["source"] == src] if "source" in df.columns else df
            if subset.empty:
                continue
            vol = (
                subset.groupby(time_col)
                .size()
                .reset_index(name="n_records")
                .sort_values(time_col)
            )
            fig_vol.add_trace(go.Bar(
                x           = vol[time_col],
                y           = vol["n_records"],
                name        = src_label,
                marker_color = color,
                opacity     = 0.85,
                hovertemplate = f"<b>{src_label}</b><br>{xlabel}: %{{x}}<br>Records: %{{y}}<extra></extra>",
            ))

    fig_vol.update_layout(**CHART_LAYOUT)

    fig_vol.update_layout(
        title=dict(
            text=f"Activity Volume ({xlabel})",
            font=dict(size=13),
            x=0
        ),
        xaxis_title=xlabel,
        yaxis_title="Number of Records",
        barmode="stack",
        height=300,
    )

    fig_vol.update_layout(
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            bordercolor=COLORS["border"],
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        )
    )

    return fig_trend, fig_vol


@app.callback(
    Output("sentiment-donut", "figure"),
    Input("interval", "n_intervals"),
)
def update_donut(_):
    """Donut chart de distribución de sentimiento global."""
    df = load_latest_parquet("sentiment_distribution")

    fig = go.Figure()

    if not df.empty and "sentiment_label" in df.columns and "count" in df.columns:
        global_dist = df.groupby("sentiment_label")["count"].sum().reset_index()
        labels      = global_dist["sentiment_label"].tolist()
        values      = global_dist["count"].tolist()
        colors      = [SENTIMENT_COLORS.get(l, COLORS["text"]) for l in labels]

        fig.add_trace(go.Pie(
            labels           = [l.capitalize() for l in labels],
            values           = values,
            hole             = 0.55,
            marker_colors    = colors,
            textinfo         = "percent+label",
            textfont         = dict(size=12),
            hovertemplate    = "<b>%{label}</b><br>Count: %{value}<br>Share: %{percent}<extra></extra>",
            insidetextorientation = "radial",
        ))

        total = sum(values)
        dominant = global_dist.loc[global_dist["count"].idxmax(), "sentiment_label"]
        fig.add_annotation(
            text=f"<b>{dominant.upper()}</b><br><span style='font-size:11px'>{total:,} total</span>",
            x=0.5, y=0.5,
            font=dict(size=14, color=SENTIMENT_COLORS.get(dominant, COLORS["text"])),
            showarrow=False,
        )

        fig.update_layout(**CHART_LAYOUT)

        fig.update_layout(
            height=320,
            showlegend=True,
            annotations=fig.layout.annotations,
        )

        fig.update_layout(
            legend=dict(
                bgcolor="rgba(0,0,0,0)",
                bordercolor=COLORS["border"],
                orientation="h",
                yanchor="top",
                y=-0.05,
                xanchor="center",
                x=0.5,
            )
)

    return fig


@app.callback(
    Output("source-comparison-chart", "figure"),
    Input("interval", "n_intervals"),
)
def update_source_comparison(_):
    """Bar chart de comparación Reddit vs Scraping."""
    df = load_latest_parquet("source_comparison")

    fig = go.Figure()

    if not df.empty:
        sources      = df["source"].tolist() if "source" in df.columns else []
        src_labels   = ["Reddit" if s == "reddit" else "Football-España" for s in sources]
        src_colors   = [COLORS["reddit"] if s == "reddit" else COLORS["scraping"] for s in sources]

        metrics = [
            ("pct_positive", "Positive %", COLORS["positive"]),
            ("pct_neutral",  "Neutral %",  COLORS["neutral"]),
            ("pct_negative", "Negative %", COLORS["negative"]),
        ]

        for col, name, color in metrics:
            if col in df.columns:
                fig.add_trace(go.Bar(
                    name             = name,
                    x                = src_labels,
                    y                = df[col].tolist(),
                    marker_color     = color,
                    hovertemplate    = f"<b>%{{x}}</b><br>{name}: %{{y:.1f}}%<extra></extra>",
                ))

        fig.update_layout(**CHART_LAYOUT)

        fig.update_layout(
            barmode="group",
            height=320,
            xaxis_title="Source",
            yaxis_title="Percentage (%)"
        )

        fig.update_layout(
            legend=dict(
                bgcolor="rgba(0,0,0,0)",
                bordercolor=COLORS["border"],
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )

    return fig


@app.callback(
    Output("keywords-chart", "figure"),
    Input("interval", "n_intervals"),
)
def update_keywords(_):
    """Horizontal bar chart de top keywords coloreado por sentimiento."""
    df_kw  = load_latest_parquet("top_keywords")
    df_kws = load_latest_parquet("keyword_sentiment")

    fig = go.Figure()

    if not df_kw.empty and "keyword" in df_kw.columns and "frequency" in df_kw.columns:
        top15 = df_kw.head(15).copy()

        # Enriquecer con sentimiento si está disponible
        if not df_kws.empty and "keyword" in df_kws.columns:
            top15 = top15.merge(df_kws[["keyword", "sentiment_mode"]], on="keyword", how="left")
            top15["sentiment_mode"] = top15["sentiment_mode"].fillna("neutral")
        else:
            top15["sentiment_mode"] = "neutral"

        top15  = top15.sort_values("frequency")
        colors = [SENTIMENT_COLORS.get(s, COLORS["neutral"]) for s in top15["sentiment_mode"]]

        fig.add_trace(go.Bar(
            x                = top15["frequency"],
            y                = top15["keyword"],
            orientation      = "h",
            marker_color     = colors,
            hovertemplate    = "<b>%{y}</b><br>Frequency: %{x}<extra></extra>",
        ))

    fig.update_layout(
        **CHART_LAYOUT,
        height     = 380,
        xaxis_title = "Frequency",
        yaxis_title = "",
        showlegend = False,
    )

    return fig


def debug_gold(_):

    df_dist = load_latest_parquet("sentiment_distribution")
    df_gold = load_latest_parquet("gold_realmadrid")
    df_asp  = load_latest_parquet("aspect_sentiment")

    return [
        html.Pre(
            f"""
sentiment_distribution:
{df_dist.head().to_string() if not df_dist.empty else "VACIO"}

gold_realmadrid:
{df_gold.head().to_string() if not df_gold.empty else "VACIO"}

aspect_sentiment:
{df_asp.head().to_string() if not df_asp.empty else "VACIO"}
            """,
            style={"color": "white"}
        )
    ]
@app.callback(
    Output("aspect-sentiment-chart", "figure"),
    Input("interval", "n_intervals"),
)
def update_aspect_sentiment(_):
    """
    Bar chart de sentimiento por aspecto (Mourinho, Mbappe, Vinicius, etc.)
    Muestra compound_mean por aspecto, coloreado por sentimiento dominante.
    """
    df = load_latest_parquet("aspect_sentiment")

    fig = go.Figure()

    if not df.empty and "aspect" in df.columns and "compound_mean" in df.columns:
        # Solo filas globales (source == "all")
        if "source" in df.columns:
            df_global = df[df["source"] == "all"].copy()
        else:
            df_global = df.copy()

        if df_global.empty:
            df_global = df.copy()

        df_global = df_global.sort_values("compound_mean")

        colors = [
            COLORS["positive"] if v >= 0.05 else
            COLORS["negative"] if v <= -0.05 else
            COLORS["neutral"]
            for v in df_global["compound_mean"]
        ]

        fig.add_trace(go.Bar(
            x             = df_global["compound_mean"],
            y             = df_global["aspect"].str.capitalize(),
            orientation   = "h",
            marker_color  = colors,
            text          = [f"{v:+.3f}" for v in df_global["compound_mean"]],
            textposition  = "outside",
            textfont      = dict(size=11),
            customdata    = df_global["n_mentions"] if "n_mentions" in df_global.columns else None,
            hovertemplate = (
                "<b>%{y}</b><br>"
                "Avg Compound: %{x:.3f}<br>"
                "Mentions: %{customdata}<extra></extra>"
            ) if "n_mentions" in df_global.columns else "<b>%{y}</b><br>Avg Compound: %{x:.3f}<extra></extra>",
        ))

        # Línea vertical en 0
        fig.add_vline(x=0, line_dash="dash", line_color=COLORS["border"])

        fig.update_layout(**CHART_LAYOUT)

        fig.update_layout(
                height=380,
                xaxis_title="Average Compound Score",
                yaxis_title="",
                showlegend=False
            )

        fig.update_xaxes(range=[-1, 1])

    return fig


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(
        host  = "0.0.0.0",
        port  = 8051,
        debug = False,
    )