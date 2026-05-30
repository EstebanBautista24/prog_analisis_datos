"""
Real Madrid Pulse — Storytelling Dashboard
Workshop 4 — Plotly Dash — Puerto 8051
Fondo blanco, estética editorial, narrativa por sección.

Secciones:
  01 — El veredicto        (KPI cards + summary)
  02 — Evolución temporal  (line chart + volume bar)
  03 — Dos voces           (donut + source comparison)
  04 — El vocabulario      (keywords bar chart)
  05 — Los protagonistas   (aspect sentiment bar)
  06 — ¿Por qué?           (NUEVO: palabras coocurrentes + snippets por aspecto)
"""

import os
import glob
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, html, dcc, Input, Output
import dash_bootstrap_components as dbc

# ─────────────────────────────────────────────────────────────────────────────
GOLD_PATH = "/opt/airflow/datalake_gold"

COLORS = {
    "positive":  "#2f7d52",   # verde pino editorial (menos "bootstrap")
    "neutral":   "#9a9081",    # taupe cálido
    "negative":  "#a8322d",    # oxblood / ladrillo
    "reddit":    "#c85a1f",     # naranja quemado
    "scraping":  "#22405e",     # azul tinta
    "bg":        "#faf9f6",     # blanco papel cálido (no estéril)
    "card_bg":   "#f3efe6",     # crema
    "border":    "#e6ded2",      # borde tenue (sólido, compatible con Plotly)
    "border_solid": "#ddd4c6",
    "text":      "#1c1a17",      # tinta casi negra, cálida
    "muted":     "#5f5a52",
    "dim":       "#9c958a",
    "accent":    "#b08a2e",       # oro apagado
    "accent2":   "#7a1818",       # vino
    "shadow":    "rgba(40, 33, 22, 0.06)",
}

FONT_HEAD = "'Playfair Display', Georgia, serif"
FONT_BODY = "'IBM Plex Sans', Helvetica, sans-serif"
FONT_MONO = "'IBM Plex Mono', monospace"

SENTIMENT_COLORS = {
    "positive": COLORS["positive"],
    "neutral":  COLORS["neutral"],
    "negative": COLORS["negative"],
}

CHART_LAYOUT = dict(
    paper_bgcolor = "rgba(0,0,0,0)",
    plot_bgcolor  = "rgba(0,0,0,0)",
    font          = dict(color=COLORS["text"], family=FONT_BODY, size=11.5),
    margin        = dict(l=40, r=20, t=40, b=40),
    legend        = dict(bgcolor="rgba(0,0,0,0)", bordercolor="rgba(0,0,0,0)"),
    hoverlabel    = dict(
        bgcolor="#1c1a17", bordercolor="#1c1a17",
        font=dict(color="#faf9f6", family=FONT_MONO, size=11),
    ),
    xaxis = dict(gridcolor="rgba(0,0,0,0.05)", linecolor=COLORS["border"],
                 tickcolor=COLORS["border"], zeroline=False,
                 tickfont=dict(size=10.5, color=COLORS["muted"])),
    yaxis = dict(gridcolor="rgba(0,0,0,0.05)", linecolor=COLORS["border"],
                 tickcolor=COLORS["border"], zeroline=False,
                 tickfont=dict(size=10.5, color=COLORS["muted"])),
)

# Etiquetas legibles para los aspectos
# Etiquetas legibles para los aspectos.
# IMPORTANTE: las claves deben coincidir EXACTAMENTE con el dict ASPECTS
# del DAG (gold_processing_dag.py). Para agregar un protagonista nuevo,
# añádelo en ambos lugares.
LABELS = {
    "mourinho":   "Mourinho",
    "mbappe":     "Mbappé",
    "vinicius":   "Vinicius Jr.",
    "carvajal":   "Carvajal",
    "transfers":  "Fichajes",
    "injuries":   "Lesiones",
    "arbeloa":    "Arbeloa",
    "bellingham": "Bellingham",
    "valverde":   "Valverde",
    "barcelona":  "Barcelona",
    "clasico":    "El Clásico",
    "tchouameni": "Tchouaméni",
    "ancelotti":  "Ancelotti",
}

# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADERS
# ─────────────────────────────────────────────────────────────────────────────
def load_latest(prefix):
    files = glob.glob(os.path.join(GOLD_PATH, f"{prefix}_*.parquet"))
    if not files:
        return pd.DataFrame()
    try:
        return pd.read_parquet(max(files, key=os.path.getmtime))
    except Exception:
        return pd.DataFrame()


def load_gold():
    df = load_latest("gold_realmadrid")
    if df.empty:
        return df
    if "published_at" in df.columns:
        df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
        df["day"]  = df["published_at"].dt.strftime("%Y-%m-%d")
        df["week"] = df["published_at"].dt.to_period("W").astype(str)
    return df


def last_updated():
    files = glob.glob(os.path.join(GOLD_PATH, "gold_realmadrid_*.parquet"))
    if not files:
        return "—"
    ts = datetime.fromtimestamp(os.path.getmtime(max(files, key=os.path.getmtime)))
    return ts.strftime("%d %b %Y, %H:%M")

# ─────────────────────────────────────────────────────────────────────────────
# COMPONENTES DE UI
# ─────────────────────────────────────────────────────────────────────────────
CARD = {
    "backgroundColor": COLORS["card_bg"],
    "border":          f"1px solid {COLORS['border']}",
    "borderRadius":    "6px",
    "padding":         "20px",
    "marginBottom":    "16px",
    "boxShadow":       f"0 1px 3px {COLORS['shadow']}",
}


def sec_label(txt):
    return html.Div(txt, style={
        "fontFamily": FONT_MONO, "fontSize": "0.62rem",
        "color": COLORS["accent"], "letterSpacing": "0.18em",
        "textTransform": "uppercase", "marginBottom": "6px",
    })


def sec_q(txt):
    return html.Div(txt, style={
        "fontFamily": FONT_HEAD, "fontSize": "1.4rem",
        "fontWeight": "700", "fontStyle": "italic",
        "color": COLORS["text"], "lineHeight": "1.3", "marginBottom": "6px",
    })


def sec_a(txt):
    return html.Div(txt, style={
        "fontFamily": FONT_BODY, "fontSize": "0.82rem",
        "color": COLORS["muted"], "lineHeight": "1.6",
        "marginBottom": "16px", "maxWidth": "680px",
    })


def hr():
    return html.Hr(style={
        "border": "none",
        "borderTop": f"1px solid {COLORS['border']}",
        "margin": "32px 0",
    })


def stat(value, label, color=None, note=None):
    return html.Div(style={"textAlign": "center", "padding": "0 16px"}, children=[
        html.Div(str(value), style={
            "fontFamily": FONT_HEAD, "fontSize": "2.4rem",
            "fontWeight": "700", "color": color or COLORS["accent"], "lineHeight": "1",
        }),
        html.Div(label, style={
            "fontFamily": FONT_MONO, "fontSize": "0.6rem",
            "color": COLORS["muted"], "letterSpacing": "0.1em",
            "textTransform": "uppercase", "marginTop": "6px",
        }),
        html.Div(note or "", style={
            "fontFamily": FONT_BODY, "fontSize": "0.7rem",
            "color": COLORS["dim"], "marginTop": "3px",
        }) if note else html.Div(),
    ])


def pill(txt, color):
    return html.Span(txt, style={
        "display": "inline-block",
        "background": f"{color}15",
        "border": f"1px solid {color}44",
        "color": color,
        "borderRadius": "3px",
        "padding": "2px 8px",
        "fontSize": "0.72rem",
        "fontFamily": FONT_MONO,
        "marginRight": "6px", "marginBottom": "4px",
    })


def legend_dot(color, label):
    """Punto de color + etiqueta, para la leyenda de sentimiento."""
    return html.Span(style={"display": "inline-flex", "alignItems": "center", "gap": "6px"}, children=[
        html.Span(style={
            "width": "11px", "height": "11px", "borderRadius": "50%",
            "background": color, "display": "inline-block",
            "border": f"1px solid {color}",
        }),
        html.Span(label, style={
            "fontFamily": FONT_BODY, "fontSize": "0.75rem", "color": COLORS["muted"],
        }),
    ])


def ibox(icon, title, body, color=None):
    return html.Div(style={
        "backgroundColor": COLORS["bg"],
        "border":     f"1px solid {COLORS['border']}",
        "borderLeft": f"3px solid {color or COLORS['accent']}",
        "borderRadius": "3px",
        "padding": "12px 14px", "marginBottom": "10px",
    }, children=[
        html.Div(style={"display": "flex", "gap": "8px"}, children=[
            html.Span(icon, style={"fontSize": "1rem", "marginTop": "1px"}),
            html.Div([
                html.Div(title, style={
                    "fontFamily": FONT_BODY, "fontSize": "0.78rem",
                    "fontWeight": "600", "color": COLORS["text"], "marginBottom": "3px",
                }),
                html.Div(body, style={
                    "fontFamily": FONT_BODY, "fontSize": "0.73rem",
                    "color": COLORS["muted"], "lineHeight": "1.5",
                }),
            ])
        ])
    ])


def snippet_card(text, compound, source, pole):
    """Card con un texto real, su score VADER y la fuente."""
    color  = COLORS["positive"] if pole == "positive" else COLORS["negative"]
    sign   = "+" if compound >= 0 else ""
    src_label = "Reddit" if source == "reddit" else "Football-España"

    return html.Div(style={
        "backgroundColor": COLORS["bg"],
        "border":     f"1px solid {COLORS['border']}",
        "borderLeft": f"3px solid {color}",
        "borderRadius": "3px",
        "padding": "10px 14px",
        "marginBottom": "8px",
    }, children=[
        html.Div(style={
            "display": "flex", "justifyContent": "space-between",
            "marginBottom": "5px",
        }, children=[
            html.Span(src_label, style={
                "fontFamily": FONT_MONO, "fontSize": "0.62rem",
                "color": COLORS["dim"], "textTransform": "uppercase",
                "letterSpacing": "0.1em",
            }),
            html.Span(f"{sign}{compound:.3f}", style={
                "fontFamily": FONT_MONO, "fontSize": "0.7rem",
                "color": color, "fontWeight": "600",
            }),
        ]),
        html.Div(
            f'"{text[:220]}{"..." if len(text) > 220 else ""}"',
            style={
                "fontFamily": FONT_BODY, "fontSize": "0.75rem",
                "color": COLORS["muted"], "lineHeight": "1.5",
                "fontStyle": "italic",
            }
        ),
    ])

# ─────────────────────────────────────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────────────────────────────────────
app = Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,500;0,700;0,800;0,900;1,500;1,700;1,900"
        "&family=IBM+Plex+Sans:wght@300;400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap",
    ],
    title="Real Madrid Pulse",
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
server = app.server

# ─────────────────────────────────────────────────────────────────────────────
# CSS GLOBAL — textura de papel, microinteracciones, selectores tipo "pill".
# Todo esto da el acabado "hecho a mano" (editorial) y evita el look genérico.
# ─────────────────────────────────────────────────────────────────────────────
app.index_string = """
<!DOCTYPE html>
<html>
<head>
  {%metas%}<title>{%title%}</title>{%favicon%}{%css%}
  <style>
    :root {
      --ink:    #1c1a17;
      --paper:  #faf9f6;
      --cream:  #f3efe6;
      --border: #e6ded2;
      --gold:   #b08a2e;
      --pos:    #2f7d52;
      --neg:    #a8322d;
    }
    * { -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale; }
    html { scroll-behavior: smooth; }
    body {
      margin: 0;
      background-color: var(--paper);
      /* grano de papel muy sutil: dos capas de ruido en SVG */
      background-image:
        url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='180' height='180'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='3' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='0.025'/%3E%3C/svg%3E");
    }
    /* Tarjetas: elevación suave al pasar el cursor */
    .rm-card {
      transition: transform .18s ease, box-shadow .18s ease, border-color .18s ease;
    }
    .rm-card:hover {
      transform: translateY(-2px);
      box-shadow: 0 6px 22px rgba(40,33,22,0.08);
      border-color: #d8cfbf !important;
    }
    /* Selectores (RadioItems) convertidos en pills */
    .rm-pills .form-check {
      display: inline-block;
      padding: 0; margin: 0 7px 7px 0;
    }
    .rm-pills .form-check-input { display: none; }
    .rm-pills .form-check-label {
      cursor: pointer;
      border: 1px solid var(--border);
      background: #fff;
      border-radius: 999px;
      padding: 5px 13px !important;
      font-family: 'IBM Plex Mono', monospace;
      font-size: 0.72rem !important;
      color: #5f5a52 !important;
      transition: all .15s ease;
      letter-spacing: 0.01em;
    }
    .rm-pills .form-check-label:hover {
      border-color: var(--gold);
      color: var(--ink) !important;
    }
    .rm-pills .form-check-input:checked + .form-check-label {
      background: var(--ink);
      border-color: var(--ink);
      color: var(--paper) !important;
    }
    /* Scrollbar discreto */
    ::-webkit-scrollbar { width: 11px; height: 11px; }
    ::-webkit-scrollbar-track { background: var(--cream); }
    ::-webkit-scrollbar-thumb {
      background: #cfc6b6; border-radius: 6px;
      border: 3px solid var(--cream);
    }
    ::-webkit-scrollbar-thumb:hover { background: var(--gold); }
    /* Selección de texto en tono dorado */
    ::selection { background: rgba(176,138,46,0.22); }
    /* Línea decorativa superior tricolor (acento de marca) */
    .rm-topbar { height: 4px; width: 100%;
      background: linear-gradient(90deg, var(--ink) 0 33%, var(--gold) 33% 66%, var(--ink) 66% 100%); }
  </style>
</head>
<body>
  <div class="rm-topbar"></div>
  {%app_entry%}
  <footer>{%config%}{%scripts%}{%renderer%}</footer>
</body>
</html>
"""

# ─────────────────────────────────────────────────────────────────────────────
# LAYOUT
# ─────────────────────────────────────────────────────────────────────────────
app.layout = html.Div(
    style={"backgroundColor": COLORS["bg"], "minHeight": "100vh",
           "fontFamily": FONT_BODY, "color": COLORS["text"]},
    children=[

        # ── HEADER ───────────────────────────────────────────────────────────
        html.Div(style={
            "borderBottom": f"3px solid {COLORS['accent']}",
            "padding": "0 48px",
            "backgroundColor": COLORS["card_bg"],
        }, children=[
            html.Div(style={
                "display": "flex", "justifyContent": "space-between",
                "borderBottom": f"1px solid {COLORS['border']}", "padding": "7px 0",
            }, children=[
                html.Div("DATA ANALYSIS PROGRAMMING — UNIVERSIDAD DISTRITAL", style={
                    "fontFamily": FONT_MONO, "fontSize": "0.58rem",
                    "color": COLORS["dim"], "letterSpacing": "0.12em",
                }),
                html.Div(id="last-updated", style={
                    "fontFamily": FONT_MONO, "fontSize": "0.58rem",
                    "color": COLORS["dim"],
                }),
            ]),
            html.Div(style={
                "padding": "18px 0 14px",
                "display": "flex", "alignItems": "flex-end",
                "justifyContent": "space-between",
            }, children=[
                html.Div([
                    html.Div("REAL MADRID PULSE", style={
                        "fontFamily": FONT_HEAD, "fontSize": "3.4rem",
                        "fontWeight": "900", "color": COLORS["text"],
                        "lineHeight": "0.95", "letterSpacing": "-0.015em",
                    }),
                    html.Div("¿Qué piensa realmente el mundo del Real Madrid?", style={
                        "fontFamily": FONT_HEAD, "fontSize": "1.05rem",
                        "fontStyle": "italic", "fontWeight": "500",
                        "color": COLORS["muted"], "marginTop": "7px",
                    }),
                ]),
                html.Div(style={"textAlign": "right"}, children=[
                    html.Div("FUENTES DE DATOS", style={
                        "fontFamily": FONT_MONO, "fontSize": "0.56rem",
                        "color": COLORS["dim"], "letterSpacing": "0.12em", "marginBottom": "5px",
                    }),
                    html.Div([
                        pill("r/realmadrid · Reddit",    COLORS["reddit"]),
                        pill("Football-España · Prensa", COLORS["scraping"]),
                    ]),
                    html.Div("Ortiz · Cruz · Bautista", style={
                        "fontFamily": FONT_MONO, "fontSize": "0.58rem",
                        "color": COLORS["dim"], "marginTop": "5px",
                    }),
                ]),
            ]),
        ]),

        # ── BANDA-LEYENDA — clave de lectura para el usuario final ────────────
        html.Div(style={
            "backgroundColor": COLORS["card_bg"],
            "borderBottom": f"1px solid {COLORS['border']}",
            "padding": "10px 48px",
            "display": "flex", "alignItems": "center",
            "gap": "26px", "flexWrap": "wrap",
        }, children=[
            html.Span("CÓMO LEER ESTO", style={
                "fontFamily": FONT_MONO, "fontSize": "0.56rem",
                "color": COLORS["dim"], "letterSpacing": "0.14em",
            }),
            # clave de colores
            html.Div(style={"display": "flex", "gap": "16px", "alignItems": "center"}, children=[
                legend_dot(COLORS["positive"], "Positivo"),
                legend_dot(COLORS["neutral"],  "Neutral"),
                legend_dot(COLORS["negative"], "Negativo"),
            ]),
            html.Div(style={"width": "1px", "height": "16px", "background": COLORS["border"]}),
            # explicación del índice de ánimo (compound score en lenguaje llano)
            html.Span([
                html.Span("Índice de ánimo", style={"fontWeight": "600", "color": COLORS["text"]}),
                html.Span("  va de ", style={"color": COLORS["muted"]}),
                html.Span("−1 (muy crítico)", style={"color": COLORS["negative"], "fontWeight": "600"}),
                html.Span(" a ", style={"color": COLORS["muted"]}),
                html.Span("+1 (muy positivo)", style={"color": COLORS["positive"], "fontWeight": "600"}),
                html.Span(". Cerca de 0 = opiniones divididas.", style={"color": COLORS["muted"]}),
            ], style={"fontFamily": FONT_BODY, "fontSize": "0.74rem"}),
        ]),

        # ── CONTENIDO PRINCIPAL ───────────────────────────────────────────────
        html.Div(style={"padding": "36px 48px", "maxWidth": "1400px"}, children=[

            # SEC 1 — VEREDICTO
            html.Div(id="section-verdict", style={"marginBottom": "36px"}),

            hr(),

            # SEC 2 — TENDENCIA
            html.Div(style={"marginBottom": "36px"}, children=[
                sec_label("02 — Evolución temporal"),
                sec_q("¿Ha cambiado el ánimo de la afición con el tiempo?"),
                sec_a(
                    "Seguimos el índice de ánimo promedio día a día. Por encima de cero indica un período "
                    "optimista; por debajo, un período de crítica o preocupación. Las barras muestran "
                    "cuántas menciones hubo — los picos revelan momentos de alta actividad."
                ),
                dbc.Row([
                    dbc.Col([
                        dbc.RadioItems(
                            id="time-granularity",
                            className="rm-pills",
                            options=[
                                {"label": " Por día",    "value": "day"},
                                {"label": " Por semana", "value": "week"},
                            ],
                            value="day", inline=True,
                            style={"marginBottom": "10px"},
                        ),
                        dcc.Graph(id="sentiment-trend-chart", config={"displayModeBar": False}),
                    ], width=8),
                    dbc.Col([
                        dcc.Graph(id="volume-trend-chart", config={"displayModeBar": False}),
                        html.Div(id="trend-insight", style={"marginTop": "10px"}),
                    ], width=4),
                ]),
            ]),

            hr(),

            # SEC 3 — FUENTES
            html.Div(style={"marginBottom": "36px"}, children=[
                sec_label("03 — Dos voces, ¿un mismo mensaje?"),
                sec_q("¿Hablan igual los fans en Reddit que los periodistas?"),
                sec_a(
                    "Reddit refleja la opinión cruda de la afición — emocional y sin filtro editorial. "
                    "Football-España es cobertura periodística — más estructurada. Comparar ambas fuentes "
                    "revela si el optimismo (o la crítica) viene de los fans o de los medios."
                ),
                dbc.Row([
                    dbc.Col(
                        html.Div(className="rm-card", style=CARD, children=[
                            html.Div("Distribución global", style={
                                "fontFamily": FONT_MONO, "fontSize": "0.62rem",
                                "color": COLORS["muted"], "marginBottom": "8px",
                            }),
                            dcc.Graph(id="sentiment-donut", config={"displayModeBar": False}),
                        ]), width=4
                    ),
                    dbc.Col(
                        html.Div(className="rm-card", style=CARD, children=[
                            html.Div("Reddit vs Football-España", style={
                                "fontFamily": FONT_MONO, "fontSize": "0.62rem",
                                "color": COLORS["muted"], "marginBottom": "8px",
                            }),
                            dcc.Graph(id="source-comparison-chart", config={"displayModeBar": False}),
                        ]), width=5
                    ),
                    dbc.Col(
                        html.Div(id="source-insight-box", style={"paddingTop": "8px"}),
                        width=3
                    ),
                ]),
            ]),

            hr(),

            # SEC 4 — KEYWORDS
            html.Div(style={"marginBottom": "36px"}, children=[
                sec_label("04 — El vocabulario del momento"),
                sec_q("¿De qué habla la gente cuando habla de Real Madrid?"),
                sec_a(
                    "A la izquierda, las palabras sueltas más frecuentes (el color indica si aparecen en "
                    "contextos positivos, negativos o neutros). A la derecha, las parejas de palabras "
                    "(bigramas) más repetidas — revelan los temas concretos de la conversación, no solo "
                    "términos aislados."
                ),
                dbc.Row([
                    dbc.Col(
                        html.Div(className="rm-card", style=CARD, children=[
                            html.Div("Palabras más frecuentes", style={
                                "fontFamily": FONT_MONO, "fontSize": "0.62rem",
                                "color": COLORS["muted"], "marginBottom": "8px",
                                "textTransform": "uppercase", "letterSpacing": "0.1em",
                            }),
                            dcc.Graph(id="keywords-chart", config={"displayModeBar": False}),
                        ]), width=7
                    ),
                    dbc.Col(
                        html.Div(className="rm-card", style=CARD, children=[
                            html.Div("Frases más repetidas (bigramas)", style={
                                "fontFamily": FONT_MONO, "fontSize": "0.62rem",
                                "color": COLORS["muted"], "marginBottom": "8px",
                                "textTransform": "uppercase", "letterSpacing": "0.1em",
                            }),
                            dcc.Graph(id="bigrams-chart", config={"displayModeBar": False}),
                        ]), width=5
                    ),
                ]),
            ]),

            hr(),

            # SEC 5 — ASPECTOS
            html.Div(style={"marginBottom": "36px"}, children=[
                sec_label("05 — Los protagonistas"),
                sec_q("¿Quién genera elogios y quién genera polémica?"),
                sec_a(
                    "Para cada jugador o tema calculamos el índice de ánimo promedio de todos los textos "
                    "donde aparece. Por encima de cero = aparece en contextos de elogio. "
                    "Por debajo = aparece en contextos de crítica o preocupación."
                ),
                dbc.Row([
                    dbc.Col(
                        html.Div(className="rm-card", style=CARD, children=[
                            dcc.Graph(id="aspect-sentiment-chart", config={"displayModeBar": False}),
                        ]), width=7
                    ),
                    dbc.Col(
                        html.Div(id="aspect-insight-box", style={"paddingTop": "4px"}),
                        width=5
                    ),
                ]),
            ]),

            hr(),

            # SEC 6 — ¿POR QUÉ? (NUEVA SECCIÓN)
            html.Div(style={"marginBottom": "36px"}, children=[
                sec_label("06 — ¿Por qué?"),
                sec_q("¿Qué palabras hacen que el sentimiento sea positivo o negativo?"),
                sec_a(
                    "Selecciona un protagonista para ver las palabras que más aparecen en sus menciones "
                    "positivas (verde) y negativas (rojo), junto con ejemplos reales de los textos con "
                    "mayor score. Esto explica por qué cada aspecto tiene el sentimiento que tiene."
                ),

                # Selector de aspecto
                html.Div(style={"marginBottom": "16px"}, children=[
                    html.Span("Protagonista: ", style={
                        "fontFamily": FONT_MONO, "fontSize": "0.72rem",
                        "color": COLORS["muted"], "marginRight": "8px",
                        "textTransform": "uppercase", "letterSpacing": "0.1em",
                    }),
                    dbc.RadioItems(
                        id="drilldown-aspect",
                        className="rm-pills",
                        options=[{"label": f" {v}", "value": k} for k, v in LABELS.items()],
                        value="bellingham",
                        inline=True,
                    ),
                ]),

                # Resumen del aspecto seleccionado
                html.Div(id="drilldown-summary", style={"marginBottom": "12px"}),

                # Gráficas de palabras coocurrentes
                dbc.Row([
                    dbc.Col(
                        html.Div(className="rm-card", style=CARD, children=[
                            html.Div("Palabras en contextos positivos", style={
                                "fontFamily": FONT_MONO, "fontSize": "0.6rem",
                                "color": COLORS["positive"], "textTransform": "uppercase",
                                "letterSpacing": "0.1em", "marginBottom": "8px",
                            }),
                            dcc.Graph(id="cowords-pos-chart", config={"displayModeBar": False}),
                        ]), width=4
                    ),
                    dbc.Col(
                        html.Div(className="rm-card", style=CARD, children=[
                            html.Div("Palabras en contextos negativos", style={
                                "fontFamily": FONT_MONO, "fontSize": "0.6rem",
                                "color": COLORS["negative"], "textTransform": "uppercase",
                                "letterSpacing": "0.1em", "marginBottom": "8px",
                            }),
                            dcc.Graph(id="cowords-neg-chart", config={"displayModeBar": False}),
                        ]), width=4
                    ),
                    dbc.Col(
                        html.Div(style={"paddingTop": "4px"}, children=[
                            html.Div("Textos representativos", style={
                                "fontFamily": FONT_MONO, "fontSize": "0.6rem",
                                "color": COLORS["dim"], "textTransform": "uppercase",
                                "letterSpacing": "0.1em", "marginBottom": "10px",
                            }),
                            html.Div(id="snippets-box"),
                        ]),
                        width=4
                    ),
                ]),
            ]),

        ]),

        dcc.Interval(id="interval", interval=5 * 60 * 1000, n_intervals=0),
    ]
)

# ─────────────────────────────────────────────────────────────────────────────
# CALLBACKS — existentes
# ─────────────────────────────────────────────────────────────────────────────

@app.callback(
    Output("last-updated", "children"),
    Input("interval", "n_intervals"),
)
def cb_ts(_):
    return f"ÚLTIMA ACTUALIZACIÓN: {last_updated()}"


@app.callback(
    Output("section-verdict", "children"),
    Input("interval", "n_intervals"),
)
def cb_verdict(_):
    df_dist = load_latest("sentiment_distribution")
    df_gold = load_gold()

    if df_dist.empty or df_gold.empty:
        return html.P("Ejecuta el Gold DAG para ver los datos.", style={"color": COLORS["muted"]})

    g_dist   = df_dist.groupby("sentiment_label")["count"].sum()
    total    = int(g_dist.sum())
    dominant = g_dist.idxmax()
    dom_pct  = round(g_dist.max() / total * 100, 1)
    pos_pct  = round(g_dist.get("positive", 0) / total * 100, 1)
    neg_pct  = round(g_dist.get("negative", 0) / total * 100, 1)
    neu_pct  = round(g_dist.get("neutral",  0) / total * 100, 1)
    compound = round(df_gold["vader_compound"].mean(), 3)

    reddit_n   = int(df_dist[df_dist["source"] == "reddit"]["count"].sum())   if "source" in df_dist.columns else 0
    scraping_n = int(df_dist[df_dist["source"] == "scraping"]["count"].sum()) if "source" in df_dist.columns else 0

    if compound >= 0.2:
        verdict = "El clima en torno al Real Madrid es marcadamente optimista."
        vcolor  = COLORS["positive"]
        detail  = (
            f"Con un índice de ánimo de {compound:+.3f}, la cobertura y la conversación en redes están dominadas "
            f"por contenido positivo. El {pos_pct}% de las {total:,} menciones analizadas expresa apoyo, elogio "
            f"o expectativa favorable hacia el club, los jugadores o la gestión."
        )
    elif compound >= 0.05:
        verdict = "El sentimiento es positivo, aunque con matices críticos relevantes."
        vcolor  = COLORS["positive"]
        detail  = (
            f"El índice de ánimo de {compound:+.3f} indica una tendencia positiva moderada. "
            f"El {pos_pct}% de las menciones es favorable, pero el {neg_pct}% de contenido negativo "
            f"sugiere temas específicos que generan controversia dentro del optimismo general."
        )
    elif compound >= -0.05:
        verdict = "La conversación está dividida — ni optimismo ni pesimismo predomina."
        vcolor  = COLORS["neutral"]
        detail  = (
            f"Con un índice de ánimo de {compound:+.3f}, el debate público sobre Real Madrid está "
            f"equilibrado entre voces positivas ({pos_pct}%) y negativas ({neg_pct}%). "
            f"Esto puede indicar un momento de transición o temas muy polarizados coexistiendo."
        )
    else:
        verdict = "El tono predominante es crítico — más preocupación que celebración."
        vcolor  = COLORS["negative"]
        detail  = (
            f"El índice de ánimo de {compound:+.3f} refleja que el {neg_pct}% de las menciones "
            f"expresa crítica o decepción. Solo el {pos_pct}% es favorable. "
            f"Los temas negativos están dominando la narrativa pública en este período."
        )

    return html.Div([
        sec_label("01 — El veredicto"),
        sec_q(verdict),
        html.Div(style={
            "display": "flex", "gap": "0",
            "borderTop":    f"1px solid {COLORS['border']}",
            "borderBottom": f"1px solid {COLORS['border']}",
            "padding": "18px 0", "marginBottom": "16px",
        }, children=[
            stat(f"{total:,}", "menciones analizadas"),
            html.Div(style={"width": "1px", "background": COLORS["border"], "margin": "0 20px"}),
            stat(f"{dom_pct}%", "sentimiento dominante", color=vcolor, note=dominant.capitalize()),
            html.Div(style={"width": "1px", "background": COLORS["border"], "margin": "0 20px"}),
            stat(f"{compound:+.3f}", "índice de ánimo global", color=vcolor),
            html.Div(style={"width": "1px", "background": COLORS["border"], "margin": "0 20px"}),
            stat(f"{reddit_n:,}", "posts Reddit", color=COLORS["reddit"]),
            html.Div(style={"width": "1px", "background": COLORS["border"], "margin": "0 20px"}),
            stat(f"{scraping_n:,}", "artículos prensa", color=COLORS["scraping"]),
        ]),

        # ── Medidor visual del ánimo (−1 … +1) ────────────────────────────────
        html.Div(style={"maxWidth": "760px", "margin": "4px 0 18px"}, children=[
            html.Div(style={
                "position": "relative", "height": "10px", "borderRadius": "999px",
                "background": "linear-gradient(90deg, #a8322d 0%, #d8cdbb 50%, #2f7d52 100%)",
                "border": f"1px solid {COLORS['border']}",
            }, children=[
                # marcador en la posición del compound global
                html.Div(style={
                    "position": "absolute",
                    "left": f"{(compound + 1) / 2 * 100:.1f}%",
                    "top": "50%", "transform": "translate(-50%, -50%)",
                    "width": "16px", "height": "16px", "borderRadius": "50%",
                    "background": COLORS["bg"], "border": f"3px solid {vcolor}",
                    "boxShadow": "0 1px 4px rgba(40,33,22,0.25)",
                }),
            ]),
            html.Div(style={
                "display": "flex", "justifyContent": "space-between", "marginTop": "5px",
                "fontFamily": FONT_MONO, "fontSize": "0.58rem", "color": COLORS["dim"],
            }, children=[
                html.Span("−1 · muy crítico"),
                html.Span("0 · dividido"),
                html.Span("muy positivo · +1"),
            ]),
        ]),

        html.P(detail, style={
            "fontFamily": FONT_BODY, "fontSize": "0.88rem",
            "color": COLORS["muted"], "lineHeight": "1.7",
            "maxWidth": "760px", "marginBottom": "10px",
        }),
        html.Div(style={"display": "flex", "gap": "6px", "flexWrap": "wrap"}, children=[
            pill(f"✓ {pos_pct}% positivo", COLORS["positive"]),
            pill(f"◦ {neu_pct}% neutro",   COLORS["neutral"]),
            pill(f"✗ {neg_pct}% negativo", COLORS["negative"]),
        ]),
    ])


@app.callback(
    Output("sentiment-trend-chart", "figure"),
    Output("volume-trend-chart",    "figure"),
    Output("trend-insight",         "children"),
    Input("time-granularity", "value"),
    Input("interval", "n_intervals"),
)
def cb_trends(granularity, _):
    df       = load_gold()
    time_col = granularity
    xlabel   = "Fecha" if granularity == "day" else "Semana"

    fig_t = go.Figure()
    if not df.empty and time_col in df.columns:
        for src, label, color in [
            ("reddit",   "Reddit",          COLORS["reddit"]),
            ("scraping", "Football-España", COLORS["scraping"]),
        ]:
            sub = df[df["source"] == src]
            if sub.empty:
                continue
            t = sub.groupby(time_col)["vader_compound"].mean().reset_index().sort_values(time_col)
            n = sub.groupby(time_col).size().reset_index(name="n")
            t = t.merge(n, on=time_col)
            fig_t.add_trace(go.Scatter(
                x=t[time_col], y=t["vader_compound"],
                name=label, mode="lines+markers",
                line=dict(color=color, width=2),
                marker=dict(size=6, color=color),
                fill="tozeroy", fillcolor="rgba(211,84,0,0.1)",
                customdata=t["n"],
                hovertemplate=(
                    f"<b>{label}</b><br>{xlabel}: %{{x}}<br>"
                    f"Score: %{{y:.3f}}<br>n=%{{customdata}}<extra></extra>"
                ),
            ))
        fig_t.add_hline(y=0,     line_dash="dot", line_color=COLORS["border"],   line_width=1)
        fig_t.add_hline(y=0.05,  line_dash="dot", line_color=COLORS["positive"], opacity=0.5, line_width=1)
        fig_t.add_hline(y=-0.05, line_dash="dot", line_color=COLORS["negative"], opacity=0.5, line_width=1)

    fig_t.update_layout(**CHART_LAYOUT)
    fig_t.update_layout(
        title=dict(text=f"Índice de ánimo promedio ({xlabel.lower()})",
                   font=dict(size=11, color=COLORS["muted"]), x=0),
        height=260, yaxis_range=[-1, 1],
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    fig_v = go.Figure()
    if not df.empty and time_col in df.columns:
        for src, label, color in [
            ("reddit",   "Reddit",          COLORS["reddit"]),
            ("scraping", "Football-España", COLORS["scraping"]),
        ]:
            sub = df[df["source"] == src]
            if sub.empty:
                continue
            v = sub.groupby(time_col).size().reset_index(name="n").sort_values(time_col)
            fig_v.add_trace(go.Bar(
                x=v[time_col], y=v["n"],
                name=label, marker_color=color, opacity=0.85,
                hovertemplate=(
                    f"<b>{label}</b><br>{xlabel}: %{{x}}<br>Menciones: %{{y}}<extra></extra>"
                ),
            ))

    fig_v.update_layout(**CHART_LAYOUT)
    fig_v.update_layout(
        title=dict(text="Volumen de menciones", font=dict(size=11, color=COLORS["muted"]), x=0),
        barmode="stack", height=260,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    boxes = []
    if not df.empty and time_col in df.columns:
        r_days = df[df["source"] == "reddit"][time_col].nunique()
        s_days = df[df["source"] == "scraping"][time_col].nunique()
        if r_days < s_days:
            boxes.append(ibox("⚠", "Cobertura desigual",
                f"Reddit tiene datos de {r_days} {'día(s)' if granularity=='day' else 'semana(s)'} "
                f"vs {s_days} de Football-España. La comparación de tendencias es parcial.",
                COLORS["accent"]))

        sc = df[df["source"] == "scraping"].groupby(time_col)["vader_compound"].mean().sort_index()
        if len(sc) >= 2:
            delta = sc.iloc[-1] - sc.iloc[0]
            if delta > 0.05:
                boxes.append(ibox("↗", "Tendencia al alza — Prensa",
                    f"El sentimiento en Football-España subió {delta:+.3f} puntos.",
                    COLORS["positive"]))
            elif delta < -0.05:
                boxes.append(ibox("↘", "Tendencia a la baja — Prensa",
                    f"El sentimiento en Football-España bajó {delta:.3f} puntos.",
                    COLORS["negative"]))

    return fig_t, fig_v, boxes


@app.callback(
    Output("sentiment-donut",         "figure"),
    Output("source-comparison-chart", "figure"),
    Output("source-insight-box",      "children"),
    Input("interval", "n_intervals"),
)
def cb_sources(_):
    df_dist = load_latest("sentiment_distribution")
    df_src  = load_latest("source_comparison")

    fig_d = go.Figure()
    if not df_dist.empty and "sentiment_label" in df_dist.columns and "count" in df_dist.columns:
        g      = df_dist.groupby("sentiment_label")["count"].sum().reset_index()
        labels = g["sentiment_label"].tolist()
        values = g["count"].tolist()
        colors = [SENTIMENT_COLORS.get(l, COLORS["neutral"]) for l in labels]
        total  = sum(values)
        dom    = g.loc[g["count"].idxmax(), "sentiment_label"]

        fig_d.add_trace(go.Pie(
            labels=[l.capitalize() for l in labels],
            values=values, hole=0.58,
            marker_colors=colors,
            textinfo="percent+label",
            textfont=dict(size=11),
            hovertemplate="<b>%{label}</b><br>%{value} menciones (%{percent})<extra></extra>",
            insidetextorientation="radial",
        ))
        fig_d.add_annotation(
            text=f"<b>{dom.upper()}</b><br><span style='font-size:10px'>{total:,} total</span>",
            x=0.5, y=0.5,
            font=dict(size=13, color=SENTIMENT_COLORS.get(dom, COLORS["text"]), family=FONT_HEAD),
            showarrow=False,
        )
    fig_d.update_layout(**CHART_LAYOUT)
    fig_d.update_layout(
        height=300, showlegend=True,
        legend=dict(orientation="h", yanchor="top", y=-0.05, xanchor="center", x=0.5),
        margin=dict(l=20, r=20, t=20, b=40),
    )

    fig_s = go.Figure()
    if not df_src.empty and "source" in df_src.columns:
        src_labels = ["Reddit" if s == "reddit" else "Football-España" for s in df_src["source"]]
        for col, name, color in [
            ("pct_positive", "Positivo", COLORS["positive"]),
            ("pct_neutral",  "Neutro",   COLORS["neutral"]),
            ("pct_negative", "Negativo", COLORS["negative"]),
        ]:
            if col in df_src.columns:
                fig_s.add_trace(go.Bar(
                    name=name, x=src_labels, y=df_src[col],
                    marker_color=color,
                    hovertemplate=f"<b>%{{x}}</b><br>{name}: %{{y:.1f}}%<extra></extra>",
                ))
    fig_s.update_layout(**CHART_LAYOUT)
    fig_s.update_layout(
        barmode="group", height=300,
        yaxis_title="Porcentaje (%)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=20, t=20, b=40),
    )

    boxes = []
    if not df_src.empty and len(df_src) >= 2:
        r = df_src[df_src["source"] == "reddit"].iloc[0]   if "reddit"   in df_src["source"].values else None
        s = df_src[df_src["source"] == "scraping"].iloc[0] if "scraping" in df_src["source"].values else None
        if r is not None and s is not None:
            diff = round(s["pct_positive"] - r["pct_positive"], 1)
            if abs(diff) > 8:
                mas   = "Football-España" if diff > 0 else "Reddit"
                menos = "Reddit" if diff > 0 else "Football-España"
                boxes.append(ibox("📰" if diff > 0 else "💬",
                    f"{mas} es más optimista",
                    f"{mas} tiene {abs(diff):.0f}pp más contenido positivo que {menos}.",
                    COLORS["positive"] if diff > 0 else COLORS["reddit"]))
            neg_diff = round(r["pct_negative"] - s["pct_negative"], 1)
            if abs(neg_diff) > 8:
                mas_neg = "Reddit" if neg_diff > 0 else "Football-España"
                boxes.append(ibox("🔥", f"Más crítica en {mas_neg}",
                    f"{mas_neg} concentra más contenido negativo.",
                    COLORS["negative"]))
    if not boxes:
        boxes.append(ibox("⚖", "Fuentes alineadas",
            "Reddit y Football-España muestran patrones de sentimiento similares.",
            COLORS["neutral"]))
    return fig_d, fig_s, boxes


@app.callback(
    Output("keywords-chart", "figure"),
    Input("interval", "n_intervals"),
)
def cb_keywords(_):
    df_kw  = load_latest("top_keywords")
    df_kws = load_latest("keyword_sentiment")
    fig    = go.Figure()

    if not df_kw.empty and "keyword" in df_kw.columns and "frequency" in df_kw.columns:
        top = df_kw.head(20).copy()
        if not df_kws.empty and "keyword" in df_kws.columns:
            top = top.merge(df_kws[["keyword", "sentiment_mode", "compound_mean"]], on="keyword", how="left")
            top["sentiment_mode"] = top["sentiment_mode"].fillna("neutral")
            top["compound_mean"]  = top["compound_mean"].fillna(0)
        else:
            top["sentiment_mode"] = "neutral"
            top["compound_mean"]  = 0.0

        top    = top.sort_values("frequency")
        colors = [SENTIMENT_COLORS.get(s, COLORS["neutral"]) for s in top["sentiment_mode"]]

        fig.add_trace(go.Bar(
            x=top["frequency"], y=top["keyword"],
            orientation="h", marker_color=colors, opacity=0.9,
            text=[f" {row['sentiment_mode'][:3].upper()} {row['compound_mean']:+.2f}"
                  for _, row in top.iterrows()],
            textposition="outside",
            textfont=dict(size=9, color=COLORS["dim"], family=FONT_MONO),
            hovertemplate="<b>%{y}</b><br>Frecuencia: %{x}<extra></extra>",
        ))

    fig.update_layout(**CHART_LAYOUT)
    fig.update_layout(
        height=500, showlegend=False,
        xaxis_title="Frecuencia en corpus",
        margin=dict(l=40, r=90, t=20, b=40),
    )
    return fig


@app.callback(
    Output("bigrams-chart", "figure"),
    Input("interval", "n_intervals"),
)
def cb_bigrams(_):
    """Bigramas (n-grams) más frecuentes — Agg3b del Gold DAG."""
    df_bg = load_latest("top_bigrams")
    fig   = go.Figure()

    if not df_bg.empty and "bigram" in df_bg.columns and "frequency" in df_bg.columns:
        top = df_bg.head(15).sort_values("frequency")
        fig.add_trace(go.Bar(
            x=top["frequency"], y=top["bigram"],
            orientation="h",
            marker_color=COLORS["scraping"], opacity=0.88,
            text=top["frequency"].astype(str),
            textposition="outside",
            textfont=dict(size=9, color=COLORS["dim"], family=FONT_MONO),
            hovertemplate="<b>%{y}</b><br>Aparece %{x} veces<extra></extra>",
        ))

    fig.update_layout(**CHART_LAYOUT)
    fig.update_layout(
        height=500, showlegend=False,
        xaxis_title="Veces que aparece la pareja de palabras",
        margin=dict(l=40, r=60, t=20, b=40),
    )
    return fig


@app.callback(
    Output("aspect-sentiment-chart", "figure"),
    Output("aspect-insight-box",     "children"),
    Input("interval", "n_intervals"),
)
def cb_aspects(_):
    df  = load_latest("aspect_sentiment")
    fig = go.Figure()
    boxes = []

    if not df.empty and "aspect" in df.columns and "compound_mean" in df.columns:
        g = df[df["source"] == "all"].copy() if "source" in df.columns else df.copy()
        if g.empty:
            g = df.copy()
        g = g.sort_values("compound_mean")

        colors = [
            COLORS["positive"] if v >= 0.05 else
            COLORS["negative"] if v <= -0.05 else
            COLORS["neutral"]
            for v in g["compound_mean"]
        ]

        custom = list(zip(
            g["n_mentions"].tolist()   if "n_mentions"   in g.columns else [0]*len(g),
            g["pct_positive"].tolist() if "pct_positive" in g.columns else [0]*len(g),
            g["pct_negative"].tolist() if "pct_negative" in g.columns else [0]*len(g),
        ))

        fig.add_trace(go.Bar(
            x=g["compound_mean"],
            y=[LABELS.get(a, a.capitalize()) for a in g["aspect"]],
            orientation="h",
            marker_color=colors,
            marker_line=dict(width=0),
            text=[f"{v:+.3f}" for v in g["compound_mean"]],
            textposition="outside",
            textfont=dict(size=10, family=FONT_MONO, color=COLORS["muted"]),
            customdata=custom,
            hovertemplate=(
                "<b>%{y}</b><br>Compound: %{x:.3f}<br>"
                "Menciones: %{customdata[0]}<br>"
                "Positivo: %{customdata[1]:.0f}%  Negativo: %{customdata[2]:.0f}%<extra></extra>"
            ),
        ))

        fig.add_vline(x=0,    line_dash="solid", line_color=COLORS["border"], line_width=1.5)
        fig.add_vrect(x0=0.05, x1=1,   fillcolor="rgba(39,174,96,0.05)", line_width=0)
        fig.add_vrect(x0=-1,   x1=-0.05, fillcolor="rgba(192,57,43,0.05)", line_width=0)

        fig.update_layout(**CHART_LAYOUT)
        fig.update_layout(
            height=380, showlegend=False,
            xaxis_title="← más crítico       índice de ánimo       más positivo →",
            xaxis_range=[-0.8, 0.85],
            margin=dict(l=40, r=70, t=20, b=48),
        )

        if not g.empty:
            best  = g.loc[g["compound_mean"].idxmax()]
            worst = g.loc[g["compound_mean"].idxmin()]
            bl    = LABELS.get(best["aspect"],  best["aspect"].capitalize())
            wl    = LABELS.get(worst["aspect"], worst["aspect"].capitalize())

            boxes.append(ibox("⭐", f"{bl} — el más celebrado",
                f"Score {best['compound_mean']:+.3f}. {bl} aparece principalmente en contextos positivos."
                + (f" Mencionado {int(best['n_mentions'])} veces." if "n_mentions" in best else ""),
                COLORS["positive"]))

            boxes.append(ibox("⚡", f"{wl} — el más polémico",
                f"Score {worst['compound_mean']:+.3f}. {wl} concentra las discusiones más críticas."
                + (f" {int(worst.get('pct_negative', 0))}% de sus menciones son negativas."
                   if "pct_negative" in worst else ""),
                COLORS["negative"]))

            boxes.append(ibox("ℹ", "Nota metodológica",
                "VADER analiza vocabulario literal. El sarcasmo deportivo puede clasificarse "
                "como neutro o positivo. Los scores reflejan tendencia estadística, no intención.",
                COLORS["dim"]))

    return fig, boxes


# ─────────────────────────────────────────────────────────────────────────────
# CALLBACKS — SECCIÓN 06 (NUEVOS)
# ─────────────────────────────────────────────────────────────────────────────

@app.callback(
    Output("drilldown-summary", "children"),
    Output("cowords-pos-chart", "figure"),
    Output("cowords-neg-chart", "figure"),
    Output("snippets-box",      "children"),
    Input("drilldown-aspect",   "value"),
    Input("interval",           "n_intervals"),
)
def cb_drilldown(aspect, _):
    """
    Callback principal de la Sección 06.
    Carga aspect_cowords y aspect_snippets para el aspecto seleccionado
    y construye: resumen, gráfica positiva, gráfica negativa y tarjetas de snippets.
    """
    df_cw = load_latest("aspect_cowords")
    df_sn = load_latest("aspect_snippets")
    df_as = load_latest("aspect_sentiment")

    aspect_label = LABELS.get(aspect, aspect.capitalize())
    empty_fig = go.Figure()
    empty_fig.update_layout(**CHART_LAYOUT, height=260, showlegend=False)

    # ── Resumen del aspecto ───────────────────────────────────────────────────
    summary = html.Div()
    if not df_as.empty and "aspect" in df_as.columns:
        row_all = df_as[(df_as["aspect"] == aspect) & (df_as.get("source", pd.Series(["all"]*len(df_as))) == "all")]
        if "source" in df_as.columns:
            row_all = df_as[(df_as["aspect"] == aspect) & (df_as["source"] == "all")]
        else:
            row_all = df_as[df_as["aspect"] == aspect]

        if not row_all.empty:
            row      = row_all.iloc[0]
            compound = row["compound_mean"]
            n        = int(row.get("n_mentions", 0))
            pct_pos  = round(row.get("pct_positive", 0), 1)
            pct_neg  = round(row.get("pct_negative", 0), 1)
            label    = row.get("sentiment_mode", "neutral")
            vcolor   = COLORS.get(label, COLORS["neutral"])

            summary = html.Div(style={
                "backgroundColor": COLORS["card_bg"],
                "border": f"1px solid {COLORS['border']}",
                "borderLeft": f"4px solid {vcolor}",
                "borderRadius": "4px",
                "padding": "12px 18px",
                "display": "flex", "gap": "32px",
                "alignItems": "center",
                "marginBottom": "12px",
            }, children=[
                html.Div([
                    html.Div(aspect_label, style={
                        "fontFamily": FONT_HEAD, "fontSize": "1.2rem",
                        "fontWeight": "700", "color": COLORS["text"],
                    }),
                    html.Div(f"Sentimiento predominante: {label}", style={
                        "fontFamily": FONT_MONO, "fontSize": "0.65rem",
                        "color": vcolor, "textTransform": "uppercase",
                        "letterSpacing": "0.1em",
                    }),
                ]),
                html.Div(style={"width": "1px", "background": COLORS["border"], "alignSelf": "stretch"}),
                html.Div([
                    html.Div(f"{compound:+.3f}", style={
                        "fontFamily": FONT_HEAD, "fontSize": "1.6rem",
                        "fontWeight": "700", "color": vcolor, "lineHeight": "1",
                    }),
                    html.Div("índice de ánimo", style={
                        "fontFamily": FONT_MONO, "fontSize": "0.58rem",
                        "color": COLORS["dim"], "textTransform": "uppercase",
                        "letterSpacing": "0.1em", "marginTop": "4px",
                    }),
                ]),
                html.Div(style={"width": "1px", "background": COLORS["border"], "alignSelf": "stretch"}),
                html.Div([
                    html.Div(str(n), style={
                        "fontFamily": FONT_HEAD, "fontSize": "1.6rem",
                        "fontWeight": "700", "color": COLORS["accent"], "lineHeight": "1",
                    }),
                    html.Div("menciones", style={
                        "fontFamily": FONT_MONO, "fontSize": "0.58rem",
                        "color": COLORS["dim"], "textTransform": "uppercase",
                        "letterSpacing": "0.1em", "marginTop": "4px",
                    }),
                ]),
                html.Div(style={"width": "1px", "background": COLORS["border"], "alignSelf": "stretch"}),
                html.Div(style={"display": "flex", "gap": "6px", "flexWrap": "wrap"}, children=[
                    pill(f"✓ {pct_pos}% positivo", COLORS["positive"]),
                    pill(f"✗ {pct_neg}% negativo", COLORS["negative"]),
                ]),
            ])

    # ── Gráfica palabras en contextos positivos ───────────────────────────────
    fig_pos = go.Figure()
    if not df_cw.empty and "aspect" in df_cw.columns:
        pos_data = (
            df_cw[(df_cw["aspect"] == aspect) & (df_cw["sentiment"] == "positive")]
            .sort_values("frequency", ascending=True)
            .head(10)
        )
        if not pos_data.empty:
            fig_pos.add_trace(go.Bar(
                x=pos_data["frequency"],
                y=pos_data["word"],
                orientation="h",
                marker_color=COLORS["positive"],
                opacity=0.85,
                text=pos_data["frequency"].astype(str),
                textposition="outside",
                textfont=dict(size=9, family=FONT_MONO, color=COLORS["dim"]),
                hovertemplate="<b>%{y}</b><br>Aparece %{x} veces en menciones positivas<extra></extra>",
            ))

    fig_pos.update_layout(**CHART_LAYOUT)
    fig_pos.update_layout(
        height=300, showlegend=False,
        xaxis_title="Frecuencia en menciones positivas",
        margin=dict(l=40, r=50, t=10, b=40),
    )

    # ── Gráfica palabras en contextos negativos ───────────────────────────────
    fig_neg = go.Figure()
    if not df_cw.empty and "aspect" in df_cw.columns:
        neg_data = (
            df_cw[(df_cw["aspect"] == aspect) & (df_cw["sentiment"] == "negative")]
            .sort_values("frequency", ascending=True)
            .head(10)
        )
        if not neg_data.empty:
            fig_neg.add_trace(go.Bar(
                x=neg_data["frequency"],
                y=neg_data["word"],
                orientation="h",
                marker_color=COLORS["negative"],
                opacity=0.85,
                text=neg_data["frequency"].astype(str),
                textposition="outside",
                textfont=dict(size=9, family=FONT_MONO, color=COLORS["dim"]),
                hovertemplate="<b>%{y}</b><br>Aparece %{x} veces en menciones negativas<extra></extra>",
            ))

    fig_neg.update_layout(**CHART_LAYOUT)
    fig_neg.update_layout(
        height=300, showlegend=False,
        xaxis_title="Frecuencia en menciones negativas",
        margin=dict(l=40, r=50, t=10, b=40),
    )

    # ── Snippets representativos ──────────────────────────────────────────────
    snippet_components = []

    if not df_sn.empty and "aspect" in df_sn.columns:
        asp_snips = df_sn[df_sn["aspect"] == aspect]

        # Título positivos
        pos_snips = asp_snips[asp_snips["sentiment_pole"] == "positive"].head(2)
        if not pos_snips.empty:
            snippet_components.append(
                html.Div("Textos más positivos", style={
                    "fontFamily": FONT_MONO, "fontSize": "0.6rem",
                    "color": COLORS["positive"], "textTransform": "uppercase",
                    "letterSpacing": "0.1em", "marginBottom": "6px",
                })
            )
            for _, row in pos_snips.iterrows():
                snippet_components.append(snippet_card(
                    text=str(row.get("text_snippet", "")),
                    compound=float(row.get("vader_compound", 0)),
                    source=str(row.get("source", "")),
                    pole="positive",
                ))

        # Título negativos
        neg_snips = asp_snips[asp_snips["sentiment_pole"] == "negative"].head(2)
        if not neg_snips.empty:
            snippet_components.append(
                html.Div("Textos más negativos", style={
                    "fontFamily": FONT_MONO, "fontSize": "0.6rem",
                    "color": COLORS["negative"], "textTransform": "uppercase",
                    "letterSpacing": "0.1em", "marginBottom": "6px", "marginTop": "10px",
                })
            )
            for _, row in neg_snips.iterrows():
                snippet_components.append(snippet_card(
                    text=str(row.get("text_snippet", "")),
                    compound=float(row.get("vader_compound", 0)),
                    source=str(row.get("source", "")),
                    pole="negative",
                ))

    if not snippet_components:
        snippet_components = [
            html.P(
                "Ejecuta el Gold DAG para cargar los snippets.",
                style={"fontFamily": FONT_BODY, "fontSize": "0.78rem", "color": COLORS["muted"]},
            )
        ]

    return summary, fig_pos, fig_neg, snippet_components


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8051, debug=False)