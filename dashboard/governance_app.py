"""
Real Madrid Pulse — Storytelling Dashboard
Workshop 4 — Plotly Dash
Puerto: localhost:8052

Lee exclusivamente desde datalake_gold/ (Gold Parquet files).
Nunca conecta directamente a Silver, Bronze o PostgreSQL.
"""

from __future__ import annotations

import glob
import os
from datetime import datetime
from typing import Iterable

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dash import Dash, Input, Output, callback, dcc, html, dash_table
import dash_bootstrap_components as dbc


# -----------------------------------------------------------------------------
# PATHS AND THEME
# -----------------------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
DEFAULT_GOLD_PATHS = [
    os.getenv("GOLD_PATH"),
    os.path.join(PROJECT_ROOT, "datalake_gold"),
    "/opt/airflow/datalake_gold",
]


COLORS = {
    "positive": "#2ecc71",
    "negative": "#e74c3c",
    "neutral": "#95a5a6",
    "accent": "#f1c40f",
    "reddit": "#f1c40f",
    "scraping": "#95a5a6",
    "bg": "#0f172a",
    "card_bg": "#111827",
    "border": "#334155",
    "text": "#e5e7eb",
    "text_muted": "#94a3b8",
}


SENTIMENT_STYLE = {
    "good": COLORS["positive"],
    "warn": COLORS["accent"],
    "bad": COLORS["negative"],
    "neutral": COLORS["neutral"],
}


def resolve_gold_path() -> str:
    for candidate in DEFAULT_GOLD_PATHS:
        if candidate and os.path.isdir(candidate):
            return candidate
    return os.path.join(PROJECT_ROOT, "datalake_gold")


GOLD_PATH = resolve_gold_path()


CARD_STYLE = {
    "backgroundColor": COLORS["card_bg"],
    "border": f"1px solid {COLORS['border']}",
    "borderRadius": "12px",
    "padding": "18px 18px 16px",
    "marginBottom": "16px",
    "height": "100%",
    "boxShadow": "0 8px 30px rgba(0, 0, 0, 0.18)",
}


CHART_CONFIG = {
    "displayModeBar": True,
    "scrollZoom": True,
    "responsive": True,
    "displaylogo": False,
}


BASE_LAYOUT = dict(
    paper_bgcolor=COLORS["card_bg"],
    plot_bgcolor=COLORS["card_bg"],
    font=dict(color=COLORS["text"], family="Arial, Helvetica, sans-serif"),
    margin=dict(l=50, r=24, t=84, b=56),
    legend=dict(
        bgcolor="rgba(0,0,0,0)",
        bordercolor=COLORS["border"],
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1,
    ),
    xaxis=dict(
        gridcolor=COLORS["border"],
        linecolor=COLORS["border"],
        tickcolor=COLORS["border"],
        automargin=True,
    ),
    yaxis=dict(
        gridcolor=COLORS["border"],
        linecolor=COLORS["border"],
        tickcolor=COLORS["border"],
        automargin=True,
    ),
)


def load_latest_parquet(prefix: str) -> tuple[pd.DataFrame, str | None]:
    pattern = os.path.join(GOLD_PATH, f"{prefix}_*.parquet")
    files = glob.glob(pattern)
    if not files:
        return pd.DataFrame(), None
    latest = max(files, key=os.path.getmtime)
    try:
        return pd.read_parquet(latest), latest
    except Exception:
        return pd.DataFrame(), latest


def latest_timestamp(path: str | None) -> str:
    if not path or not os.path.exists(path):
        return "No data"
    ts = datetime.fromtimestamp(os.path.getmtime(path))
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def humanize_metric(name: str) -> str:
    mapping = {
        "pipeline_run_ts": "Pipeline run timestamp",
        "total_records": "Total records",
        "records_reddit": "Records - Reddit",
        "records_scraping": "Records - Web scraping",
        "body_text_missing_rate": "Body text missing rate",
        "schema_compliance_rate": "Schema compliance rate",
        "duplicate_rate": "Duplicate rate",
        "expected_runs_weekly": "Expected runs weekly",
        "actual_runs_scraping": "Actual runs - Web scraping",
        "actual_runs_reddit": "Actual runs - Reddit",
        "ingestion_compliance_scraping": "Ingestion compliance - Web scraping",
        "ingestion_compliance_reddit": "Ingestion compliance - Reddit",
        "outlier_rate_reading_time_min": "Outlier rate - reading_time_min",
        "outlier_rate_score": "Outlier rate - score",
        "outlier_rate_num_comments": "Outlier rate - num_comments",
    }
    if name in mapping:
        return mapping[name]
    if name.startswith("null_rate_"):
        return "Null rate - " + name.replace("null_rate_", "").replace("_", " ")
    if name.startswith("outlier_rate_"):
        return "Outlier rate - " + name.replace("outlier_rate_", "").replace("_", " ")
    return name.replace("_", " ").title()


def format_value(metric: str, value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "N/A"
    if metric.endswith("_ts"):
        return str(value)
    if metric.endswith("_rate") or metric.startswith(("null_rate_", "outlier_rate_", "ingestion_compliance_")):
        return f"{float(value):.2f}%"
    if metric in {"schema_compliance_rate"}:
        return f"{float(value):.2f}%"
    if metric in {"total_records", "records_reddit", "records_scraping", "actual_runs_scraping", "actual_runs_reddit", "expected_runs_weekly"}:
        return f"{int(float(value)):,}"
    if isinstance(value, (int, float)):
        number = float(value)
        return f"{number:,.2f}" if abs(number) < 1000 else f"{number:,.0f}"
    return str(value)


def make_kpi_card(value: str, label: str, subtitle: str | None = None, color: str | None = None) -> html.Div:
    return html.Div(
        style={**CARD_STYLE, "textAlign": "center"},
        children=[
            html.Div(
                value,
                style={
                    "fontSize": "2.2rem",
                    "fontWeight": "800",
                    "lineHeight": "1.1",
                    "color": color or COLORS["accent"],
                },
            ),
            html.Div(
                label,
                style={
                    "marginTop": "8px",
                    "fontSize": "0.83rem",
                    "textTransform": "uppercase",
                    "letterSpacing": "0.06em",
                    "color": COLORS["text_muted"],
                },
            ),
            html.Div(
                subtitle or "",
                style={"marginTop": "5px", "fontSize": "0.78rem", "color": COLORS["text_muted"]},
            ) if subtitle else html.Div(),
        ],
    )


def pick_source_color(source: str) -> str:
    if source == "reddit":
        return COLORS["reddit"]
    if source == "scraping":
        return COLORS["scraping"]
    return COLORS["neutral"]


def load_dashboard_data(source_filter: str):
    gov_df, gov_path = load_latest_parquet("governance")
    gold_df, gold_path = load_latest_parquet("gold_realmadrid")

    if gold_df.empty:
        return gov_df, gold_df, gov_path, gold_path, pd.DataFrame(), None

    if "published_at" in gold_df.columns:
        gold_df = gold_df.copy()
        gold_df["published_at"] = pd.to_datetime(gold_df["published_at"], errors="coerce")

    if source_filter and source_filter != "all" and "source" in gold_df.columns:
        filtered_gold = gold_df[gold_df["source"] == source_filter].copy()
    else:
        filtered_gold = gold_df.copy()

    if not gov_df.empty and source_filter != "all":
        gov_df = build_dynamic_governance_row(filtered_gold, gov_path or gold_path)

    return gov_df, gold_df, gov_path, gold_path, filtered_gold, latest_timestamp(gov_path or gold_path)


def build_dynamic_governance_row(df: pd.DataFrame, source_path: str | None) -> pd.DataFrame:
    total = len(df)
    if total == 0:
        return pd.DataFrame([{"pipeline_run_ts": latest_timestamp(source_path), "total_records": 0}])

    required_cols = ["url", "title", "author", "published_at", "body_text", "source"]
    row: dict[str, object] = {
        "pipeline_run_ts": latest_timestamp(source_path),
        "total_records": total,
        "records_reddit": int((df["source"] == "reddit").sum()) if "source" in df.columns else 0,
        "records_scraping": int((df["source"] == "scraping").sum()) if "source" in df.columns else 0,
    }

    if "body_text_missing" in df.columns:
        row["body_text_missing_rate"] = round(float(df["body_text_missing"].fillna(0).mean() * 100), 2)
    else:
        row["body_text_missing_rate"] = None

    if all(col in df.columns for col in required_cols):
        row["schema_compliance_rate"] = round(float(df[required_cols].notnull().all(axis=1).mean() * 100), 2)
    else:
        row["schema_compliance_rate"] = None

    if "url" in df.columns and total > 0:
        row["duplicate_rate"] = round(float(df["url"].duplicated().mean() * 100), 2)
    else:
        row["duplicate_rate"] = None

    null_key_cols = [
        "url",
        "title",
        "author",
        "published_at",
        "body_text",
        "body_text_clean",
        "source",
        "vader_compound",
        "text_processed",
    ]
    for col in null_key_cols:
        if col in df.columns:
            row[f"null_rate_{col}"] = round(float(df[col].isna().mean() * 100), 2)

    if "source" in df.columns and "bronze_source" in df.columns:
        expected_weekly = 7
        actual_scraping = int(df.loc[df["source"] == "scraping", "bronze_source"].nunique())
        actual_reddit = int(df.loc[df["source"] == "reddit", "bronze_source"].nunique())
        row.update(
            {
                "actual_runs_scraping": actual_scraping,
                "actual_runs_reddit": actual_reddit,
                "expected_runs_weekly": expected_weekly,
                "ingestion_compliance_scraping": round(min(actual_scraping / expected_weekly * 100, 100), 2),
                "ingestion_compliance_reddit": round(min(actual_reddit / expected_weekly * 100, 100), 2),
            }
        )

    if "source" in df.columns:
        scrap = df[df["source"] == "scraping"]
        if "reading_time_min" in scrap.columns and not scrap.empty:
            vals = scrap["reading_time_min"].dropna()
            if not vals.empty:
                q1 = vals.quantile(0.25)
                q3 = vals.quantile(0.75)
                iqr = q3 - q1
                outliers = ((vals < q1 - 1.5 * iqr) | (vals > q3 + 1.5 * iqr)).sum()
                row["outlier_rate_reading_time_min"] = round(float(outliers / len(vals) * 100), 2)

        reddit = df[df["source"] == "reddit"]
        for col in ["score", "num_comments"]:
            if col in reddit.columns and not reddit.empty:
                vals = reddit[col].dropna()
                if not vals.empty:
                    q1 = vals.quantile(0.25)
                    q3 = vals.quantile(0.75)
                    iqr = q3 - q1
                    outliers = ((vals < q1 - 1.5 * iqr) | (vals > q3 + 1.5 * iqr)).sum()
                    row[f"outlier_rate_{col}"] = round(float(outliers / len(vals) * 100), 2)

    return pd.DataFrame([row])


def source_scope_label(source_filter: str) -> str:
    return {
        "all": "All sources",
        "reddit": "Reddit",
        "scraping": "Web scraping",
    }.get(source_filter, "All sources")


def get_current_row(gov_df: pd.DataFrame, gold_df: pd.DataFrame, source_filter: str, source_path: str | None) -> pd.Series:
    if not gov_df.empty:
        return gov_df.iloc[0]
    dynamic = build_dynamic_governance_row(gold_df, source_path)
    return dynamic.iloc[0] if not dynamic.empty else pd.Series(dtype=object)


def collect_metric_rows(row: pd.Series) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for metric, value in row.items():
        if metric == "pipeline_run_ts":
            continue
        entries.append(
            {
                "Category": categorize_metric(metric),
                "KPI": humanize_metric(metric),
                "Value": format_value(metric, value),
                "RawMetric": metric,
            }
        )
    priority = {"Core": 0, "Completeness": 1, "Quality": 2, "Volume": 3, "Ingestion": 4, "Text": 5, "Distribution": 6, "Other": 7}
    return sorted(entries, key=lambda item: (priority.get(item["Category"], 99), item["KPI"]))


def categorize_metric(metric: str) -> str:
    if metric in {"total_records", "records_reddit", "records_scraping", "duplicate_rate", "schema_compliance_rate", "body_text_missing_rate"}:
        return "Core"
    if metric.startswith("null_rate_"):
        return "Completeness"
    if metric.startswith("outlier_rate_"):
        return "Quality"
    if metric.startswith("ingestion_") or metric.startswith("actual_runs_") or metric.startswith("expected_runs_"):
        return "Ingestion"
    if metric.startswith("text_length_") or metric.startswith("token_count_"):
        return "Text"
    if metric.startswith("sentiment_pct_") or metric.startswith("compound_"):
        return "Distribution"
    return "Other"


def build_null_chart(df: pd.DataFrame, source_filter: str) -> go.Figure:
    key_cols = [
        "url",
        "title",
        "author",
        "published_at",
        "body_text",
        "body_text_clean",
        "source",
        "vader_compound",
        "text_processed",
    ]
    rows = []
    total = len(df)
    for col in key_cols:
        if col in df.columns and total > 0:
            rate = round(float(df[col].isna().mean() * 100), 2)
            rows.append((col, rate))

    rows = sorted(rows, key=lambda item: item[1], reverse=True)

    fig = go.Figure()
    if rows:
        labels = [item[0].replace("_", " ").title() for item in rows]
        values = [item[1] for item in rows]
        colors = [
            SENTIMENT_STYLE["bad"] if value >= 10 else SENTIMENT_STYLE["warn"] if value > 0 else SENTIMENT_STYLE["good"]
            for value in values
        ]
        fig.add_trace(
            go.Bar(
                name="Null rate",
                x=values,
                y=labels,
                orientation="h",
                marker_color=colors,
                hovertemplate="<b>%{y}</b><br>Null rate: %{x:.2f}%<extra></extra>",
            )
        )

    fig.update_layout(
        **BASE_LAYOUT,
        title=dict(
            text="Null Rate by Key Field",
            x=0,
            font=dict(size=16),
        ),
        xaxis_title="Null rate (%)",
        yaxis_title="Field",
        height=390,
        showlegend=True,
        annotations=[
            dict(
                text=f"Source: latest Gold base parquet · Scope: {source_scope_label(source_filter)}",
                x=0,
                y=-0.23,
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(color=COLORS["text_muted"], size=11),
                align="left",
            )
        ],
    )
    fig.update_xaxes(range=[0, max([20.0] + [value for _, value in rows]) * 1.1 if rows else 20], fixedrange=False)
    return fig


def build_volume_chart(df: pd.DataFrame, source_filter: str, granularity: str) -> go.Figure:
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    if df.empty or "published_at" not in df.columns:
        fig.update_layout(
            **BASE_LAYOUT,
            title=dict(text="Volume of Records by Period and Source", x=0, font=dict(size=16)),
            xaxis_title="Period",
            yaxis_title="Number of records",
            height=410,
            annotations=[
                dict(
                    text=f"Source: latest Gold base parquet · Scope: {source_scope_label(source_filter)}",
                    x=0,
                    y=-0.23,
                    xref="paper",
                    yref="paper",
                    showarrow=False,
                    font=dict(color=COLORS["text_muted"], size=11),
                    align="left",
                )
            ],
        )
        return fig

    frame = df.copy()
    frame = frame.dropna(subset=["published_at"])
    if frame.empty:
        return build_volume_chart(pd.DataFrame(), source_filter, granularity)

    freq_map = {"day": "D", "week": "W", "month": "M"}
    label_map = {"day": "%Y-%m-%d", "week": "%Y-%m-%d", "month": "%Y-%m"}
    freq = freq_map.get(granularity, "W")
    label_fmt = label_map.get(granularity, "%Y-%m-%d")
    frame["period"] = frame["published_at"].dt.to_period(freq).dt.start_time

    source_order = [src for src in ["reddit", "scraping"] if "source" in frame.columns and src in frame["source"].astype(str).unique()]
    if not source_order and "source" in frame.columns:
        source_order = sorted(frame["source"].dropna().astype(str).unique().tolist())

    period_index = sorted(frame["period"].dropna().unique().tolist())
    if not period_index:
        return build_volume_chart(pd.DataFrame(), source_filter, granularity)

    for src in source_order:
        subset = frame[frame["source"] == src] if "source" in frame.columns else frame
        grouped = subset.groupby("period").size().reindex(period_index, fill_value=0).reset_index(name="n_records")
        fig.add_trace(
            go.Bar(
                name=source_scope_label(src),
                x=[pd.Timestamp(value).strftime(label_fmt) for value in grouped["period"]],
                y=grouped["n_records"],
                marker_color=pick_source_color(src),
                opacity=0.9,
                hovertemplate=f"<b>{source_scope_label(src)}</b><br>Period: %{{x}}<br>Records: %{{y}}<extra></extra>",
            ),
            secondary_y=False,
        )

    total = frame.groupby("period").size().reindex(period_index, fill_value=0).reset_index(name="n_records")
    fig.add_trace(
        go.Scatter(
            name="Total volume",
            x=[pd.Timestamp(value).strftime(label_fmt) for value in total["period"]],
            y=total["n_records"],
            mode="lines+markers",
            line=dict(color=COLORS["positive"], width=3),
            marker=dict(size=7),
            hovertemplate="<b>Total volume</b><br>Period: %{x}<br>Records: %{y}<extra></extra>",
        ),
        secondary_y=True,
    )

    fig.update_layout(
        **BASE_LAYOUT,
        title=dict(text="Volume of Records by Period and Source", x=0, font=dict(size=16)),
        barmode="group",
        height=410,
        annotations=[
            dict(
                text=f"Source: latest Gold base parquet · Scope: {source_scope_label(source_filter)}",
                x=0,
                y=-0.23,
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(color=COLORS["text_muted"], size=11),
                align="left",
            )
        ],
    )
    fig.update_xaxes(title_text="Period", tickangle=-35)
    fig.update_yaxes(title_text="Number of records", secondary_y=False)
    fig.update_yaxes(title_text="Total records", secondary_y=True, showgrid=False)
    return fig


def build_outlier_chart(df: pd.DataFrame, source_filter: str) -> go.Figure:
    metrics = []
    if "source" in df.columns:
        if source_filter in {"all", "scraping"} and "reading_time_min" in df.columns:
            values = df[df["source"] == "scraping"]["reading_time_min"].dropna() if source_filter != "scraping" else df["reading_time_min"].dropna()
            if not values.empty:
                q1 = values.quantile(0.25)
                q3 = values.quantile(0.75)
                iqr = q3 - q1
                rate = round(float((((values < q1 - 1.5 * iqr) | (values > q3 + 1.5 * iqr)).sum()) / len(values) * 100), 2)
                metrics.append(("Reading Time Min", rate))
        if source_filter in {"all", "reddit"}:
            for field in ["score", "num_comments"]:
                if field in df.columns:
                    values = df[df["source"] == "reddit"][field].dropna() if source_filter != "reddit" else df[field].dropna()
                    if not values.empty:
                        q1 = values.quantile(0.25)
                        q3 = values.quantile(0.75)
                        iqr = q3 - q1
                        rate = round(float((((values < q1 - 1.5 * iqr) | (values > q3 + 1.5 * iqr)).sum()) / len(values) * 100), 2)
                        metrics.append((field.replace("_", " ").title(), rate))

    metrics = sorted(metrics, key=lambda item: item[1], reverse=True)
    fig = go.Figure()
    if metrics:
        labels = [item[0] for item in metrics]
        values = [item[1] for item in metrics]
        colors = [
            SENTIMENT_STYLE["bad"] if value >= 10 else SENTIMENT_STYLE["warn"] if value > 0 else SENTIMENT_STYLE["good"]
            for value in values
        ]
        fig.add_trace(
            go.Bar(
                name="Outlier rate",
                x=labels,
                y=values,
                marker_color=colors,
                hovertemplate="<b>%{x}</b><br>Outlier rate: %{y:.2f}%<extra></extra>",
            )
        )

    fig.update_layout(
        **BASE_LAYOUT,
        title=dict(text="Outlier Rate by Numeric Field", x=0, font=dict(size=16)),
        xaxis_title="Numeric field",
        yaxis_title="Outlier rate (%)",
        height=360,
        showlegend=True,
        annotations=[
            dict(
                text=f"Source: latest Gold base parquet · Scope: {source_scope_label(source_filter)}",
                x=0,
                y=-0.28,
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(color=COLORS["text_muted"], size=11),
                align="left",
            )
        ],
    )
    fig.update_yaxes(range=[0, max([10.0] + [value for _, value in metrics]) * 1.15 if metrics else 10], fixedrange=False)
    return fig


def build_table(rows: Iterable[dict[str, str]]) -> dash_table.DataTable:
    data = list(rows)
    return dash_table.DataTable(
        data=data,
        columns=[
            {"name": "Category", "id": "Category"},
            {"name": "KPI", "id": "KPI"},
            {"name": "Value", "id": "Value"},
        ],
        sort_action="native",
        filter_action="native",
        page_action="native",
        page_size=16,
        style_table={
            "overflowX": "hidden",
            "overflowY": "auto",
            "maxHeight": "560px",
            "borderRadius": "10px",
        },
        style_header={
            "backgroundColor": COLORS["bg"],
            "color": COLORS["text"],
            "fontWeight": "700",
            "border": f"1px solid {COLORS['border']}",
        },
        style_cell={
            "backgroundColor": COLORS["card_bg"],
            "color": COLORS["text"],
            "border": f"1px solid {COLORS['border']}",
            "fontSize": "13px",
            "padding": "10px",
            "whiteSpace": "normal",
            "height": "auto",
            "textAlign": "left",
            "minWidth": "0px",
            "maxWidth": "100%",
        },
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": "#0b1220"},
            {"if": {"filter_query": '{Category} = "Core"'}, "borderLeft": f"3px solid {COLORS['positive']}"},
            {"if": {"filter_query": '{Category} = "Completeness"'}, "borderLeft": f"3px solid {COLORS['negative']}"},
            {"if": {"filter_query": '{Category} = "Quality"'}, "borderLeft": f"3px solid {COLORS['accent']}"},
            {"if": {"filter_query": '{Category} = "Ingestion"'}, "borderLeft": f"3px solid {COLORS['neutral']}"},
        ],
    )


def card_color(metric: str, value: object) -> str:
    if metric in {"schema_compliance_rate"}:
        return COLORS["positive"]
    if metric == "duplicate_rate":
        return COLORS["negative"] if float(value or 0) > 0 else COLORS["positive"]
    if metric == "body_text_missing_rate":
        return COLORS["negative"] if float(value or 0) > 0 else COLORS["positive"]
    return COLORS["accent"]


def build_kpi_cards(row: pd.Series) -> list[dbc.Col]:
    body_missing = row.get("body_text_missing_rate", None)
    duplicate_rate = row.get("duplicate_rate", None)
    schema_rate = row.get("schema_compliance_rate", None)
    total_records = row.get("total_records", 0)

    cards = [
        dbc.Col(make_kpi_card(format_value("total_records", total_records), "Total records", "Latest Gold / governance run", COLORS["accent"]), md=3, xs=6),
        dbc.Col(make_kpi_card(format_value("body_text_missing_rate", body_missing), "Body text missing", "Key content completeness", card_color("body_text_missing_rate", body_missing)), md=3, xs=6),
        dbc.Col(make_kpi_card(format_value("duplicate_rate", duplicate_rate), "Duplicate rate", "Post-dedup quality check", card_color("duplicate_rate", duplicate_rate)), md=3, xs=6),
        dbc.Col(make_kpi_card(format_value("schema_compliance_rate", schema_rate), "Schema compliance", "Required fields present", card_color("schema_compliance_rate", schema_rate)), md=3, xs=6),
    ]
    return cards


app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],
    title="Real Madrid Pulse - Governance Dashboard",
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
server = app.server


app.layout = html.Div(
    style={
        "backgroundColor": COLORS["bg"],
        "minHeight": "100vh",
        "color": COLORS["text"],
        "fontFamily": "Arial, Helvetica, sans-serif",
    },
    children=[
        html.Div(
            style={
                "backgroundColor": COLORS["card_bg"],
                "borderBottom": f"2px solid {COLORS['accent']}",
                "padding": "22px 40px 18px",
                "display": "flex",
                "justifyContent": "space-between",
                "gap": "16px",
                "alignItems": "center",
                "flexWrap": "wrap",
            },
            children=[
                html.Div(
                    children=[
                        html.Div(
                            "REAL MADRID PULSE - GOVERNANCE DASHBOARD",
                            style={
                                "fontSize": "1.55rem",
                                "fontWeight": "800",
                                "color": COLORS["accent"],
                                "letterSpacing": "0.06em",
                            },
                        ),
                        html.Div(
                            "Data quality, completeness, and compliance view over the latest Gold layer.",
                            style={"marginTop": "5px", "fontSize": "0.92rem", "color": COLORS["text_muted"]},
                        ),
                    ]
                ),
                html.Div(
                    style={"textAlign": "right"},
                    children=[
                        html.Div(
                            "Equipo: Esteban Alexander Bautista Solano · Jefferson David Ortiz Buitrago · Cristian Andres Cruz Puentes",
                            style={"fontSize": "0.82rem", "color": COLORS["text_muted"]},
                        ),
                        html.Div(
                            id="last-updated",
                            style={"fontSize": "0.82rem", "color": COLORS["text_muted"], "marginTop": "4px"},
                        ),
                    ],
                ),
            ],
        ),

        html.Div(
            style={"padding": "22px 40px 28px"},
            children=[
                dbc.Row(
                    className="g-3 mb-3",
                    children=[
                        dbc.Col(
                            html.Div(
                                style={**CARD_STYLE, "padding": "14px 16px"},
                                children=[
                                    html.Div("Dashboard filters", style={"fontSize": "0.78rem", "color": COLORS["text_muted"], "textTransform": "uppercase", "letterSpacing": "0.08em", "marginBottom": "8px"}),
                                    dbc.Row(
                                        className="g-2",
                                        children=[
                                            dbc.Col(
                                                dcc.Dropdown(
                                                    id="source-filter",
                                                    options=[
                                                        {"label": "All sources", "value": "all"},
                                                        {"label": "Reddit", "value": "reddit"},
                                                        {"label": "Web scraping", "value": "scraping"},
                                                    ],
                                                    value="all",
                                                    clearable=False,
                                                    searchable=False,
                                                    style={"color": "#111827"},
                                                ),
                                                md=4,
                                                xs=12,
                                            ),
                                            dbc.Col(
                                                dcc.RadioItems(
                                                    id="period-filter",
                                                    options=[
                                                        {"label": " Daily", "value": "day"},
                                                        {"label": " Weekly", "value": "week"},
                                                        {"label": " Monthly", "value": "month"},
                                                    ],
                                                    value="week",
                                                    inline=True,
                                                    inputStyle={"marginRight": "5px"},
                                                    labelStyle={
                                                        "marginRight": "14px",
                                                        "fontSize": "0.88rem",
                                                        "cursor": "pointer",
                                                        "color": COLORS["text"],
                                                    },
                                                ),
                                                md=5,
                                                xs=12,
                                            ),
                                            dbc.Col(
                                                dbc.Button(
                                                    "Refresh latest Gold",
                                                    id="refresh-button",
                                                    color="warning",
                                                    outline=True,
                                                    size="sm",
                                                ),
                                                md=3,
                                                xs=12,
                                                style={"textAlign": "right"},
                                            ),
                                        ],
                                    ),
                                ],
                            ),
                            width=12,
                        ),
                    ],
                ),

                dbc.Row(id="kpi-row", className="g-3 mb-2"),

                dbc.Row(
                    className="g-3 mb-3",
                    children=[
                        dbc.Col(
                            html.Div(
                                style=CARD_STYLE,
                                children=[
                                    html.H5("Null Rate by Key Field", style={"color": COLORS["text"], "fontWeight": "700", "marginBottom": "6px"}),
                                    html.Div(
                                        "Ordered from highest to lowest severity. Green means clean, amber means watch, and red means urgent.",
                                        style={"fontSize": "0.8rem", "color": COLORS["text_muted"], "marginBottom": "10px"},
                                    ),
                                    dcc.Graph(id="null-rate-chart", config=CHART_CONFIG),
                                ],
                            ),
                            width=12,
                        ),
                    ],
                ),

                dbc.Row(
                    className="g-3 mb-3",
                    children=[
                        dbc.Col(
                            html.Div(
                                style=CARD_STYLE,
                                children=[
                                    html.H5("Volume of Records by Period and Source", style={"color": COLORS["text"], "fontWeight": "700", "marginBottom": "6px"}),
                                    html.Div(
                                        "Bars show records by source; the line shows total volume for the selected period.",
                                        style={"fontSize": "0.8rem", "color": COLORS["text_muted"], "marginBottom": "10px"},
                                    ),
                                    dcc.Graph(id="volume-chart", config=CHART_CONFIG),
                                ],
                            ),
                            md=7,
                            xs=12,
                        ),
                        dbc.Col(
                            html.Div(
                                style=CARD_STYLE,
                                children=[
                                    html.H5("Outlier Rate by Numeric Field", style={"color": COLORS["text"], "fontWeight": "700", "marginBottom": "6px"}),
                                    html.Div(
                                        "Outliers are measured with the IQR rule over the latest filtered scope.",
                                        style={"fontSize": "0.8rem", "color": COLORS["text_muted"], "marginBottom": "10px"},
                                    ),
                                    dcc.Graph(id="outlier-chart", config=CHART_CONFIG),
                                ],
                            ),
                            md=5,
                            xs=12,
                        ),
                    ],
                ),

                dbc.Row(
                    className="g-3",
                    children=[
                        dbc.Col(
                            html.Div(
                                style=CARD_STYLE,
                                children=[
                                    html.H5("Governance KPI Table", style={"color": COLORS["text"], "fontWeight": "700", "marginBottom": "6px"}),
                                    html.Div(
                                        "Current values for all governance KPIs calculated from the latest Gold layer.",
                                        style={"fontSize": "0.8rem", "color": COLORS["text_muted"], "marginBottom": "10px"},
                                    ),
                                    html.Div(id="kpi-table"),
                                ],
                            ),
                            width=12,
                        ),
                    ],
                ),
            ],
        ),

        dcc.Interval(id="interval", interval=5 * 60 * 1000, n_intervals=0),
    ],
)


@callback(
    Output("last-updated", "children"),
    Output("kpi-row", "children"),
    Output("null-rate-chart", "figure"),
    Output("volume-chart", "figure"),
    Output("outlier-chart", "figure"),
    Output("kpi-table", "children"),
    Input("source-filter", "value"),
    Input("period-filter", "value"),
    Input("refresh-button", "n_clicks"),
    Input("interval", "n_intervals"),
)
def refresh_dashboard(source_filter: str, period_filter: str, _refresh_clicks, _interval_ticks):
    gov_df, gold_df, gov_path, gold_path, filtered_gold, updated_at = load_dashboard_data(source_filter)
    row = get_current_row(gov_df, filtered_gold, source_filter, gov_path or gold_path)

    kpi_cards = build_kpi_cards(row)
    null_fig = build_null_chart(filtered_gold, source_filter)
    volume_fig = build_volume_chart(filtered_gold, source_filter, period_filter)
    outlier_fig = build_outlier_chart(filtered_gold, source_filter)
    table_rows = collect_metric_rows(row)
    table = build_table(table_rows)

    timestamp_label = f"Last updated: {updated_at}" if updated_at else "Last updated: No data"
    return timestamp_label, kpi_cards, null_fig, volume_fig, outlier_fig, table


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8052, debug=True)
