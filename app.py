"""
app.py — Maroclear Transaction Suspension Risk Dashboard
--------------------------------------------------------
Local:  streamlit run app.py
Hosted: deploy to Streamlit Community Cloud (share.streamlit.io)

Data modes (auto-detected in order):
  1. Local files  — reports/predictions.csv  (dev / local runs)
  2. File upload  — user uploads CSV via the sidebar  (hosted / demo)
"""

import warnings
warnings.filterwarnings("ignore")

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import io

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Maroclear — Suspension Risk",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Colour palette ────────────────────────────────────────────────────────────

RED   = "#D64045"
GREEN = "#2ECC71"
BLUE  = "#2C7BE5"
AMBER = "#F59E0B"
GREY  = "#6B7280"

FAILURE_STATUSES = {"FLAR", "RFSL", "SFSL", "PLAR"}

# ── CSS tweaks ────────────────────────────────────────────────────────────────

st.markdown("""
<style>
    .metric-card {
        background: #ffffff;
        border: 1px solid #E5E7EB;
        border-radius: 10px;
        padding: 16px 20px;
        text-align: center;
    }
    .metric-label { font-size: 13px; color: #6B7280; margin-bottom: 4px; }
    .metric-value { font-size: 28px; font-weight: 700; color: #1A1A2E; }
    .metric-delta { font-size: 13px; margin-top: 4px; }
    .risk-high  { color: #D64045; }
    .risk-low   { color: #2ECC71; }
    .risk-amber { color: #F59E0B; }
    section[data-testid="stSidebar"] { background-color: #1A1A2E; }
    section[data-testid="stSidebar"] * { color: #F9FAFB !important; }
</style>
""", unsafe_allow_html=True)


# ── Data loading ──────────────────────────────────────────────────────────────

def _enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns shared by both loading paths."""
    df["TRADEDATE"] = pd.to_datetime(df["TRADEDATE"], errors="coerce")
    df["is_suspension"] = df["TRADESTATUS"].apply(
        lambda x: 1 if x in FAILURE_STATUSES else (0 if x == "OK" else np.nan)
    )
    df["Date"]      = df["TRADEDATE"].dt.date
    df["Month"]     = df["TRADEDATE"].dt.to_period("M").astype(str)
    df["DayOfWeek"] = df["TRADEDATE"].dt.day_name()
    df["risk_label"] = df["suspension_predicted"].map({1: "HIGH RISK", 0: "LOW RISK"})
    return df


@st.cache_data(show_spinner="Loading data...")
def load_local() -> pd.DataFrame | None:
    """Try to load from local file system (dev / server deployment)."""
    pred_path = Path("reports/predictions.csv")
    if not pred_path.exists():
        return None

    df = pd.read_csv(pred_path, low_memory=False)

    # Optionally enrich with raw data if present
    raw_cols = [
        "TRADEREFERENCE", "TRADETIME", "TRADECURRENCY",
        "SETTLEMENTTYPE", "SETTLEMENTCYCLE", "SETTLEMENTDATE", "SETTLEDDATE",
        "INSTRUMENTTYPE", "MARKETSEGMENT", "ALTSECURITYID",
        "SETTLEMENTAMOUNT", "TRADEPRICE", "TRADEQUANTITY", "volume_globale",
    ]
    raw_path = Path("data/raw/data.csv")
    if raw_path.exists():
        raw = pd.read_csv(raw_path, low_memory=False)
        available = [c for c in raw_cols if c in raw.columns]
        df = df.merge(raw[["TRADEREFERENCE"] + available], on="TRADEREFERENCE", how="left")

    feat_cols = [
        "TRADEREFERENCE",
        "cutoff_dépassé", "buyer_historical_suspens", "vendeur_historique_suspens",
        "daily_activity", "global_exchange_frequency", "ratio_instruction_vs_market",
        "liquidité_volume_5j", "RSI_5", "MACD_diff",
        "taux_changement_prix", "taux_changement_volume",
    ]
    proc_path = Path("data/processed/test.csv")
    if proc_path.exists():
        proc = pd.read_csv(proc_path, low_memory=False)
        available = [c for c in feat_cols if c in proc.columns]
        df = df.merge(proc[available], on="TRADEREFERENCE", how="left")

    return _enrich(df)


@st.cache_data(show_spinner="Parsing uploaded file...")
def load_uploaded(file_bytes: bytes) -> pd.DataFrame:
    """Parse a user-uploaded predictions CSV."""
    df = pd.read_csv(io.BytesIO(file_bytes), low_memory=False)
    required = {"TRADEREFERENCE", "TRADEDATE", "TRADERBPID", "TRADESTATUS",
                "suspension_probability", "suspension_predicted"}
    missing = required - set(df.columns)
    if missing:
        st.error(f"Uploaded file is missing columns: {', '.join(sorted(missing))}")
        st.stop()
    return _enrich(df)

# ── Resolve data source: local file OR uploaded file ─────────────────────────

df = load_local()  # returns None when running hosted with no local files

with st.sidebar:
    st.markdown("## 📊 Maroclear")
    st.markdown("**Suspension Risk Dashboard**")
    st.markdown("---")

    if df is None:
        # Hosted mode — no local predictions file found
        st.markdown("**Upload predictions CSV**")
        st.caption("Output of `predict.py` — required to load the dashboard.")
        uploaded = st.file_uploader(
            "predictions.csv",
            type="csv",
            label_visibility="collapsed",
        )
        if uploaded is None:
            st.info("Upload a predictions CSV to get started.")
            st.markdown("""
            **Expected columns:**
            - `TRADEREFERENCE`
            - `TRADEDATE`
            - `TRADERBPID`
            - `TRADESTATUS`
            - `suspension_probability`
            - `suspension_predicted`
            """)
            st.stop()
        df = load_uploaded(uploaded.read())
    else:
        st.caption("Local data loaded automatically.")

    st.markdown("---")

    page = st.radio(
        "Navigation",
        ["Daily Risk Overview", "Participant Monitoring",
         "Security Heatmap", "Prediction Drill-through"],
        label_visibility="collapsed",
    )

    st.markdown("---")
    st.markdown("**Filters**")

    # Date range
    min_date = df["Date"].min()
    max_date = df["Date"].max()
    date_range = st.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    # Handle single-date selection gracefully
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = end_date = date_range[0] if date_range else min_date

    st.markdown("---")
    st.caption(f"Last prediction run: {df['TRADEDATE'].max().strftime('%d %b %Y')}")

# ── Apply date filter ─────────────────────────────────────────────────────────

fdf = df[(df["Date"] >= start_date) & (df["Date"] <= end_date)].copy()

if fdf.empty:
    st.warning("No data for the selected date range.")
    st.stop()


# ── Helpers ───────────────────────────────────────────────────────────────────

def kpi_card(label: str, value: str, delta: str = "", colour: str = "#1A1A2E"):
    delta_html = f'<div class="metric-delta" style="color:{colour}">{delta}</div>' if delta else ""
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value" style="color:{colour}">{value}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)


def pct(val):
    return f"{val:.1%}" if not np.isnan(val) else "—"


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — Daily Risk Overview
# ══════════════════════════════════════════════════════════════════════════════

if page == "Daily Risk Overview":
    st.title("Daily Risk Overview")
    st.caption(f"{start_date.strftime('%d %b %Y')}  →  {end_date.strftime('%d %b %Y')}")

    # ── KPI cards ─────────────────────────────────────────────────────────
    total    = len(fdf)
    flagged  = int(fdf["suspension_predicted"].sum())
    pred_rt  = flagged / total if total else 0
    actual   = int(fdf["is_suspension"].sum())
    settled  = int(fdf["is_suspension"].notna().sum())
    actual_rt = actual / settled if settled else np.nan
    high_risk_parts = fdf[fdf["suspension_predicted"] == 1]["TRADERBPID"].nunique()

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        kpi_card("Total Trades", f"{total:,}")
    with c2:
        kpi_card("Flagged HIGH RISK", f"{flagged:,}",
                 delta=pct(pred_rt) + " of trades",
                 colour=RED if flagged > 0 else GREEN)
    with c3:
        kpi_card("Predicted Suspension Rate", pct(pred_rt),
                 colour=RED if pred_rt > 0.1 else (AMBER if pred_rt > 0.05 else GREEN))
    with c4:
        kpi_card("High-Risk Participants", str(high_risk_parts),
                 colour=RED if high_risk_parts > 5 else GREY)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Daily trend ───────────────────────────────────────────────────────
    daily = (
        fdf.groupby("Date")
        .agg(
            total_trades       =("TRADEREFERENCE", "count"),
            flagged_trades     =("suspension_predicted", "sum"),
            actual_suspensions =("is_suspension", "sum"),
            settled_trades     =("is_suspension", lambda x: x.notna().sum()),
        )
        .reset_index()
    )
    daily["predicted_rate"] = daily["flagged_trades"]    / daily["total_trades"]
    daily["actual_rate"]    = daily["actual_suspensions"] / daily["settled_trades"].replace(0, np.nan)
    daily["Date"] = pd.to_datetime(daily["Date"])

    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(
        x=daily["Date"], y=daily["predicted_rate"],
        name="Predicted Rate", line=dict(color=BLUE, width=2),
        hovertemplate="%{x|%d %b %Y}<br>Predicted: %{y:.1%}<extra></extra>",
    ))
    fig_trend.add_trace(go.Scatter(
        x=daily["Date"], y=daily["actual_rate"],
        name="Actual Rate", line=dict(color=RED, width=2, dash="dot"),
        hovertemplate="%{x|%d %b %Y}<br>Actual: %{y:.1%}<extra></extra>",
    ))
    fig_trend.add_hline(y=0.1, line_dash="dash", line_color=AMBER,
                        annotation_text="10% threshold")
    fig_trend.update_layout(
        title="Suspension Rate Trend",
        xaxis_title=None, yaxis_title="Rate",
        yaxis_tickformat=".0%",
        legend=dict(orientation="h", y=1.1),
        height=320, margin=dict(t=50, b=20),
        plot_bgcolor="white", paper_bgcolor="white",
    )
    fig_trend.update_xaxes(showgrid=False)
    fig_trend.update_yaxes(showgrid=True, gridcolor="#F3F4F6")
    st.plotly_chart(fig_trend, use_container_width=True)

    # ── Bottom row: top participants + outcome donut ───────────────────────
    col_left, col_right = st.columns([3, 2])

    with col_left:
        top_parts = (
            fdf[fdf["suspension_predicted"] == 1]
            .groupby("TRADERBPID")["TRADEREFERENCE"]
            .count()
            .sort_values(ascending=False)
            .head(10)
            .reset_index()
        )
        top_parts.columns = ["Participant", "Flagged Trades"]

        fig_bar = px.bar(
            top_parts, x="Flagged Trades", y="Participant",
            orientation="h",
            color="Flagged Trades",
            color_continuous_scale=[[0, "#FECACA"], [1, RED]],
            title="Top 10 High-Risk Participants",
        )
        fig_bar.update_layout(
            height=340, margin=dict(t=50, b=20),
            showlegend=False, coloraxis_showscale=False,
            plot_bgcolor="white", paper_bgcolor="white",
            yaxis=dict(autorange="reversed"),
        )
        fig_bar.update_xaxes(showgrid=True, gridcolor="#F3F4F6")
        fig_bar.update_yaxes(showgrid=False)
        st.plotly_chart(fig_bar, use_container_width=True)

    with col_right:
        outcome_counts = (
            fdf["is_suspension"]
            .map({1.0: "Confirmed Suspension", 0.0: "Settled OK"})
            .fillna("Pending / OPEN")
            .value_counts()
            .reset_index()
        )
        outcome_counts.columns = ["Outcome", "Count"]

        fig_donut = px.pie(
            outcome_counts, names="Outcome", values="Count",
            hole=0.55,
            color="Outcome",
            color_discrete_map={
                "Confirmed Suspension": RED,
                "Settled OK":           GREEN,
                "Pending / OPEN":       GREY,
            },
            title="Trade Outcome Breakdown",
        )
        fig_donut.update_layout(
            height=340, margin=dict(t=50, b=20),
            legend=dict(orientation="v", x=0.8),
            plot_bgcolor="white", paper_bgcolor="white",
        )
        fig_donut.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig_donut, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — Participant Monitoring
# ══════════════════════════════════════════════════════════════════════════════

elif page == "Participant Monitoring":
    st.title("Participant Monitoring")
    st.caption(f"{start_date.strftime('%d %b %Y')}  →  {end_date.strftime('%d %b %Y')}")

    # ── League table ──────────────────────────────────────────────────────
    league = (
        fdf.groupby("TRADERBPID")
        .agg(
            Total         =("TRADEREFERENCE", "count"),
            Flagged       =("suspension_predicted", "sum"),
            Confirmed     =("is_suspension", "sum"),
            Settled       =("is_suspension", lambda x: x.notna().sum()),
            Avg_Risk      =("suspension_probability", "mean"),
        )
        .reset_index()
    )
    league["Predicted Rate"] = league["Flagged"]  / league["Total"]
    league["Actual Rate"]    = league["Confirmed"] / league["Settled"].replace(0, np.nan)
    league["Rank"]           = league["Predicted Rate"].rank(ascending=False, method="dense").astype(int)
    league = league.sort_values("Rank")
    league.columns = [
        "Participant", "Total Trades", "Flagged", "Confirmed Suspensions",
        "Settled", "Avg Risk Score", "Predicted Rate", "Actual Rate", "Rank"
    ]

    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.markdown("#### Risk League Table")
        display = league[["Rank", "Participant", "Total Trades", "Flagged",
                           "Predicted Rate", "Actual Rate", "Avg Risk Score"]].copy()
        display["Predicted Rate"] = display["Predicted Rate"].apply(pct)
        display["Actual Rate"]    = display["Actual Rate"].apply(pct)
        display["Avg Risk Score"] = display["Avg Risk Score"].apply(lambda x: f"{x:.3f}")
        st.dataframe(
            display,
            use_container_width=True,
            hide_index=True,
            height=420,
            column_config={
                "Rank":           st.column_config.NumberColumn(width="small"),
                "Participant":    st.column_config.TextColumn(width="medium"),
                "Total Trades":   st.column_config.NumberColumn(width="small"),
                "Flagged":        st.column_config.NumberColumn(width="small"),
                "Predicted Rate": st.column_config.TextColumn(width="medium"),
                "Actual Rate":    st.column_config.TextColumn(width="medium"),
                "Avg Risk Score": st.column_config.TextColumn(width="medium"),
            },
        )

    with col_right:
        st.markdown("#### Select Participant")
        selected = st.selectbox(
            "Participant",
            options=league["Participant"].tolist(),
            label_visibility="collapsed",
        )
        p_row = league[league["Participant"] == selected].iloc[0]

        st.markdown("<br>", unsafe_allow_html=True)
        kpi_card("Total Trades",    f"{int(p_row['Total Trades']):,}")
        st.markdown("<br>", unsafe_allow_html=True)
        kpi_card("Flagged Trades",  f"{int(p_row['Flagged']):,}",
                 colour=RED if p_row["Flagged"] > 0 else GREEN)
        st.markdown("<br>", unsafe_allow_html=True)
        kpi_card("Predicted Rate",  pct(p_row["Predicted Rate"]),
                 colour=RED if p_row["Predicted Rate"] > 0.1 else AMBER)
        st.markdown("<br>", unsafe_allow_html=True)
        kpi_card("Risk Rank",       f"#{int(p_row['Rank'])} of {len(league)}")

    st.markdown("---")

    # ── Participant trend ─────────────────────────────────────────────────
    p_daily = (
        fdf[fdf["TRADERBPID"] == selected]
        .groupby("Date")
        .agg(
            total   =("TRADEREFERENCE", "count"),
            flagged =("suspension_predicted", "sum"),
        )
        .reset_index()
    )
    p_daily["rate"] = p_daily["flagged"] / p_daily["total"]
    p_daily["Date"] = pd.to_datetime(p_daily["Date"])

    fig_pt = px.line(
        p_daily, x="Date", y="rate",
        title=f"Daily Predicted Suspension Rate — {selected}",
        labels={"rate": "Predicted Rate", "Date": ""},
        color_discrete_sequence=[BLUE],
    )
    fig_pt.add_hline(y=0.1, line_dash="dash", line_color=AMBER,
                     annotation_text="10% threshold")
    fig_pt.update_layout(
        height=260, margin=dict(t=50, b=20),
        yaxis_tickformat=".0%",
        plot_bgcolor="white", paper_bgcolor="white",
    )
    fig_pt.update_xaxes(showgrid=False)
    fig_pt.update_yaxes(showgrid=True, gridcolor="#F3F4F6")
    st.plotly_chart(fig_pt, use_container_width=True)

    # ── Buyer vs seller historical risk (if feature columns exist) ────────
    if "buyer_historical_suspens" in fdf.columns and "vendeur_historique_suspens" in fdf.columns:
        st.markdown("---")
        bvs = (
            fdf.groupby("TRADERBPID")
            .agg(
                buyer_risk  =("buyer_historical_suspens",   "mean"),
                seller_risk =("vendeur_historique_suspens", "mean"),
                flagged     =("suspension_predicted",        "sum"),
            )
            .sort_values("flagged", ascending=False)
            .head(15)
            .reset_index()
        )
        fig_bvs = go.Figure()
        fig_bvs.add_bar(
            name="Buyer Historical Risk",
            x=bvs["TRADERBPID"], y=bvs["buyer_risk"],
            marker_color=BLUE,
        )
        fig_bvs.add_bar(
            name="Seller Historical Risk",
            x=bvs["TRADERBPID"], y=bvs["seller_risk"],
            marker_color=AMBER,
        )
        fig_bvs.update_layout(
            barmode="group",
            title="5-Day Historical Suspension Rate — Buyer vs Seller (Top 15)",
            yaxis_tickformat=".0%",
            height=320, margin=dict(t=50, b=20),
            legend=dict(orientation="h", y=1.1),
            plot_bgcolor="white", paper_bgcolor="white",
            xaxis_title=None,
        )
        fig_bvs.update_xaxes(showgrid=False)
        fig_bvs.update_yaxes(showgrid=True, gridcolor="#F3F4F6")
        st.plotly_chart(fig_bvs, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — Security Heatmap
# ══════════════════════════════════════════════════════════════════════════════

elif page == "Security Heatmap":
    st.title("Security Heatmap")
    st.caption(f"{start_date.strftime('%d %b %Y')}  →  {end_date.strftime('%d %b %Y')}")

    # ── Instrument type filter ────────────────────────────────────────────
    col_f1, col_f2 = st.columns(2)
    if "INSTRUMENTTYPE" in fdf.columns:
        types = ["All"] + sorted(fdf["INSTRUMENTTYPE"].dropna().unique().tolist())
        with col_f1:
            sel_type = st.selectbox("Instrument Type", types)
        if sel_type != "All":
            fdf = fdf[fdf["INSTRUMENTTYPE"] == sel_type]

    if "MARKETSEGMENT" in fdf.columns:
        segments = ["All"] + sorted(fdf["MARKETSEGMENT"].dropna().unique().tolist())
        with col_f2:
            sel_seg = st.selectbox("Market Segment", segments)
        if sel_seg != "All":
            fdf = fdf[fdf["MARKETSEGMENT"] == sel_seg]

    # ── Per-ISIN aggregate ────────────────────────────────────────────────
    sec = (
        fdf.groupby("SECURITYID")
        .agg(
            Total   =("TRADEREFERENCE",       "count"),
            Flagged =("suspension_predicted",  "sum"),
            Actual  =("is_suspension",         "sum"),
            AvgRisk =("suspension_probability","mean"),
        )
        .reset_index()
        .sort_values("Flagged", ascending=False)
    )
    sec["Predicted Rate"] = sec["Flagged"] / sec["Total"]

    # ── Top 15 bar chart ──────────────────────────────────────────────────
    top15 = sec.head(15)
    fig_sec = px.bar(
        top15, x="Flagged", y="SECURITYID",
        orientation="h",
        color="AvgRisk",
        color_continuous_scale=[[0, "#FECACA"], [1, RED]],
        labels={"SECURITYID": "ISIN", "Flagged": "Flagged Trades", "AvgRisk": "Avg Risk Score"},
        title="Most Flagged Securities",
        hover_data={"Total": True, "Predicted Rate": ":.1%", "Actual": True},
    )
    fig_sec.update_layout(
        height=380, margin=dict(t=50, b=20),
        coloraxis_colorbar=dict(title="Risk Score", tickformat=".0%"),
        plot_bgcolor="white", paper_bgcolor="white",
        yaxis=dict(autorange="reversed"),
    )
    fig_sec.update_xaxes(showgrid=True, gridcolor="#F3F4F6")
    fig_sec.update_yaxes(showgrid=False)
    st.plotly_chart(fig_sec, use_container_width=True)

    # ── Heatmap: ISIN × Month ─────────────────────────────────────────────
    st.markdown("#### Risk Score by ISIN and Month")
    top_isins = sec.head(20)["SECURITYID"].tolist()
    heat_data = (
        fdf[fdf["SECURITYID"].isin(top_isins)]
        .groupby(["SECURITYID", "Month"])["suspension_probability"]
        .mean()
        .reset_index()
    )
    if not heat_data.empty:
        heat_pivot = heat_data.pivot(index="SECURITYID", columns="Month", values="suspension_probability")
        fig_heat = px.imshow(
            heat_pivot,
            color_continuous_scale=[[0, "#F0FFF4"], [0.5, AMBER], [1, RED]],
            zmin=0, zmax=1,
            labels={"color": "Avg Risk Score"},
            aspect="auto",
        )
        fig_heat.update_layout(
            height=420, margin=dict(t=20, b=20),
            xaxis_title=None, yaxis_title=None,
            coloraxis_colorbar=dict(tickformat=".0%"),
        )
        fig_heat.update_traces(hovertemplate="ISIN: %{y}<br>Month: %{x}<br>Risk: %{z:.1%}<extra></extra>")
        st.plotly_chart(fig_heat, use_container_width=True)

    # ── Scatter: Liquidity vs Risk ─────────────────────────────────────────
    if "liquidité_volume_5j" in fdf.columns:
        st.markdown("#### Liquidity vs Risk Score")
        scatter_data = (
            fdf.groupby("SECURITYID")
            .agg(
                avg_liquidity =("liquidité_volume_5j", "mean"),
                avg_risk      =("suspension_probability", "mean"),
                flagged       =("suspension_predicted", "sum"),
            )
            .reset_index()
        )
        fig_sc = px.scatter(
            scatter_data, x="avg_liquidity", y="avg_risk",
            size="flagged", size_max=30,
            color="avg_risk",
            color_continuous_scale=[[0, GREEN], [0.5, AMBER], [1, RED]],
            hover_name="SECURITYID",
            labels={
                "avg_liquidity": "Avg 5-Day Liquidity (MAD)",
                "avg_risk":      "Avg Risk Score",
                "flagged":       "Flagged Trades",
            },
            title="Liquidity vs Risk Score per Security",
        )
        fig_sc.add_hline(y=0.5, line_dash="dash", line_color=RED,
                         annotation_text="High-risk threshold")
        fig_sc.update_layout(
            height=360, margin=dict(t=50, b=20),
            coloraxis_showscale=False,
            plot_bgcolor="white", paper_bgcolor="white",
        )
        fig_sc.update_xaxes(showgrid=True, gridcolor="#F3F4F6")
        fig_sc.update_yaxes(showgrid=True, gridcolor="#F3F4F6", tickformat=".0%")
        st.plotly_chart(fig_sc, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — Prediction Drill-through
# ══════════════════════════════════════════════════════════════════════════════

elif page == "Prediction Drill-through":
    st.title("Prediction Drill-through")
    st.caption("Row-level investigation of flagged transactions")

    # ── Filters ───────────────────────────────────────────────────────────
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        risk_filter = st.selectbox(
            "Risk level", ["HIGH RISK only", "All trades"]
        )
    with col_f2:
        participants = ["All"] + sorted(fdf["TRADERBPID"].dropna().unique().tolist())
        sel_part = st.selectbox("Participant", participants)
    with col_f3:
        securities = ["All"] + sorted(fdf["SECURITYID"].dropna().unique().tolist())
        sel_sec = st.selectbox("Security (ISIN)", securities)

    drill = fdf.copy()
    if risk_filter == "HIGH RISK only":
        drill = drill[drill["suspension_predicted"] == 1]
    if sel_part != "All":
        drill = drill[drill["TRADERBPID"] == sel_part]
    if sel_sec != "All":
        drill = drill[drill["SECURITYID"] == sel_sec]

    if drill.empty:
        st.info("No transactions match the current filters.")
        st.stop()

    # ── KPI cards ─────────────────────────────────────────────────────────
    total_drill    = len(drill)
    avg_prob       = drill["suspension_probability"].mean()
    confirmed      = int(drill["is_suspension"].sum())
    confirmed_pct  = confirmed / drill["is_suspension"].notna().sum() if drill["is_suspension"].notna().sum() else 0
    cutoff_col     = "cutoff_dépassé" if "cutoff_dépassé" in drill.columns else None
    cutoff_breach  = int(drill[cutoff_col].sum()) if cutoff_col else "—"

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        kpi_card("Flagged Transactions", f"{total_drill:,}")
    with c2:
        kpi_card("Avg Risk Score", pct(avg_prob),
                 colour=RED if avg_prob > 0.5 else AMBER)
    with c3:
        kpi_card("Confirmed Suspensions", pct(confirmed_pct),
                 delta=f"{confirmed} confirmed",
                 colour=RED if confirmed_pct > 0.3 else AMBER)
    with c4:
        kpi_card("After-Cutoff Submissions",
                 str(cutoff_breach) if cutoff_breach != "—" else "—",
                 colour=AMBER)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Gauge ─────────────────────────────────────────────────────────────
    col_g, col_dist = st.columns([1, 2])

    with col_g:
        fig_gauge = go.Figure(go.Indicator(
            mode  = "gauge+number",
            value = round(avg_prob * 100, 1),
            title = {"text": "Avg Suspension Probability", "font": {"size": 14}},
            number= {"suffix": "%", "font": {"size": 28}},
            gauge = {
                "axis":  {"range": [0, 100], "tickformat": ".0f"},
                "bar":   {"color": RED if avg_prob > 0.5 else AMBER},
                "steps": [
                    {"range": [0,  25], "color": "#D1FAE5"},
                    {"range": [25, 50], "color": "#FEF9C3"},
                    {"range": [50, 75], "color": "#FEE2E2"},
                    {"range": [75,100], "color": "#FECACA"},
                ],
                "threshold": {
                    "line": {"color": RED, "width": 3},
                    "thickness": 0.8,
                    "value": 50,
                },
            },
        ))
        fig_gauge.update_layout(height=240, margin=dict(t=30, b=10, l=20, r=20))
        st.plotly_chart(fig_gauge, use_container_width=True)

    with col_dist:
        fig_dist = px.histogram(
            drill, x="suspension_probability",
            nbins=20,
            color_discrete_sequence=[BLUE],
            labels={"suspension_probability": "Risk Score", "count": "Trades"},
            title="Risk Score Distribution",
        )
        fig_dist.add_vline(x=0.5, line_dash="dash", line_color=RED,
                           annotation_text="Threshold")
        fig_dist.update_layout(
            height=240, margin=dict(t=50, b=20),
            plot_bgcolor="white", paper_bgcolor="white",
            bargap=0.05,
        )
        fig_dist.update_xaxes(showgrid=False, tickformat=".0%")
        fig_dist.update_yaxes(showgrid=True, gridcolor="#F3F4F6")
        st.plotly_chart(fig_dist, use_container_width=True)

    st.markdown("---")

    # ── Feature signal bars (if features available) ───────────────────────
    feature_cols = {
        "buyer_historical_suspens":   "Buyer Hist. Risk",
        "vendeur_historique_suspens": "Seller Hist. Risk",
        "ratio_instruction_vs_market":"Instruction/Market Ratio",
        "RSI_5":                      "RSI 5",
        "MACD_diff":                  "MACD Diff",
        "taux_changement_prix":       "Price Change Rate",
        "taux_changement_volume":     "Volume Change Rate",
    }
    present_feats = {k: v for k, v in feature_cols.items() if k in drill.columns}

    if present_feats:
        st.markdown("#### Average Feature Signals — Flagged Trades vs All Trades")
        feat_flagged = drill[present_feats.keys()].mean()
        feat_all     = fdf[present_feats.keys()].mean()
        feat_df = pd.DataFrame({
            "Feature":     [present_feats[k] for k in present_feats],
            "Flagged Avg": feat_flagged.values,
            "Overall Avg": feat_all.values,
        })
        fig_feat = go.Figure()
        fig_feat.add_bar(name="Flagged",  x=feat_df["Feature"], y=feat_df["Flagged Avg"], marker_color=RED)
        fig_feat.add_bar(name="Overall",  x=feat_df["Feature"], y=feat_df["Overall Avg"], marker_color=BLUE)
        fig_feat.update_layout(
            barmode="group",
            height=280, margin=dict(t=20, b=20),
            legend=dict(orientation="h", y=1.05),
            plot_bgcolor="white", paper_bgcolor="white",
            xaxis_title=None,
        )
        fig_feat.update_xaxes(showgrid=False)
        fig_feat.update_yaxes(showgrid=True, gridcolor="#F3F4F6")
        st.plotly_chart(fig_feat, use_container_width=True)

    # ── Detail table ──────────────────────────────────────────────────────
    st.markdown("#### Transaction Detail")

    base_cols = ["Date", "TRADEREFERENCE", "TRADERBPID", "CTRTRADERBPID",
                 "SECURITYID", "TRADESTATUS", "suspension_probability",
                 "suspension_predicted", "risk_label", "is_suspension"]
    extra_cols = ["SETTLEMENTAMOUNT", "TRADEPRICE", "TRADEQUANTITY",
                  "SETTLEMENTTYPE", cutoff_col]
    show_cols = base_cols + [c for c in extra_cols if c and c in drill.columns]
    show_cols = [c for c in show_cols if c in drill.columns]

    display_drill = drill[show_cols].copy()
    display_drill = display_drill.sort_values("suspension_probability", ascending=False)

    # Rename for readability
    rename_map = {
        "TRADEREFERENCE":    "Trade Ref",
        "TRADERBPID":        "Trader",
        "CTRTRADERBPID":     "Counterparty",
        "SECURITYID":        "ISIN",
        "TRADESTATUS":       "Status",
        "suspension_probability": "Risk Score",
        "suspension_predicted":   "Predicted",
        "risk_label":        "Label",
        "is_suspension":     "Suspended?",
        "SETTLEMENTAMOUNT":  "Amount (MAD)",
        "TRADEPRICE":        "Price",
        "TRADEQUANTITY":     "Quantity",
        "SETTLEMENTTYPE":    "Sett. Type",
        "cutoff_dépassé":    "After Cutoff",
    }
    display_drill = display_drill.rename(columns={k: v for k, v in rename_map.items() if k in display_drill.columns})

    st.dataframe(
        display_drill,
        use_container_width=True,
        hide_index=True,
        height=400,
        column_config={
            "Risk Score":   st.column_config.ProgressColumn(
                "Risk Score", min_value=0, max_value=1, format="%.2f"
            ),
            "Amount (MAD)": st.column_config.NumberColumn(format="%.0f"),
            "Price":        st.column_config.NumberColumn(format="%.2f"),
        },
    )

    # Download
    csv = display_drill.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download filtered results as CSV",
        data=csv,
        file_name=f"flagged_trades_{start_date}_{end_date}.csv",
        mime="text/csv",
    )
