# Power BI Dashboard — Build Guide
**Project:** Maroclear Transaction Suspension Detection  
**Pages:** 4  
**Data sources:** BigQuery (`maroclear-dwh.suspension_detection`)

---

## Prerequisites

1. **Power BI Desktop** — latest version (August 2024+)
2. **Google BigQuery connector** — built-in, no additional install needed
3. **Google account** with `roles/bigquery.dataViewer` on `maroclear-dwh`
4. ETL pipeline has run at least once so tables have data

---

## Step 1 — Connect to BigQuery

1. Open Power BI Desktop → **Get Data** → **Google BigQuery**
2. Sign in with your Google account
3. Navigate to `maroclear-dwh` → `suspension_detection`
4. Do **not** load tables directly from the navigator. Close the navigator.
5. Open **Transform Data** → **Advanced Editor** for each query below.

**Load these five queries from `powerbi/queries.pq`:**

| Query name | Source object | Used on |
|---|---|---|
| `DailyKPIs` | `agg_daily_suspension_rate` | Page 1 |
| `ParticipantSummary` | `v_participant_risk_summary` | Page 2 |
| `SecurityHeatmap` | `v_security_suspension_heatmap` | Page 3 |
| `HighRiskTrades` | `v_high_risk_trades` | Page 4 |
| `DimDate` | `dim_date` | All pages (slicer) |

---

## Step 2 — Build the Data Model

In **Model view**, create these relationships (all one-to-many, single direction):

```
DimDate[Date]  ──►  DailyKPIs[Date]
DimDate[Date]  ──►  ParticipantSummary[Date]
DimDate[Date]  ──►  SecurityHeatmap[Date]
DimDate[Date]  ──►  HighRiskTrades[trade_date]
```

Then:
- Right-click `DimDate` → **Mark as Date Table** → select the `Date` column
- Set `DimDate[Date]` as the **Date Table** column

---

## Step 3 — Add DAX Measures

Open `powerbi/measures.dax`. For each measure block:

1. Select the target table in the Fields pane
2. Click **New Measure**
3. Paste the DAX expression
4. Assign a **Display Folder** matching the comment headers (e.g. `KPI`, `Participant`, `Security`, `Drill-through`)

---

## Step 4 — Theme and Colours

Before building pages, apply consistent colours. In **View** → **Themes** → **Customize current theme**:

| Element | Hex |
|---|---|
| Background | `#F5F7FA` |
| Page canvas | `#FFFFFF` |
| HIGH RISK accent | `#D64045` (red) |
| LOW RISK accent | `#2ECC71` (green) |
| Neutral / info | `#2C7BE5` (blue) |
| Text primary | `#1A1A2E` |
| Text secondary | `#6B7280` |
| Card background | `#FFFFFF` |
| Card border | `#E5E7EB` |

---

## Page 1 — Daily Risk Overview

**Purpose:** Morning briefing for the operations manager. One glance shows yesterday's risk exposure.

### Canvas layout

```
┌──────────────────────────────────────────────────────────────────────┐
│  HEADER: "Daily Risk Overview"          Last refreshed: [timestamp]  │
├──────────────┬──────────────┬──────────────┬──────────────────────── │
│  KPI Card 1  │  KPI Card 2  │  KPI Card 3  │  KPI Card 4             │
│ Total Trades │ Flagged      │ Predicted    │ High Risk               │
│              │ Trades       │ Susp. Rate   │ Participants            │
├──────────────┴──────────────┴──────────────┴────────────────────────┤
│                                                                       │
│   LINE CHART: Predicted Suspension Rate over time (last 30 days)     │
│   Secondary line: Actual Suspension Rate                             │
│                                                                       │
├──────────────────────────────────┬──────────────────────────────────┤
│  BAR CHART: Top 10 Participants  │  DONUT: Trade outcome breakdown   │
│  by Flagged Trades               │  Confirmed / Settled OK / Pending │
└──────────────────────────────────┴──────────────────────────────────┘
                         DATE RANGE SLICER (top right)
```

### Visual specifications

**KPI Card 1 — Total Trades**
- Visual: Card
- Value: `[Total Trades]`
- Format: whole number with comma separator
- Label: "Total Trades"

**KPI Card 2 — Flagged Trades**
- Visual: Card
- Value: `[Flagged Trades]`
- Conditional formatting: red if > 0, green if 0
- Label: "Flagged HIGH RISK"

**KPI Card 3 — Predicted Suspension Rate**
- Visual: Card (New Card visual)
- Value: `[Predicted Suspension Rate]`
- Format: percentage, 1 decimal
- Callout value: `[Predicted Rate vs Prior Period]` (shows delta with arrow)
- Label: "Predicted Suspension Rate"

**KPI Card 4 — High Risk Participants**
- Visual: Card
- Value: `[High Risk Participants]`
- Label: "High Risk Participants"

**Line Chart — Rate Over Time**
- Visual: Line chart
- X-axis: `DimDate[Date]`
- Y-axis line 1: `[Predicted Suspension Rate]` — blue, solid
- Y-axis line 2: `[Actual Suspension Rate]` — red, dashed
- Legend: enabled
- Data labels: off
- Tooltip: Date, Predicted Rate, Actual Rate, Total Trades
- Title: "Suspension Rate Trend"

**Bar Chart — Top 10 Participants**
- Visual: Clustered bar chart
- Y-axis: `ParticipantSummary[Participant]`
- X-axis: `[Participant Flagged Trades]`
- Visual filter: `[Top 10 Participants Flag] = 1`
- Bars: red (`#D64045`)
- Data labels: on (show value)
- Sort: descending by Flagged Trades
- Title: "Top 10 High-Risk Participants"

**Donut — Outcome Breakdown**
- Visual: Donut chart
- Legend: `HighRiskTrades[Outcome]`
- Values: `[Flagged Transaction Count]`
- Slice colours: Confirmed Suspension = red, Settled OK = green, Pending = grey
- Title: "Flagged Trade Outcomes"

**Date Slicer**
- Visual: Slicer (Date range style)
- Field: `DimDate[Date]`
- Default: Last 30 days (set using relative date filter)
- Position: top right, above KPI cards

---

## Page 2 — Participant Monitoring

**Purpose:** Rank participants by suspension risk. Drill into a single participant's trend over time.

### Canvas layout

```
┌──────────────────────────────────────────────────────────────────────┐
│  HEADER: "Participant Monitoring"                   [Date Slicer]    │
├──────────────────────────────────┬───────────────────────────────────┤
│                                  │  KPI Cards (for selected          │
│  LEAGUE TABLE                    │  participant from slicer):        │
│  Participant | Trades | Flagged  │  Flagged | Pred. Rate | Act. Rate │
│  | Act. Susp | Avg Risk | Rank  │                                    │
│                                  │  LINE CHART: Predicted Rate       │
│  (scrollable, sorted by Rank)    │  trend for selected participant   │
│                                  │  (last 90 days)                   │
├──────────────────────────────────┴───────────────────────────────────┤
│                                                                       │
│  BAR CHART: Buyer vs Seller historical suspension rate               │
│  Grouped by participant (Top 15 only)                                │
└──────────────────────────────────────────────────────────────────────┘
```

### Visual specifications

**League Table**
- Visual: Table
- Columns:
  - `ParticipantSummary[Participant]`
  - `[Participant Total Trades]`
  - `[Participant Flagged Trades]`
  - `[Participant Predicted Rate]` — format as %, conditional colour red>5% / amber 1-5% / green <1%
  - `[Participant Actual Rate]` — format as %
  - `[Participant Risk Rank]` — sort ascending
- Sort default: `[Participant Risk Rank]` ascending
- Enable drill-through to Page 4 on participant row click

**Participant Slicer**
- Visual: Slicer (dropdown)
- Field: `ParticipantSummary[Participant]`
- Label: "Select Participant"
- Single-select mode

**KPI Cards (context-sensitive to slicer)**
- Flagged Trades: `[Participant Flagged Trades]`
- Predicted Rate: `[Participant Predicted Rate]`
- Actual Rate: `[Participant Actual Rate]`
- Avg Risk Score: `[Participant Avg Risk Score]` — formatted as %

**Line Chart — Participant Trend**
- Visual: Line chart
- X-axis: `DimDate[Date]`
- Y-axis: `[Participant Predicted Rate]`
- Filter: scoped to the selected participant via the slicer
- Title: `"Suspension Rate Trend"` (dynamic if desired via `[Drill-through Title]`)
- Tooltip: Date, Flagged Trades, Total Trades, Predicted Rate

**Grouped Bar — Buyer vs Seller Risk**
- Visual: Clustered bar chart
- Y-axis: Participant (top 15 by flagged trades)
- X-axis group 1: Average `buyer_historical_suspens` from `HighRiskTrades`
- X-axis group 2: Average `vendeur_historique_suspens` from `HighRiskTrades`
- Colours: Buyer = blue, Seller = orange
- Title: "5-Day Historical Suspension Rate — Buyer vs Seller"

---

## Page 3 — Security Heatmap

**Purpose:** Identify which securities are systemically prone to suspension.

### Canvas layout

```
┌──────────────────────────────────────────────────────────────────────┐
│  HEADER: "Security Risk Heatmap"                    [Date Slicer]   │
├───────────────────────────────────────┬──────────────────────────────┤
│                                       │  KPI Cards:                  │
│  MATRIX HEATMAP                       │  ISINs Flagged | Avg Score   │
│  Rows: ISIN                           │                              │
│  Columns: Month (from DimDate)        │  INSTRUMENT TYPE SLICER      │
│  Values: Avg Risk Score               │  (Bonds / Equities / etc.)   │
│  (colour scale: white→red)            │                              │
│                                       │  MARKET SEGMENT SLICER       │
├───────────────────────────────────────┤                              │
│  BAR CHART:                           │                              │
│  Top 15 ISINs by Flagged Trades       │                              │
│  (colour = Avg Risk Score gradient)   │                              │
├───────────────────────────────────────┴──────────────────────────────┤
│  SCATTER: Avg Liquidity 5D (X) vs Avg Risk Score (Y)                 │
│  Bubble size = Flagged Trades  |  Colour = Market Segment            │
└──────────────────────────────────────────────────────────────────────┘
```

### Visual specifications

**Matrix Heatmap**
- Visual: Matrix
- Rows: `SecurityHeatmap[ISIN]`
- Columns: `DimDate[Month]` (or `DimDate[Year]` + `DimDate[Month]` as hierarchy)
- Values: `[Security Avg Risk Score]`
- Cell conditional formatting: gradient from white (0.0) to `#D64045` (1.0)
- Row subtotals: off
- Column subtotals: off
- Sort rows by: `[Security Flagged Trades]` descending
- Title: "Risk Score by ISIN and Month"

**Bar Chart — Top 15 ISINs**
- Visual: Clustered bar chart
- Y-axis: `SecurityHeatmap[ISIN]`
- X-axis: `[Security Flagged Trades]`
- Visual filter: top 15 by Flagged Trades
- Conditional bar colour: gradient on `[Security Avg Risk Score]`
- Title: "Most Flagged Securities"
- Sort: descending by Flagged Trades

**Scatter — Liquidity vs Risk**
- Visual: Scatter chart
- X-axis: `[Security Avg Liquidity]`
- Y-axis: `[Security Avg Risk Score]`
- Size: `[Security Flagged Trades]`
- Legend (colour): `SecurityHeatmap[Market Segment]`
- Tooltip: ISIN, Instrument Type, Market Segment, Flagged Trades, Avg Risk Score, Avg RSI
- Title: "Liquidity vs Risk Score per Security"
- Annotation: Add a constant line at Y = 0.5 to mark the high-risk threshold

**Slicers**
- Instrument Type: `SecurityHeatmap[Instrument Type]` — dropdown, multi-select
- Market Segment: `SecurityHeatmap[Market Segment]` — dropdown, multi-select

---

## Page 4 — Prediction Drill-through

**Purpose:** Row-level investigation of flagged transactions. Reachable by right-clicking any participant or ISIN on pages 2 and 3.

### Canvas layout

```
┌──────────────────────────────────────────────────────────────────────┐
│  ← BACK BUTTON                                                       │
│  DYNAMIC TITLE: "High-Risk Trades — [Participant / ISIN]"            │
├──────────────┬──────────────┬──────────────┬─────────────────────────┤
│  KPI Card    │  KPI Card    │  KPI Card    │  KPI Card               │
│  Count of    │  Avg Risk    │  Confirmed   │  Cutoff                 │
│  Flagged     │  Score       │  Suspension% │  Breaches               │
├──────────────┴──────────────┴──────────────┴─────────────────────────┤
│  GAUGE: Average Suspension Probability (0 to 1, threshold line 0.5)  │
├──────────────────────────────────────────────────────────────────────┤
│  DETAIL TABLE (scrollable):                                          │
│  Date | Trade Ref | Trader | Counterparty | ISIN | Amount | Price   │
│  | Status | Risk Score | Outcome | Cutoff | Buyer Hist. | Sell Hist.│
└──────────────────────────────────────────────────────────────────────┘
```

### Visual specifications

**Back Button**
- Visual: Button → Back
- Position: top left
- Style: minimal, small

**Dynamic Title**
- Visual: Card
- Value: `[Drill-through Title]`
- Font: large, bold, no border

**KPI Card — Flagged Count**
- Value: `[Flagged Transaction Count]`
- Label: "Flagged Transactions"

**KPI Card — Avg Risk Score**
- Value: `[Avg Suspension Probability]`
- Format: percentage 1 decimal
- Label: "Avg Suspension Probability"
- Conditional colour: red if > 0.5

**KPI Card — Confirmed Suspension %**
- Value: `[Confirmed Suspension %]`
- Format: percentage 1 decimal
- Label: "Confirmed Suspensions"

**KPI Card — Cutoff Breaches**
- Value: `[Cutoff Breaches]`
- Label: "After-Cutoff Submissions"

**Gauge — Risk Score**
- Visual: Gauge
- Value: `[Avg Suspension Probability]`
- Min: 0, Max: 1
- Target: 0.5 (suspension threshold)
- Fill colour: gradient red above 0.5, green below
- Title: "Average Risk Score"

**Detail Table**
- Visual: Table (enable word wrap off for clean rows)
- Columns in order:

| Column | Source | Format |
|---|---|---|
| Date | `HighRiskTrades[trade_date]` | DD/MM/YYYY |
| Trade Reference | `HighRiskTrades[trade_reference]` | text |
| Trader | `HighRiskTrades[trader_id]` | text |
| Counterparty | `HighRiskTrades[counterparty_id]` | text |
| ISIN | `HighRiskTrades[isin]` | text |
| Amount (MAD) | `HighRiskTrades[settlement_amount]` | #,##0 |
| Price | `HighRiskTrades[trade_price]` | #,##0.00 |
| Trade Status | `HighRiskTrades[trade_status]` | text |
| Risk Score | `HighRiskTrades[suspension_probability]` | 0.00% — conditional red >0.5 |
| Outcome | `HighRiskTrades[Outcome]` | text — conditional colour |
| After Cutoff | `HighRiskTrades[cutoff_depasse]` | "Yes" / "No" via format |
| Buyer Hist. Risk | `HighRiskTrades[buyer_historical_suspens]` | 0.0% |
| Seller Hist. Risk | `HighRiskTrades[vendeur_historique_suspens]` | 0.0% |

- Sort default: `suspension_probability` descending
- Row conditional formatting on `Outcome`: Confirmed Suspension = red background, Settled OK = green

**Drill-through setup:**
1. In the Drill-through well on Page 4, add `HighRiskTrades[trader_id]`
2. Also add `HighRiskTrades[isin]`
3. This makes any participant name or ISIN on pages 2 and 3 right-clickable with "Drill through → Prediction Drill-through"

---

## Step 5 — Cross-page Interactions

| Source visual | Target | Interaction |
|---|---|---|
| Date slicer (all pages) | All visuals | Filter |
| League table row (Page 2) | Page 2 participant line chart | Highlight |
| League table row (Page 2) | Page 4 drill-through | Drill-through (right-click) |
| Bar chart ISIN (Page 3) | Page 4 drill-through | Drill-through (right-click) |
| Scatter bubble (Page 3) | Bar chart (Page 3) | Cross-highlight |

To configure: **Format** → **Edit interactions** — set each source/target pair above.

---

## Step 6 — Scheduled Refresh

1. Publish the `.pbix` to **Power BI Service**
2. In the dataset settings → **Gateway connections** → connect to your BigQuery service account
3. **Scheduled refresh** → set to run at 07:00 daily (after `sp_daily_load()` completes)
4. Enable **refresh failure notifications** to the ops team email

---

## File Summary

```
powerbi/
├── queries.pq          ← Power Query M for all 5 data sources
├── measures.dax        ← All DAX measures, organised by page/table
└── DASHBOARD_BUILD.md  ← This file
```
