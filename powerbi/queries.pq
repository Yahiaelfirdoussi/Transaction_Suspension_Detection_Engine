// ============================================================================
// Power Query M — BigQuery Data Sources
// Maroclear Transaction Suspension Detection Dashboard
//
// How to use:
//   Power BI Desktop → Transform Data → Advanced Editor
//   Paste each query into a new blank query and rename it as shown.
//
// Prerequisites:
//   • Google BigQuery connector enabled in Power BI Desktop
//   • Sign in with a Google account that has BigQuery read access
//   • Replace "maroclear-dwh" with your actual GCP project ID if different
// ============================================================================


// ── Query: DailyKPIs ─────────────────────────────────────────────────────────
// Source : agg_daily_suspension_rate
// Used by: Page 1 — Daily Risk Overview (KPI cards, trend line)

let
    Source = GoogleBigQuery.Database("maroclear-dwh"),
    Dataset = Source{[Name="suspension_detection"]}[Data],
    Table = Dataset{[Name="agg_daily_suspension_rate"]}[Data],

    // Cast and rename for friendlier field names in Power BI
    Typed = Table.TransformColumnTypes(Table, {
        {"agg_date",                   type date},
        {"total_trades",               Int64.Type},
        {"flagged_trades",             Int64.Type},
        {"predicted_suspension_rate",  type number},
        {"actual_suspensions",         Int64.Type},
        {"actual_suspension_rate",     type number},
        {"high_risk_participants",     Int64.Type},
        {"avg_suspension_probability", type number},
        {"_refreshed_at",              type datetimezone}
    }),

    Renamed = Table.RenameColumns(Typed, {
        {"agg_date",                   "Date"},
        {"total_trades",               "Total Trades"},
        {"flagged_trades",             "Flagged Trades"},
        {"predicted_suspension_rate",  "Predicted Rate"},
        {"actual_suspensions",         "Actual Suspensions"},
        {"actual_suspension_rate",     "Actual Rate"},
        {"high_risk_participants",     "High Risk Participants"},
        {"avg_suspension_probability", "Avg Risk Score"},
        {"_refreshed_at",              "Refreshed At"}
    })
in
    Renamed


// ── Query: HighRiskTrades ─────────────────────────────────────────────────────
// Source : v_high_risk_trades (view — last 30 days, suspension_predicted = 1)
// Used by: Page 4 — Prediction Drill-through

let
    Source = GoogleBigQuery.Database("maroclear-dwh"),
    Dataset = Source{[Name="suspension_detection"]}[Data],
    View = Dataset{[Name="v_high_risk_trades"]}[Data],

    Typed = Table.TransformColumnTypes(View, {
        {"trade_date",                   type date},
        {"trade_reference",              type text},
        {"trader_id",                    type text},
        {"counterparty_id",              type text},
        {"isin",                         type text},
        {"instrument_type",              type text},
        {"market_segment",               type text},
        {"settlement_type",              type text},
        {"trade_status",                 type text},
        {"settlement_amount",            type number},
        {"trade_quantity",               Int64.Type},
        {"trade_price",                  type number},
        {"market_volume",                type number},
        {"is_suspension",                Int64.Type},
        {"suspension_probability",       type number},
        {"suspension_predicted",         Int64.Type},
        {"risk_label",                   type text},
        {"cutoff_depasse",               Int64.Type},
        {"buyer_historical_suspens",     type number},
        {"vendeur_historique_suspens",   type number},
        {"ratio_instruction_vs_market",  type number},
        {"model_version",                type text},
        {"predicted_at",                 type datetimezone}
    }),

    // Add a readable suspension outcome column
    WithOutcome = Table.AddColumn(Typed, "Outcome", each
        if [is_suspension] = 1 then "Confirmed Suspension"
        else if [is_suspension] = 0 then "Settled OK"
        else "Pending",
        type text
    )
in
    WithOutcome


// ── Query: ParticipantSummary ─────────────────────────────────────────────────
// Source : v_participant_risk_summary
// Used by: Page 2 — Participant Monitoring (league table, trend lines)

let
    Source = GoogleBigQuery.Database("maroclear-dwh"),
    Dataset = Source{[Name="suspension_detection"]}[Data],
    View = Dataset{[Name="v_participant_risk_summary"]}[Data],

    Typed = Table.TransformColumnTypes(View, {
        {"participant_id",           type text},
        {"trade_date",               type date},
        {"total_trades",             Int64.Type},
        {"flagged_trades",           Int64.Type},
        {"actual_suspensions",       Int64.Type},
        {"avg_risk_score",           type number},
        {"actual_suspension_rate",   type number}
    }),

    Renamed = Table.RenameColumns(Typed, {
        {"participant_id",         "Participant"},
        {"trade_date",             "Date"},
        {"total_trades",           "Total Trades"},
        {"flagged_trades",         "Flagged Trades"},
        {"actual_suspensions",     "Actual Suspensions"},
        {"avg_risk_score",         "Avg Risk Score"},
        {"actual_suspension_rate", "Suspension Rate"}
    })
in
    Renamed


// ── Query: SecurityHeatmap ────────────────────────────────────────────────────
// Source : v_security_suspension_heatmap
// Used by: Page 3 — Security Heatmap

let
    Source = GoogleBigQuery.Database("maroclear-dwh"),
    Dataset = Source{[Name="suspension_detection"]}[Data],
    View = Dataset{[Name="v_security_suspension_heatmap"]}[Data],

    Typed = Table.TransformColumnTypes(View, {
        {"isin",              type text},
        {"instrument_type",   type text},
        {"market_segment",    type text},
        {"trade_date",        type date},
        {"total_trades",      Int64.Type},
        {"flagged_trades",    Int64.Type},
        {"actual_suspensions",Int64.Type},
        {"avg_risk_score",    type number},
        {"avg_rsi_5",         type number},
        {"avg_liquidity_5d",  type number}
    }),

    Renamed = Table.RenameColumns(Typed, {
        {"isin",               "ISIN"},
        {"instrument_type",    "Instrument Type"},
        {"market_segment",     "Market Segment"},
        {"trade_date",         "Date"},
        {"total_trades",       "Total Trades"},
        {"flagged_trades",     "Flagged Trades"},
        {"actual_suspensions", "Actual Suspensions"},
        {"avg_risk_score",     "Avg Risk Score"},
        {"avg_rsi_5",          "Avg RSI 5"},
        {"avg_liquidity_5d",   "Avg Liquidity 5D"}
    })
in
    Renamed


// ── Query: DimDate ────────────────────────────────────────────────────────────
// Source : dim_date
// Used by: Shared date slicer across all pages (mark as Date Table in model)

let
    Source = GoogleBigQuery.Database("maroclear-dwh"),
    Dataset = Source{[Name="suspension_detection"]}[Data],
    Table = Dataset{[Name="dim_date"]}[Data],

    // Only load trading days to keep the slicer clean
    TradingDays = Table.SelectRows(Table, each [is_trading_day] = true),

    Typed = Table.TransformColumnTypes(TradingDays, {
        {"date_key",     Int64.Type},
        {"full_date",    type date},
        {"day_of_week",  Int64.Type},
        {"day_name",     type text},
        {"week_of_year", Int64.Type},
        {"month",        Int64.Type},
        {"month_name",   type text},
        {"quarter",      Int64.Type},
        {"year",         Int64.Type}
    }),

    Renamed = Table.RenameColumns(Typed, {
        {"full_date",    "Date"},
        {"day_of_week",  "Day of Week"},
        {"day_name",     "Day Name"},
        {"week_of_year", "Week"},
        {"month",        "Month Number"},
        {"month_name",   "Month"},
        {"quarter",      "Quarter"},
        {"year",         "Year"}
    })
in
    Renamed
