# Data Warehouse Design — Transaction Suspension Detection

**Project:** Maroclear SA — End-of-Studies Internship  
**Author:** Yahya Elfirdoussi  
**File:** `sql/schema.sql`  
**Target platform:** Google BigQuery

---

## Overview

The data warehouse stores every trade processed by the ML pipeline — raw transaction fields, engineered features, and the XGBoost model's suspension prediction — in a single, queryable fact table. Pre-aggregated KPIs feed the Power BI dashboard without hitting the full fact table on every refresh.

The schema follows a **star schema** pattern: one central fact table surrounded by three dimension tables and one aggregate table.

---

## Schema Diagram

```
                    ┌─────────────┐
                    │  dim_date   │
                    │ (date_key)  │
                    └──────┬──────┘
                           │
┌──────────────────┐        │        ┌────────────────────┐
│ dim_participant  │        │        │   dim_participant   │
│  (trader_key)   ├────────┼────────┤ (counterparty_key)  │
└──────────────────┘        │        └────────────────────┘
                            │
                   ┌────────┴────────┐
                   │ fact_transactions│  ──── agg_daily_suspension_rate
                   │  PARTITION BY   │
                   │   trade_date    │
                   │  CLUSTER BY     │
                   │ security_key,   │
                   │  trader_key     │
                   └────────┬────────┘
                            │
                   ┌────────┴────────┐
                   │  dim_security   │
                   │ (security_key)  │
                   └─────────────────┘
```

---

## Tables

### Staging Tables

Two staging tables act as the landing zone for daily CSV loads before the ETL procedures run.

#### `stg_transactions`
Mirrors the raw transaction CSV produced by `preprocessing.py`. Every column from the source file is present here, including all 14 engineered features computed by `feature_engineering.py`.

| Column group | Columns |
|---|---|
| Identifiers | `TRADEREFERENCE`, `TRADERBPID`, `CTRTRADERBPID`, `SECURITYID` |
| Trade economics | `SETTLEMENTAMOUNT`, `TRADEPRICE`, `TRADEQUANTITY`, `volume_globale` |
| Settlement metadata | `TRADESTATUS`, `SETTLEMENTTYPE`, `SETTLEMENTCYCLE`, `SETTLEMENTDATE`, `SETTLEDDATE` |
| Engineered features | `cutoff_depasse`, `buyer_historical_suspens`, `vendeur_historique_suspens`, `daily_activity`, `global_exchange_frequency`, `ratio_instruction_vs_market`, `liquidite_volume_5j`, `RSI_5`, `MACD_diff`, `taux_changement_prix`, `taux_changement_volume`, `log_tradeprice`, `ratio_volume_prix`, `day_of_week` |

#### `stg_predictions`
Mirrors the output CSV from `predict.py`. Joined to `stg_transactions` on `TRADEREFERENCE` during the fact table load.

| Column | Description |
|---|---|
| `TRADEREFERENCE` | Join key back to transactions |
| `suspension_probability` | XGBoost P(suspension) ∈ [0, 1] |
| `suspension_predicted` | Binary flag: 1 = HIGH RISK |
| `risk_label` | Human-readable: "HIGH RISK" / "LOW RISK" |
| `model_version` | Tracks which model artefact produced this score |
| `predicted_at` | Timestamp of the scoring run |
| `_source_file` | Source CSV filename for lineage |

---

### Dimension Tables

All dimension tables use a **surrogate key** computed via `FARM_FINGERPRINT(natural_key)`. This is deterministic — the same participant ID always produces the same integer key — so dimensions can be loaded independently of the fact table without a key lookup step.

#### `dim_date`
Pre-populated once for the full trading date range (2020–2030) using `GENERATE_DATE_ARRAY`. The `is_trading_day` flag defaults to all weekdays and should be overridden from the Casablanca Stock Exchange holiday calendar.

| Column | Description |
|---|---|
| `date_key` | `YYYYMMDD` integer surrogate (e.g. `20240115`) |
| `full_date` | `DATE` type |
| `day_of_week` | 0 = Monday … 6 = Sunday |
| `is_trading_day` | `BOOL` — FALSE for weekends and public holidays |
| `week_of_year`, `month`, `quarter`, `year` | Standard calendar decomposition |

#### `dim_participant`
Covers every `TRADERBPID` and `CTRTRADERBPID` seen across all trades. One row per participant — the same table is joined twice to the fact table (once as trader, once as counterparty).

| Column | Description |
|---|---|
| `participant_key` | `FARM_FINGERPRINT(participant_id)` |
| `participant_id` | Natural key: `TRADERBPID` value |
| `first_trade_date` / `last_trade_date` | Automatically maintained by the load procedure |

#### `dim_security`
One row per ISIN. The `sector` and `market_cap_tier` columns are nullable placeholders for future enrichment from an external reference feed (e.g. Bourse de Casablanca data).

| Column | Description |
|---|---|
| `security_key` | `FARM_FINGERPRINT(security_id)` |
| `security_id` | Natural key: `SECURITYID` (ISIN) |
| `instrument_type` | From `INSTRUMENTTYPE` |
| `market_segment` | From `MARKETSEGMENT` |
| `sector` / `market_cap_tier` | NULL until enriched externally |

---

### Fact Table — `fact_transactions`

The core of the schema. One row per trade. Columns are grouped into five logical bands:

| Band | Columns | Immutability |
|---|---|---|
| **Keys** | `transaction_key`, `trade_reference`, `date_key`, `trader_key`, `counterparty_key`, `security_key` | Immutable once inserted |
| **Raw trade fields** | `trade_date`, `trade_time`, `settlement_amount`, `trade_price`, `trade_quantity`, `market_volume`, `trade_status`, etc. | Immutable once inserted |
| **Target label** | `is_suspension` (1 / 0 / NULL) | Updated when OPEN trades settle |
| **Engineered features** | 14 columns from `feature_engineering.py` | Immutable once inserted |
| **ML prediction** | `suspension_probability`, `suspension_predicted`, `risk_label`, `model_version`, `predicted_at` | Updated on each model re-score |

**`is_suspension` derivation:**

```sql
CASE
  WHEN trade_status IN ('FLAR', 'RFSL', 'SFSL', 'PLAR') THEN 1
  WHEN trade_status = 'OK'                               THEN 0
  ELSE NULL  -- OPEN: not yet settled
END
```

**Partitioning and clustering:**

```sql
PARTITION BY trade_date
CLUSTER BY security_key, trader_key
```

Partitioning by `trade_date` means every daily MERGE operation only touches a single partition. Clustering by `security_key, trader_key` optimises the common Power BI query pattern of filtering by ISIN or participant within a date range.

---

### Aggregate Table — `agg_daily_suspension_rate`

Pre-computed daily KPIs. The Power BI daily overview page reads from this table rather than running `COUNT` / `AVG` queries over the full fact table on every dashboard refresh.

| Column | Description |
|---|---|
| `total_trades` | All trades on this date |
| `flagged_trades` | `suspension_predicted = 1` |
| `predicted_suspension_rate` | `flagged / total` |
| `actual_suspensions` | `is_suspension = 1` (lags by settlement cycle) |
| `actual_suspension_rate` | Computed over settled trades only (excludes NULLs) |
| `high_risk_participants` | Distinct `trader_key` values with at least one flag |
| `avg_suspension_probability` | Mean model output across all trades that day |

---

## Incremental Load Logic

### Daily ETL sequence

```
stg_transactions  ─┐
                    ├──► sp_load_dim_participant()
                    ├──► sp_load_dim_security()
stg_predictions   ─┤
                    ├──► sp_load_fact_transactions(run_date)
                    └──► sp_refresh_agg_daily(run_date)
```

The master procedure `sp_daily_load()` runs all four steps in order for `CURRENT_DATE() - 1`. Call it from a BigQuery Scheduled Query or an Airflow DAG task.

---

### `sp_load_dim_participant` — SCD Type 1 MERGE

```sql
MERGE dim_participant AS tgt
USING (combined TRADERBPID + CTRTRADERBPID from staging) AS src
ON tgt.participant_key = src.participant_key

WHEN MATCHED AND date range changed  → UPDATE first/last trade dates
WHEN NOT MATCHED                     → INSERT new participant
```

No history is preserved — old attribute values are overwritten. Sufficient for this use case because participant identity (the ID itself) never changes; only the activity date range expands.

---

### `sp_load_dim_security` — SCD Type 1 MERGE

Same pattern as participants. `COALESCE` ensures that an existing attribute (e.g. `instrument_type`) is not overwritten with NULL if the staging row happens to be missing it.

---

### `sp_load_fact_transactions(run_date)` — Conditional MERGE

This is the most important procedure. The join key is `(trade_reference, trade_date)` — the second column satisfies BigQuery's requirement to include the partition key in a MERGE condition, which limits the operation to a single partition.

```
WHEN MATCHED AND suspension_probability changed  → UPDATE prediction columns only
WHEN NOT MATCHED                                 → INSERT full row
```

**Why raw fields are never overwritten:**  
A trade's economics (price, quantity, amount) are facts of record. Overwriting them on a re-run would corrupt the audit trail. Only the ML prediction columns — which legitimately change when a new model version scores the same trade — are updated.

---

### `sp_refresh_agg_daily(target_date)` — Delete + Insert

```sql
DELETE FROM agg_daily_suspension_rate WHERE agg_date = target_date;
INSERT INTO agg_daily_suspension_rate SELECT ... FROM fact_transactions WHERE trade_date = target_date;
```

This is the safest pattern for aggregate tables in BigQuery. A MERGE would work but requires matching on every column. Delete + insert is simpler, partition-safe, and fully idempotent — running it twice on the same date produces the same result.

---

## Views (Power BI Data Sources)

| View | Feeds | Filter |
|---|---|---|
| `v_high_risk_trades` | Prediction drill-through page | `suspension_predicted = 1`, last 30 days |
| `v_participant_risk_summary` | Participant monitoring page | All dates, grouped by participant + date |
| `v_security_suspension_heatmap` | Security heatmap page | All dates, grouped by ISIN + date |

The views join all dimensions so Power BI only needs a single table-per-page data source. The 30-day filter on `v_high_risk_trades` keeps the query fast; remove it for historical analysis.

---

## Key Design Decisions

**Surrogate keys via `FARM_FINGERPRINT`**  
Using `FARM_FINGERPRINT(natural_key)` produces a deterministic INT64 surrogate. This means the dimension and fact load procedures can be run independently in any order — no sequence generator or lookup step is needed to resolve keys.

**Prediction columns are mutable; raw columns are not**  
The ML model can be retrained and all trades re-scored. The MERGE procedure handles this by updating only the prediction band when it detects a changed `suspension_probability`. The trade itself (price, quantity, counterparty) is a legal record that must not change.

**`is_suspension` is derived in the warehouse, not in Python**  
The target label is computed from `TRADESTATUS` inside the MERGE `SELECT` using a `CASE` expression. This keeps the derivation rule in one place and ensures it is applied consistently regardless of which CSV file is loaded.

**`NULL` for OPEN trades**  
Trades with `TRADESTATUS = 'OPEN'` have no ground truth yet — settlement has not occurred. Storing `NULL` rather than 0 prevents them from distorting `actual_suspension_rate` calculations. The aggregate procedure uses `COUNTIF(is_suspension IS NOT NULL)` as the denominator for this reason.

**Aggregate table over live queries**  
The Power BI daily overview page needs four KPI cards to load in under two seconds. Running `COUNT(*)` over a partitioned fact table with millions of rows on every dashboard open is unnecessary. The aggregate table is a materialized result that refreshes once per night.

---

## How to Deploy

```bash
# 1. Create the dataset and all tables / procedures / views
bq query --use_legacy_sql=false < sql/schema.sql

# 2. Populate dim_date (one-time; already included in schema.sql)
#    The INSERT in the schema file covers 2020-01-01 to 2030-12-31.

# 3. Load yesterday's transactions into staging
bq load --source_format=CSV \
  maroclear-dwh:suspension_detection.stg_transactions \
  data/processed/features_YYYY-MM-DD.csv

# 4. Load yesterday's predictions into staging
bq load --source_format=CSV \
  maroclear-dwh:suspension_detection.stg_predictions \
  reports/predictions_YYYY-MM-DD.csv

# 5. Run the master ETL procedure
bq query --use_legacy_sql=false \
  "CALL \`maroclear-dwh.suspension_detection.sp_daily_load\`();"
```

Steps 3–5 should be scheduled daily (e.g. via Cloud Scheduler + BigQuery Scheduled Queries, or an Airflow DAG) to run after `predict.py` finishes.
