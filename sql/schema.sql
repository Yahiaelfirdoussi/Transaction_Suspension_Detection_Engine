-- ============================================================================
-- Maroclear Transaction Suspension Detection — BigQuery Star Schema
-- Project  : maroclear-dwh
-- Dataset  : suspension_detection
-- Author   : Yahya Elfirdoussi
--
-- Star schema overview:
--
--   dim_date ──────────────────────────────────────┐
--   dim_participant (trader)  ────────────────────── fact_transactions ── agg_daily_suspension_rate
--   dim_participant (counterparty) ───────────────┘
--   dim_security ──────────────────────────────────┘
--
-- Incremental load strategy:
--   Dimensions  → MERGE (SCD Type 1: upsert on natural key)
--   Fact table  → MERGE (INSERT new trades, UPDATE prediction columns only)
--   Aggregates  → DELETE + INSERT per partition date (idempotent)
-- ============================================================================


-- ============================================================================
-- 0. DATASET
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS `maroclear-dwh.suspension_detection`
OPTIONS (location = 'EU');


-- ============================================================================
-- 1. STAGING TABLES
-- Raw CSV data is loaded here first (e.g. via bq load or Dataflow).
-- The ETL procedures read from staging, then discard or archive.
-- ============================================================================

-- Mirrors the raw transaction CSV (predict.py input + feature columns)
CREATE TABLE IF NOT EXISTS `maroclear-dwh.suspension_detection.stg_transactions`
(
  -- ── Core identifiers ──
  TRADEREFERENCE    STRING,
  TRADEDATE         STRING,    -- Loaded as STRING, cast in procedure
  TRADETIME         STRING,    -- HH:MM:SS
  TRADECURRENCY     STRING,
  TRADESTATUS       STRING,    -- FLAR / RFSL / SFSL / PLAR / OK / OPEN
  TRADERBPID        STRING,    -- Participant on this side of the trade
  CTRTRADERBPID     STRING,    -- Counterparty participant
  SECURITYID        STRING,    -- ISIN
  ALTSECURITYID     STRING,
  INSTRUMENTTYPE    STRING,
  MARKETSEGMENT     STRING,
  SETTLEMENTTYPE    STRING,    -- XRVP (buy side) / XDVP (sell side)
  SETTLEMENTCYCLE   STRING,
  SETTLEMENTDATE    STRING,
  SETTLEDDATE       STRING,
  EXCHANGEREFERENCE STRING,
  MATCHEDTRADEREF   STRING,

  -- ── Trade economics ──
  SETTLEMENTAMOUNT  FLOAT64,   -- Settlement amount in MAD
  TRADEPRICE        FLOAT64,   -- Unit price
  TRADEQUANTITY     INT64,     -- Number of securities
  volume_globale    FLOAT64,   -- Market-level volume for this ISIN that day

  -- ── Engineered features (populated by preprocessing.py before staging) ──
  cutoff_depasse              INT64,    -- 1 if TRADETIME hour > 15:00
  buyer_historical_suspens    FLOAT64,  -- 5-day rolling buyer fail rate
  vendeur_historique_suspens  FLOAT64,  -- 5-day rolling seller fail rate
  daily_activity              INT64,    -- Trades by TRADERBPID on TRADEDATE
  global_exchange_frequency   FLOAT64,  -- Fraction of trading days SECURITYID appeared
  ratio_instruction_vs_market FLOAT64,  -- SETTLEMENTAMOUNT / volume_globale
  liquidite_volume_5j         FLOAT64,  -- 5-day rolling avg SETTLEMENTAMOUNT by ISIN
  RSI_5                       FLOAT64,  -- 5-period RSI (XRVP side only, NULL for XDVP)
  MACD_diff                   FLOAT64,  -- MACD momentum per ISIN
  taux_changement_prix        FLOAT64,  -- Price pct change vs prior row
  taux_changement_volume      FLOAT64,  -- Volume pct change vs prior row
  log_tradeprice              FLOAT64,  -- log(1 + TRADEPRICE)
  ratio_volume_prix           FLOAT64,  -- TRADEQUANTITY / TRADEPRICE
  day_of_week                 INT64     -- 0 = Monday … 6 = Sunday
)
OPTIONS (description = 'Staging table — raw CSV rows loaded daily before ETL');

-- Mirrors the predict.py output CSV
CREATE TABLE IF NOT EXISTS `maroclear-dwh.suspension_detection.stg_predictions`
(
  TRADEREFERENCE         STRING    NOT NULL,
  TRADEDATE              STRING,
  TRADERBPID             STRING,
  CTRTRADERBPID          STRING,
  SECURITYID             STRING,
  TRADESTATUS            STRING,
  suspension_probability FLOAT64,  -- XGBoost P(suspension) in [0, 1]
  suspension_predicted   INT64,    -- 1 = HIGH RISK, 0 = LOW RISK
  risk_label             STRING,   -- "HIGH RISK" / "LOW RISK"
  model_version          STRING,   -- e.g. "xgb_v1.0"
  predicted_at           TIMESTAMP,
  _source_file           STRING    -- origin CSV filename for lineage
)
OPTIONS (description = 'Staging table — XGBoost predictions from predict.py');


-- ============================================================================
-- 2. DIMENSION TABLES
-- ============================================================================

-- ── dim_date ─────────────────────────────────────────────────────────────────
-- Pre-populated once with all calendar dates in the trading range.
-- is_trading_day is maintained separately (e.g. from exchange holiday calendar).

CREATE TABLE IF NOT EXISTS `maroclear-dwh.suspension_detection.dim_date`
(
  date_key       INT64   NOT NULL,  -- Surrogate: YYYYMMDD integer (e.g. 20240115)
  full_date      DATE    NOT NULL,
  day_of_week    INT64   NOT NULL,  -- 0 = Monday … 6 = Sunday
  day_name       STRING  NOT NULL,  -- "Monday" … "Sunday"
  day_of_month   INT64   NOT NULL,
  week_of_year   INT64   NOT NULL,
  month          INT64   NOT NULL,
  month_name     STRING  NOT NULL,
  quarter        INT64   NOT NULL,
  year           INT64   NOT NULL,
  is_trading_day BOOL    NOT NULL DEFAULT FALSE
)
OPTIONS (description = 'Calendar dimension — populate once with generate_date_array');

-- Populate dim_date for a given range (run once at setup)
-- Replace the date range with your actual trading history range.
INSERT INTO `maroclear-dwh.suspension_detection.dim_date`
SELECT
  CAST(FORMAT_DATE('%Y%m%d', d) AS INT64)  AS date_key,
  d                                         AS full_date,
  EXTRACT(DAYOFWEEK FROM d) - 2             AS day_of_week,  -- BigQuery: 1=Sun → adjust to 0=Mon
  FORMAT_DATE('%A', d)                      AS day_name,
  EXTRACT(DAY   FROM d)                     AS day_of_month,
  EXTRACT(WEEK  FROM d)                     AS week_of_year,
  EXTRACT(MONTH FROM d)                     AS month,
  FORMAT_DATE('%B', d)                      AS month_name,
  EXTRACT(QUARTER FROM d)                   AS quarter,
  EXTRACT(YEAR  FROM d)                     AS year,
  -- Weekdays only as a baseline; update is_trading_day from exchange calendar
  EXTRACT(DAYOFWEEK FROM d) NOT IN (1, 7)   AS is_trading_day
FROM UNNEST(
  GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2030-12-31', INTERVAL 1 DAY)
) AS d;


-- ── dim_participant ───────────────────────────────────────────────────────────
-- Covers both TRADERBPID (trader) and CTRTRADERBPID (counterparty).
-- SCD Type 1: attributes overwritten in-place; no history tracked.

CREATE TABLE IF NOT EXISTS `maroclear-dwh.suspension_detection.dim_participant`
(
  participant_key  INT64     NOT NULL,  -- FARM_FINGERPRINT(participant_id)
  participant_id   STRING    NOT NULL,  -- Natural key: TRADERBPID / CTRTRADERBPID
  first_trade_date DATE,
  last_trade_date  DATE,
  _loaded_at       TIMESTAMP NOT NULL,
  _updated_at      TIMESTAMP NOT NULL
)
OPTIONS (description = 'Participant dimension — buyer/seller master, SCD Type 1');


-- ── dim_security ──────────────────────────────────────────────────────────────
-- One row per ISIN. Enrichment columns (sector, market_cap_tier) can be
-- backfilled from an external reference feed.

CREATE TABLE IF NOT EXISTS `maroclear-dwh.suspension_detection.dim_security`
(
  security_key     INT64     NOT NULL,  -- FARM_FINGERPRINT(security_id)
  security_id      STRING    NOT NULL,  -- Natural key: SECURITYID (ISIN)
  alt_security_id  STRING,              -- ALTSECURITYID
  instrument_type  STRING,              -- INSTRUMENTTYPE
  market_segment   STRING,              -- MARKETSEGMENT
  sector           STRING,              -- NULL until enriched from reference feed
  market_cap_tier  STRING,              -- NULL until enriched (e.g. "LARGE", "MID", "SMALL")
  first_seen_date  DATE,
  last_seen_date   DATE,
  _loaded_at       TIMESTAMP NOT NULL,
  _updated_at      TIMESTAMP NOT NULL
)
OPTIONS (description = 'Security dimension — ISIN master, SCD Type 1');


-- ============================================================================
-- 3. FACT TABLE
-- ============================================================================
-- Partitioned by trade_date (daily partitions, no expiry).
-- Clustered by security_key, trader_key for efficient Power BI queries that
-- filter by ISIN or participant within a date range.

CREATE TABLE IF NOT EXISTS `maroclear-dwh.suspension_detection.fact_transactions`
(
  -- ── Keys ──
  transaction_key       INT64     NOT NULL,  -- FARM_FINGERPRINT(trade_reference)
  trade_reference       STRING    NOT NULL,  -- Natural key: TRADEREFERENCE

  -- ── Dimension foreign keys ──
  date_key              INT64     NOT NULL,  -- → dim_date.date_key
  trader_key            INT64     NOT NULL,  -- → dim_participant (TRADERBPID)
  counterparty_key      INT64,               -- → dim_participant (CTRTRADERBPID)
  security_key          INT64     NOT NULL,  -- → dim_security

  -- ── Raw transaction fields (immutable once loaded) ──
  trade_date            DATE      NOT NULL,
  trade_time            STRING,              -- HH:MM:SS string
  trade_currency        STRING,
  trade_status          STRING,              -- FLAR / RFSL / SFSL / PLAR / OK / OPEN
  settlement_type       STRING,              -- XRVP = buy, XDVP = sell
  settlement_cycle      STRING,
  settlement_date       DATE,
  settled_date          DATE,
  exchange_reference    STRING,
  matched_trade_ref     STRING,
  settlement_amount     FLOAT64,             -- MAD
  trade_price           FLOAT64,             -- Unit price
  trade_quantity        INT64,               -- Number of securities / units
  market_volume         FLOAT64,             -- volume_globale: total market volume for ISIN

  -- ── Target label ──
  -- 1 = suspended (FLAR/RFSL/SFSL/PLAR), 0 = settled OK, NULL = still OPEN
  is_suspension         INT64,

  -- ── Engineered features ──
  cutoff_depasse              INT64,    -- 1 if TRADETIME > 15:00
  buyer_historical_suspens    FLOAT64,  -- 5-day rolling buyer fail rate (XRVP side)
  vendeur_historique_suspens  FLOAT64,  -- 5-day rolling seller fail rate (XDVP side)
  daily_activity              INT64,    -- Number of trades by TRADERBPID on trade_date
  global_exchange_frequency   FLOAT64,  -- Fraction of trading days ISIN appeared
  ratio_instruction_vs_market FLOAT64,  -- settlement_amount / market_volume
  liquidite_volume_5j         FLOAT64,  -- 5-day rolling avg settlement_amount by ISIN
  rsi_5                       FLOAT64,  -- 5-period RSI (XRVP rows only; NULL for XDVP)
  macd_diff                   FLOAT64,  -- MACD momentum signal per ISIN
  taux_changement_prix        FLOAT64,  -- Price pct change vs prior row
  taux_changement_volume      FLOAT64,  -- Volume pct change vs prior row
  log_tradeprice              FLOAT64,  -- log(1 + trade_price)
  ratio_volume_prix           FLOAT64,  -- trade_quantity / trade_price
  day_of_week                 INT64,    -- 0 = Monday … 6 = Sunday

  -- ── ML prediction (updated on each model run; raw fields unchanged) ──
  suspension_probability  FLOAT64,   -- XGBoost P(suspension) in [0, 1]
  suspension_predicted    INT64,     -- 1 = HIGH RISK, 0 = LOW RISK
  risk_label              STRING,    -- "HIGH RISK" / "LOW RISK"
  model_version           STRING,    -- e.g. "xgb_v1.0" — tracks which run scored this row
  predicted_at            TIMESTAMP, -- Timestamp of the scoring run

  -- ── ETL metadata ──
  _loaded_at   TIMESTAMP NOT NULL,
  _updated_at  TIMESTAMP NOT NULL,
  _source_file STRING               -- Origin CSV filename for lineage
)
PARTITION BY trade_date
CLUSTER BY security_key, trader_key
OPTIONS (
  description               = 'Core fact table — one row per trade with raw fields, engineered features, and ML prediction',
  require_partition_filter  = FALSE
);


-- ============================================================================
-- 4. AGGREGATE TABLE
-- Pre-computed daily KPIs — keeps Power BI queries fast without scanning
-- the full fact table. Refreshed nightly by sp_refresh_agg_daily.
-- ============================================================================

CREATE TABLE IF NOT EXISTS `maroclear-dwh.suspension_detection.agg_daily_suspension_rate`
(
  agg_date                   DATE      NOT NULL,
  total_trades               INT64     NOT NULL,
  flagged_trades             INT64     NOT NULL,  -- suspension_predicted = 1
  predicted_suspension_rate  FLOAT64   NOT NULL,  -- flagged_trades / total_trades
  actual_suspensions         INT64,               -- is_suspension = 1 (NULL if day not yet settled)
  actual_suspension_rate     FLOAT64,             -- actual_suspensions / settled trades
  high_risk_participants     INT64,               -- distinct trader_keys flagged that day
  avg_suspension_probability FLOAT64,
  _refreshed_at              TIMESTAMP NOT NULL
)
PARTITION BY agg_date
OPTIONS (description = 'Pre-aggregated daily KPIs — feeds Power BI daily overview page');


-- ============================================================================
-- 5. INCREMENTAL LOAD PROCEDURES
-- ============================================================================

-- ── 5a. sp_load_dim_participant ───────────────────────────────────────────────
-- Reads both TRADERBPID and CTRTRADERBPID from staging.
-- SCD Type 1: INSERT new participants, UPDATE date range on existing.

CREATE OR REPLACE PROCEDURE `maroclear-dwh.suspension_detection.sp_load_dim_participant`()
BEGIN
  MERGE `maroclear-dwh.suspension_detection.dim_participant` AS tgt
  USING (
    SELECT
      FARM_FINGERPRINT(participant_id)  AS participant_key,
      participant_id,
      MIN(CAST(TRADEDATE AS DATE))      AS first_trade_date,
      MAX(CAST(TRADEDATE AS DATE))      AS last_trade_date
    FROM (
      -- Trader side
      SELECT TRADERBPID    AS participant_id, TRADEDATE
      FROM `maroclear-dwh.suspension_detection.stg_transactions`
      WHERE TRADERBPID IS NOT NULL
      UNION ALL
      -- Counterparty side
      SELECT CTRTRADERBPID AS participant_id, TRADEDATE
      FROM `maroclear-dwh.suspension_detection.stg_transactions`
      WHERE CTRTRADERBPID IS NOT NULL
    )
    GROUP BY participant_id
  ) AS src
  ON tgt.participant_key = src.participant_key

  -- Expand the known date range when new trades arrive
  WHEN MATCHED AND (
    src.first_trade_date < tgt.first_trade_date OR
    src.last_trade_date  > tgt.last_trade_date
  ) THEN UPDATE SET
    first_trade_date = LEAST(tgt.first_trade_date, src.first_trade_date),
    last_trade_date  = GREATEST(tgt.last_trade_date, src.last_trade_date),
    _updated_at      = CURRENT_TIMESTAMP()

  -- First time we see this participant
  WHEN NOT MATCHED BY TARGET THEN INSERT (
    participant_key, participant_id,
    first_trade_date, last_trade_date,
    _loaded_at, _updated_at
  ) VALUES (
    src.participant_key, src.participant_id,
    src.first_trade_date, src.last_trade_date,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
  );
END;


-- ── 5b. sp_load_dim_security ─────────────────────────────────────────────────
-- SCD Type 1: INSERT new ISINs, UPDATE last_seen_date + attributes.

CREATE OR REPLACE PROCEDURE `maroclear-dwh.suspension_detection.sp_load_dim_security`()
BEGIN
  MERGE `maroclear-dwh.suspension_detection.dim_security` AS tgt
  USING (
    SELECT
      FARM_FINGERPRINT(SECURITYID)        AS security_key,
      SECURITYID                           AS security_id,
      MAX(ALTSECURITYID)                   AS alt_security_id,
      MAX(INSTRUMENTTYPE)                  AS instrument_type,
      MAX(MARKETSEGMENT)                   AS market_segment,
      MIN(CAST(TRADEDATE AS DATE))         AS first_seen_date,
      MAX(CAST(TRADEDATE AS DATE))         AS last_seen_date
    FROM `maroclear-dwh.suspension_detection.stg_transactions`
    WHERE SECURITYID IS NOT NULL
    GROUP BY SECURITYID
  ) AS src
  ON tgt.security_key = src.security_key

  -- Refresh attributes and extend the seen-date range
  WHEN MATCHED AND src.last_seen_date > tgt.last_seen_date THEN UPDATE SET
    alt_security_id = COALESCE(src.alt_security_id, tgt.alt_security_id),
    instrument_type = COALESCE(src.instrument_type, tgt.instrument_type),
    market_segment  = COALESCE(src.market_segment,  tgt.market_segment),
    last_seen_date  = src.last_seen_date,
    _updated_at     = CURRENT_TIMESTAMP()

  WHEN NOT MATCHED BY TARGET THEN INSERT (
    security_key, security_id, alt_security_id,
    instrument_type, market_segment,
    first_seen_date, last_seen_date,
    _loaded_at, _updated_at
  ) VALUES (
    src.security_key, src.security_id, src.alt_security_id,
    src.instrument_type, src.market_segment,
    src.first_seen_date, src.last_seen_date,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
  );
END;


-- ── 5c. sp_load_fact_transactions ────────────────────────────────────────────
-- Loads one trading day (run_date) from staging into the fact table.
--
-- Rules:
--   • New trade reference  → INSERT full row including prediction columns.
--   • Existing trade, new prediction score → UPDATE prediction columns only.
--     Raw fields (amounts, prices, quantities) are never overwritten.
--   • The partition filter (trade_date = run_date) keeps MERGE scoped to a
--     single partition, avoiding a full table scan.

CREATE OR REPLACE PROCEDURE `maroclear-dwh.suspension_detection.sp_load_fact_transactions`(
  run_date DATE  -- Trading day to load; typically CURRENT_DATE() - 1
)
BEGIN
  MERGE `maroclear-dwh.suspension_detection.fact_transactions` AS tgt
  USING (
    SELECT
      -- ── Keys ──
      FARM_FINGERPRINT(t.TRADEREFERENCE)                             AS transaction_key,
      t.TRADEREFERENCE                                               AS trade_reference,
      CAST(FORMAT_DATE('%Y%m%d', CAST(t.TRADEDATE AS DATE)) AS INT64) AS date_key,
      FARM_FINGERPRINT(t.TRADERBPID)                                 AS trader_key,
      IF(t.CTRTRADERBPID IS NOT NULL,
         FARM_FINGERPRINT(t.CTRTRADERBPID), NULL)                    AS counterparty_key,
      FARM_FINGERPRINT(t.SECURITYID)                                 AS security_key,

      -- ── Raw fields ──
      CAST(t.TRADEDATE AS DATE)             AS trade_date,
      t.TRADETIME                           AS trade_time,
      t.TRADECURRENCY                       AS trade_currency,
      t.TRADESTATUS                         AS trade_status,
      t.SETTLEMENTTYPE                      AS settlement_type,
      t.SETTLEMENTCYCLE                     AS settlement_cycle,
      SAFE.PARSE_DATE('%Y-%m-%d', t.SETTLEMENTDATE) AS settlement_date,
      SAFE.PARSE_DATE('%Y-%m-%d', t.SETTLEDDATE)    AS settled_date,
      t.EXCHANGEREFERENCE                   AS exchange_reference,
      t.MATCHEDTRADEREF                     AS matched_trade_ref,
      t.SETTLEMENTAMOUNT                    AS settlement_amount,
      t.TRADEPRICE                          AS trade_price,
      t.TRADEQUANTITY                       AS trade_quantity,
      t.volume_globale                      AS market_volume,

      -- ── Target label ──
      CASE
        WHEN t.TRADESTATUS IN ('FLAR', 'RFSL', 'SFSL', 'PLAR') THEN 1
        WHEN t.TRADESTATUS = 'OK'                               THEN 0
        ELSE NULL  -- OPEN: not yet settled, label unknown
      END AS is_suspension,

      -- ── Engineered features ──
      t.cutoff_depasse,
      t.buyer_historical_suspens,
      t.vendeur_historique_suspens,
      t.daily_activity,
      t.global_exchange_frequency,
      t.ratio_instruction_vs_market,
      t.liquidite_volume_5j,
      t.RSI_5                       AS rsi_5,
      t.MACD_diff                   AS macd_diff,
      t.taux_changement_prix,
      t.taux_changement_volume,
      t.log_tradeprice,
      t.ratio_volume_prix,
      t.day_of_week,

      -- ── Prediction (left join: may be NULL if scoring hasn't run yet) ──
      p.suspension_probability,
      p.suspension_predicted,
      p.risk_label,
      p.model_version,
      p.predicted_at,

      -- ── Metadata ──
      CURRENT_TIMESTAMP() AS _loaded_at,
      CURRENT_TIMESTAMP() AS _updated_at,
      p._source_file

    FROM `maroclear-dwh.suspension_detection.stg_transactions` AS t
    LEFT JOIN `maroclear-dwh.suspension_detection.stg_predictions` AS p
           ON t.TRADEREFERENCE = p.TRADEREFERENCE
    WHERE CAST(t.TRADEDATE AS DATE) = run_date

  ) AS src
  ON  tgt.trade_reference = src.trade_reference
  AND tgt.trade_date      = src.trade_date        -- satisfies partition pruning

  -- Row exists and has been re-scored with a different probability
  WHEN MATCHED AND (
    src.suspension_probability IS NOT NULL AND
    src.suspension_probability IS DISTINCT FROM tgt.suspension_probability
  ) THEN UPDATE SET
    suspension_probability = src.suspension_probability,
    suspension_predicted   = src.suspension_predicted,
    risk_label             = src.risk_label,
    model_version          = src.model_version,
    predicted_at           = src.predicted_at,
    _updated_at            = CURRENT_TIMESTAMP()

  -- Brand-new trade: insert the full row
  WHEN NOT MATCHED BY TARGET THEN INSERT ROW;
END;


-- ── 5d. sp_refresh_agg_daily ─────────────────────────────────────────────────
-- Idempotent: delete and re-insert the aggregate for the given date.
-- Safe to re-run multiple times on the same day without double-counting.

CREATE OR REPLACE PROCEDURE `maroclear-dwh.suspension_detection.sp_refresh_agg_daily`(
  target_date DATE
)
BEGIN
  -- Clear existing aggregate for this partition
  DELETE FROM `maroclear-dwh.suspension_detection.agg_daily_suspension_rate`
  WHERE agg_date = target_date;

  -- Recompute from fact table
  INSERT INTO `maroclear-dwh.suspension_detection.agg_daily_suspension_rate`
  SELECT
    trade_date                                AS agg_date,
    COUNT(*)                                  AS total_trades,
    COUNTIF(suspension_predicted = 1)         AS flagged_trades,
    SAFE_DIVIDE(
      COUNTIF(suspension_predicted = 1),
      COUNT(*)
    )                                         AS predicted_suspension_rate,
    COUNTIF(is_suspension = 1)                AS actual_suspensions,
    SAFE_DIVIDE(
      COUNTIF(is_suspension = 1),
      COUNTIF(is_suspension IS NOT NULL)
    )                                         AS actual_suspension_rate,
    COUNT(DISTINCT CASE
      WHEN suspension_predicted = 1 THEN trader_key
    END)                                      AS high_risk_participants,
    ROUND(AVG(suspension_probability), 4)     AS avg_suspension_probability,
    CURRENT_TIMESTAMP()                       AS _refreshed_at

  FROM `maroclear-dwh.suspension_detection.fact_transactions`
  WHERE trade_date = target_date;
END;


-- ── 5e. sp_daily_load — master orchestrator ───────────────────────────────────
-- Runs the full ETL pipeline for the previous trading day.
-- Call from a BigQuery Scheduled Query, Cloud Scheduler, or Airflow DAG.

CREATE OR REPLACE PROCEDURE `maroclear-dwh.suspension_detection.sp_daily_load`()
BEGIN
  DECLARE run_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

  -- Step 1: Refresh dimensions (new participants and ISINs first)
  CALL `maroclear-dwh.suspension_detection.sp_load_dim_participant`();
  CALL `maroclear-dwh.suspension_detection.sp_load_dim_security`();

  -- Step 2: Load enriched trades + predictions into fact table
  CALL `maroclear-dwh.suspension_detection.sp_load_fact_transactions`(run_date);

  -- Step 3: Refresh daily KPI aggregate for Power BI
  CALL `maroclear-dwh.suspension_detection.sp_refresh_agg_daily`(run_date);
END;


-- ============================================================================
-- 6. CONVENIENCE VIEWS  (Power BI data sources)
-- ============================================================================

-- ── v_high_risk_trades ───────────────────────────────────────────────────────
-- Drill-through view: flagged transactions with full context.
-- Scoped to rolling 30 days for dashboard performance.

CREATE OR REPLACE VIEW `maroclear-dwh.suspension_detection.v_high_risk_trades` AS
SELECT
  f.trade_date,
  f.trade_reference,
  tp.participant_id                 AS trader_id,
  cp.participant_id                 AS counterparty_id,
  s.security_id                     AS isin,
  s.instrument_type,
  s.market_segment,
  f.settlement_type,
  f.trade_status,
  f.settlement_amount,
  f.trade_quantity,
  f.trade_price,
  f.market_volume,
  f.is_suspension,
  f.suspension_probability,
  f.suspension_predicted,
  f.risk_label,
  -- Key risk signals surfaced for the ops team
  f.cutoff_depasse,
  f.buyer_historical_suspens,
  f.vendeur_historique_suspens,
  f.ratio_instruction_vs_market,
  f.model_version,
  f.predicted_at
FROM `maroclear-dwh.suspension_detection.fact_transactions`       AS f
JOIN  `maroclear-dwh.suspension_detection.dim_participant`        AS tp
   ON f.trader_key       = tp.participant_key
LEFT JOIN `maroclear-dwh.suspension_detection.dim_participant`    AS cp
   ON f.counterparty_key = cp.participant_key
JOIN  `maroclear-dwh.suspension_detection.dim_security`           AS s
   ON f.security_key     = s.security_key
WHERE f.suspension_predicted = 1
  AND f.trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);


-- ── v_participant_risk_summary ───────────────────────────────────────────────
-- Per-participant daily league table — feeds the Participant Monitoring page.

CREATE OR REPLACE VIEW `maroclear-dwh.suspension_detection.v_participant_risk_summary` AS
SELECT
  p.participant_id,
  f.trade_date,
  COUNT(*)                               AS total_trades,
  COUNTIF(f.suspension_predicted = 1)    AS flagged_trades,
  COUNTIF(f.is_suspension = 1)           AS actual_suspensions,
  ROUND(AVG(f.suspension_probability), 4) AS avg_risk_score,
  SAFE_DIVIDE(
    COUNTIF(f.is_suspension = 1),
    COUNTIF(f.is_suspension IS NOT NULL)
  )                                      AS actual_suspension_rate
FROM `maroclear-dwh.suspension_detection.fact_transactions`  AS f
JOIN `maroclear-dwh.suspension_detection.dim_participant`    AS p
  ON f.trader_key = p.participant_key
GROUP BY p.participant_id, f.trade_date;


-- ── v_security_suspension_heatmap ────────────────────────────────────────────
-- Aggregated by ISIN — feeds the Security Heatmap dashboard page.

CREATE OR REPLACE VIEW `maroclear-dwh.suspension_detection.v_security_suspension_heatmap` AS
SELECT
  s.security_id                          AS isin,
  s.instrument_type,
  s.market_segment,
  f.trade_date,
  COUNT(*)                               AS total_trades,
  COUNTIF(f.suspension_predicted = 1)    AS flagged_trades,
  COUNTIF(f.is_suspension = 1)           AS actual_suspensions,
  ROUND(AVG(f.suspension_probability), 4) AS avg_risk_score,
  ROUND(AVG(f.rsi_5), 2)                 AS avg_rsi_5,
  ROUND(AVG(f.liquidite_volume_5j), 2)   AS avg_liquidity_5d
FROM `maroclear-dwh.suspension_detection.fact_transactions`  AS f
JOIN `maroclear-dwh.suspension_detection.dim_security`       AS s
  ON f.security_key = s.security_key
GROUP BY s.security_id, s.instrument_type, s.market_segment, f.trade_date;
