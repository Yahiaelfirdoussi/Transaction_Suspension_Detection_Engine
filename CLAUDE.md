# CLAUDE.md — Transaction Suspension Detection Engine

## What this project is

A production-grade ML system built during an end-of-studies internship at **Maroclear SA** — Morocco's national central securities depository. It predicts stock transaction suspensions before settlement date so operations teams can intervene proactively, and feeds results into a real-time Power BI monitoring dashboard.

A suspension occurs when a trade fails to settle on the expected date — either the seller fails to deliver securities (`FLAR`) or the buyer fails to confirm receipt (`RFSL`). These are rare (~5–15% of trades) but operationally costly events.

---

## Project structure

```
suspension-detection/
├── preprocessing.py          # Load raw CSV, engineer features, temporal split, encode, scale
├── feature_engineering.py    # All temporal + behavioural + technical feature functions
├── feature_selection.py      # RandomForest importance-based feature selection
├── train.py                  # Train LR / DT / RF / XGBoost, print comparison table
├── predict.py                # Score new transaction CSVs for suspension risk
├── notebooks/
│   └── exploration.ipynb     # Full EDA and modelling notebook
├── data/
│   ├── raw/                  # Raw transaction CSV (git-ignored)
│   └── processed/            # Temporally-split train/test sets (git-ignored)
├── models/                   # Saved model artefacts (git-ignored)
├── reports/                  # Prediction outputs
├── requirements.txt
└── README.md
```

---

## How to run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Place raw transaction data
#    Copy your CSV to: data/raw/data.csv
#    Expected columns: date, buyer_id, seller_id, security_id, amount, volume,
#                      price, settlement_status, cutoff_time, market_volume

# 3. Preprocess (temporal split, feature engineering, encoding, scaling)
python preprocessing.py

# 4. Train all models and print comparison
python train.py

# 5. Score new transactions
python predict.py --input data/raw/new_trades.csv --output reports/predictions.csv
```

---

## Data schema

The raw input CSV contains one row per transaction with these fields:

| Column | Type | Description |
|---|---|---|
| `date` | date | Trade date |
| `buyer_id` | string | Buyer participant code |
| `seller_id` | string | Seller participant code |
| `security_id` | string | ISIN or internal security code |
| `amount` | float | Settlement amount in MAD |
| `volume` | int | Number of securities |
| `price` | float | Unit price |
| `settlement_status` | string | `FLAR`, `RFSL`, `SFSL`, `PLAR`, or `OK` |
| `cutoff_time` | datetime | Time instruction was submitted |
| `market_volume` | float | Total market volume for this security that day |

Target label is derived from `settlement_status`: any non-`OK` status = suspension (1), `OK` = no suspension (0).

---

## Features engineered

All features are computed with `shift(1)` to avoid data leakage — the current transaction is never included in its own rolling window.

| Feature | Type | Description |
|---|---|---|
| `buyer_historical_suspens` | Temporal | Buyer's 5-day rolling suspension rate |
| `vendeur_historique_suspens` | Temporal | Seller's 5-day rolling suspension rate |
| `daily_activity` | Temporal | Number of trades by this trader today |
| `global_exchange_frequency` | Temporal | How often this security trades overall |
| `liquidité_volume_5j` | Temporal | 5-day rolling average settlement volume |
| `cutoff_dépassé` | Binary | 1 if trade submitted after 15:00 cutoff |
| `ratio_instruction_vs_market` | Ratio | Settlement amount ÷ market volume |
| `RSI_5` | Technical | 5-period relative strength index |
| `MACD_diff` | Technical | MACD momentum signal |
| `taux_changement_prix` | Behavioural | Price change rate vs prior day |
| `taux_changement_volume` | Behavioural | Volume change rate vs prior day |
| `day_of_week` | Calendar | Monday = 0, Friday = 4 |

---

## Model training

Four models are trained and compared. The train/test split is **temporal** — the first 80% of trading days are used for training, the last 20% for evaluation. Random splits are explicitly avoided because rolling features would leak future data into training.

Class imbalance (~5–15% suspension rate) is handled via:
- `class_weight='balanced'` in sklearn models
- `scale_pos_weight` in XGBoost set to `(n_negatives / n_positives)`

**Primary metric is recall** — missing a suspension is more costly than a false alarm for the operations team.

```python
# Train all models
python train.py

# Output: comparison table with F1, Recall, Precision, AUC for each model
```

XGBoost is expected to be the best performer. Results are printed to stdout and saved to `reports/model_comparison.csv`.

---

## Inference

```bash
python predict.py \
  --input  data/raw/new_trades.csv \
  --output reports/predictions.csv
```

Output CSV adds three columns to the input:
- `suspension_probability` — float between 0 and 1
- `suspension_predicted` — binary 0 or 1
- `suspension_label` — human-readable `"At risk"` or `"Clear"`

The same preprocessing pipeline (encoder + scaler fit on training data) is applied to new data. These artefacts are loaded from `models/`.

---

## Power BI dashboard

The project feeds a real-time Power BI monitoring dashboard used by Maroclear's operations team. The dashboard connects to the output of `predict.py` via a scheduled refresh.

**Dashboard pages:**
1. **Daily risk overview** — KPI cards for total trades, flagged trades, suspension rate, high-risk participants
2. **Participant monitoring** — buyer/seller suspension rates ranked, trend lines per participant
3. **Security heatmap** — which securities have the highest suspension frequency
4. **Prediction drill-through** — row-level view of flagged transactions with probability scores

**Data warehouse layer (BigQuery-equivalent schema):**

```
fact_transactions          — one row per trade, all raw fields + engineered features + prediction
dim_participants           — buyer/seller master data
dim_securities             — ISIN, sector, market cap tier
dim_date                   — calendar table with trading day flag, week, month, quarter
agg_daily_suspension_rate  — pre-aggregated daily KPIs for dashboard performance
```

---

## Key design decisions

**Temporal split over random split** — financial time series must be split chronologically. Random splits leak rolling window features (e.g. buyer's 5-day failure rate) computed using future data into training, producing optimistically biased metrics that don't reflect production performance.

**Shift(1) on all rolling features** — every rolling window uses `.shift(1)` to exclude the current transaction. Without this, the target would inform its own features.

**Recall over precision** — for operations teams, a missed suspension (false negative) creates failed settlements and regulatory exposure. A false alarm only costs a few minutes of review. Models are selected and tuned to maximise recall subject to a minimum precision floor.

**No data leakage in preprocessing** — the encoder and scaler are fit exclusively on training data, then applied (`.transform()` only) to test and inference data. Fitting on the full dataset before splitting is a common mistake explicitly avoided here.

---

## Dependencies

```
pandas>=2.0
numpy>=1.24
scikit-learn>=1.3
xgboost>=2.0
ta                  # Technical analysis indicators (RSI, MACD)
imbalanced-learn    # SMOTE (optional, not used by default)
plotly
seaborn
matplotlib
joblib
```

---

## Extending this project

**To add a new feature:** implement it in `feature_engineering.py` as a function that takes the sorted dataframe and returns a new column. Call it from `preprocessing.py` before the train/test split.

**To add a new model:** add a training block in `train.py` following the existing pattern. The comparison table is generated automatically from whatever models are in the `results` dict.

**To connect to a live data source:** replace the CSV loader in `preprocessing.py` with a database connector (e.g. `sqlalchemy` + PostgreSQL or BigQuery client). The rest of the pipeline is unchanged.

**To schedule daily inference:** wrap `predict.py` in a cron job or Airflow DAG that pulls yesterday's trades, scores them, and appends to the Power BI data source before market open.

---

## Author

**Yahya Elfirdoussi** — Data Scientist & ML Engineer
Built during end-of-studies internship at Maroclear SA, Casablanca.
📧 yahiaelfirdoussi7@gmail.com
🔗 [LinkedIn](https://linkedin.com/in/yahya-elfirdoussi) · [Portfolio](https://yahiaelfirdoussi.netlify.app) · [GitHub](https://github.com/yahiaelfirdoussi)
