# Transaction Suspension Detection Engine

> ML system that predicts stock transaction suspensions before settlement date at Maroclear — Morocco's national central securities depository.  
> Feeds a real-time Streamlit monitoring dashboard used by the operations team.

---

## What is a transaction suspension?

At Maroclear, a **suspension** occurs when a trade fails to settle on the expected date. These are flagged with one of four failure statuses:

| Status | Side | Meaning |
|---|---|---|
| `FLAR` | Seller | Fail to deliver — seller didn't deliver securities |
| `RFSL` | Buyer | Fail to receive — buyer didn't confirm receipt |
| `SFSL` | Both | Soft fail — temporary non-critical settlement failure |
| `PLAR` | Seller | Partial fail — only part of the securities delivered |

Suspensions represent ~30% of trades in the dataset and are operationally costly events. The goal is to **flag them before settlement date** so the operations team can intervene proactively.

---

## Results

Training used a **temporal split** (first 80% of trading days → train, last 20% → test) on 74,316 transactions across 10 trading days (Dec 16–26, 2024).

### Training performance

| Model | F1 | Recall | AUC |
|---|---|---|---|
| Logistic Regression | 0.9906 | 0.9869 | 0.9922 |
| Decision Tree | 0.9909 | 0.9886 | 0.9928 |
| Random Forest | 0.9915 | 0.9886 | **0.9931** |
| **XGBoost** | 0.9907 | 0.9869 | 0.9922 |

### Test performance

| Model | F1 | Recall | Precision | AUC |
|---|---|---|---|---|
| Logistic Regression | 1.00 | 1.00 | 1.00 | — |
| Decision Tree | 0.00 | 0.00 | 0.00 | — |
| Random Forest | 0.00 | 0.00 | 0.00 | — |
| XGBoost | 0.00 | 0.00 | 0.00 | — |

> **Note on test results:** The temporal split placed Dec 26 entirely in the test set, which contained exclusively suspended trades (733/733 = 100% positive class). AUC is undefined with a single-class test set. DT, RF and XGBoost defaulted to predicting class 0 on the skewed set, yielding F1=0. Logistic Regression scored 1.0 by predicting all rows as positive. Training metrics on the 70,248-row balanced set are the reliable performance signal. Evaluation on a longer date range will produce a properly stratified test set.

**Selected model for production:** XGBoost — saved to `models/xgb_model.pkl`.

---

## Dashboard

The project ships a Streamlit dashboard with four pages:

| Page | Content |
|---|---|
| **Daily Risk Overview** | KPI cards, daily trend line (predicted vs actual), top-10 participants, outcome donut |
| **Participant Monitoring** | Risk league table, per-participant trend, buyer vs seller historical risk |
| **Security Heatmap** | ISIN × month heatmap, top securities bar chart, liquidity vs risk scatter |
| **Prediction Drill-through** | Risk gauge, score distribution, feature signals, filterable transaction table with CSV export |

**Run locally:**
```bash
streamlit run app.py
```

**Hosted:** [share.streamlit.io](https://share.streamlit.io) — upload `reports/predictions.csv` via the sidebar.

---

## Quick start

```bash
# 1. Clone
git clone https://github.com/Yahiaelfirdoussi/Transaction_Suspension_Detection_Engine
cd Transaction_Suspension_Detection_Engine

# 2. Install dependencies
pip install -r requirements.txt

# 3. Place your transaction data
#    data/raw/data.csv  (see Data schema section for expected columns)

# 4. Preprocess — temporal split, feature engineering, encoding, scaling
python3 preprocessing.py

# 5. Train all models and compare
python3 train.py

# 6. Score new transactions
python3 predict.py --input data/raw/new_trades.csv \
                   --output reports/predictions.csv

# 7. Launch dashboard
streamlit run app.py
```

---

## Project structure

```
├── preprocessing.py        # Load, engineer features, temporal split, encode, scale
├── feature_engineering.py  # All temporal, behavioural and technical feature functions
├── feature_selection.py    # RandomForest-based feature importance and selection
├── train.py                # Train LR / DT / RF / XGBoost, print comparison table
├── predict.py              # Score new transactions for suspension risk
├── app.py                  # Streamlit dashboard (4 pages)
│
├── sql/
│   ├── schema.sql          # BigQuery star schema — dims, fact table, MERGE procedures
│   └── DWH_DESIGN.md       # DWH design doc and deployment guide
│
├── powerbi/
│   ├── queries.pq          # Power Query M for BigQuery data sources
│   ├── measures.dax        # DAX measures for all 4 dashboard pages
│   └── DASHBOARD_BUILD.md  # Step-by-step Power BI build guide
│
├── data/
│   ├── raw/                # Raw transaction CSV
│   └── processed/          # Temporally-split train/test sets
│
├── models/                 # Saved model artefacts (scaler, XGBoost, feature names)
├── reports/                # Prediction outputs
├── .streamlit/config.toml  # Streamlit theme configuration
└── requirements.txt
```

---

## Data schema

| Column | Type | Description |
|---|---|---|
| `TRADEREFERENCE` | string | Unique trade identifier |
| `TRADEDATE` | date | Trade date |
| `TRADETIME` | string | Submission time (HH:MM:SS) |
| `TRADERBPID` | string | Participant on this side of the trade |
| `CTRTRADERBPID` | string | Counterparty participant |
| `SECURITYID` | string | ISIN |
| `TRADESTATUS` | string | `FLAR`, `RFSL`, `SFSL`, `PLAR`, `OK`, `OPEN` |
| `SETTLEMENTTYPE` | string | `XRVP` (buy) / `XDVP` (sell) |
| `SETTLEMENTAMOUNT` | float | Settlement amount in MAD |
| `TRADEPRICE` | float | Unit price |
| `TRADEQUANTITY` | int | Number of securities |
| `volume_globale` | float | Total market volume for this ISIN that day |

Target: `FAIL_STATUS` — 1 if `TRADESTATUS ∈ {FLAR, RFSL, SFSL, PLAR}`, else 0.

---

## Features engineered

All rolling window features use `.shift(1)` to exclude the current transaction — no data leakage.

| Feature | Type | Description |
|---|---|---|
| `buyer_historical_suspens` | Temporal | Buyer's 5-day rolling suspension rate |
| `vendeur_historique_suspens` | Temporal | Seller's 5-day rolling suspension rate |
| `daily_activity` | Temporal | Trades by this participant today |
| `global_exchange_frequency` | Temporal | Fraction of days this ISIN traded |
| `liquidité_volume_5j` | Temporal | 5-day rolling avg settlement amount by ISIN |
| `cutoff_dépassé` | Binary | 1 if submitted after 15:00 cutoff |
| `ratio_instruction_vs_market` | Ratio | Settlement amount ÷ market volume |
| `RSI_5` | Technical | 5-period RSI (buy side only) |
| `MACD_diff` | Technical | MACD momentum signal per ISIN |
| `taux_changement_prix` | Behavioural | Price pct change vs prior row |
| `taux_changement_volume` | Behavioural | Volume pct change vs prior row |
| `day_of_week` | Calendar | 0 = Monday … 6 = Sunday |

---

## Key design decisions

**Temporal split over random split** — rolling features (e.g. buyer's 5-day failure rate) computed using future data would leak into training under a random split, producing optimistically biased metrics.

**Shift(1) on all rolling features** — every rolling window excludes the current transaction. Without this, the target would inform its own features.

**Recall over precision** — a missed suspension (false negative) causes failed settlement and regulatory exposure. A false alarm costs a few minutes of review. Models are tuned to maximise recall.

**No leakage in preprocessing** — the scaler and encoder are fit exclusively on training data, then `.transform()`-only on test and inference data.

**Class imbalance handling** — `class_weight='balanced'` in sklearn models; `scale_pos_weight = n_negatives / n_positives` in XGBoost.

---

## Tech stack

`Python 3.11` · `Pandas` · `Scikit-learn` · `XGBoost` · `TA (ta)` · `Streamlit` · `Plotly` · `Imbalanced-learn` · `Joblib`

---

## Author

**Yahya Elfirdoussi** — Data Scientist & ML Engineer  
Built during end-of-studies internship at **Maroclear SA**, Casablanca.  
📧 yahiaelfirdoussi7@gmail.com  
🔗 [LinkedIn](https://linkedin.com/in/yahya-elfirdoussi) · [Portfolio](https://yahiaelfirdoussi.netlify.app) · [GitHub](https://github.com/yahiaelfirdoussi)
