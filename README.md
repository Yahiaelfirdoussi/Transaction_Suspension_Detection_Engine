# Transaction Suspension Detection Engine

> ML system that detects stock transaction suspensions at Morocco's national securities depository (Maroclear).  
> Built to reduce manual review workload and enable proactive operations monitoring.

---

## What is a transaction suspension?

At Maroclear (Morocco's central securities depository), a **suspension** occurs when a stock trade fails to settle on the expected date — the seller fails to deliver securities or the buyer fails to deliver funds. These are flagged with statuses:

| Status | Meaning |
|---|---|
| `FLAR` | Fail to deliver — seller didn't deliver securities |
| `RFSL` | Fail to receive — buyer didn't confirm receipt |
| `SFSL` | Soft fail to settle — temporary non-critical failure |
| `PLAR` | Partial fail — only part of securities delivered |

The goal is to **predict suspensions before settlement date** so operations teams can intervene proactively.

---

## Results

| Model | Test F1 | Test Recall | Test Precision | Test AUC |
|---|---|---|---|---|
| Logistic Regression | — | — | — | — |
| Decision Tree | — | — | — | — |
| Random Forest | — | — | — | — |
| **XGBoost** | **—** | **—** | **—** | **—** |

*Results use a temporal split — run `python src/train.py` to populate.*

---

## Why temporal split?

This project uses **chronological train/test splitting** instead of random splitting.

**The problem with random splits on financial data:**
If you randomly shuffle transactions, rolling window features (e.g. "how many suspensions did this trader have in the last 5 days?") get computed using future data. The model learns patterns that don't exist in production.

**The solution:**
- Sort all trades by date
- Train on the first 80% of trading days
- Test on the last 20% of trading days
- All rolling window features look backward only

This mirrors real production conditions where the model is trained on historical data and evaluated on the most recent period.

---

## Features engineered

| Feature | Description | Type |
|---|---|---|
| `buyer_historical_suspens` | Buyer's 5-day rolling suspension rate | Temporal |
| `vendeur_historique_suspens` | Seller's 5-day rolling suspension rate | Temporal |
| `daily_activity` | Number of trades by this trader today | Temporal |
| `global_exchange_frequency` | How often this security trades | Temporal |
| `liquidité_volume_5j` | 5-day rolling average settlement volume | Temporal |
| `cutoff_dépassé` | Trade submitted after 15:00 cutoff | Binary |
| `ratio_instruction_vs_market` | Settlement amount vs market volume | Ratio |
| `RSI_5` | 5-period relative strength index | Technical |
| `MACD_diff` | MACD momentum signal | Technical |
| `taux_changement_prix` | Price change rate | Behavioural |
| `taux_changement_volume` | Volume change rate | Behavioural |
| `day_of_week` | Day of week (Monday = 0) | Temporal |

---

## Quick start

```bash
# 1. Clone
git clone https://github.com/yahiaelfirdoussi/suspension-detection
cd suspension-detection

# 2. Install dependencies
pip install -r requirements.txt

# 3. Place your data
#    Copy your transaction CSV to:  data/raw/data.csv

# 4. Preprocess (temporal split)
python src/preprocessing.py

# 5. Train
python src/train.py

# 6. Score new transactions
python src/predict.py --input data/raw/new_trades.csv \
                      --output reports/predictions.csv
```

---

## Project structure

```
suspension-detection/
├── src/
│   ├── preprocessing.py      # Load, engineer features, TEMPORAL split, encode, scale
│   ├── feature_engineering.py # All temporal and behavioural feature functions
│   ├── feature_selection.py  # RandomForest-based feature importance & selection
│   ├── train.py              # Train LR / DT / RF / XGBoost, print comparison
│   └── predict.py            # Score new transactions for suspension risk
│
├── notebooks/
│   └── exploration.ipynb     # Full EDA and modelling notebook
│
├── data/
│   ├── raw/                  # Raw transaction CSV (git-ignored)
│   └── processed/            # Temporally-split train/test sets (git-ignored)
│
├── models/                   # Saved model artefacts (git-ignored)
├── reports/                  # Prediction outputs
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Key design decisions

**Temporal split** — financial time series data must be split chronologically. Random splits leak future rolling features into training and produce optimistically biased evaluation metrics.

**Class imbalance** — suspensions are rare events (~5-15% of trades). Handled via `class_weight` in sklearn models and `scale_pos_weight` in XGBoost.

**No data leakage in rolling features** — all rolling window computations (buyer/seller historical failure rates, 5-day volume) use `shift(1)` to exclude the current transaction.

**Recall over precision** — for operations teams, missing a suspension (false negative) is more costly than a false alarm. Models are evaluated primarily on recall.

---

## Context

Built during an end-of-studies internship at **Maroclear SA** (Casablanca) — Morocco's national central securities depository, responsible for the custody and settlement of all Moroccan stock market transactions.

---

## Tech stack

`Python 3.11` · `Pandas` · `Scikit-learn` · `XGBoost` · `TA-Lib (ta)` · `Imbalanced-learn` · `Plotly` · `Seaborn`

---

## Author

**Yahya Elfirdoussi** — Data Scientist & ML Engineer  
📧 yahiaelfirdoussi7@gmail.com  
🔗 [LinkedIn](https://linkedin.com/in/yahya-elfirdoussi) · [Portfolio](https://yahiaelfirdoussi.netlify.app) · [GitHub](https://github.com/yahiaelfirdoussi)
