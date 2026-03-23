"""
preprocessing.py
----------------
Data loading, cleaning, feature engineering, encoding, scaling,
and TEMPORAL train/test splitting for the transaction suspension
detection pipeline.

Why temporal split?
-------------------
This is financial transaction data ordered in time. Using a random
split would leak future information into training (e.g. historical
failure rates computed on data that includes future rows).
Instead we split chronologically:
  - Train : first 80% of trading days
  - Test  : last 20% of trading days
This simulates real production conditions where the model is trained
on historical data and evaluated on the most recent period.
"""

import os
import sys
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.utils import class_weight
from pickle import dump
import warnings
warnings.filterwarnings("ignore")

sys.path.append(os.path.dirname(__file__))
from feature_engineering import (
    create_cutoff_depasse,
    create_buyer_historical_failures,
    create_vendeur_historique_suspens,
    create_trader_daily_activity,
    create_global_exchange_frequency,
    create_ratio_instruction_vs_market,
    create_liquidite_volume_5j,
    calculateRsiParTitre,
    createMacdRapideParTitre,
    enrichirComportemental,
)

# ── Constants ─────────────────────────────────────────────────────────────────

RAW_DATA_PATH    = "data/raw/data.csv"
OUTPUT_TRAIN     = "data/processed/train.csv"
OUTPUT_TEST      = "data/processed/test.csv"
TARGET           = "FAIL_STATUS"
FAILURE_STATUSES = ["FLAR", "RFSL", "SFSL", "PLAR"]

# Columns to drop before modelling (identifiers, leaky, redundant)
DROP_COLS = [
    "TRADEREFERENCE", "TRADETIME", "TRADECURRENCY", "TRADESTATUS",
    "TRADERBPID", "CTRTRADERBPID", "SECURITYID", "ALTSECURITYID",
    "INSTRUMENTTYPE", "MARKETSEGMENT", "SETTLEMENTDATE", "SETTLEDDATE",
    "SETTLEMENTCYCLE", "EXCHANGEREFERENCE", "MATCHEDTRADEREF",
    "TRADEDATE_only", "moyenne_volume_5j", "IPC",
]

# Categorical columns to one-hot encode
OHE_COLS = ["SECURITYID", "INSTRUMENTTYPE", "MARKETSEGMENT",
            "SETTLEMENTTYPE", "SETTLEMENTCYCLE"]

# Columns to scale (all numeric except binary flags and target)
BINARY_COLS = ["cutoff_dépassé", "FAIL_STATUS", "day_of_week"]


# ── Load ──────────────────────────────────────────────────────────────────────

def load_raw(path: str = RAW_DATA_PATH) -> pd.DataFrame:
    """Load the raw transaction CSV."""
    df = pd.read_csv(path, low_memory=False)
    df["TRADEDATE"] = pd.to_datetime(df["TRADEDATE"], errors="coerce")
    print(f"[load]  {df.shape[0]:,} rows  |  date range: "
          f"{df['TRADEDATE'].min().date()} → {df['TRADEDATE'].max().date()}")
    return df


# ── Target ────────────────────────────────────────────────────────────────────

def build_target(df: pd.DataFrame) -> pd.DataFrame:
    """Create binary FAIL_STATUS: 1 = suspension, 0 = settled/open."""
    df = df.copy()
    df[TARGET] = df["TRADESTATUS"].apply(
        lambda x: 1 if x in FAILURE_STATUSES else 0
    )
    print(f"[target]  suspension rate = {df[TARGET].mean():.1%}")
    return df


# ── Feature engineering ───────────────────────────────────────────────────────

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all temporal and behavioural feature engineering.
    All rolling windows look BACKWARD only — no data leakage.
    """
    print("[features]  engineering temporal features...")
    df = create_cutoff_depasse(df)
    df = create_buyer_historical_failures(df)       # 5-day rolling buyer fail rate
    df = create_vendeur_historique_suspens(df)      # 5-day rolling seller fail rate
    df = create_trader_daily_activity(df)           # daily trade count per trader
    df = create_global_exchange_frequency(df)       # security exchange frequency
    df = create_ratio_instruction_vs_market(df)     # settlement vs market volume
    df = create_liquidite_volume_5j(df)             # 5-day rolling liquidity
    df = createMacdRapideParTitre(df)               # MACD momentum per security
    df = enrichirComportemental(df)                 # price/volume change rates

    # Day of week (temporal signal)
    df["day_of_week"] = df["TRADEDATE"].dt.dayofweek

    # Drop OPEN trades (not settled yet — no ground truth)
    df = df[df["TRADESTATUS"] != "OPEN"].copy()

    if "TRADEDATE_only" in df.columns:
        df.drop(columns=["TRADEDATE_only"], inplace=True)

    print(f"[features]  shape after engineering: {df.shape}")
    return df


# ── Temporal split ────────────────────────────────────────────────────────────

def temporal_split(df: pd.DataFrame, test_ratio: float = 0.2):
    """
    Split by trading date — NO random shuffling.

    The split point is the (1 - test_ratio) quantile of unique trading dates.
    All trades on or before the cutoff go to train; all trades after go to test.

    This guarantees:
    - No future data in training set
    - Historical features (rolling windows) are computed correctly
    - Realistic evaluation of production performance
    """
    unique_dates = df["TRADEDATE"].sort_values().unique()
    cutoff_idx   = int(len(unique_dates) * (1 - test_ratio))
    cutoff_date  = unique_dates[cutoff_idx]

    train = df[df["TRADEDATE"] <= cutoff_date].copy()
    test  = df[df["TRADEDATE"] >  cutoff_date].copy()

    print(f"[split]  cutoff date : {pd.Timestamp(cutoff_date).date()}")
    print(f"[split]  train : {train.shape[0]:,} rows  "
          f"({train[TARGET].mean():.1%} suspensions)")
    print(f"[split]  test  : {test.shape[0]:,} rows  "
          f"({test[TARGET].mean():.1%} suspensions)")
    return train, test


# ── Encode & scale ────────────────────────────────────────────────────────────

def encode_and_scale(train: pd.DataFrame, test: pd.DataFrame,
                     scaler_path: str = None):
    """
    1. Drop identifier / leaky columns.
    2. One-hot encode categorical columns.
    3. Standard-scale continuous features.

    Fit everything on TRAIN only — applied to test separately (no leakage).

    Returns X_train, y_train, X_test, y_test, scaler, feature_names
    """
    meta_cols = [c for c in DROP_COLS if c in train.columns]

    # Save meta for later analysis
    train_meta = train[["TRADEDATE"] + [c for c in meta_cols if c in train.columns]].copy()
    test_meta  = test[["TRADEDATE"]  + [c for c in meta_cols if c in test.columns]].copy()

    # Drop meta columns
    train = train.drop(columns=meta_cols + ["TRADEDATE"], errors="ignore")
    test  = test.drop(columns=meta_cols  + ["TRADEDATE"], errors="ignore")

    # One-hot encode (fit on train)
    ohe_present = [c for c in OHE_COLS if c in train.columns]
    if ohe_present:
        train = pd.get_dummies(train, columns=ohe_present, drop_first=True)
        test  = pd.get_dummies(test,  columns=ohe_present, drop_first=True)
        # Align columns — test may be missing some OHE columns
        train, test = train.align(test, join="left", axis=1, fill_value=0)

    # Drop remaining object/datetime columns
    train = train.select_dtypes(exclude=["object", "datetime64[ns]"])
    test  = test.select_dtypes(exclude=["object", "datetime64[ns]"])

    # Separate X / y
    y_train = train[TARGET].astype(int)
    y_test  = test[TARGET].astype(int)
    X_train = train.drop(columns=[TARGET])
    X_test  = test.drop(columns=[TARGET])

    # Standard scaling — fit on train only
    binary_present = [c for c in BINARY_COLS if c in X_train.columns]
    scale_cols = [c for c in X_train.columns if c not in binary_present]

    scaler = StandardScaler()
    X_train[scale_cols] = scaler.fit_transform(X_train[scale_cols])
    X_test[scale_cols]  = scaler.transform(X_test[scale_cols])

    if scaler_path:
        os.makedirs(os.path.dirname(scaler_path), exist_ok=True)
        dump(scaler, open(scaler_path, "wb"))
        print(f"[prep]  scaler → {scaler_path}")

    print(f"[prep]  X_train={X_train.shape}  X_test={X_test.shape}")
    return X_train, y_train, X_test, y_test, scaler, X_train.columns.tolist()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    df = load_raw()
    df = build_target(df)
    df = engineer_features(df)

    train_df, test_df = temporal_split(df)

    os.makedirs("data/processed", exist_ok=True)
    train_df.to_csv(OUTPUT_TRAIN, index=False)
    test_df.to_csv(OUTPUT_TEST,   index=False)
    print(f"[save]  train → {OUTPUT_TRAIN}")
    print(f"[save]  test  → {OUTPUT_TEST}")

    X_train, y_train, X_test, y_test, scaler, features = encode_and_scale(
        train_df, test_df, scaler_path="models/scaler.pkl"
    )
    print("✅  Preprocessing done.")
