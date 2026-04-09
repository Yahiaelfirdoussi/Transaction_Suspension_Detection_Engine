"""
predict.py
----------
Load the trained XGBoost model and score new incoming transactions.
Outputs a CSV with suspension probability and binary prediction.

Usage
-----
python src/predict.py --input data/raw/new_trades.csv --output reports/predictions.csv
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np
from pickle import load
import warnings
warnings.filterwarnings("ignore")

sys.path.append(os.path.dirname(__file__))
from feature_engineering import (
    create_cutoff_depasse, create_buyer_historical_failures,
    create_vendeur_historique_suspens, create_trader_daily_activity,
    create_global_exchange_frequency, create_ratio_instruction_vs_market,
    create_liquidite_volume_5j, calculateRsiParTitre,
    createMacdRapideParTitre, enrichirComportemental,
)

THRESHOLD = 0.5


def preprocess_new(df: pd.DataFrame, scaler, feature_names: list) -> pd.DataFrame:
    """Apply feature engineering and scaling to new transactions."""
    df = df.copy()
    df["TRADEDATE"] = pd.to_datetime(df["TRADEDATE"], errors="coerce")
    df["day_of_week"] = df["TRADEDATE"].dt.dayofweek

    # Feature engineering
    df = create_cutoff_depasse(df)
    df = create_buyer_historical_failures(df)
    df = create_vendeur_historique_suspens(df)
    df = create_trader_daily_activity(df)
    df = create_global_exchange_frequency(df)
    df = create_ratio_instruction_vs_market(df)
    df = create_liquidite_volume_5j(df)
    df = createMacdRapideParTitre(df)
    df = enrichirComportemental(df)

    # One-hot encode
    ohe_cols = ["SECURITYID", "INSTRUMENTTYPE", "MARKETSEGMENT",
                "SETTLEMENTTYPE", "SETTLEMENTCYCLE"]
    ohe_present = [c for c in ohe_cols if c in df.columns]
    df = pd.get_dummies(df, columns=ohe_present, drop_first=True)

    # Keep only numeric
    df = df.select_dtypes(exclude=["object", "datetime64[ns]"])
    df = df.drop(columns=[c for c in df.columns
                          if "date" in c.lower() or "time" in c.lower()],
                 errors="ignore")

    # Align to training feature set
    for col in feature_names:
        if col not in df.columns:
            df[col] = 0
    df = df[feature_names]

    # Scale
    binary_cols = ["cutoff_dépassé", "day_of_week"]
    scale_cols = [c for c in feature_names if c not in binary_cols]
    df[scale_cols] = scaler.transform(df[scale_cols])

    return df


def run(input_path: str, output_path: str,
        model_path:   str = "models/xgb_model.pkl",
        scaler_path:  str = "models/scaler.pkl",
        features_path: str = "models/feature_names.pkl",
        threshold: float = THRESHOLD):

    model         = load(open(model_path, "rb"))
    scaler        = load(open(scaler_path, "rb"))
    feature_names = load(open(features_path, "rb"))

    raw = pd.read_csv(input_path, low_memory=False)
    print(f"[predict]  {len(raw):,} transactions loaded")

    X = preprocess_new(raw, scaler, feature_names)

    proba = model.predict_proba(X)[:, 1]
    preds = (proba >= threshold).astype(int)

    # Build output — keep key identifiers
    id_cols = ["TRADEREFERENCE", "TRADEDATE", "TRADERBPID",
               "CTRTRADERBPID", "SECURITYID", "TRADESTATUS"]
    out = raw[[c for c in id_cols if c in raw.columns]].copy()
    out["suspension_probability"] = proba.round(4)
    out["suspension_predicted"]   = preds
    out["risk_label"] = pd.Series(preds).map({1: "HIGH RISK", 0: "LOW RISK"}).values

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    out.to_csv(output_path, index=False)

    print(f"[predict]  flagged as high risk: {preds.sum()} / {len(preds)} ({preds.mean():.1%})")
    print(f"[predict]  results → {output_path}")
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Score new transactions for suspension risk")
    parser.add_argument("--input",    required=True,  help="Path to new transactions CSV")
    parser.add_argument("--output",   required=True,  help="Path to save predictions CSV")
    parser.add_argument("--model",    default="models/xgb_model.pkl")
    parser.add_argument("--scaler",   default="models/scaler.pkl")
    parser.add_argument("--features", default="models/feature_names.pkl")
    parser.add_argument("--threshold", type=float, default=THRESHOLD)
    args = parser.parse_args()

    run(args.input, args.output, args.model, args.scaler, args.features, args.threshold)
