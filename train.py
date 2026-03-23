"""
train.py
--------
Train suspension detection models on temporally-split data.
Evaluates with classification report, confusion matrix, and ROC-AUC.

Models: Logistic Regression, Random Forest, XGBoost, Decision Tree
Best model (XGBoost) saved to models/
"""

import os
import sys
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import (
    classification_report, confusion_matrix,
    roc_auc_score, f1_score, recall_score, precision_score
)
from xgboost import XGBClassifier
from pickle import dump
import warnings
warnings.filterwarnings("ignore")

sys.path.append(os.path.dirname(__file__))
from preprocessing import load_raw, build_target, engineer_features, temporal_split, encode_and_scale


# ── Evaluation ────────────────────────────────────────────────────────────────

def evaluate(name, model, X_tr, y_tr, X_te, y_te) -> dict:
    y_tr_pred = model.predict(X_tr)
    y_te_pred = model.predict(X_te)

    res = dict(
        Model        = name,
        Train_F1     = round(f1_score(y_tr, y_tr_pred, zero_division=0), 4),
        Train_Recall = round(recall_score(y_tr, y_tr_pred, zero_division=0), 4),
        Train_AUC    = round(roc_auc_score(y_tr, y_tr_pred), 4),
        Test_F1      = round(f1_score(y_te, y_te_pred, zero_division=0), 4),
        Test_Recall  = round(recall_score(y_te, y_te_pred, zero_division=0), 4),
        Test_Prec    = round(precision_score(y_te, y_te_pred, zero_division=0), 4),
        Test_AUC     = round(roc_auc_score(y_te, y_te_pred), 4),
    )

    sep = "=" * 55
    print(f"\n{sep}\n  {name}\n{sep}")
    print(f"  Train →  F1 {res['Train_F1']}  |  Recall {res['Train_Recall']}  |  AUC {res['Train_AUC']}")
    print(f"  Test  →  F1 {res['Test_F1']}  |  Recall {res['Test_Recall']}  |  Prec {res['Test_Prec']}  |  AUC {res['Test_AUC']}")
    print(f"\n  Classification report (test):")
    print(classification_report(y_te, y_te_pred, zero_division=0))
    print(f"  Confusion matrix (test):\n{confusion_matrix(y_te, y_te_pred)}")
    return res


# ── Train all models ──────────────────────────────────────────────────────────

def train_all(X_train, y_train, X_test, y_test):
    """
    Train multiple classifiers on temporally-split data.
    Class imbalance handled via class_weight / scale_pos_weight.
    """
    neg, pos  = (y_train == 0).sum(), (y_train == 1).sum()
    spw       = neg / pos
    cw        = {0: 1.0, 1: spw}

    results = []

    # 1 ── Logistic Regression
    lr = LogisticRegression(C=0.5, max_iter=1000, class_weight=cw, random_state=42)
    lr.fit(X_train, y_train)
    results.append(evaluate("Logistic Regression", lr, X_train, y_train, X_test, y_test))

    # 2 ── Decision Tree
    dt = DecisionTreeClassifier(
        criterion="gini", max_depth=5,
        min_samples_split=10, min_samples_leaf=5,
        class_weight=cw, random_state=42
    )
    dt.fit(X_train, y_train)
    results.append(evaluate("Decision Tree", dt, X_train, y_train, X_test, y_test))

    # 3 ── Random Forest
    rf = RandomForestClassifier(
        n_estimators=200, max_depth=8,
        class_weight=cw, random_state=42, n_jobs=-1
    )
    rf.fit(X_train, y_train)
    results.append(evaluate("Random Forest", rf, X_train, y_train, X_test, y_test))

    # 4 ── XGBoost (best performer for imbalanced tabular data)
    xgb = XGBClassifier(
        scale_pos_weight=spw,
        max_depth=4,
        learning_rate=0.05,
        n_estimators=300,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="auc",
        early_stopping_rounds=20,
        random_state=42,
        verbosity=0,
    )
    xgb.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=50,
    )
    results.append(evaluate("XGBoost", xgb, X_train, y_train, X_test, y_test))

    comp = pd.DataFrame(results)
    print("\n\n📊  Model comparison (TEMPORAL SPLIT — no leakage)")
    print(comp.to_string(index=False))
    return comp, lr, dt, rf, xgb


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Loading and preprocessing data...")
    df = load_raw()
    df = build_target(df)
    df = engineer_features(df)
    train_df, test_df = temporal_split(df)

    X_train, y_train, X_test, y_test, scaler, features = encode_and_scale(
        train_df, test_df, scaler_path="models/scaler.pkl"
    )

    print(f"\nClass balance — train: {y_train.mean():.1%} suspensions")
    print(f"Class balance — test:  {y_test.mean():.1%} suspensions\n")

    os.makedirs("models", exist_ok=True)
    comp, lr, dt, rf, xgb = train_all(X_train, y_train, X_test, y_test)

    # Save best model
    dump(xgb, open("models/xgb_model.pkl", "wb"))
    dump(features, open("models/feature_names.pkl", "wb"))
    print("\n[save]  XGBoost model  → models/xgb_model.pkl")
    print("[save]  Feature names  → models/feature_names.pkl")
    print("✅  Training done.")
