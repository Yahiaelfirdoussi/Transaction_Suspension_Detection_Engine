import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import MACD
import numpy as np

def create_cutoff_depasse(df):
    df["cutoff_dépassé"] = df["TRADETIME"].apply(
    lambda x: 1 if int(x.split(":")[0]) > 15 or (int(x.split(":")[0]) == 15 and int(x.split(":")[1]) > 0) else 0
)
    return df

def create_buyer_historical_failures(df):
    # Ensure TRADEDATE is datetime
    df["TRADEDATE"] = pd.to_datetime(df["TRADEDATE"])

    # Buyer-side failure statuses
    buyer_fail_statuses = ['RFSL', 'SFSL']

    # Only XRVP: TRADERBPID is the buyer
    df_buyers = df[df["SETTLEMENTTYPE"] == "XRVP"].copy()
    df_buyers = df_buyers.sort_values("TRADEDATE").reset_index()

    # Initialize column
    df["buyer_historical_suspens"] = 0.0

    for buyer_id in df_buyers["TRADERBPID"].unique():
        buyer_data = df_buyers[df_buyers["TRADERBPID"] == buyer_id]

        for i, row in buyer_data.iterrows():
            current_date = row["TRADEDATE"]
            idx = row["index"]

            # Historical window
            window_data = buyer_data[
                (buyer_data["TRADEDATE"] < current_date) &
                (buyer_data["TRADEDATE"] >= current_date - pd.Timedelta(days=5))
            ]

            total_trades = len(window_data)
            failed_trades = len(window_data[
                (window_data["FAIL_STATUS"] == 1) &
                (window_data["TRADESTATUS"].isin(buyer_fail_statuses))
            ])

            if total_trades > 0:
                df.at[idx, "buyer_historical_suspens"] = failed_trades / total_trades

    return df

def create_vendeur_historique_suspens(df):
    # S'assurer que les dates sont bien en datetime
    df["TRADEDATE"] = pd.to_datetime(df["TRADEDATE"])

    # Initialiser la colonne des suspens historiques vendeur
    df["vendeur_historique_suspens"] = 0.0

    # Ne prendre que les lignes où TRADERBPID est le vendeur (cas XDVP)
    df_vendeurs = df[df["SETTLEMENTTYPE"] == "XDVP"].copy()
    df_vendeurs = df_vendeurs.sort_values("TRADEDATE").reset_index()

    # Liste des vendeurs uniques
    vendeurs_uniques = df_vendeurs["TRADERBPID"].unique()

    for vendeur in vendeurs_uniques:
        vendeur_data = df_vendeurs[df_vendeurs["TRADERBPID"] == vendeur]

        for i, row in vendeur_data.iterrows():
            current_date = row["TRADEDATE"]
            idx = row["index"]

            # Fenêtre glissante de 30 jours avant la transaction
            history = vendeur_data[
                (vendeur_data["TRADEDATE"] < current_date) &
                (vendeur_data["TRADEDATE"] >= current_date - pd.Timedelta(days=5))
            ]

            total_trades = len(history)
            failed_trades = len(history[
                (history["FAIL_STATUS"] == 1) &
                (history["TRADESTATUS"].isin(["RFSL", "SFSL"]))  # suspens titres
            ])

            if total_trades > 0:
                df.at[idx, "vendeur_historique_suspens"] = failed_trades / total_trades

    return df

def create_trader_daily_activity(df):
    df["TRADEDATE"] = pd.to_datetime(df["TRADEDATE"])

    # Group by TRADERBPID and TRADEDATE, count occurrences
    trader_activity = (
        df.groupby(["TRADERBPID", "TRADEDATE"])
        .size()
        .rename("daily_activity")
    )

    # Map back the activity count into the dataframe
    df["daily_activity"] = df[["TRADERBPID", "TRADEDATE"]].apply(
        lambda row: trader_activity.get((row["TRADERBPID"], row["TRADEDATE"]), 0), axis=1
    )

    return df

def create_global_exchange_frequency(df):
    df["TRADEDATE"] = pd.to_datetime(df["TRADEDATE"])

    # Normalize to remove time part
    df["TRADEDATE_only"] = df["TRADEDATE"].dt.normalize()

    # Total number of unique days
    total_days = df["TRADEDATE_only"].nunique()

    # Days each SECURITYID appears
    freq_dict = df.groupby("SECURITYID")["TRADEDATE_only"].nunique() / total_days

    # Assign directly without merge
    df["global_exchange_frequency"] = df["SECURITYID"].map(freq_dict)
    

    return df

def create_ratio_instruction_vs_market(df):
    df["TRADEDATE"] = pd.to_datetime(df["TRADEDATE"])
    df["ratio_instruction_vs_market"] = df["SETTLEMENTAMOUNT"] / df["volume_globale"]
    return df

def create_liquidite_volume_5j(df):
    # Conversion de la date
    df["TRADEDATE"] = pd.to_datetime(df["TRADEDATE"], dayfirst=True)

    # Tri par ISIN et date
    df = df.sort_values(["SECURITYID", "TRADEDATE"])

    # Moyenne glissante 5 jours (hors jour courant)
    df["liquidité_volume_5j"] = (
        df.groupby("SECURITYID")["SETTLEMENTAMOUNT"]
        .transform(lambda x: x.shift(1).rolling(window=5, min_periods=1).mean())
    )

    # Moyenne globale sur toute la colonne (hors NaN)
    moyenne_globale = df["liquidité_volume_5j"].mean()

    # Remplissage des NaN
    df["liquidité_volume_5j"] = df["liquidité_volume_5j"].ffill()
    df["liquidité_volume_5j"] = df["liquidité_volume_5j"].fillna(moyenne_globale)

    # Colonne constante avec la moyenne globale (pour comparaison éventuelle)
    df["moyenne_volume_5j"] = moyenne_globale

    return df

def calculateRsiParTitre(df, window=5, priceColumn="TRADEPRICE", isinColumn="SECURITYID"):
    # Ne prendre que les lignes d'achat
    df_filtered = df[df["SETTLEMENTTYPE"] == "XRVP"].copy()
    df_filtered = df_filtered.sort_values(by=[isinColumn, "TRADEDATE"])

    # Fonction RSI
    def computeRsi(group):
        delta = group[priceColumn].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avgGain = gain.rolling(window=window, min_periods=1).mean()
        avgLoss = loss.rolling(window=window, min_periods=1).mean()
        rs = avgGain / (avgLoss + 1e-10)
        return 100 - (100 / (1 + rs))

    # Calcul RSI pour chaque titre
    df_filtered[f"RSI_{window}"] = df_filtered.groupby(isinColumn, group_keys=False).apply(computeRsi)

    # Créer la colonne vide dans le df principal
    df[f"RSI_{window}"] = np.nan

    # Remplir seulement les lignes d’achat
    mask = df["SETTLEMENTTYPE"] == "XRVP"
    df.loc[mask, f"RSI_{window}"] = df_filtered[f"RSI_{window}"].values

    return df

from ta.trend import MACD

def createMacdRapideParTitre(df,priceColumn="TRADEPRICE", isinColumn="SECURITYID", slow=5, fast=3, signal=2):
    df = df.sort_values(by=[isinColumn, "TRADEDATE"]).copy()
    df["MACD_diff"] = 0.0

    for isin in df[isinColumn].unique():
        mask = df[isinColumn] == isin
        macd = MACD(close=df.loc[mask, priceColumn],
                    window_slow=slow,
                    window_fast=fast,
                    window_sign=signal)
        df.loc[mask, "MACD_diff"] = macd.macd_diff().fillna(0)

    return df

def enrichirComportemental(df):
    df["TRADEDATE"] = pd.to_datetime(df["TRADEDATE"], dayfirst=True)

    # Taux de changement (TRADEPRICE et TRADEQUANTITY)
    df = df.sort_values("TRADEDATE")
    df["taux_changement_prix"] = df["TRADEPRICE"].pct_change().fillna(0)
    df["taux_changement_volume"] = df["TRADEQUANTITY"].pct_change().fillna(0)

    # Transformation logarithmique
    df["log_tradeprice"] = np.log1p(df["TRADEPRICE"])

    # Ratio volume / prix
    df["ratio_volume_prix"] = df["TRADEQUANTITY"] / df["TRADEPRICE"]
    df["ratio_volume_prix"] = df["ratio_volume_prix"].replace([np.inf, -np.inf], np.nan).fillna(0)
    return df
