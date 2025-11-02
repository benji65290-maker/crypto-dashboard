import requests
import pandas as pd
import numpy as np
import os
import json
import time
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from datetime import datetime
from flask import Flask # pyright: ignore[reportMissingImports]
import threading

# ======================================================
# 🌐 Flask - serveur "keep-alive" pour Render (plan gratuit)
# ======================================================
app = Flask(__name__)

@app.route('/')
def home():
    return "✅ Crypto Bot actif sur Render (Flask keep-alive)"

# ======================================================
# 🔐 Configuration Google Sheets via variables Render
# ======================================================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

info = json.loads(os.getenv("GOOGLE_SERVICE_JSON"))
creds = Credentials.from_service_account_info(info, scopes=SCOPES)
gc = gspread.authorize(creds)
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

# ======================================================
# ⚙️ Fonctions Binance + RSI + MAJ Google Sheet
# ======================================================

def get_klines(symbol, interval="1h", limit=100):
    """Récupère les données de Binance, avec gestion d'erreurs"""
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Vérifie si Binance renvoie une erreur
        if isinstance(data, dict) and "code" in data:
            print(f"⚠️ Binance error for {symbol}: {data}")
            return pd.DataFrame()

        df = pd.DataFrame(data, columns=[
            "timestamp", "open", "high", "low", "close", "volume",
            "_", "__", "___", "____", "_____", "______"
        ])
        df["close"] = df["close"].astype(float)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        return df[["timestamp", "close"]]

    except Exception as e:
        print(f"❌ Erreur récupération {symbol}: {e}")
        return pd.DataFrame()


def compute_RSI(series, period=14):
    """Calcule l'indicateur RSI"""
    delta = series.diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(period).mean()
    avg_loss = pd.Series(loss).rolling(period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def update_sheet():
    """Mets à jour la feuille Google avec les données crypto"""
    print("🚀 Démarrage de la mise à jour des données crypto...")
    sh = gc.open_by_key(SHEET_ID)

    # Vérifie ou crée l'onglet MarketData
    try:
        ws = sh.worksheet("MarketData")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title="MarketData", rows="100", cols="10")

    cryptos = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "LINKUSDT", "MATICUSDT"]
    rows = []

    for symbol in cryptos:
        df = get_klines(symbol)
        if df.empty:
            print(f"⚠️ Aucune donnée récupérée pour {symbol}")
            continue

        rsi = compute_RSI(df["close"])
        price = df["close"].iloc[-1]
        rsi_value = round(rsi.iloc[-1], 2)
        rows.append([symbol, price, rsi_value])
        print(f"✅ {symbol} → {price}$ | RSI: {rsi_value}")

    if rows:
        df_out = pd.DataFrame(rows, columns=["Crypto", "Dernier Prix", "RSI"])
        ws.clear()
        set_with_dataframe(ws, df_out)
        print(f"✅ Feuille mise à jour à {datetime.now().strftime('%H:%M:%S')}")
    else:
        print("⚠️ Aucune donnée valide à écrire dans Google Sheets.")


def run_bot():
    """Boucle principale d'actualisation du bot"""
    while True:
        try:
            update_sheet()
        except Exception as e:
            print(f"❌ Erreur pendant update_sheet: {e}")
        time.sleep(3600)  # Actualisation toutes les heures


# ======================================================
# 🚀 Point d'entrée principal
# ======================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))  # 🔧 Render fournit automatiquement cette variable
    threading.Thread(target=run_bot).start()
    app.run(host="0.0.0.0", port=port)


