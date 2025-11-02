import threading
import time
import requests
import pandas as pd
import numpy as np
import os
import json
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from flask import Flask

app = Flask(__name__)

# ======================================================
# 🔐 Authentification Google Sheets
# ======================================================
print("🔐 Initialisation des credentials Google...")
try:
    info = json.loads(os.getenv("GOOGLE_SERVICE_JSON"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
except Exception as e:
    print(f"❌ Erreur credentials Google : {e}")
    raise SystemExit()

# ======================================================
# ⚙️ Fonctions de marché (CoinGecko)
# ======================================================
def get_price_history(symbol_id):
    """Récupère les prix horaires récents sur CoinGecko"""
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{symbol_id}/market_chart"
        params = {"vs_currency": "usd", "days": 2, "interval": "hourly"}
        r = requests.get(url, params=params, timeout=10)
        data = r.json().get("prices", [])
        if not data:
            print(f"⚠️ Pas de data pour {symbol_id}")
            return None
        df = pd.DataFrame(data, columns=["timestamp", "close"])
        df["close"] = df["close"].astype(float)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        return df
    except Exception as e:
        print(f"⚠️ Erreur get_price_history({symbol_id}): {e}")
        return None

def compute_RSI(series, period=14):
    delta = series.diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(period).mean()
    avg_loss = pd.Series(loss).rolling(period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def signal_RSI(rsi):
    last = rsi.iloc[-1]
    if last < 30:
        return "🟢 Achat potentiel"
    elif last > 70:
        return "🔴 Vente potentielle"
    else:
        return "⚪ Neutre"

# ======================================================
# 📊 Mise à jour Google Sheets
# ======================================================
def update_sheet():
    try:
        sh = gc.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet("MarketData")
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title="MarketData", rows="100", cols="10")

        cryptos = {
            "bitcoin": "BTC",
            "ethereum": "ETH",
            "solana": "SOL",
            "binancecoin": "BNB",
            "cardano": "ADA",
            "dogecoin": "DOGE",
            "avalanche-2": "AVAX",
            "ripple": "XRP",
            "chainlink": "LINK",
            "matic-network": "MATIC"
        }

        rows = []
        for symbol_id, short in cryptos.items():
            df = get_price_history(symbol_id)
            if df is None or df.empty:
                continue
            rsi = compute_RSI(df["close"])
            signal = signal_RSI(rsi)
            price = df["close"].iloc[-1]
            rows.append([short, round(price, 3), round(rsi.iloc[-1], 2), signal])
            print(f"✅ {short} → {price}$ | RSI {round(rsi.iloc[-1], 2)} | {signal}")

        if not rows:
            print("⚠️ Aucune donnée récupérée, aucune ligne écrite.")
            return

        df_out = pd.DataFrame(rows, columns=["Crypto", "Dernier Prix", "RSI", "Signal"])
        ws.clear()
        set_with_dataframe(ws, df_out)
        print(f"✅ Feuille mise à jour à {time.strftime('%H:%M:%S')}.")

    except Exception as e:
        print(f"❌ Erreur update_sheet() : {e}")

# ======================================================
# 🔁 Boucle principale + Keep alive
# ======================================================
def run_bot():
    print("🚀 Démarrage de la mise à jour des données crypto (CoinGecko)...")
    while True:
        update_sheet()
        time.sleep(3600)  # toutes les heures

def keep_alive():
    url = os.getenv("RENDER_EXTERNAL_URL", "https://crypto-dashboard-8tn8.onrender.com")
    while True:
        try:
            requests.get(url, timeout=10)
            print("💤 Ping keep-alive envoyé à Render.")
        except Exception as e:
            print(f"⚠️ Erreur keep_alive : {e}")
        time.sleep(600)  # toutes les 10 min

# ======================================================
# 🌐 Flask route
# ======================================================
@app.route("/")
def home():
    return "✅ Crypto bot actif via CoinGecko et connecté à Google Sheets."

# ======================================================
# 🧠 Lancement
# ======================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=port)
