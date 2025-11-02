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
print("🔐 Initialisation des credentials Google...", flush=True)
try:
    info = json.loads(os.getenv("GOOGLE_SERVICE_JSON"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
    print("✅ Credentials Google OK", flush=True)
except Exception as e:
    print(f"❌ Erreur credentials Google : {e}", flush=True)
    raise SystemExit()

# ======================================================
# ⚙️ Fonctions CoinPaprika (plus tolérante que CoinGecko)
# ======================================================
def get_price_history(symbol_id):
    """Récupère les prix horaires récents sur CoinPaprika"""
    try:
        url = f"https://api.coinpaprika.com/v1/tickers/{symbol_id}/historical"
        params = {
            "start": (pd.Timestamp.utcnow() - pd.Timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "interval": "1h",
            "limit": 48
        }
        r = requests.get(url, params=params, timeout=15)
        print(f"🌐 [{symbol_id}] Status {r.status_code}", flush=True)
        if r.status_code != 200:
            print(f"⚠️ Erreur HTTP {r.status_code} pour {symbol_id}", flush=True)
            return None
        data = r.json()
        if not data:
            print(f"⚠️ Pas de data pour {symbol_id}", flush=True)
            return None
        df = pd.DataFrame(data)
        df.rename(columns={"price": "close", "timestamp": "timestamp"}, inplace=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df[["timestamp", "close"]]
    except Exception as e:
        print(f"⚠️ Erreur get_price_history({symbol_id}): {e}", flush=True)
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
    print("🧠 [DEBUG] Début update_sheet()", flush=True)
    try:
        sh = gc.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet("MarketData")
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title="MarketData", rows="100", cols="10")

        cryptos = {
            "btc-bitcoin": "BTC",
            "eth-ethereum": "ETH",
            "sol-solana": "SOL",
            "bnb-binance-coin": "BNB",
            "ada-cardano": "ADA",
            "doge-dogecoin": "DOGE",
            "avax-avalanche": "AVAX",
            "xrp-xrp": "XRP",
            "link-chainlink": "LINK",
            "matic-polygon": "MATIC"
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
            print(f"✅ {short} → {price}$ | RSI {round(rsi.iloc[-1], 2)} | {signal}", flush=True)
            time.sleep(1)  # petite pause pour politesse API

        if not rows:
            print("⚠️ Aucune donnée récupérée.", flush=True)
            return

        df_out = pd.DataFrame(rows, columns=["Crypto", "Dernier Prix", "RSI", "Signal"])
        ws.clear()
        set_with_dataframe(ws, df_out)
        print(f"✅ Feuille mise à jour à {time.strftime('%H:%M:%S')}.", flush=True)

    except Exception as e:
        print(f"❌ Erreur update_sheet() : {e}", flush=True)

# ======================================================
# 🔁 Threads
# ======================================================
def run_bot():
    print("🚀 Lancement du bot principal", flush=True)
    update_sheet()  # première mise à jour immédiate
    while True:
        print("⏳ Attente avant prochaine mise à jour (1h)...", flush=True)
        time.sleep(3600)
        update_sheet()

def keep_alive():
    url = os.getenv("RENDER_EXTERNAL_URL", "https://crypto-dashboard-8tn8.onrender.com")
    while True:
        try:
            requests.get(url, timeout=10)
            print("💤 Ping keep-alive envoyé.", flush=True)
        except Exception as e:
            print(f"⚠️ Erreur keep_alive : {e}", flush=True)
        time.sleep(600)

# ======================================================
# 🌐 Flask routes
# ======================================================
@app.route("/")
def home():
    return "✅ Crypto bot actif via CoinPaprika et Google Sheets."

@app.route("/run")
def manual_run():
    threading.Thread(target=update_sheet, daemon=True).start()
    return "🧠 Mise à jour manuelle lancée !"

# ======================================================
# 🧠 Lancement
# ======================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=port)
