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
# ⚙️ API Coinbase – Données OHLC
# ======================================================
def get_candles(symbol_pair, granularity):
    url = f"https://api.exchange.coinbase.com/products/{symbol_pair}/candles"
    params = {"granularity": granularity}
    headers = {"User-Agent": "CryptoBot/1.0"}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        if r.status_code != 200:
            print(f"🌐 [{symbol_pair}] HTTP {r.status_code} ({granularity}s)", flush=True)
            return None
        data = r.json()
        if not data:
            return None
        df = pd.DataFrame(data, columns=["time", "low", "high", "open", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="s")
        df = df.sort_values("time")
        return df
    except Exception as e:
        print(f"⚠️ Erreur get_candles({symbol_pair}, {granularity}): {e}", flush=True)
        return None

# ======================================================
# 📈 RSI réel
# ======================================================
def calculate_RSI(prices, period=14):
    deltas = np.diff(prices)
    seed = deltas[:period]
    up = seed[seed >= 0].sum() / period
    down = -seed[seed < 0].sum() / period
    rs = up / down if down != 0 else 0
    rsi = np.zeros_like(prices)
    rsi[:period] = 100. - 100. / (1. + rs)

    for i in range(period, len(prices)):
        delta = deltas[i - 1]
        gain = max(delta, 0)
        loss = -min(delta, 0)
        up = (up * (period - 1) + gain) / period
        down = (down * (period - 1) + loss) / period
        rs = up / down if down != 0 else 0
        rsi[i] = 100. - 100. / (1. + rs)

    return round(rsi[-1], 2)

# ======================================================
# 📈 EMA pour tendance
# ======================================================
def get_trend(df):
    ema20 = df["close"].ewm(span=20, adjust=False).mean()
    ema50 = df["close"].ewm(span=50, adjust=False).mean()
    return "Bull" if ema20.iloc[-1] > ema50.iloc[-1] else "Bear"

# ======================================================
# 🧮 Analyse multi-période
# ======================================================
def analyze_symbol(symbol_pair):
    periods = {
        "1h": 3600,
        "6h": 21600,
        "1d": 86400
    }
    results = {}

    for label, gran in periods.items():
        df = get_candles(symbol_pair, gran)
        if df is None or df.empty:
            continue
        closes = df["close"].values
        if len(closes) < 15:
            continue
        rsi = calculate_RSI(closes)
        trend = get_trend(df)
        results[label] = {"RSI": rsi, "Trend": trend}

    if not results:
        return None

    trends = [v["Trend"] for v in results.values()]
    bulls = trends.count("Bull")
    bears = trends.count("Bear")
    consensus = (
        "🟢 Achat fort" if bulls >= 2 else
        "🔴 Vente forte" if bears >= 2 else
        "⚪ Neutre"
    )

    out = {
        "Crypto": symbol_pair.split("-")[0],
        "RSI_1h": results.get("1h", {}).get("RSI"),
        "Trend_1h": results.get("1h", {}).get("Trend"),
        "RSI_6h": results.get("6h", {}).get("RSI"),
        "Trend_6h": results.get("6h", {}).get("Trend"),
        "RSI_1d": results.get("1d", {}).get("RSI"),
        "Trend_1d": results.get("1d", {}).get("Trend"),
        "Consensus": consensus,
        "LastUpdate": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    return out

# ======================================================
# 📊 Mise à jour Google Sheets
# ======================================================
def update_sheet():
    try:
        sh = gc.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet("MultiTF")
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title="MultiTF", rows="100", cols="15")

        cryptos = [
            "BTC-USD", "ETH-USD", "SOL-USD", "BNB-USD",
            "ADA-USD", "DOGE-USD", "AVAX-USD", "XRP-USD",
            "LINK-USD", "MATIC-USD"
        ]

        rows = []
        for pair in cryptos:
            res = analyze_symbol(pair)
            if res:
                rows.append(res)
                print(f"✅ {res['Crypto']} → {res['Consensus']}", flush=True)
            time.sleep(2)

        if not rows:
            print("⚠️ Aucune donnée récupérée", flush=True)
            return

        df_out = pd.DataFrame(rows)
        ws.clear()
        set_with_dataframe(ws, df_out)
        print("✅ Feuille 'MultiTF' mise à jour !", flush=True)

    except Exception as e:
        print(f"❌ Erreur update_sheet() : {e}", flush=True)

# ======================================================
# 🔁 Threads
# ======================================================
def run_bot():
    print("🚀 Lancement du bot Multi-Timeframe", flush=True)
    update_sheet()
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
# 🌐 Flask
# ======================================================
@app.route("/")
def home():
    return "✅ Crypto Bot Multi-Timeframe actif (1h / 6h / 1D)"

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
