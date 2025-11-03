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

# ===========================
# API Coinbase OHLC
# ===========================
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

# ===========================
# Calculs indicateurs
# ===========================
def compute_indicators(df):
    df = df.copy()
    delta = df["close"].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(14).mean()
    avg_loss = pd.Series(loss).rolling(14).mean()
    rs = avg_gain / avg_loss
    df["RSI14"] = 100 - (100 / (1 + rs))

    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()

    df["EMA20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["EMA50"] = df["close"].ewm(span=50, adjust=False).mean()

    df["BB_Mid"] = df["close"].rolling(20).mean()
    df["BB_Std"] = df["close"].rolling(20).std()
    df["BB_Upper"] = df["BB_Mid"] + 2 * df["BB_Std"]
    df["BB_Lower"] = df["BB_Mid"] - 2 * df["BB_Std"]

    df["Volume_Mean"] = df["volume"].rolling(20).mean()

    return df

# ===========================
# Analyse par symbole
# ===========================
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
        df = compute_indicators(df)
        last = df.iloc[-1]

        rsi = round(last["RSI14"], 2)
        trend = "Bull" if last["EMA20"] > last["EMA50"] else "Bear"

        macd_crossover = "❌ Aucun"
        if df["MACD"].iloc[-2] < df["MACD_Signal"].iloc[-2] and last["MACD"] > last["MACD_Signal"]:
            macd_crossover = "📈 Bullish crossover"
        elif df["MACD"].iloc[-2] > df["MACD_Signal"].iloc[-2] and last["MACD"] < last["MACD_Signal"]:
            macd_crossover = "📉 Bearish crossover"

        bb_position = "〰️ Neutre"
        if last["close"] > last["BB_Upper"]:
            bb_position = "⬆️ Surachat"
        elif last["close"] < last["BB_Lower"]:
            bb_position = "⬇️ Survente"

        volume_trend = "⬇️ Volume baissier"
        if last["volume"] > last["Volume_Mean"]:
            volume_trend = "⬆️ Volume haussier"

        results[label] = {
            "RSI": rsi,
            "Trend": trend,
            "MACD": macd_crossover,
            "Bollinger": bb_position,
            "Volume": volume_trend
        }

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
        "MACD_1h": results.get("1h", {}).get("MACD"),
        "Bollinger_1h": results.get("1h", {}).get("Bollinger"),
        "Volume_1h": results.get("1h", {}).get("Volume"),
        "RSI_6h": results.get("6h", {}).get("RSI"),
        "Trend_6h": results.get("6h", {}).get("Trend"),
        "MACD_6h": results.get("6h", {}).get("MACD"),
        "Bollinger_6h": results.get("6h", {}).get("Bollinger"),
        "Volume_6h": results.get("6h", {}).get("Volume"),
        "RSI_1d": results.get("1d", {}).get("RSI"),
        "Trend_1d": results.get("1d", {}).get("Trend"),
        "MACD_1d": results.get("1d", {}).get("MACD"),
        "Bollinger_1d": results.get("1d", {}).get("Bollinger"),
        "Volume_1d": results.get("1d", {}).get("Volume"),
        "Consensus": consensus,
        "LastUpdate": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    return out

# ===========================
# Mise à jour Google Sheet
# ===========================
def update_sheet():
    try:
        sh = gc.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet("MultiTF")
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title="MultiTF", rows="100", cols="20")

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

# ===========================
# Threads
# ===========================
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

# ===========================
# Flask
# ===========================
@app.route("/")
def home():
    return "✅ Crypto Bot Multi-Timeframe actif (1h / 6h / 1D)"

@app.route("/run")
def manual_run():
    threading.Thread(target=update_sheet, daemon=True).start()
    return "🧠 Mise à jour manuelle lancée !"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=port)
