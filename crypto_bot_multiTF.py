import threading
import time
import requests
import pandas as pd
import numpy as np
import os
import json
import gspread
from datetime import datetime, timezone
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from flask import Flask

app = Flask(__name__)

# ======================================================
# ⚙️ CONFIGURATION
# ======================================================
TOTAL_CAPITAL = 10000 
RISK_PER_TRADE_PCT = 0.01 

CB_BASE = "https://api.exchange.coinbase.com"
PRODUCTS = {
    "BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD",
    "BNB": "BNB-USD", "ADA": "ADA-USD", "DOGE": "DOGE-USD",
    "AVAX": "AVAX-USD", "XRP": "XRP-USD", "LINK": "LINK-USD",
    "MATIC": "MATIC-USD", "DOT": "DOT-USD", "LTC": "LTC-USD",
    "ATOM": "ATOM-USD", "UNI": "UNI-USD", "NEAR": "NEAR-USD"
}

# ======================================================
# 🔐 AUTH
# ======================================================
print("🔐 Initialisation...", flush=True)
try:
    info = json.loads(os.getenv("GOOGLE_SERVICE_JSON"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
    print("✅ Auth Google OK", flush=True)
except Exception as e:
    print(f"❌ Erreur Auth: {e}", flush=True)

# ======================================================
# 🧠 MOTEUR
# ======================================================

def get_candles(product_id: str, granularity=3600):
    try:
        url = f"{CB_BASE}/products/{product_id}/candles"
        # On tente jusqu'à 3 fois si erreur
        for _ in range(3):
            r = requests.get(url, params={"granularity": granularity}, timeout=10)
            if r.status_code == 200:
                break
            if r.status_code == 429: # Trop rapide
                time.sleep(2)
            else:
                time.sleep(1)
        
        if r.status_code != 200:
            return None
            
        data = r.json()
        if not data: return None
        
        df = pd.DataFrame(data, columns=["ts", "low", "high", "open", "close", "volume"])
        cols = ["low", "high", "open", "close", "volume"]
        df[cols] = df[cols].astype(float)
        df["ts"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert(None)
        df = df.sort_values("ts").reset_index(drop=True)
        return df
    except Exception as e:
        print(f"⚠️ Err API {product_id}: {e}", flush=True)
        return None

def analyze_crypto(symbol, pid, btc_trend):
    # 1. Récupération 1H
    df_1h = get_candles(pid, 3600)
    time.sleep(0.6) # Pause obligatoire anti-ban
    
    # 2. Récupération 1D
    df_1d = get_candles(pid, 86400)
    time.sleep(0.6) # Pause obligatoire
    
    if df_1h is None or df_1d is None: 
        print(f"❌ Données manquantes pour {symbol}", flush=True)
        return None
    
    # Indicateurs
    def rsi(series):
        delta = series.diff()
        gain = np.where(delta > 0, delta, 0.0)
        loss = np.where(delta < 0, -delta, 0.0)
        rs = pd.Series(gain).rolling(14).mean() / pd.Series(loss).rolling(14).mean()
        return 100 - (100 / (1 + rs))

    df_1h["RSI"] = rsi(df_1h["close"])
    df_1h["ATR"] = (df_1h["high"] - df_1h["low"]).rolling(14).mean()
    df_1h["EMA50"] = df_1h["close"].ewm(span=50).mean()
    df_1d["EMA200"] = df_1d["close"].ewm(span=200).mean()
    
    # Bollinger Squeeze
    sma = df_1h["close"].rolling(20).mean()
    std = df_1h["close"].rolling(20).std()
    squeeze = ((sma + 2*std) - (sma - 2*std)) / sma < 0.10
    
    # Divergence
    price_lows = df_1h["close"].iloc[-10:]
    rsi_vals = df_1h["RSI"].iloc[-10:]
    div = price_lows.iloc[-1] < price_lows.iloc[0] and rsi_vals.iloc[-1] > rsi_vals.iloc[0]

    # Scoring
    price = df_1h["close"].iloc[-1]
    score = 0
    
    # Tendance Fond
    trend_d1 = "🟢 HAUSSE" if price > df_1d["EMA200"].iloc[-1] else "🔴 BAISSE"
    if trend_d1 == "🟢 HAUSSE": score += 30
    
    # Tendance Court terme
    if price > df_1h["EMA50"].iloc[-1]: score += 20
    
    # RSI
    cur_rsi = df_1h["RSI"].iloc[-1]
    if 40 < cur_rsi < 65: score += 20
    
    # Bonus
    if div: score += 15
    if squeeze: score += 15
    
    # Pénalité BTC
    if symbol != "BTC" and btc_trend == "BEAR":
        score = max(0, score - 30)

    # Signal Textuel
    signal = "⚪ NEUTRE"
    if score >= 75: signal = "🟢 ACHAT FORT"
    elif score >= 50: signal = "🟡 ACHAT FAIBLE"
    elif score <= 20: signal = "🔴 VENTE FORT"
    elif score < 40: signal = "🟠 VENTE"
    
    if trend_d1 == "🔴 BAISSE" and score > 60: signal = "⚠️ REBOND"

    # Position Sizing
    sl = price - (df_1h["ATR"].iloc[-1] * 2)
    tp = price + (df_1h["ATR"].iloc[-1] * 2.5)
    risk_usd = TOTAL_CAPITAL * RISK_PER_TRADE_PCT
    if btc_trend == "BEAR" and symbol != "BTC": risk_usd /= 2
    
    pos_usd = 0
    if (price - sl) > 0 and "ACHAT" in signal:
        pos_usd = min((risk_usd / (price - sl)) * price, TOTAL_CAPITAL * 0.15)

    return {
        "Crypto": symbol, "Prix": price, "Signal": signal, "Score": score,
        "Tendance_Fond": trend_d1, "Pos_USD": round(pos_usd), 
        "Stop_Loss": round(sl, 4), "Take_Profit": round(tp, 4),
        "RSI": round(cur_rsi, 1), "Divergence": "✅ OUI" if div else "",
        "Squeeze": "💥 PRÊT" if squeeze else ""
    }

def update_sheet():
    print("🧠 Démarrage Analyse...", flush=True)
    
    # 1. Check BTC
    btc_df = get_candles(PRODUCTS["BTC"], 86400)
    btc_trend = "NEUTRE"
    if btc_df is not None:
        ma200 = btc_df["close"].ewm(span=200).mean().iloc[-1]
        btc_trend = "BULL" if btc_df["close"].iloc[-1] > ma200 else "BEAR"
    print(f"👑 BTC Global: {btc_trend}", flush=True)

    results = []
    for sym, pid in PRODUCTS.items():
        print(f"👉 Scan {sym}...", flush=True)
        res = analyze_crypto(sym, pid, btc_trend)
        if res:
            results.append(res)
            print(f"   ✅ {sym}: {res['Signal']}")
        else:
            print(f"   ⚠️ Echec {sym}")

    if results:
        try:
            sh = gc.open_by_key(SHEET_ID)
            try: ws = sh.worksheet("MultiTF")
            except: ws = sh.add_worksheet("MultiTF", 100, 20)
            
            df = pd.DataFrame(results)
            cols = ["Crypto", "Prix", "Signal", "Score", "Tendance_Fond", 
                    "Pos_USD", "Stop_Loss", "Take_Profit", "RSI", 
                    "Divergence", "Squeeze"]
            df["Update"] = datetime.now(timezone.utc).strftime("%H:%M")
            
            ws.clear()
            set_with_dataframe(ws, df[cols + ["Update"]])
            print("🚀 Google Sheet mis à jour !", flush=True)
        except Exception as e:
            print(f"❌ Erreur Sheet: {e}", flush=True)
    else:
        print("❌ Aucun résultat trouvé (problème API ?)", flush=True)

def run_bot():
    update_sheet()
    while True:
        time.sleep(3600)
        update_sheet()

def keep_alive():
    url = os.getenv("RENDER_EXTERNAL_URL")
    if url:
        while True:
            time.sleep(600)
            try: requests.get(url)
            except: pass

@app.route("/")
def index(): return "Bot V4 Stable"

if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))