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
# ⚙️ CONFIGURATION PRO
# ======================================================
TOTAL_CAPITAL = 10000 
RISK_PER_TRADE_PCT = 0.01 

CB_BASE = "https://api.exchange.coinbase.com"
# On analyse le BTC en premier (obligatoire)
PRODUCTS = {
    "BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD",
    "BNB": "BNB-USD", "ADA": "ADA-USD", "DOGE": "DOGE-USD",
    "AVAX": "AVAX-USD", "XRP": "XRP-USD", "LINK": "LINK-USD",
    "MATIC": "MATIC-USD", "DOT": "DOT-USD", "LTC": "LTC-USD",
    "ATOM": "ATOM-USD", "UNI": "UNI-USD", "NEAR": "NEAR-USD",
    "AAVE": "AAVE-USD", "ALGO": "ALGO-USD"
}

# ======================================================
# 🔐 AUTHENTIFICATION
# ======================================================
print("🔐 Initialisation...", flush=True)
try:
    info = json.loads(os.getenv("GOOGLE_SERVICE_JSON"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
    print("✅ Google Auth OK", flush=True)
except Exception as e:
    print(f"❌ Erreur Auth: {e}", flush=True)

# ======================================================
# 🧠 MOTEUR D'ANALYSE
# ======================================================

def get_candles(product_id: str, granularity=3600):
    try:
        url = f"{CB_BASE}/products/{product_id}/candles"
        r = requests.get(url, params={"granularity": granularity}, timeout=10)
        
        if r.status_code == 429:
            time.sleep(2)
            return None
        if r.status_code != 200:
            return None
            
        data = r.json()
        if not data: return None
        
        df = pd.DataFrame(data, columns=["ts", "low", "high", "open", "close", "volume"])
        
        # Conversion robuste
        cols = ["low", "high", "open", "close", "volume"]
        df[cols] = df[cols].astype(float)
        df["ts"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert(None)
        df = df.sort_values("ts").reset_index(drop=True)
        return df
    except Exception:
        return None

# --- Indicateurs ---
def rsi(series, period=14):
    delta = series.diff()
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    roll_up = pd.Series(gain).rolling(period).mean()
    roll_down = pd.Series(loss).rolling(period).mean()
    rs = roll_up / roll_down
    return 100 - (100 / (1 + rs))

def ema(series, span):
    return series.ewm(span=span, adjust=False).mean()

def atr(df, period=14):
    high_low = df["high"] - df["low"]
    high_close = np.abs(df["high"] - df["close"].shift())
    low_close = np.abs(df["low"] - df["close"].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    return ranges.max(axis=1).rolling(period).mean()

def bollinger_squeeze(df):
    sma = df["close"].rolling(20).mean()
    std = df["close"].rolling(20).std()
    upper = sma + (2 * std)
    lower = sma - (2 * std)
    if sma.iloc[-1] == 0: return False
    bandwidth = (upper - lower) / sma
    return bandwidth < 0.10 

def detect_divergence(df):
    if len(df) < 15: return False
    price = df["close"].iloc[-10:]
    rsi_vals = df["RSI14"].iloc[-10:]
    # Prix fait un plus bas, RSI fait un plus haut (Divergence Bull)
    if price.iloc[-1] < price.iloc[0] and rsi_vals.iloc[-1] > rsi_vals.iloc[0]:
        return True
    return False

# ======================================================
# 📊 ANALYSE INTELLIGENTE
# ======================================================

# Variable globale pour stocker la tendance du Roi Bitcoin
BTC_MARKET_STATE = "NEUTRE"

def analyze_market_context():
    """Analyse le BTC pour définir l'état global du marché."""
    global BTC_MARKET_STATE
    df_day = get_candles(PRODUCTS["BTC"], granularity=86400)
    if df_day is not None:
        df_day["EMA200"] = ema(df_day["close"], 200)
        price = df_day["close"].iloc[-1]
        ma200 = df_day["EMA200"].iloc[-1]
        
        if price > ma200:
            BTC_MARKET_STATE = "BULL"
        else:
            BTC_MARKET_STATE = "BEAR"
        print(f"👑 TENDANCE GLOBALE BITCOIN: {BTC_MARKET_STATE}", flush=True)

def analyze_crypto(symbol, pid):
    # Récupération Données
    df_1h = get_candles(pid, 3600)
    df_1d = get_candles(pid, 86400)
    
    if df_1h is None or df_1d is None: return None
    
    # Calculs
    df_1h["RSI14"] = rsi(df_1h["close"], 14)
    df_1h["ATR14"] = atr(df_1h, 14)
    df_1h["EMA50"] = ema(df_1h["close"], 50)
    df_1d["EMA200"] = ema(df_1d["close"], 200)
    
    # Indicateurs avancés
    squeeze = bollinger_squeeze(df_1h)
    div = detect_divergence(df_1h)
    
    current_price = df_1h["close"].iloc[-1]
    
    # --- LOGIQUE DE SCORE ---
    score = 0
    # 1. Tendance de fond (Daily)
    is_bull_d1 = current_price > df_1d["EMA200"].iloc[-1]
    if is_bull_d1: score += 30
    
    # 2. Tendance court terme (1H)
    is_bull_h1 = current_price > df_1h["EMA50"].iloc[-1]
    if is_bull_h1: score += 20
    
    # 3. RSI Propre
    rsi_val = df_1h["RSI14"].iloc[-1]
    if 40 < rsi_val < 65: score += 20
    
    # 4. Bonus "Sniper"
    if div: score += 15
    if squeeze: score += 15
    
    # --- FILTRE BTC (Le plus important) ---
    # Si BTC est Bearish, on pénalise lourdement le score des Alts
    if symbol != "BTC" and BTC_MARKET_STATE == "BEAR":
        score -= 30 # Pénalité de marché baissier
        if score < 0: score = 0

    # --- SIGNAL VISUEL ---
    trend_str = "🟢 HAUSSE" if is_bull_d1 else "🔴 BAISSE"
    
    signal = "⚪ NEUTRE"
    if score >= 75: signal = "🟢 ACHAT FORT"
    elif score >= 50: signal = "🟡 ACHAT FAIBLE"
    elif score <= 20: signal = "🔴 VENTE FORT"
    elif score < 40: signal = "🟠 VENTE"
    
    # Rebond technique en marché baissier (Risqué)
    if trend_str == "🔴 BAISSE" and score > 60:
        signal = "⚠️ REBOND (RISQUE)"

    # --- VISUALISATION ---
    rsi_str = f"{round(rsi_val, 0)}"
    if rsi_val > 70: rsi_str += " 🔥" # Surchauffe
    elif rsi_val < 30: rsi_str += " 🧊" # Survente

    div_str = "✅ OUI" if div else ""
    squeeze_str = "💥 PRÊT" if squeeze else ""

    # --- MONEY MANAGEMENT ---
    sl_dist = df_1h["ATR14"].iloc[-1] * 2.0
    sl_price = current_price - sl_dist
    tp_price = current_price + (sl_dist * 2.5) # Ratio 2.5
    
    # Taille de position
    risk_usd = TOTAL_CAPITAL * RISK_PER_TRADE_PCT
    if symbol != "BTC" and BTC_MARKET_STATE == "BEAR":
        risk_usd = risk_usd / 2 # On divise le risque par 2 si le marché est mauvais
        
    diff = current_price - sl_price
    pos_usd = 0
    if diff > 0:
        pos_tokens = risk_usd / diff
        pos_usd = pos_tokens * current_price
        
    # Limite max par sécurité
    pos_usd = min(pos_usd, TOTAL_CAPITAL * 0.15)
    
    # Pas de position d'achat si le signal est vente
    if "VENTE" in signal or "NEUTRE" in signal:
        pos_usd = 0

    return {
        "Crypto": symbol,
        "Prix": current_price,
        "Signal": signal,
        "Score": score,
        "Tendance_Fond": trend_str,
        "Pos_Suggérée_USD": round(pos_usd, 0),
        "Stop_Loss": round(sl_price, 4),
        "Take_Profit": round(tp_price, 4),
        "RSI": rsi_str,
        "Div_Bull": div_str,
        "Squeeze": squeeze_str
    }

# ======================================================
# 🔄 UPDATE SHEET
# ======================================================
def update_sheet():
    print("🧠 Scan marché...", flush=True)
    analyze_market_context() # D'abord le BTC !
    
    try:
        sh = gc.open_by_key(SHEET_ID)
        try: ws = sh.worksheet("MultiTF")
        except: ws = sh.add_worksheet("MultiTF", 100, 20)
            
        results = []
        for sym, pid in PRODUCTS.items():
            time.sleep(0.5) # Anti-ban
            data = analyze_crypto(sym, pid)
            if data:
                print(f"{sym}: {data['Signal']} (Score: {data['Score']})")
                results.append(data)

        if results:
            df = pd.DataFrame(results)
            # Colonnes propres
            cols = ["Crypto", "Prix", "Signal", "Score", "Tendance_Fond", 
                    "Pos_Suggérée_USD", "Stop_Loss", "Take_Profit", 
                    "RSI", "Div_Bull", "Squeeze"]
            
            # Ajout Timestamp
            df["Mise_à_jour"] = datetime.now(timezone.utc).strftime("%H:%M")
            
            ws.clear()
            set_with_dataframe(ws, df[cols + ["Mise_à_jour"]])
            print("✅ Sheet mis à jour (Version Visuelle)")

    except Exception as e:
        print(f"❌ Erreur: {e}")

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
def index(): return "Bot Trading Pro V3"

if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))