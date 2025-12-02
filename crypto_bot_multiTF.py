import threading
import time
import pandas as pd
import numpy as np
import os
import json
import gspread
import ccxt
import pytz
from datetime import datetime
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from flask import Flask

app = Flask(__name__)

# ======================================================
# ⚙️ CONFIGURATION
# ======================================================
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

TOTAL_CAPITAL = 10000 
RISK_PER_TRADE_PCT = 0.01 
UPDATE_FREQUENCY = 900  # Mise à jour toutes les 15 minutes (en secondes)

WATCHLIST = [
    "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "ADA/USDT", 
    "DOGE/USDT", "AVAX/USDT", "XRP/USDT", "LINK/USDT", "MATIC/USDT", 
    "DOT/USDT", "LTC/USDT", "ATOM/USDT", "NEAR/USDT", "PEPE/USDT",
    "SHIB/USDT", "TRX/USDT", "FET/USDT", "RENDER/USDT", "INJ/USDT"
]

# ======================================================
# 🔐 CONNEXIONS
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
    print(f"❌ Erreur Google: {e}", flush=True)

exchange = None
try:
    if BINANCE_API_KEY and BINANCE_SECRET_KEY:
        exchange = ccxt.binance({
            'apiKey': BINANCE_API_KEY,
            'secret': BINANCE_SECRET_KEY,
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        print("✅ Binance Client Configured", flush=True)
    else:
        print("⚠️ Mode Simulation", flush=True)
except Exception as e:
    print(f"❌ Erreur Config Binance: {e}", flush=True)

# ======================================================
# 🧠 INDICATEURS TECHNIQUES
# ======================================================

def calculate_adx(df, period=14):
    plus_dm = df['high'].diff()
    minus_dm = df['low'].diff()
    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm > 0] = 0
    
    tr1 = pd.DataFrame(df['high'] - df['low'])
    tr2 = pd.DataFrame(abs(df['high'] - df['close'].shift(1)))
    tr3 = pd.DataFrame(abs(df['low'] - df['close'].shift(1)))
    frames = [tr1, tr2, tr3]
    tr = pd.concat(frames, axis=1, join='inner').max(axis=1)
    
    atr = tr.rolling(period).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1/period).mean() / atr)
    minus_di = 100 * (abs(minus_dm).ewm(alpha=1/period).mean() / atr)
    dx = (abs(plus_di - minus_di) / abs(plus_di + minus_di)) * 100
    adx = dx.rolling(period).mean()
    return adx, atr

def get_live_price(symbol):
    """Récupère le prix EXACT du moment (Ticker)"""
    try:
        ticker = exchange.fetch_ticker(symbol)
        return ticker['last']
    except Exception as e:
        print(f"⚠️ Erreur Ticker {symbol}: {e}")
        return None

def get_binance_history(symbol, timeframe, limit=100):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        return df
    except Exception as e:
        # print(f"⚠️ Erreur History {symbol}: {e}")
        return None

def get_user_balance():
    positions = {}
    if not exchange: return positions
    try:
        balance = exchange.fetch_balance()
        items = balance['total']
        for asset, amount in items.items():
            if amount > 0:
                pair = f"{asset}/USDT"
                positions[pair] = amount
        return positions
    except Exception as e:
        print(f"⚠️ Erreur Balance: {e}")
        return {}

def analyze_market_and_portfolio():
    print("🧠 Démarrage Analyse Complète...", flush=True)
    
    # 1. Portefeuille
    my_positions = get_user_balance()
    
    results = []
    
    # 2. Tendance BTC
    btc_df = get_binance_history("BTC/USDT", "1d", limit=200)
    market_trend = "NEUTRE"
    if btc_df is not None:
        ma200 = btc_df['close'].ewm(span=200).mean().iloc[-1]
        market_trend = "BULL" if btc_df['close'].iloc[-1] > ma200 else "BEAR"
    
    print(f"👑 Tendance BTC: {market_trend}", flush=True)

    # 3. Scan Watchlist
    for symbol in WATCHLIST:
        # Données Historiques (pour indicateurs)
        df_1h = get_binance_history(symbol, "1h")
        df_1d = get_binance_history(symbol, "1d")
        
        # Donnée Temps Réel (pour affichage prix)
        live_price = get_live_price(symbol)
        
        if df_1h is None or df_1d is None or live_price is None: 
            continue
        
        # --- CALCULS ---
        # RSI
        delta = df_1h['close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        rs = gain.rolling(14).mean() / loss.rolling(14).mean()
        rsi = 100 - (100 / (1 + rs))
        current_rsi = rsi.iloc[-1]
        
        # ADX & ATR
        df_1h['ADX'], df_1h['ATR'] = calculate_adx(df_1h)
        current_adx = df_1h['ADX'].iloc[-1]
        current_atr = df_1h['ATR'].iloc[-1]
        
        # Volume
        vol_mean = df_1h['volume'].rolling(20).mean().iloc[-1]
        current_vol = df_1h['volume'].iloc[-1]
        vol_ratio = current_vol / vol_mean if vol_mean > 0 else 0
        has_volume = vol_ratio > 0.8
        
        # Moyennes Mobiles (Tendances)
        ema200_1d = df_1d['close'].ewm(span=200).mean().iloc[-1] # Long terme
        ema50_1h = df_1h['close'].ewm(span=50).mean().iloc[-1]   # Court terme
        
        trend_fond = "🟢 HAUSSE" if live_price > ema200_1d else "🔴 BAISSE"
        
        # --- SCORING ---
        score = 0
        if trend_fond == "🟢 HAUSSE": score += 30
        if live_price > ema50_1h: score += 20
        if 40 < current_rsi < 65: score += 15 
        if current_adx > 20: score += 15
        if has_volume: score += 20
        
        # Pénalité Bear Market
        if market_trend == "BEAR" and symbol != "BTC/USDT":
            score = max(0, score - 30)
            
        # --- CONSEIL & FORMATAGE ---
        amount_owned = my_positions.get(symbol, 0)
        value_owned = amount_owned * live_price
        
        advice = "⚪ NEUTRE"
        action = ""
        
        if value_owned > 10: # J'en possède
            if trend_fond == "🔴 BAISSE" and score < 40:
                advice = "🚨 VENDRE"
                action = "URGENT"
            elif score > 70:
                advice = "🟢 GARDER"
            else:
                advice = "🟠 SURVEILLER"
        else: # Je n'en ai pas
            if market_trend == "BEAR":
                advice = "⛔ ATTENDRE"
            elif score > 80 and current_adx > 25:
                advice = "🔥 ACHAT FORT"
            elif score > 60:
                advice = "✅ ACHAT"

        # --- OUTPUT COMPLET ---
        results.append({
            "Crypto": symbol,
            "Prix": f"${live_price:.4f}", # Format $
            "Mon_Bag": f"${value_owned:.1f}" if value_owned > 10 else "-", # Format $
            "Conseil": advice,
            "Action": action,
            "Score": score,
            # Les Indicateurs pour comprendre
            "Trend_D1": trend_fond,
            "EMA_Long": f"${ema200_1d:.4f}",
            "EMA_Court": f"${ema50_1h:.4f}",
            "RSI": round(current_rsi, 1),
            "ADX": round(current_adx, 1),
            "Vol_Ratio": f"{round(vol_ratio, 2)}x", # 1.5x le volume habituel
            "ATR": f"${current_atr:.4f}" # Utile pour Stop Loss
        })

    # 4. Ecriture Sheet
    if results:
        try:
            sh = gc.open_by_key(SHEET_ID)
            try: ws = sh.worksheet("PortfolioManager")
            except: ws = sh.add_worksheet("PortfolioManager", 100, 20)
            
            df = pd.DataFrame(results)
            # Tri par Action (Urgent en premier) puis Score
            df = df.sort_values(by=["Action", "Score"], ascending=[False, False])
            
            # Heure Paris
            paris_tz = pytz.timezone('Europe/Paris')
            now_paris = datetime.now(paris_tz).strftime("%H:%M:%S")
            df["Update_Paris"] = now_paris
            
            ws.clear()
            set_with_dataframe(ws, df)
            print(f"🚀 Sheet mis à jour à {now_paris}", flush=True)
        except Exception as e:
            print(f"❌ Erreur Sheet: {e}", flush=True)

# ======================================================
# 🔄 BOUCLE
# ======================================================
def run_bot():
    print("⏳ Démarrage...", flush=True)
    analyze_market_and_portfolio()
    while True:
        print(f"💤 Pause {UPDATE_FREQUENCY}s...", flush=True)
        time.sleep(UPDATE_FREQUENCY)
        analyze_market_and_portfolio()

def keep_alive():
    url = os.getenv("RENDER_EXTERNAL_URL")
    if url:
        while True:
            time.sleep(600)
            try: requests.get(url)
            except: pass

@app.route("/")
def index(): return "Bot V7 Live Prices Active"

if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))