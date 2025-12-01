import threading
import time
import pandas as pd
import numpy as np
import os
import json
import gspread
import ccxt  # Nouvelle librairie Pro
from datetime import datetime, timezone
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from flask import Flask

app = Flask(__name__)

# ======================================================
# ⚙️ CONFIGURATION BINANCE & RISQUE
# ======================================================
# Récupération des clés depuis les variables d'environnement Render
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

TOTAL_CAPITAL = 10000 
RISK_PER_TRADE_PCT = 0.01 

# Liste des cryptos à surveiller (Symboles Binance)
WATCHLIST = [
    "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "ADA/USDT", 
    "DOGE/USDT", "AVAX/USDT", "XRP/USDT", "LINK/USDT", "MATIC/USDT", 
    "DOT/USDT", "LTC/USDT", "ATOM/USDT", "NEAR/USDT", "PEPE/USDT"
]

# ======================================================
# 🔐 CONNEXIONS (Google + Binance)
# ======================================================
print("🔐 Initialisation...", flush=True)

# 1. Google Sheets
try:
    info = json.loads(os.getenv("GOOGLE_SERVICE_JSON"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
    print("✅ Google Auth OK", flush=True)
except Exception as e:
    print(f"❌ Erreur Google: {e}", flush=True)

# 2. Binance
exchange = None
try:
    if BINANCE_API_KEY and BINANCE_SECRET_KEY:
        exchange = ccxt.binance({
            'apiKey': BINANCE_API_KEY,
            'secret': BINANCE_SECRET_KEY,
            'enableRateLimit': True, # Indispensable pour éviter le ban
            'options': {'defaultType': 'spot'}
        })
        print("✅ Binance Client Configured", flush=True)
    else:
        print("⚠️ Pas de clés Binance trouvées (Mode Simulation)", flush=True)
except Exception as e:
    print(f"❌ Erreur Config Binance: {e}", flush=True)

# ======================================================
# 🧠 MOTEUR D'ANALYSE
# ======================================================

def get_binance_data(symbol, timeframe, limit=100):
    """Récupère les bougies directement depuis Binance via CCXT"""
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        return df
    except Exception as e:
        print(f"⚠️ Erreur data {symbol}: {e}")
        return None

def get_user_balance():
    """Récupère les positions actuelles de l'utilisateur"""
    positions = {}
    if not exchange: return positions
    try:
        balance = exchange.fetch_balance()
        # On ne garde que ce qui n'est pas zéro
        items = balance['total']
        for asset, amount in items.items():
            if amount > 0: # On filtre les poussières
                # Convertir l'asset (ex: SOL) en paire (SOL/USDT) pour matcher la watchlist
                pair = f"{asset}/USDT"
                positions[pair] = amount
        return positions
    except Exception as e:
        print(f"⚠️ Erreur Balance: {e}")
        return {}

def analyze_market_and_portfolio():
    print("🧠 Démarrage Analyse Complète...", flush=True)
    
    # 1. Récupérer le portefeuille réel
    my_positions = get_user_balance()
    if my_positions:
        print(f"💰 Portefeuille détecté: {my_positions}", flush=True)
    
    results = []
    
    # 2. Vérifier la tendance BTC (King Maker)
    btc_df = get_binance_data("BTC/USDT", "1d", limit=200)
    market_trend = "NEUTRE"
    if btc_df is not None:
        ma200 = btc_df['close'].ewm(span=200).mean().iloc[-1]
        market_trend = "BULL" if btc_df['close'].iloc[-1] > ma200 else "BEAR"
    
    print(f"👑 Tendance Marché (BTC): {market_trend}", flush=True)

    # 3. Analyser chaque crypto
    for symbol in WATCHLIST:
        print(f"👉 Scan {symbol}...", flush=True)
        
        # Données
        df_1h = get_binance_data(symbol, "1h")
        df_1d = get_binance_data(symbol, "1d")
        
        if df_1h is None or df_1d is None: continue
        
        # --- CALCULS ---
        current_price = df_1h['close'].iloc[-1]
        
        # RSI
        delta = df_1h['close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(14).mean()
        avg_loss = loss.rolling(14).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        current_rsi = rsi.iloc[-1]
        
        # Moyennes Mobiles
        ema50_1h = df_1h['close'].ewm(span=50).mean().iloc[-1]
        ema200_1d = df_1d['close'].ewm(span=200).mean().iloc[-1]
        
        # Score Technique
        score = 0
        trend_fond = "🔴 BAISSE"
        if current_price > ema200_1d:
            score += 30
            trend_fond = "🟢 HAUSSE"
        if current_price > ema50_1h: score += 20
        if 40 < current_rsi < 65: score += 20
        
        # --- LOGIQUE INTELLIGENTE (PORTFOLIO AWARE) ---
        user_owns_it = symbol in my_positions
        amount_owned = my_positions.get(symbol, 0)
        value_owned = amount_owned * current_price
        
        advice = "⚪ NEUTRE"
        action_required = ""
        
        # Cas 1 : Je possède la crypto (Ex: Tes SOL)
        if user_owns_it and value_owned > 10: # Seuil min 10$
            if trend_fond == "🔴 BAISSE" and score < 40:
                advice = "🚨 VENDRE (Protection)"
                action_required = "URGENT"
            elif score > 60:
                advice = "🟢 GARDER (Hold)"
            else:
                advice = "🟠 SURVEILLER"
                
        # Cas 2 : Je ne possède pas la crypto
        else:
            if market_trend == "BEAR":
                advice = "⛔ ATTENDRE (Marché Bear)"
            elif score > 75:
                advice = "🚀 ACHETER"
                
        # --- OUTPUT ---
        results.append({
            "Crypto": symbol,
            "Prix": current_price,
            "J'en ai ?": f"✅ {round(value_owned,1)}$" if value_owned > 10 else "❌",
            "Conseil IA": advice,
            "Action": action_required,
            "Trend_Fond": trend_fond,
            "Score": score,
            "RSI": round(current_rsi, 1)
        })

    # 4. Ecriture Sheet
    if results:
        try:
            sh = gc.open_by_key(SHEET_ID)
            try: ws = sh.worksheet("PortfolioManager")
            except: ws = sh.add_worksheet("PortfolioManager", 100, 20)
            
            df = pd.DataFrame(results)
            # Tri intelligent : Les actions urgentes en haut
            df = df.sort_values(by="Action", ascending=False) 
            
            df["Update"] = datetime.now(timezone.utc).strftime("%H:%M")
            ws.clear()
            set_with_dataframe(ws, df)
            print("🚀 Google Sheet mis à jour (Mode Binance)", flush=True)
        except Exception as e:
            print(f"❌ Erreur Sheet: {e}", flush=True)

# ======================================================
# 🔄 SERVER
# ======================================================
def run_bot():
    analyze_market_and_portfolio()
    while True:
        time.sleep(3600)
        analyze_market_and_portfolio()

def keep_alive():
    url = os.getenv("RENDER_EXTERNAL_URL")
    if url:
        while True:
            time.sleep(600)
            try: requests.get(url)
            except: pass

@app.route("/")
def index(): return "Binance Portfolio Bot Active"

if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))