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
UPDATE_FREQUENCY = 900  # 15 minutes

WATCHLIST = [
    "BTC/USDC", "ETH/USDC", "SOL/USDC", "BNB/USDC", "ADA/USDC", 
    "DOGE/USDC", "AVAX/USDC", "XRP/USDC", "LINK/USDC", "MATIC/USDC", 
    "DOT/USDC", "LTC/USDC", "ATOM/USDC", "NEAR/USDC", "PEPE/USDC",
    "SHIB/USDC", "TRX/USDC", "FET/USDC", "RENDER/USDC", "INJ/USDC"
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
# 🛠️ OUTILS
# ======================================================

def smart_format(value, is_currency=True):
    if value is None: return "-"
    suffix = " $" if is_currency else ""
    if value >= 1000:
        return f"{value:,.2f}{suffix}".replace(",", " ")
    elif value >= 1:
        return f"{value:.2f}{suffix}"
    elif value >= 0.001:
        return f"{value:.4f}{suffix}"
    else:
        return f"{value:.8f}{suffix}"

def get_binance_data(symbol, timeframe, limit=100):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        return df
    except Exception:
        return None

def get_live_price(symbol):
    try:
        return exchange.fetch_ticker(symbol)['last']
    except:
        return None

def get_user_balance():
    positions = {}
    if not exchange: return positions
    try:
        balance = exchange.fetch_balance()
        for asset, amount in balance['total'].items():
            if amount > 0:
                pair = f"{asset}/USDC"
                positions[pair] = amount
        return positions
    except Exception as e:
        print(f"⚠️ Erreur Balance: {e}")
        return {}

# ======================================================
# 🧠 INDICATEURS & ANALYSE
# ======================================================

def calculate_indicators(df_1h, df_1d):
    # RSI
    delta = df_1h['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    rs = gain.rolling(14).mean() / loss.rolling(14).mean()
    rsi = 100 - (100 / (1 + rs))
    
    # ADX & ATR
    plus_dm = df_1h['high'].diff()
    minus_dm = df_1h['low'].diff()
    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm > 0] = 0
    tr1 = df_1h['high'] - df_1h['low']
    tr2 = abs(df_1h['high'] - df_1h['close'].shift(1))
    tr3 = abs(df_1h['low'] - df_1h['close'].shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    atr = tr.rolling(14).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1/14).mean() / atr)
    minus_di = 100 * (abs(minus_dm).ewm(alpha=1/14).mean() / atr)
    dx = (abs(plus_di - minus_di) / abs(plus_di + minus_di)) * 100
    adx = dx.rolling(14).mean()

    # Moyennes Mobiles
    ema50_1h = df_1h['close'].ewm(span=50).mean()
    ema200_1d = df_1d['close'].ewm(span=200).mean()

    return {
        "rsi": rsi.iloc[-1],
        "adx": adx.iloc[-1],
        "atr": atr.iloc[-1],
        "ema50_1h": ema50_1h.iloc[-1],
        "ema200_1d": ema200_1d.iloc[-1],
        "vol_mean": df_1h['volume'].rolling(20).mean().iloc[-1],
        "vol_cur": df_1h['volume'].iloc[-1]
    }

def analyze_market_and_portfolio():
    print("🧠 Analyse V9...", flush=True)
    my_positions = get_user_balance()
    results = []
    
    # Tendance BTC
    btc_df = get_binance_data("BTC/USDC", "1d", limit=200)
    market_trend = "NEUTRE"
    if btc_df is not None:
        ma200 = btc_df['close'].ewm(span=200).mean().iloc[-1]
        market_trend = "BULL" if btc_df['close'].iloc[-1] > ma200 else "BEAR"

    for symbol in WATCHLIST:
        try:
            df_1h = get_binance_data(symbol, "1h")
            df_1d = get_binance_data(symbol, "1d")
            live_price = get_live_price(symbol)
            
            if df_1h is None or df_1d is None or live_price is None: continue

            inds = calculate_indicators(df_1h, df_1d)
            
            # --- CALCUL SL / TP (Stratégie ATR) ---
            # Stop Loss = Prix - 2x ATR
            # Take Profit = Prix + 3x ATR (Ratio 1.5)
            stop_loss = live_price - (2.0 * inds["atr"])
            take_profit = live_price + (3.0 * inds["atr"])
            
            # --- INTELLIGENCE & SCORING ---
            score = 0
            reasons = [] 

            # 1. Tendance Fond
            trend_fond = "🔴 BAISSE"
            if live_price > inds["ema200_1d"]:
                trend_fond = "🟢 HAUSSE"
                score += 30
                reasons.append("Fond Haussier")

            # 2. Court terme
            if live_price > inds["ema50_1h"]:
                score += 20
            
            # 3. RSI
            if inds["rsi"] > 70:
                reasons.append("⚠️ RSI Surchauffe")
            elif inds["rsi"] < 30:
                score += 10
                reasons.append("🧊 RSI Survente")
            elif 45 < inds["rsi"] < 65:
                score += 15
                reasons.append("RSI Sain")

            # 4. ADX (Force)
            if inds["adx"] > 25:
                score += 15
            
            # 5. Volume
            vol_ratio = inds["vol_cur"] / inds["vol_mean"] if inds["vol_mean"] > 0 else 0
            if vol_ratio > 1.5:
                score += 20
                reasons.append(f"Volume x{round(vol_ratio,1)}")

            # Pénalité
            if market_trend == "BEAR" and "BTC" not in symbol:
                score = max(0, score - 30)

            # --- CONSEIL & TEXTE ---
            amount_owned = my_positions.get(symbol, 0)
            value_owned = amount_owned * live_price
            
            advice = "⚪ NEUTRE"
            action = ""

            if value_owned > 10:
                if trend_fond == "🔴 BAISSE" and score < 45:
                    advice = "🚨 VENDRE"
                    action = "URGENT"
                    reasons.insert(0, "🛑 Stop Loss Touché (virtuel)")
                elif score > 70:
                    advice = "🟢 GARDER"
                    reasons.insert(0, "✅ Profit Run")
                else:
                    advice = "🟠 SURVEILLER"
            else:
                if market_trend == "BEAR":
                    advice = "⛔ ATTENDRE"
                elif score > 80 and inds["adx"] > 25:
                    advice = "🔥 ACHAT FORT"
                    reasons.insert(0, "🎯 Sniper")
                elif score > 60:
                    advice = "✅ ACHAT"

            # --- CONSTRUCTION LIGNE ---
            results.append({
                "Crypto": symbol.replace("/USDC", ""),
                "Prix": smart_format(live_price),
                "Mon_Bag": smart_format(value_owned) if value_owned > 10 else "-",
                "Conseil": advice,
                "Action": action,
                # Nouvelles colonnes SL / TP
                "Stop_Loss ($)": smart_format(stop_loss),
                "Take_Profit ($)": smart_format(take_profit),
                "Score": score,
                "Trend": trend_fond,
                "RSI": round(inds["rsi"], 1),
                "Analyse 🧠": " + ".join(reasons) # Déplacé à la fin
            })

        except Exception as e:
            print(f"⚠️ Skip {symbol}: {e}")

    # ECRITURE
    if results:
        try:
            sh = gc.open_by_key(SHEET_ID)
            try: ws = sh.worksheet("PortfolioManager")
            except: ws = sh.add_worksheet("PortfolioManager", 100, 20)
            
            df = pd.DataFrame(results)
            # Tri intelligent
            df = df.sort_values(by=["Action", "Score"], ascending=[False, False])
            
            # Réorganisation des colonnes pour mettre Analyse à la fin
            cols = ["Crypto", "Prix", "Mon_Bag", "Conseil", "Action", 
                    "Stop_Loss ($)", "Take_Profit ($)", 
                    "Score", "Trend", "RSI", "Analyse 🧠"]
            
            # On s'assure d'avoir toutes les colonnes + Update
            paris_tz = pytz.timezone('Europe/Paris')
            df["Mise_à_jour"] = datetime.now(paris_tz).strftime("%H:%M")
            
            # Sélection finale
            final_df = df[cols + ["Mise_à_jour"]]
            
            ws.clear()
            set_with_dataframe(ws, final_df)
            print("🚀 Sheet V9 mis à jour !", flush=True)
        except Exception as e:
            print(f"❌ Erreur Sheet: {e}", flush=True)

# ======================================================
# 🔄 BOUCLE
# ======================================================
def run_bot():
    print("⏳ Démarrage V9...", flush=True)
    analyze_market_and_portfolio()
    while True:
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
def index(): return "Bot V9 Strategy Active"

if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))