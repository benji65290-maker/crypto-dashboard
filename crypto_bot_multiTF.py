import threading
import time
import pandas as pd
import numpy as np
import os
import json
import gspread
import ccxt
import pytz
import requests
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
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

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
print("🔐 Initialisation V12 (Hedge Fund)...", flush=True)

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
# 🛠️ OUTILS & ALERTEUR
# ======================================================

def smart_format(value, is_currency=True):
    if value is None: return "-"
    suffix = " $" if is_currency else ""
    if value >= 1000: return f"{value:,.2f}{suffix}".replace(",", " ")
    elif value >= 1: return f"{value:.2f}{suffix}"
    elif value >= 0.001: return f"{value:.4f}{suffix}"
    else: return f"{value:.8f}{suffix}"

def send_discord_alert(message, color_code=0x3498db):
    if not DISCORD_WEBHOOK_URL: return
    try:
        data = {
            "embeds": [{
                "title": "🏛️ Hedge Fund Bot V12",
                "description": message,
                "color": color_code,
                "footer": {"text": "Analyse Multi-Timeframe & Macro"}
            }]
        }
        requests.post(DISCORD_WEBHOOK_URL, json=data)
    except Exception as e:
        print(f"⚠️ Erreur Discord: {e}")

def get_binance_data(symbol, timeframe, limit=100):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        return df
    except: return None

def get_live_price(symbol):
    try: return exchange.fetch_ticker(symbol)['last']
    except: return None

def get_portfolio_data():
    positions = {}
    cash_usd = 0.0
    total_equity_usd = 0.0
    
    if not exchange: return positions, 0, 10000
    
    try:
        tickers = exchange.fetch_tickers() 
        balance = exchange.fetch_balance()
        
        # 1. Cash (USDT + USDC)
        usdt = balance['total'].get('USDT', 0)
        usdc = balance['total'].get('USDC', 0)
        cash_usd = usdt + usdc
        
        # 2. Positions Crypto
        for asset, amount in balance['total'].items():
            if amount > 0 and asset not in ["USDT", "USDC"]:
                price = 0
                pair_usdc = f"{asset}/USDC"
                pair_usdt = f"{asset}/USDT"
                
                if pair_usdc in tickers: price = tickers[pair_usdc]['last']
                elif pair_usdt in tickers: price = tickers[pair_usdt]['last']
                
                val_usd = amount * price
                if val_usd > 1: # Filtre poussières
                    total_equity_usd += val_usd
                    positions[f"{asset}/USDC"] = amount
        
        total_equity_usd += cash_usd
        return positions, cash_usd, total_equity_usd
    except Exception as e:
        print(f"⚠️ Erreur Portfolio: {e}")
        return {}, 0, 10000

# ======================================================
# 📜 GESTION HISTORIQUE & PNL
# ======================================================
def check_history_and_alert(symbol, new_action, new_advice, price, reason):
    try:
        sh = gc.open_by_key(SHEET_ID)
        try: ws_hist = sh.worksheet("Journal_Trading")
        except: ws_hist = sh.add_worksheet("Journal_Trading", 1000, 10); ws_hist.append_row(["Date", "Crypto", "Prix", "Ancien_Signal", "Nouveau_Signal", "Raison"])

        records = ws_hist.get_all_records()
        last_signal = "AUCUN"
        
        for row in reversed(records):
            if row.get("Crypto") == symbol:
                last_signal = row.get("Nouveau_Signal")
                break
        
        is_alert_worthy = False
        color = 0x95a5a6

        if new_action == "URGENT" and "URGENT" not in last_signal:
            is_alert_worthy = True
            color = 0xe74c3c # Rouge
        elif "ACHAT FORT" in new_advice and "ACHAT FORT" not in last_signal:
            is_alert_worthy = True
            color = 0x2ecc71 # Vert
            
        if is_alert_worthy:
            paris_tz = pytz.timezone('Europe/Paris')
            now_str = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M")
            ws_hist.append_row([now_str, symbol, smart_format(price), last_signal, f"{new_action} {new_advice}", reason])
            msg = f"**{symbol}** : {new_action} {new_advice}\n💰 Prix: {smart_format(price)}\n🧠 Raison: {reason}"
            send_discord_alert(msg, color)
            print(f"🔔 Alerte envoyée pour {symbol}")

    except Exception as e:
        print(f"⚠️ Erreur Journal: {e}")

# ======================================================
# 🧠 INDICATEURS AVANCÉS (V12)
# ======================================================
def calculate_advanced_indicators(symbol):
    # On récupère 3 timeframes
    df_1h = get_binance_data(symbol, "1h")
    df_4h = get_binance_data(symbol, "4h") # Nouveau
    df_1d = get_binance_data(symbol, "1d")
    
    if df_1h is None or df_4h is None or df_1d is None: return None

    # --- Indicateurs 1H ---
    delta = df_1h['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    rs = gain.rolling(14).mean() / loss.rolling(14).mean()
    rsi_1h = 100 - (100 / (1 + rs))
    
    # ADX 1H
    tr1 = df_1h['high'] - df_1h['low']
    tr2 = abs(df_1h['high'] - df_1h['close'].shift(1))
    tr3 = abs(df_1h['low'] - df_1h['close'].shift(1))
    atr_1h = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1).rolling(14).mean()
    
    plus_dm = df_1h['high'].diff()
    minus_dm = df_1h['low'].diff()
    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm > 0] = 0
    plus_di = 100 * (plus_dm.ewm(alpha=1/14).mean() / atr_1h)
    minus_di = 100 * (abs(minus_dm).ewm(alpha=1/14).mean() / atr_1h)
    dx = (abs(plus_di - minus_di) / abs(plus_di + minus_di)) * 100
    adx_1h = dx.rolling(14).mean()

    # --- Indicateurs 4H & 1D (Trend Filters) ---
    ema50_4h = df_4h['close'].ewm(span=50).mean().iloc[-1]
    ema200_1d = df_1d['close'].ewm(span=200).mean().iloc[-1]

    # Volumes
    vol_mean = df_1h['volume'].rolling(20).mean().iloc[-1]
    vol_cur = df_1h['volume'].iloc[-1]

    return {
        "rsi_1h": rsi_1h.iloc[-1],
        "adx_1h": adx_1h.iloc[-1],
        "atr_1h": atr_1h.iloc[-1],
        "ema50_4h": ema50_4h,
        "ema200_1d": ema200_1d,
        "vol_ratio": vol_cur / vol_mean if vol_mean > 0 else 0,
        "price_4h": df_4h['close'].iloc[-1] # Prix clôture 4h
    }

def analyze_market_health():
    """Analyse la santé globale du marché (Macro)"""
    # 1. Tendance BTC
    btc_df = get_binance_data("BTC/USDC", "1d", limit=200)
    btc_trend = "NEUTRE"
    if btc_df is not None:
        ma200 = btc_df['close'].ewm(span=200).mean().iloc[-1]
        btc_trend = "BULL" if btc_df['close'].iloc[-1] > ma200 else "BEAR"
        
    # 2. Tendance ETH/BTC (Force des Alts)
    eth_btc_df = get_binance_data("ETH/BTC", "1d", limit=100)
    alt_season = False
    if eth_btc_df is not None:
        ma50 = eth_btc_df['close'].ewm(span=50).mean().iloc[-1]
        if eth_btc_df['close'].iloc[-1] > ma50:
            alt_season = True # Les Alts surperforment BTC

    return btc_trend, alt_season

def analyze_market_and_portfolio():
    print("🧠 Analyse V12 Hedge Fund...", flush=True)
    my_positions, cash_available, total_capital = get_portfolio_data()
    print(f"💰 Cash: {smart_format(cash_available)} | Total: {smart_format(total_capital)}")
    
    btc_trend, alt_season = analyze_market_health()
    print(f"👑 Macro: BTC={btc_trend} | Alts={alt_season}", flush=True)

    results = []
    
    # Ligne 1 : Trésorerie
    results.append({
        "Crypto": "💰 TRÉSORERIE",
        "Prix": "-",
        "Mon_Bag": smart_format(cash_available),
        "Conseil": "LIQUIDITÉS",
        "Action": "",
        "Score": 1000,
        "Trend 1D": "-",
        "Trend 4H": "-",
        "RSI": "-",
        "Analyse Complète 🧠": f"Capital prêt: {smart_format(cash_available)} | Attente Setup"
    })

    # Ligne 2 : État du Marché (Pour info)
    market_msg = "BTC Haussier 🐂" if btc_trend == "BULL" else "BTC Baissier 🐻"
    alt_msg = "Alts Forts 🚀" if alt_season else "Alts Faibles 💤"
    results.append({
        "Crypto": "🌍 MARCHÉ",
        "Prix": "-",
        "Mon_Bag": "-",
        "Conseil": "INFO",
        "Action": "",
        "Score": 999,
        "Trend 1D": btc_trend,
        "Trend 4H": "-",
        "RSI": "-",
        "Analyse Complète 🧠": f"{market_msg} | {alt_msg}"
    })

    for symbol in WATCHLIST:
        try:
            live_price = get_live_price(symbol)
            if live_price is None: continue

            inds = calculate_advanced_indicators(symbol)
            if inds is None: continue

            # --- CALCUL SL / TP ---
            stop_loss = live_price - (2.0 * inds["atr_1h"])
            take_profit = live_price + (3.0 * inds["atr_1h"])
            
            # --- SCORING V12 (Triple Confirmation) ---
            score = 0
            details = []
            
            # 1. Long Terme (1D) : Au dessus de MA200 ?
            trend_1d = "🔴"
            if live_price > inds["ema200_1d"]:
                trend_1d = "🟢"
                score += 30
                details.append("Fond Haussier")
            else:
                details.append("Fond Baissier")

            # 2. Moyen Terme (4H) : Au dessus de MA50 ?
            trend_4h = "🔴"
            if live_price > inds["ema50_4h"]:
                trend_4h = "🟢"
                score += 20
            
            # 3. Court Terme (RSI 1H)
            if 45 < inds["rsi_1h"] < 65: 
                score += 10
            elif inds["rsi_1h"] < 30: 
                score += 5; details.append("Rebond possible")
            elif inds["rsi_1h"] > 70:
                details.append("Surchauffe")
                
            # 4. Force & Volume
            if inds["adx_1h"] > 25: 
                score += 15
                details.append("Trend Fort")
            
            if inds["vol_ratio"] > 1.5: 
                score += 20
                details.append(f"Vol x{round(inds['vol_ratio'],1)}")

            # --- FILTRES MACRO (Veto) ---
            # Si BTC est Bear, on massacre le score des Alts
            if btc_trend == "BEAR" and "BTC" not in symbol:
                score = max(0, score - 40)
                details.append("Veto BTC Bear")
            
            # Si Alts faibles, on réduit un peu
            if not alt_season and "BTC" not in symbol:
                score -= 10
                details.append("Dominance BTC")

            # --- CONSEIL ---
            amount_owned = my_positions.get(symbol, 0)
            value_owned = amount_owned * live_price
            
            advice = "⚪ NEUTRE"
            action = ""

            if value_owned > 10: # Mode Gestion
                if trend_1d == "🔴" and score < 40:
                    advice = "🚨 VENDRE"
                    action = "URGENT"
                elif score > 70: advice = "🟢 GARDER"
                else: advice = "🟠 SURVEILLER"
            else: # Mode Chasseur
                if btc_trend == "BEAR":
                    advice = "⛔ ATTENDRE"
                elif score > 85 and inds["adx_1h"] > 25: # Il faut un score très haut maintenant
                    advice = "🔥 ACHAT FORT"
                    details.insert(0, "✅ Triple Confirm")
                elif score > 65:
                    advice = "✅ ACHAT"

            # --- HISTORIQUE & ALERTE ---
            check_history_and_alert(
                symbol.replace("/USDC", ""), action, advice, live_price, " | ".join(details)
            )

            results.append({
                "Crypto": symbol.replace("/USDC", ""),
                "Prix": smart_format(live_price),
                "Mon_Bag": smart_format(value_owned) if value_owned > 10 else "-",
                "Conseil": advice,
                "Action": action,
                "Stop_Loss ($)": smart_format(stop_loss),
                "Take_Profit ($)": smart_format(take_profit),
                "Score": score,
                "Trend 1D": trend_1d,
                "Trend 4H": trend_4h, # Nouvelle colonne
                "RSI": round(inds["rsi_1h"], 1),
                "Analyse Complète 🧠": " | ".join(details)
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
            df = df.sort_values(by=["Score"], ascending=False)
            
            # On met Trésorerie et Marché en haut
            df_top = df[df["Score"] >= 999]
            df_others = df[df["Score"] < 999]
            df_final = pd.concat([df_top, df_others])
            
            paris_tz = pytz.timezone('Europe/Paris')
            df_final["Mise_à_jour"] = datetime.now(paris_tz).strftime("%H:%M")
            
            cols = ["Crypto", "Prix", "Mon_Bag", "Conseil", "Action", 
                    "Stop_Loss ($)", "Take_Profit ($)", 
                    "Score", "Trend 1D", "Trend 4H", "RSI", "Mise_à_jour", "Analyse Complète 🧠"]
            
            ws.clear()
            set_with_dataframe(ws, df_final[cols])
            print("🚀 Sheet V12 mis à jour !", flush=True)
        except Exception as e:
            print(f"❌ Erreur Sheet: {e}", flush=True)

# ======================================================
# 🔄 MAIN
# ======================================================
def run_bot():
    print("⏳ Démarrage V12...", flush=True)
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
def index(): return "Bot V12 Hedge Fund Active"

if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))