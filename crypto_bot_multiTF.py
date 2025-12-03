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
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL") # Pour l'auto-ping

# On force un update toutes les 15 minutes
UPDATE_FREQUENCY = 900 

WATCHLIST = [
    "BTC/USDC", "ETH/USDC", "SOL/USDC", "BNB/USDC", "ADA/USDC", 
    "DOGE/USDC", "AVAX/USDC", "XRP/USDC", "LINK/USDC", "MATIC/USDC", 
    "DOT/USDC", "LTC/USDC", "ATOM/USDC", "NEAR/USDC", "PEPE/USDC",
    "SHIB/USDC", "TRX/USDC", "FET/USDC", "RENDER/USDC", "INJ/USDC"
]

# ======================================================
# 🔐 CONNEXIONS
# ======================================================
print("🔐 Initialisation V13 (Fiabilité & Macro)...", flush=True)

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
                "title": "🏛️ Institutionnel V13",
                "description": message,
                "color": color_code,
                "footer": {"text": "Order Flow & Sentiment Analysis"}
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
        
        usdt = balance['total'].get('USDT', 0)
        usdc = balance['total'].get('USDC', 0)
        cash_usd = usdt + usdc
        
        for asset, amount in balance['total'].items():
            if amount > 0 and asset not in ["USDT", "USDC"]:
                price = 0
                pair_usdc = f"{asset}/USDC"
                pair_usdt = f"{asset}/USDT"
                
                if pair_usdc in tickers: price = tickers[pair_usdc]['last']
                elif pair_usdt in tickers: price = tickers[pair_usdt]['last']
                
                val_usd = amount * price
                if val_usd > 1:
                    total_equity_usd += val_usd
                    positions[f"{asset}/USDC"] = amount
        
        total_equity_usd += cash_usd
        return positions, cash_usd, total_equity_usd
    except Exception as e:
        print(f"⚠️ Erreur Portfolio: {e}")
        return {}, 0, 10000

# ======================================================
# 🌊 DATA INSTITUTIONNELLES
# ======================================================

def get_fear_and_greed():
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5)
        data = r.json()
        val = int(data['data'][0]['value'])
        classification = data['data'][0]['value_classification']
        return val, classification
    except:
        return 50, "Neutral"

def get_order_book_pressure(symbol):
    try:
        book = exchange.fetch_order_book(symbol, limit=20)
        bid_vol = sum([bid[1] for bid in book['bids']])
        ask_vol = sum([ask[1] for ask in book['asks']])
        if ask_vol == 0: return 1.0
        return bid_vol / ask_vol
    except:
        return 1.0

def get_fibonacci_support(df_1d):
    try:
        recent_high = df_1d['high'].tail(90).max()
        recent_low = df_1d['low'].tail(90).min()
        diff = recent_high - recent_low
        fib_0618 = recent_high - (diff * 0.618)
        fib_0786 = recent_high - (diff * 0.786)
        current_price = df_1d['close'].iloc[-1]
        
        if current_price > fib_0618: return fib_0618
        else: return fib_0786
    except:
        return 0

# ======================================================
# 📜 HISTORIQUE
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
            is_alert_worthy = True; color = 0xe74c3c
        elif "ACHAT FORT" in new_advice and "ACHAT FORT" not in last_signal:
            is_alert_worthy = True; color = 0x2ecc71
            
        if is_alert_worthy:
            paris_tz = pytz.timezone('Europe/Paris')
            now_str = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M")
            ws_hist.append_row([now_str, symbol, smart_format(price), last_signal, f"{new_action} {new_advice}", reason])
            msg = f"**{symbol}** : {new_action} {new_advice}\n💰 Prix: {smart_format(price)}\n🧠 Raison: {reason}"
            send_discord_alert(msg, color)
            print(f"🔔 Alerte pour {symbol}")

    except Exception as e:
        print(f"⚠️ Erreur Journal: {e}")

# ======================================================
# 🧠 INDICATEURS TECHNIQUES
# ======================================================
def calculate_advanced_indicators(symbol):
    df_1h = get_binance_data(symbol, "1h")
    df_4h = get_binance_data(symbol, "4h")
    df_1d = get_binance_data(symbol, "1d", limit=200)
    
    if df_1h is None or df_4h is None or df_1d is None: return None

    # RSI 1H
    delta = df_1h['close'].diff()
    rs = delta.where(delta>0,0).rolling(14).mean() / (-delta.where(delta<0,0)).rolling(14).mean()
    rsi_1h = 100 - (100 / (1 + rs))
    
    # ADX 1H
    tr = pd.concat([df_1h['high']-df_1h['low'], abs(df_1h['high']-df_1h['close'].shift(1)), abs(df_1h['low']-df_1h['close'].shift(1))], axis=1).max(axis=1).rolling(14).mean()
    atr_1h = tr
    plus_di = 100 * (df_1h['high'].diff().clip(lower=0).ewm(alpha=1/14).mean() / atr_1h)
    minus_di = 100 * (abs(df_1h['low'].diff().clip(upper=0)).ewm(alpha=1/14).mean() / atr_1h)
    adx_1h = (abs(plus_di - minus_di) / abs(plus_di + minus_di) * 100).rolling(14).mean()

    # Trends
    ema50_4h = df_4h['close'].ewm(span=50).mean().iloc[-1]
    ema200_1d = df_1d['close'].ewm(span=200).mean().iloc[-1]

    # Data Institutionnelles
    ob_ratio = get_order_book_pressure(symbol)
    fibo_support = get_fibonacci_support(df_1d)

    return {
        "rsi_1h": rsi_1h.iloc[-1],
        "adx_1h": adx_1h.iloc[-1],
        "atr_1h": atr_1h.iloc[-1],
        "ema50_4h": ema50_4h,
        "ema200_1d": ema200_1d,
        "ob_ratio": ob_ratio,
        "fibo_support": fibo_support
    }

def analyze_market_and_portfolio():
    print("🧠 Analyse V13...", flush=True)
    my_positions, cash_available, total_capital = get_portfolio_data()
    
    # Macro Data
    btc_df = get_binance_data("BTC/USDC", "1d", limit=200)
    btc_trend = "BULL" if btc_df['close'].iloc[-1] > btc_df['close'].ewm(span=200).mean().iloc[-1] else "BEAR"
    
    fng_val, fng_class = get_fear_and_greed()
    print(f"💰 Cash: {smart_format(cash_available)} | Sentiment: {fng_val} ({fng_class})")

    results = []
    
    # Ligne 1 : Trésorerie
    results.append({
        "Crypto": "💰 TRÉSORERIE",
        "Prix": "-", "Mon_Bag": smart_format(cash_available), "Conseil": "CAPITAL", "Action": "", "Score": 1000,
        "Trend 1D": "-", "Pressure": "-", "Support Fibo": "-", "Analyse Complète 🧠": f"Prêt à tirer: {smart_format(cash_available)}"
    })

    # Ligne 2 : Macro
    market_icon = "🐻" if btc_trend == "BEAR" else "🐂"
    fng_icon = "😱" if fng_val < 30 else ("🤑" if fng_val > 70 else "😐")
    results.append({
        "Crypto": "🌍 MACRO",
        "Prix": "-", "Mon_Bag": "-", "Conseil": "INFO", "Action": "", "Score": 999,
        "Trend 1D": btc_trend, "Pressure": "-", "Support Fibo": "-", 
        "Analyse Complète 🧠": f"BTC {market_icon} | Sentiment: {fng_class} {fng_val}/100 {fng_icon}"
    })

    for symbol in WATCHLIST:
        try:
            live_price = get_live_price(symbol)
            if live_price is None: continue
            inds = calculate_advanced_indicators(symbol)
            if inds is None: continue

            stop_loss = live_price - (2.0 * inds["atr_1h"])
            take_profit = live_price + (3.0 * inds["atr_1h"])
            
            # --- SCORING V13 ---
            score = 0
            details = []
            
            # Trend 1D
            trend_1d = "🔴"
            if live_price > inds["ema200_1d"]:
                trend_1d = "🟢"
                score += 30
            
            # Pressure
            pressure_str = "Neutral"
            if inds["ob_ratio"] > 1.5: 
                score += 20; pressure_str = "🟢 BUY WALL"; details.append(f"Pression Achat ({round(inds['ob_ratio'],1)}x)")
            elif inds["ob_ratio"] < 0.6:
                pressure_str = "🔴 SELL WALL"; details.append("Mur de Vente")
                
            # RSI & ADX
            if 45 < inds["rsi_1h"] < 65: score += 10
            elif inds["rsi_1h"] < 30: score += 5; details.append("Survente")
            if inds["adx_1h"] > 25: score += 15
            
            # Sentiment Contrarian
            if fng_val < 25 and inds["rsi_1h"] < 30: score += 20; details.append("💎 Buy the Fear")

            if btc_trend == "BEAR" and "BTC" not in symbol: score = max(0, score - 40); details.append("BTC Bear")

            # --- CONSEIL ---
            amount_owned = my_positions.get(symbol, 0)
            value_owned = amount_owned * live_price
            advice = "⚪ NEUTRE"; action = ""

            if value_owned > 10:
                if trend_1d == "🔴" and score < 40: advice = "🚨 VENDRE"; action = "URGENT"
                elif score > 70: advice = "🟢 GARDER"
                else: advice = "🟠 SURVEILLER"
            else:
                if btc_trend == "BEAR":
                    advice = "⛔ ATTENDRE"
                    if inds["fibo_support"] > 0: details.append(f"Cible Fibo: {smart_format(inds['fibo_support'])}")
                elif score > 85: advice = "🔥 ACHAT FORT"; details.insert(0, "✅ Sniper")
                elif score > 65: advice = "✅ ACHAT"

            check_history_and_alert(symbol.replace("/USDC", ""), action, advice, live_price, " | ".join(details))

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
                "Pressure": pressure_str,
                "Support Fibo": smart_format(inds["fibo_support"]),
                "Analyse Complète 🧠": " | ".join(details)
            })

        except Exception as e:
            print(f"⚠️ Skip {symbol}: {e}")

    if results:
        try:
            sh = gc.open_by_key(SHEET_ID)
            try: ws = sh.worksheet("PortfolioManager")
            except: ws = sh.add_worksheet("PortfolioManager", 100, 20)
            
            df = pd.DataFrame(results)
            df = df.sort_values(by=["Score"], ascending=False)
            df_top = df[df["Score"] >= 999]
            df_others = df[df["Score"] < 999]
            df_final = pd.concat([df_top, df_others])
            
            paris_tz = pytz.timezone('Europe/Paris')
            df_final["Mise_à_jour"] = datetime.now(paris_tz).strftime("%H:%M")
            
            cols = ["Crypto", "Prix", "Mon_Bag", "Conseil", "Action", 
                    "Stop_Loss ($)", "Take_Profit ($)", 
                    "Score", "Trend 1D", "Pressure", "Support Fibo", "Mise_à_jour", "Analyse Complète 🧠"]
            
            ws.clear()
            set_with_dataframe(ws, df_final[cols])
            print(f"🚀 Sheet V13 mis à jour à {df_final['Mise_à_jour'].iloc[0]}", flush=True)
        except Exception as e:
            print(f"❌ Erreur Sheet: {e}", flush=True)

# ======================================================
# 🔄 BOUCLE ROBUSTE (Keep-Alive)
# ======================================================
def run_bot():
    print("⏳ Démarrage V13...", flush=True)
    analyze_market_and_portfolio()
    while True:
        time.sleep(UPDATE_FREQUENCY)
        analyze_market_and_portfolio()

def keep_alive():
    url = RENDER_EXTERNAL_URL
    if url:
        while True:
            time.sleep(300) # Ping toutes les 5 min
            try: 
                requests.get(url)
                print("💤 Ping Keep-Alive sent", flush=True)
            except: pass

@app.route("/")
def index(): return "Bot V13 Live"

if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))