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
# ⚙️ CONFIGURATION (V18 FIX)
# ======================================================
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")

UPDATE_FREQUENCY = 900  # 15 minutes
RISK_PER_TRADE_PCT = 0.02 # 2% de risque
MIN_ORDER_SIZE_USD = 11.0 # Minimum Binance

# Watchlist de base
CORE_WATCHLIST = ["BTC/USDC", "ETH/USDC", "SOL/USDC", "BNB/USDC"]

# ======================================================
# 🔐 CONNEXIONS
# ======================================================
print("🔐 Initialisation V18 (Narrateur)...", flush=True)

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

def smart_format(value, is_currency=True, precision=2):
    if value is None: return "-"
    suffix = " $" if is_currency else ""
    if value >= 1000: return f"{value:,.{precision}f}{suffix}".replace(",", " ")
    elif value >= 1: return f"{value:.{precision}f}{suffix}"
    elif value >= 0.001: return f"{value:.4f}{suffix}"
    else: return f"{value:.8f}{suffix}"

def send_discord_alert(message, color_code=0x3498db):
    if not DISCORD_WEBHOOK_URL: return
    try:
        data = {
            "embeds": [{
                "title": "📜 Analyste V18",
                "description": message,
                "color": color_code,
                "footer": {"text": "MACD • Bollinger • Narrative AI"}
            }]
        }
        requests.post(DISCORD_WEBHOOK_URL, json=data)
    except Exception as e:
        print(f"⚠️ Erreur Discord: {e}")

def get_dynamic_watchlist(limit=25):
    try:
        tickers = exchange.fetch_tickers()
        pairs = []
        for symbol, data in tickers.items():
            if "/USDC" in symbol and "quoteVolume" in data:
                pairs.append((symbol, data['quoteVolume']))
        pairs.sort(key=lambda x: x[1], reverse=True)
        top_pairs = [p[0] for p in pairs[:limit]]
        final_list = list(set(CORE_WATCHLIST + top_pairs))
        return final_list
    except: return CORE_WATCHLIST

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
                if pair_usdc in tickers: 
                    price = tickers[pair_usdc]['last']
                    val_usd = amount * price
                    if val_usd > 1:
                        total_equity_usd += val_usd
                        positions[pair_usdc] = amount
        
        total_equity_usd += cash_usd
        return positions, cash_usd, total_equity_usd
    except Exception as e:
        print(f"⚠️ Erreur Portfolio: {e}")
        return {}, 0, 10000

# ======================================================
# 📜 HISTORIQUE & ALERTE NARRATIVE
# ======================================================
def check_history_and_alert(symbol, new_action, new_advice, price, narrative, size_usd, fees):
    try:
        sh = gc.open_by_key(SHEET_ID)
        try: ws_hist = sh.worksheet("Journal_Trading")
        except: ws_hist = sh.add_worksheet("Journal_Trading", 1000, 10); ws_hist.append_row(["Date", "Crypto", "Prix", "Signal", "Analyse_Narrative"])

        records = ws_hist.get_all_records()
        last_signal = "AUCUN"
        relevant = [r for r in records if r.get("Crypto") == symbol]
        if relevant: last_signal = relevant[-1].get("Signal", "AUCUN")
        
        full_signal = f"{new_action} {new_advice}".strip()
        is_alert_worthy = False
        color = 0x95a5a6

        if new_action == "URGENT" and "URGENT" not in last_signal:
            is_alert_worthy = True; color = 0xe74c3c
        elif "ACHAT" in new_advice and "ACHAT" not in last_signal:
            is_alert_worthy = True; color = 0x2ecc71
            
        if is_alert_worthy:
            paris_tz = pytz.timezone('Europe/Paris')
            now_str = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M")
            ws_hist.append_row([now_str, symbol, smart_format(price), full_signal, narrative])
            
            # Message Discord V18
            msg = f"**{symbol}** : {full_signal}\n"
            msg += f"💰 Prix: {smart_format(price)}\n"
            if "ACHAT" in new_advice:
                msg += f"📦 Mise: {smart_format(size_usd)} (Frais ~{smart_format(fees)})\n"
            msg += f"📝 **Analyse:**\n{narrative}"
            send_discord_alert(msg, color)

    except Exception as e:
        print(f"⚠️ Erreur Journal: {e}")

# ======================================================
# 🧠 INDICATEURS TECHNIQUES COMPLETS V18
# ======================================================
def calculate_all_indicators(symbol):
    df_1h = get_binance_data(symbol, "1h", limit=100)
    df_1d = get_binance_data(symbol, "1d", limit=200)
    if df_1h is None or df_1d is None: return None

    # 1. RSI & ATR
    delta = df_1h['close'].diff()
    rs = delta.where(delta>0,0).rolling(14).mean() / (-delta.where(delta<0,0)).rolling(14).mean()
    rsi_1h = 100 - (100 / (1 + rs))
    
    tr = pd.concat([df_1h['high']-df_1h['low'], abs(df_1h['high']-df_1h['close'].shift(1)), abs(df_1h['low']-df_1h['close'].shift(1))], axis=1).max(axis=1).rolling(14).mean()
    atr_1h = tr

    # 2. ADX
    plus_di = 100 * (df_1h['high'].diff().clip(lower=0).ewm(alpha=1/14).mean() / atr_1h)
    minus_di = 100 * (abs(df_1h['low'].diff().clip(upper=0)).ewm(alpha=1/14).mean() / atr_1h)
    adx_1h = (abs(plus_di - minus_di) / abs(plus_di + minus_di) * 100).rolling(14).mean()

    # 3. MACD (NOUVEAU V18)
    exp1 = df_1h['close'].ewm(span=12, adjust=False).mean()
    exp2 = df_1h['close'].ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    signal = macd.ewm(span=9, adjust=False).mean()
    hist = macd - signal

    # 4. Bollinger Bands (NOUVEAU V18)
    sma20 = df_1h['close'].rolling(window=20).mean()
    std = df_1h['close'].rolling(window=20).std()
    bb_upper = sma20 + (2 * std)
    bb_lower = sma20 - (2 * std)
    # Protection division par zéro
    bb_width = (bb_upper - bb_lower) / sma20
    bb_width = bb_width.fillna(0)

    # 5. Trends & Distances
    ema50_1h = df_1h['close'].ewm(span=50).mean().iloc[-1]
    ema200_1d = df_1d['close'].ewm(span=200).mean().iloc[-1]
    current_price = df_1h['close'].iloc[-1]
    dist_ma200_pct = ((current_price - ema200_1d) / ema200_1d) * 100

    # 6. Order Book
    try:
        book = exchange.fetch_order_book(symbol, limit=20)
        bid = sum([b[1] for b in book['bids']])
        ask = sum([a[1] for a in book['asks']])
        ob_ratio = bid / ask if ask > 0 else 1.0
    except: ob_ratio = 1.0

    return {
        "rsi": rsi_1h.iloc[-1], "adx": adx_1h.iloc[-1], "atr": atr_1h.iloc[-1],
        "macd_line": macd.iloc[-1], "macd_signal": signal.iloc[-1], "macd_hist": hist.iloc[-1],
        "bb_width": bb_width.iloc[-1], "bb_upper": bb_upper.iloc[-1], "bb_lower": bb_lower.iloc[-1],
        "ema50_1h": ema50_1h, "dist_ma200": dist_ma200_pct,
        "ob_ratio": ob_ratio
    }

def analyze_market_and_portfolio():
    print("🧠 Analyse V18 Narrateur...", flush=True)
    my_positions, cash_available, total_capital = get_portfolio_data()
    dynamic_list = list(set(CORE_WATCHLIST + list(my_positions.keys()) + get_dynamic_watchlist(25)))
    
    # Macro
    btc_df = get_binance_data("BTC/USDC", "1d", limit=200)
    btc_trend = "BULL" if btc_df['close'].iloc[-1] > btc_df['close'].ewm(span=200).mean().iloc[-1] else "BEAR"
    
    try: fng_val = int(requests.get("https://api.alternative.me/fng/?limit=1", timeout=3).json()['data'][0]['value'])
    except: fng_val = 50

    results = []
    
    # Header
    results.append({
        "Crypto": "💰 TRÉSORERIE", "Prix": "-", "Mon_Bag": smart_format(cash_available), 
        "Conseil": "CAPITAL", "Action": "", "Score": 1000, "Mise ($)": "-", "Frais Est.": "-",
        "Analyse Complète 🧠": f"Capital prêt: {smart_format(cash_available)}"
    })
    results.append({
        "Crypto": "🌍 MACRO", "Prix": "-", "Mon_Bag": "-", 
        "Conseil": "INFO", "Action": "", "Score": 999, "Mise ($)": "-", "Frais Est.": "-",
        "Analyse Complète 🧠": f"BTC {'🐻' if btc_trend=='BEAR' else '🐂'} | Sentiment: {fng_val}/100"
    })

    for symbol in dynamic_list:
        try:
            live_price = get_live_price(symbol)
            if live_price is None: continue
            inds = calculate_all_indicators(symbol)
            if inds is None: continue

            # --- OPTIMISATION MICRO-CAPITAL ---
            stop_loss_price = live_price - (2.0 * inds["atr"])
            risk_per_share = live_price - stop_loss_price
            risk_budget = total_capital * RISK_PER_TRADE_PCT 
            
            pos_size_usd = 0
            forced_msg = ""
            if risk_per_share > 0:
                pos_size_usd = (risk_budget / risk_per_share) * live_price
            
            if pos_size_usd > 0:
                if pos_size_usd < MIN_ORDER_SIZE_USD:
                    pos_size_usd = MIN_ORDER_SIZE_USD; forced_msg = " (Min)"
                if pos_size_usd > cash_available: pos_size_usd = cash_available
            
            fees_est = pos_size_usd * 0.001
            tp1 = live_price + (risk_per_share * 2.0)
            
            # --- SCORING & NARRATION V18 ---
            score = 0
            narrative = [] # Liste de phrases
            
            # 1. Tendance de Fond (MA200)
            trend_icon = "🔴"
            if inds["dist_ma200"] > 0: 
                score += 30; trend_icon = "🟢"
                narrative.append(f"{trend_icon} Fond Haussier (+{round(inds['dist_ma200'],1)}% vs MA200).")
            else: 
                narrative.append(f"{trend_icon} Tendance baissière (Sous MA200).")

            # 2. Momentum (RSI + MACD)
            macd_status = "Bearish"
            if inds["macd_line"] > inds["macd_signal"]:
                score += 10; macd_status = "Bullish"

            if 45 < inds["rsi"] < 65: 
                score += 10
                narrative.append(f"⚡ Momentum sain (MACD {macd_status}).")
            elif inds["rsi"] < 30: 
                score += 5
                narrative.append(f"🧊 Survente excessive (RSI {round(inds['rsi'])}), rebond possible.")
            elif inds["rsi"] > 70:
                narrative.append(f"🔥 Surchauffe (RSI {round(inds['rsi'])}), risque de correction.")

            # 3. Volatilité (Bollinger)
            if inds["bb_width"] < 0.05: # Squeeze très serré
                score += 10
                narrative.append("💥 Volatilité compressée (Bollinger Squeeze), mouvement imminent.")

            # 4. Force & Obstacles (ADX + OrderBook)
            if inds["adx"] > 25: 
                score += 15
                narrative.append(f"🚀 Tendance forte (ADX {round(inds['adx'])}).")
            
            if inds["ob_ratio"] > 1.5: 
                score += 20
                narrative.append(f"🟢 Support acheteur puissant (Ratio {round(inds['ob_ratio'],1)}).")
            elif inds["ob_ratio"] < 0.6: 
                score -= 20
                narrative.append(f"⛔ Mur de Vente détecté (Ratio {round(inds['ob_ratio'],1)}).")

            # Filtre Macro
            if btc_trend == "BEAR" and "BTC" not in symbol: 
                score = max(0, score - 40)
                narrative.append("⚠️ Prudence: BTC Baissier.")

            # --- CONSEIL ---
            value_owned = my_positions.get(symbol, 0) * live_price
            advice = "⚪ NEUTRE"; action = ""

            if value_owned > 10:
                if trend_icon == "🔴" and score < 40: advice = "🚨 VENDRE"; action = "URGENT"
                elif score > 70: advice = "🟢 GARDER"
                else: advice = "🟠 SURVEILLER"
            else:
                if btc_trend == "BEAR": advice = "⛔ ATTENDRE"
                elif score > 80 and inds["adx"] > 25: advice = "🔥 ACHAT FORT"
                elif score > 60: advice = "✅ ACHAT"

            # Construction finale du texte
            full_narrative = " ".join(narrative)
            
            check_history_and_alert(symbol.replace("/USDC", ""), action, advice, live_price, full_narrative, pos_size_usd, fees_est)

            results.append({
                "Crypto": symbol.replace("/USDC", ""),
                "Prix": smart_format(live_price),
                "Mon_Bag": smart_format(value_owned) if value_owned > 10 else "-",
                "Conseil": advice,
                "Action": action,
                "Mise ($)": f"{smart_format(pos_size_usd)}{forced_msg}" if advice in ["✅ ACHAT", "🔥 ACHAT FORT"] else "-",
                "Frais Est.": f"{smart_format(fees_est)}" if advice in ["✅ ACHAT", "🔥 ACHAT FORT"] else "-",
                "Stop Loss": smart_format(stop_loss_price),
                "TP1": smart_format(tp1),
                "Score": score,
                "Analyse Complète 🧠": full_narrative
            })

        except Exception as e:
            # print(f"⚠️ Skip {symbol}: {e}")
            pass

    if results:
        try:
            sh = gc.open_by_key(SHEET_ID)
            try: ws = sh.worksheet("PortfolioManager")
            except: ws = sh.add_worksheet("PortfolioManager", 100, 20)
            
            df = pd.DataFrame(results)
            df = df.sort_values(by=["Score"], ascending=False)
            df_final = pd.concat([df[df["Score"] >= 999], df[df["Score"] < 999]])
            
            paris_tz = pytz.timezone('Europe/Paris')
            df_final["Update"] = datetime.now(paris_tz).strftime("%H:%M")
            
            cols = ["Crypto", "Prix", "Mon_Bag", "Conseil", "Action", 
                    "Mise ($)", "Frais Est.", "Stop Loss", "TP1", 
                    "Score", "Update", "Analyse Complète 🧠"]
            
            ws.clear()
            set_with_dataframe(ws, df_final[cols])
            print("🚀 Sheet V18 Narrateur mis à jour !", flush=True)
        except Exception as e:
            print(f"❌ Erreur Sheet: {e}", flush=True)

# ======================================================
# 🔄 SERVEUR (CORRIGÉ)
# ======================================================
def run_bot():
    print("⏳ Démarrage V18...", flush=True)
    analyze_market_and_portfolio()
    while True:
        time.sleep(UPDATE_FREQUENCY)
        analyze_market_and_portfolio()

def keep_alive():
    url = RENDER_EXTERNAL_URL
    if url:
        while True:
            time.sleep(300)
            try: 
                requests.get(url)
                print("💤 Ping")
            except: 
                pass

@app.route("/")
def index(): return "Bot V18 Analyst Active"

if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))