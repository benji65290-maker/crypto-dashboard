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
# ⚙️ CONFIGURATION (V21 PATCH)
# ======================================================
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")

UPDATE_FREQUENCY = 900  # 15 minutes
RISK_PER_TRADE_PCT = 0.02 
MIN_ORDER_SIZE_USD = 11.0 

CORE_WATCHLIST = ["BTC/USDC", "ETH/USDC", "SOL/USDC", "BNB/USDC"]

# ======================================================
# 🔐 CONNEXIONS
# ======================================================
print("🔐 Initialisation V21 (Robustesse)...", flush=True)

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
    if value is None or str(value) == "nan": return "-"
    suffix = " $" if is_currency else ""
    try:
        val = float(value)
        if val >= 1000: return f"{val:,.{precision}f}{suffix}".replace(",", " ")
        elif val >= 1: return f"{val:.{precision}f}{suffix}"
        elif val >= 0.001: return f"{val:.4f}{suffix}"
        else: return f"{val:.8f}{suffix}"
    except: return "-"

def send_discord_alert(message, color_code=0x3498db):
    if not DISCORD_WEBHOOK_URL: return
    try:
        data = {
            "embeds": [{
                "title": "🛠️ Dashboard V21",
                "description": message,
                "color": color_code,
                "footer": {"text": "Debug & Fix Version"}
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

def get_binance_data(symbol, timeframe, limit=500): # Augmenté à 500 pour stabilité
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        # Conversion explicite en float pour éviter les erreurs de type
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].astype(float)
        return df
    except: return None

def get_live_price(symbol):
    try: return float(exchange.fetch_ticker(symbol)['last'])
    except: return None

def get_portfolio_data():
    positions = {}
    cash_usd = 0.0
    total_equity_usd = 0.0
    
    if not exchange: return positions, 0, 10000
    
    try:
        tickers = exchange.fetch_tickers() 
        balance = exchange.fetch_balance()
        
        usdt = float(balance['total'].get('USDT', 0))
        usdc = float(balance['total'].get('USDC', 0))
        cash_usd = usdt + usdc
        
        for asset, amount in balance['total'].items():
            amount = float(amount)
            if amount > 0 and asset not in ["USDT", "USDC"]:
                price = 0
                pair_usdc = f"{asset}/USDC"
                if pair_usdc in tickers: 
                    price = float(tickers[pair_usdc]['last'])
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
# 📜 HISTORIQUE
# ======================================================
def get_all_history():
    try:
        sh = gc.open_by_key(SHEET_ID)
        try: ws_hist = sh.worksheet("Journal_Trading")
        except: 
            ws_hist = sh.add_worksheet("Journal_Trading", 1000, 10)
            ws_hist.append_row(["Date", "Crypto", "Prix", "Signal", "Analyse"])
            return []
        return ws_hist.get_all_records()
    except: return []

def append_history_log(symbol, price, full_signal, narrative):
    try:
        sh = gc.open_by_key(SHEET_ID)
        ws_hist = sh.worksheet("Journal_Trading")
        paris_tz = pytz.timezone('Europe/Paris')
        now_str = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M")
        ws_hist.append_row([now_str, symbol, smart_format(price), full_signal, narrative])
    except: pass

# ======================================================
# 🧠 INDICATEURS TECHNIQUES (ROBUSTE)
# ======================================================
def calculate_all_indicators(symbol):
    df_1h = get_binance_data(symbol, "1h", limit=500)
    df_1d = get_binance_data(symbol, "1d", limit=200)
    if df_1h is None or df_1d is None: return None

    # RSI (avec gestion d'erreur)
    delta = df_1h['close'].diff()
    rs = delta.where(delta>0,0).rolling(14, min_periods=1).mean() / (-delta.where(delta<0,0)).rolling(14, min_periods=1).mean()
    rsi_1h = 100 - (100 / (1 + rs))
    
    # ATR (Robustesse V21)
    tr1 = df_1h['high'] - df_1h['low']
    tr2 = abs(df_1h['high'] - df_1h['close'].shift(1))
    tr3 = abs(df_1h['low'] - df_1h['close'].shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    # min_periods=1 force le calcul même si données partielles
    atr_1h = tr.rolling(14, min_periods=1).mean() 

    # ADX
    plus_dm = df_1h['high'].diff()
    minus_dm = df_1h['low'].diff()
    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm > 0] = 0
    
    # On évite la division par zéro avec replace
    atr_safe = atr_1h.replace(0, 1) 
    
    plus_di = 100 * (plus_dm.ewm(alpha=1/14, min_periods=1).mean() / atr_safe)
    minus_di = 100 * (abs(minus_dm).ewm(alpha=1/14, min_periods=1).mean() / atr_safe)
    dx = (abs(plus_di - minus_di) / abs(plus_di + minus_di) * 100)
    adx_1h = dx.rolling(14, min_periods=1).mean()

    # MACD
    exp1 = df_1h['close'].ewm(span=12, adjust=False, min_periods=1).mean()
    exp2 = df_1h['close'].ewm(span=26, adjust=False, min_periods=1).mean()
    macd = exp1 - exp2
    signal = macd.ewm(span=9, adjust=False, min_periods=1).mean()

    # Bollinger
    sma20 = df_1h['close'].rolling(window=20, min_periods=1).mean()
    std = df_1h['close'].rolling(window=20, min_periods=1).std()
    bb_width = ((sma20 + 2*std) - (sma20 - 2*std)) / sma20
    bb_width = bb_width.fillna(0)

    # Trends
    ema50_1h = df_1h['close'].ewm(span=50, min_periods=1).mean().iloc[-1]
    ema200_1d = df_1d['close'].ewm(span=200, min_periods=1).mean().iloc[-1]
    current_price = df_1h['close'].iloc[-1]
    
    dist_ma200_pct = 0
    if ema200_1d > 0:
        dist_ma200_pct = ((current_price - ema200_1d) / ema200_1d) * 100

    # Order Book
    try:
        book = exchange.fetch_order_book(symbol, limit=20)
        bid = sum([b[1] for b in book['bids']])
        ask = sum([a[1] for a in book['asks']])
        ob_ratio = bid / ask if ask > 0 else 1.0
    except: ob_ratio = 1.0

    # Volume Ratio
    vol_mean = df_1h['volume'].rolling(20, min_periods=1).mean().iloc[-1]
    vol_cur = df_1h['volume'].iloc[-1]
    vol_ratio = vol_cur / vol_mean if vol_mean > 0 else 0

    return {
        "rsi": rsi_1h.iloc[-1], "adx": adx_1h.iloc[-1], "atr": atr_1h.iloc[-1],
        "macd_line": macd.iloc[-1], "macd_signal": signal.iloc[-1],
        "bb_width": bb_width.iloc[-1],
        "ema50_1h": ema50_1h, "dist_ma200": dist_ma200_pct,
        "ob_ratio": ob_ratio, "vol_ratio": vol_ratio
    }

def analyze_market_and_portfolio():
    print("🧠 Analyse V21 Robustesse...", flush=True)
    my_positions, cash_available, total_capital = get_portfolio_data()
    dynamic_list = list(set(CORE_WATCHLIST + list(my_positions.keys()) + get_dynamic_watchlist(25)))
    
    history_records = get_all_history()
    
    btc_df = get_binance_data("BTC/USDC", "1d", limit=200)
    btc_trend = "NEUTRE"
    if btc_df is not None:
        ma200 = btc_df['close'].ewm(span=200, min_periods=1).mean().iloc[-1]
        btc_trend = "BULL" if btc_df['close'].iloc[-1] > ma200 else "BEAR"
    
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
        time.sleep(1.0) 
        try:
            live_price = get_live_price(symbol)
            if live_price is None: continue
            inds = calculate_all_indicators(symbol)
            if inds is None: continue

            # --- CORRECTION ATR & RISK (V21) ---
            # Si ATR est nul ou NaN, on force une valeur par défaut (3% du prix)
            atr_val = inds["atr"]
            if pd.isna(atr_val) or atr_val <= 0:
                atr_val = live_price * 0.03
            
            stop_loss_price = live_price - (2.0 * atr_val)
            risk_per_share = live_price - stop_loss_price
            
            # Sécurité supplémentaire
            if risk_per_share <= 0: risk_per_share = live_price * 0.01

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
            
            # SCORING
            score = 0
            narrative = []
            
            trend_icon = "🔴"
            if inds["dist_ma200"] > 0: score += 30; trend_icon = "🟢"; narrative.append(f"{trend_icon} Fond Haussier (+{round(inds['dist_ma200'],1)}%)")
            else: narrative.append(f"{trend_icon} Sous MA200")

            macd_status = "Bearish"
            if inds["macd_line"] > inds["macd_signal"]: score += 10; macd_status = "Bullish"

            if 45 < inds["rsi"] < 65: score += 10; narrative.append(f"RSI Sain (MACD {macd_status})")
            elif inds["rsi"] < 30: score += 5; narrative.append(f"Survente (RSI {round(inds['rsi'])})")
            elif inds["rsi"] > 70: narrative.append(f"Surchauffe (RSI {round(inds['rsi'])})")

            if inds["bb_width"] < 0.05: score += 10; narrative.append("Squeeze Bollinger")

            if inds["adx"] > 25: score += 15; narrative.append(f"Trend Fort")
            
            if inds["ob_ratio"] > 1.5: score += 20; narrative.append(f"Buy Wall ({round(inds['ob_ratio'],1)}x)")
            elif inds["ob_ratio"] < 0.6: score -= 20; narrative.append(f"Sell Wall ({round(inds['ob_ratio'],1)}x)")

            if inds["vol_ratio"] > 1.5: score += 10; narrative.append(f"Vol {round(inds['vol_ratio'],1)}x")

            if btc_trend == "BEAR" and "BTC" not in symbol: score = max(0, score - 40); narrative.append("BTC Bear")

            # Conseil
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

            full_narrative = " | ".join(narrative)
            
            last_signal = "AUCUN"
            relevant = [r for r in history_records if r.get("Crypto") == symbol]
            if relevant: last_signal = relevant[-1].get("Signal", "AUCUN")
            
            full_signal = f"{action} {advice}".strip()
            is_new = False
            if action == "URGENT" and "URGENT" not in last_signal: is_new = True
            elif "ACHAT" in advice and "ACHAT" not in last_signal: is_new = True
            
            if is_new:
                append_history_log(symbol, live_price, full_signal, full_narrative)
                msg = f"**{symbol}** : {full_signal}\n💰 Prix: {smart_format(live_price)}\n📦 Mise: {smart_format(pos_size_usd)}\n📝 {full_narrative}"
                send_discord_alert(msg, 0x3498db)

            # R:R Calculation (Corrigé V21)
            reward = tp1 - live_price
            rr_display = 0
            if risk_per_share > 0:
                rr_display = round(reward / risk_per_share, 2)

            results.append({
                "Crypto": symbol.replace("/USDC", ""),
                "Prix": smart_format(live_price),
                "Mon_Bag": smart_format(value_owned) if value_owned > 10 else "-",
                "Conseil": advice,
                "Action": action,
                "Mise ($)": f"{smart_format(pos_size_usd)}{forced_msg}" if "ACHAT" in advice else "-",
                "Frais Est.": f"{smart_format(fees_est)}" if "ACHAT" in advice else "-",
                "Stop Loss": smart_format(stop_loss_price),
                "TP1": smart_format(tp1),
                "Score": score,
                "R:R": rr_display,
                "RSI": round(inds["rsi"], 1),
                "ADX": round(inds["adx"], 1),
                "Vol Ratio": round(inds["vol_ratio"], 1),
                "Dist MA200%": round(inds["dist_ma200"], 1),
                "OrderBook": round(inds["ob_ratio"], 2),
                "Analyse Complète 🧠": full_narrative
            })

        except Exception as e:
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
                    "Mise ($)", "Frais Est.", "Stop Loss", "TP1", "Score", "R:R",
                    "RSI", "ADX", "Vol Ratio", "Dist MA200%", "OrderBook", 
                    "Update", "Analyse Complète 🧠"]
            
            ws.clear()
            set_with_dataframe(ws, df_final[cols])
            print("🚀 Sheet V21 Robustesse mis à jour !", flush=True)
        except Exception as e:
            print(f"❌ Erreur Sheet: {e}", flush=True)

# ======================================================
# 🔄 SERVEUR
# ======================================================
def run_bot():
    print("⏳ Démarrage V21...", flush=True)
    analyze_market_and_portfolio()
    while True:
        time.sleep(UPDATE_FREQUENCY)
        analyze_market_and_portfolio()

def keep_alive():
    url = RENDER_EXTERNAL_URL
    if url:
        while True:
            time.sleep(300)
            try: requests.get(url); print("💤 Ping")
            except: pass

@app.route("/")
def index(): return "Bot V21 Robust Active"

if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))