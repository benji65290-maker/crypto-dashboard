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
# ⚙️ CONFIGURATION V25.1 (STRATÈGE PRÉCIS)
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
print("🔐 Initialisation V25.1...", flush=True)

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
                "title": "🛡️ Stratège V25.1",
                "description": message,
                "color": color_code,
                "footer": {"text": "Analyse Macro & Micro"}
            }]
        }
        requests.post(DISCORD_WEBHOOK_URL, json=data)
    except: pass

def get_dynamic_watchlist(all_tickers, limit=20):
    try:
        pairs = []
        for symbol, data in all_tickers.items():
            if "/USDC" in symbol and "quoteVolume" in data:
                pairs.append((symbol, float(data['quoteVolume'])))
        pairs.sort(key=lambda x: x[1], reverse=True)
        top_pairs = [p[0] for p in pairs[:limit]]
        return list(set(CORE_WATCHLIST + top_pairs))
    except: return CORE_WATCHLIST

def get_binance_data(symbol, timeframe, limit=200):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].astype(float)
        return df
    except: return None

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
# 🧠 INDICATEURS COMPLETS
# ======================================================
def calculate_all_indicators(symbol):
    # Pause sécurité Anti-Ban
    time.sleep(2.0)
    
    df_1h = get_binance_data(symbol, "1h")
    if df_1h is None: return None
    
    time.sleep(1.0)
    df_1d = get_binance_data(symbol, "1d")
    if df_1d is None: return None

    # RSI & ATR
    delta = df_1h['close'].diff()
    rs = delta.where(delta>0,0).rolling(14).mean() / (-delta.where(delta<0,0)).rolling(14).mean()
    rsi_1h = 100 - (100 / (1 + rs))
    
    tr = pd.concat([df_1h['high']-df_1h['low'], abs(df_1h['high']-df_1h['close'].shift(1)), abs(df_1h['low']-df_1h['close'].shift(1))], axis=1).max(axis=1)
    atr_1h = tr.rolling(14).mean() 

    # ADX
    atr_safe = atr_1h.replace(0, 1) 
    plus_di = 100 * (df_1h['high'].diff().clip(lower=0).ewm(alpha=1/14).mean() / atr_safe)
    minus_di = 100 * (abs(df_1h['low'].diff().clip(upper=0)).ewm(alpha=1/14).mean() / atr_safe)
    adx_1h = (abs(plus_di - minus_di) / abs(plus_di + minus_di) * 100).rolling(14).mean()

    # MACD
    exp1 = df_1h['close'].ewm(span=12, adjust=False).mean()
    exp2 = df_1h['close'].ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    signal = macd.ewm(span=9, adjust=False).mean()

    # Bollinger
    sma20 = df_1h['close'].rolling(window=20).mean()
    std = df_1h['close'].rolling(window=20).std()
    bb_width = ((sma20 + 2*std) - (sma20 - 2*std)) / sma20
    bb_width = bb_width.fillna(0)

    # Trends
    ema50_1h = df_1h['close'].ewm(span=50).mean().iloc[-1]
    ema200_1d = df_1d['close'].ewm(span=200).mean().iloc[-1]
    current_price = df_1h['close'].iloc[-1]
    dist_ma200_pct = ((current_price - ema200_1d) / ema200_1d) * 100

    # Pivot Points
    last_day = df_1d.iloc[-2]
    high_d, low_d, close_d = last_day['high'], last_day['low'], last_day['close']
    pivot = (high_d + low_d + close_d) / 3
    r1, r2 = (2 * pivot) - low_d, pivot + (high_d - low_d)

    # Volume Ratio
    vol_mean = df_1h['volume'].rolling(20).mean().iloc[-1]
    vol_cur = df_1h['volume'].iloc[-1]
    vol_ratio = vol_cur / vol_mean if vol_mean > 0 else 0

    return {
        "rsi": rsi_1h.iloc[-1], "adx": adx_1h.iloc[-1], "atr": atr_1h.iloc[-1],
        "macd_line": macd.iloc[-1], "macd_signal": signal.iloc[-1],
        "bb_width": bb_width.iloc[-1],
        "ema50_1h": ema50_1h, "dist_ma200": dist_ma200_pct,
        "vol_ratio": vol_ratio,
        "pivot_r1": r1, "pivot_r2": r2
    }

def analyze_market_and_portfolio():
    print("🧠 Analyse V25.1 Stratège...", flush=True)
    
    # 1. Tickers Uniques
    try: all_tickers = exchange.fetch_tickers()
    except: return

    # 2. Portefeuille & Cash
    positions = {}
    cash_usd = 0.0
    total_equity_usd = 0.0
    try:
        balance = exchange.fetch_balance()
        usdt = float(balance['total'].get('USDT', 0))
        usdc = float(balance['total'].get('USDC', 0))
        cash_usd = usdt + usdc
        
        for asset, amount in balance['total'].items():
            amount = float(amount)
            if amount > 0 and asset not in ["USDT", "USDC"]:
                pair_usdc = f"{asset}/USDC"
                if pair_usdc in all_tickers: 
                    price = float(all_tickers[pair_usdc]['last'])
                    val_usd = amount * price
                    if val_usd > 1:
                        total_equity_usd += val_usd
                        positions[pair_usdc] = amount
        total_equity_usd += cash_usd
    except: pass

    # 3. Watchlist & Historique
    dynamic_list = list(set(CORE_WATCHLIST + list(positions.keys()) + get_dynamic_watchlist(all_tickers, 20)))
    history_records = get_all_history()
    
    # 4. Tendance BTC (Calcul Robuste)
    btc_trend = "NEUTRE"
    btc_ma200 = 0
    try:
        # On fait un appel spécifique pour le BTC Daily car c'est critique
        time.sleep(1.0)
        btc_df = get_binance_data("BTC/USDC", "1d", limit=200)
        if btc_df is not None:
            btc_ma200 = btc_df['close'].ewm(span=200).mean().iloc[-1]
            btc_price = btc_df['close'].iloc[-1]
            btc_trend = "BULL" if btc_price > btc_ma200 else "BEAR"
    except: pass
    
    try: fng_val = int(requests.get("https://api.alternative.me/fng/?limit=1", timeout=3).json()['data'][0]['value'])
    except: fng_val = 50

    results = []
    
    # Lignes Headers (Score artificiel pour le tri)
    results.append({
        "Crypto": "💰 TRÉSORERIE", "Prix": "-", "Mon_Bag": smart_format(cash_usd), 
        "Conseil": "CAPITAL", "Action": "", "Score": 2000, "Mise ($)": "-", "Frais Est.": "-",
        "Analyse Complète 🧠": f"Capital prêt: {smart_format(cash_usd)}"
    })
    
    macro_icon = "🐻" if btc_trend == "BEAR" else "🐂"
    results.append({
        "Crypto": "🌍 MACRO", "Prix": "-", "Mon_Bag": "-", 
        "Conseil": "INFO", "Action": "", "Score": 1999, "Mise ($)": "-", "Frais Est.": "-",
        "Analyse Complète 🧠": f"BTC {btc_trend} {macro_icon} | Sentiment: {fng_val}/100"
    })

    for symbol in dynamic_list:
        try:
            live_price = float(all_tickers[symbol]['last']) if symbol in all_tickers else 0
            if live_price == 0: continue
            
            inds = calculate_all_indicators(symbol)
            if inds is None: continue

            # --- ANALYSE DE FOND ---
            score = 0
            narrative = []
            
            # Trend
            if inds["dist_ma200"] > 0: 
                score += 30; narrative.append("🟢 Fond Haussier")
            else: 
                narrative.append("🔴 Fond Baissier")

            # Momentum
            macd_status = "Bearish"
            if inds["macd_line"] > inds["macd_signal"]: 
                score += 10; macd_status = "Bullish"
            
            if 45 < inds["rsi"] < 65: 
                score += 10; narrative.append(f"RSI Sain (MACD {macd_status})")
            elif inds["rsi"] < 30: 
                score += 5; narrative.append(f"Survente (RSI {round(inds['rsi'])})")
            elif inds["rsi"] > 70: 
                narrative.append(f"Surchauffe (RSI {round(inds['rsi'])})")

            # Volatilité
            if inds["bb_width"] < 0.05: 
                score += 10; narrative.append("⚡ Squeeze Bollinger")

            # Force
            if inds["adx"] > 25: 
                score += 15; narrative.append(f"Trend Fort")
            
            # Volume
            if inds["vol_ratio"] > 1.5: 
                score += 10; narrative.append(f"Vol {round(inds['vol_ratio'],1)}x")

            # Filtre Macro BTC
            if btc_trend == "BEAR" and "BTC" not in symbol: 
                score = max(0, score - 40)
                narrative.append("⚠️ BTC Bear")

            # --- DÉCISION CONSEIL ---
            value_owned = positions.get(symbol, 0) * live_price
            advice = "⚪ NEUTRE"; action = ""

            # Stratégie si on possède
            if value_owned > 10:
                if inds["dist_ma200"] < 0 and score < 40: 
                    advice = "🚨 VENDRE"; action = "URGENT"
                elif score > 70: 
                    advice = "🟢 GARDER"
                else: 
                    advice = "🟠 SURVEILLER"
            
            # Stratégie si on ne possède pas
            else:
                if btc_trend == "BEAR": 
                    advice = "⛔ ATTENDRE"
                elif score > 80 and inds["adx"] > 25: 
                    advice = "🔥 ACHAT FORT"
                elif score > 60: 
                    advice = "✅ ACHAT"
                else:
                    advice = "⚪ NEUTRE" # Retour à la normale si pas de signal

            # --- EXECUTION ---
            atr_val = inds["atr"] if pd.notna(inds["atr"]) and inds["atr"] > 0 else live_price * 0.03
            stop_loss_trigger = live_price - (2.0 * atr_val)
            stop_loss_limit = stop_loss_trigger * 0.995 
            trailing = inds["ema50_1h"] if live_price > inds["ema50_1h"] else stop_loss_trigger

            risk_per_share = live_price - stop_loss_trigger
            if risk_per_share <= 0: risk_per_share = live_price * 0.01

            target_price = inds["pivot_r1"]
            if target_price < live_price: target_price = inds["pivot_r2"]
            
            real_rr = round((target_price - live_price) / risk_per_share, 2) if risk_per_share > 0 else 0

            # Taille Position
            risk_budget = total_equity_usd * RISK_PER_TRADE_PCT 
            pos_size_usd = 0
            forced_msg = ""
            if risk_per_share > 0:
                pos_size_usd = (risk_budget / risk_per_share) * live_price
            if pos_size_usd > 0:
                if pos_size_usd < MIN_ORDER_SIZE_USD: pos_size_usd = MIN_ORDER_SIZE_USD; forced_msg = " (Min)"
                if pos_size_usd > cash_usd: pos_size_usd = cash_usd
            fees_est = pos_size_usd * 0.001

            # Construction Phrase Analyse
            full_narrative = " | ".join(narrative)
            
            # Alerting
            last_signal = "AUCUN"
            relevant = [r for r in history_records if r.get("Crypto") == symbol]
            if relevant: last_signal = relevant[-1].get("Signal", "AUCUN")
            
            full_signal = f"{action} {advice}".strip()
            is_new = False
            if action == "URGENT" and "URGENT" not in last_signal: is_new = True
            elif "ACHAT" in advice and "ACHAT" not in last_signal: is_new = True
            
            if is_new:
                append_history_log(symbol, live_price, full_signal, full_narrative)
                msg = f"**{symbol}** : {full_signal}\n💰 {smart_format(live_price)}\n🎯 SL: {smart_format(stop_loss_trigger)}\n📝 {full_narrative}"
                send_discord_alert(msg, 0x3498db)

            results.append({
                "Crypto": symbol.replace("/USDC", ""),
                "Prix": smart_format(live_price),
                "Mon_Bag": smart_format(value_owned) if value_owned > 10 else "-",
                "Conseil": advice,
                "Action": action,
                "Mise ($)": f"{smart_format(pos_size_usd)}{forced_msg}" if "ACHAT" in advice else "-",
                "Frais Est.": f"{smart_format(fees_est)}" if "ACHAT" in advice else "-",
                "SL Déclenchement": smart_format(stop_loss_trigger), 
                "SL Limite": smart_format(stop_loss_limit),         
                "Trailing Stop": smart_format(trailing),
                "TP (Cible)": smart_format(target_price),    
                "Score": score,
                "R:R": real_rr,
                "RSI": round(inds["rsi"], 1),
                "ADX": round(inds["adx"], 1),
                "Vol Ratio": round(inds["vol_ratio"], 1),
                "Dist MA200%": round(inds["dist_ma200"], 1),
                "Update": "", # Sera rempli après
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
            # Tri intelligent : Score décroissant
            df = df.sort_values(by=["Score"], ascending=False)
            
            # On force les lignes Headers en haut
            df_headers = df[df["Score"] >= 1999]
            df_content = df[df["Score"] < 1999]
            df_final = pd.concat([df_headers, df_content])
            
            paris_tz = pytz.timezone('Europe/Paris')
            df_final["Update"] = datetime.now(paris_tz).strftime("%H:%M")
            
            cols = ["Crypto", "Prix", "Mon_Bag", "Conseil", "Action", 
                    "Mise ($)", "Frais Est.", 
                    "SL Déclenchement", "SL Limite", "Trailing Stop", "TP (Cible)", 
                    "Score", "R:R", "RSI", "ADX", "Vol Ratio", "Dist MA200%", 
                    "Update", "Analyse Complète 🧠"]
            
            ws.clear()
            set_with_dataframe(ws, df_final[cols])
            print("🚀 Sheet V25.1 Stratège mis à jour !", flush=True)
        except Exception as e:
            print(f"❌ Erreur Sheet: {e}", flush=True)

# ======================================================
# 🔄 SERVEUR
# ======================================================
def run_bot():
    print("⏳ Démarrage V25.1...", flush=True)
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
def index(): return "Bot V25.1 Active"

if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))