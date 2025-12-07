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
# ⚙️ CONFIGURATION V26 (CAMELEON OPTIMISÉ)
# ======================================================
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")

# Fréquence ajustée pour capter les ranges (10 min)
UPDATE_FREQUENCY = 600  
RISK_PER_TRADE_PCT = 0.02 
MIN_ORDER_SIZE_USD = 11.0 

CORE_WATCHLIST = ["BTC/USDC", "ETH/USDC", "SOL/USDC", "BNB/USDC"]

# ======================================================
# 🔐 CONNEXIONS
# ======================================================
print("🔐 Initialisation V26...", flush=True)

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
        else: return f"{value:.8f}{suffix}"
    except: return "-"

def send_discord_alert(message, color_code=0x3498db):
    if not DISCORD_WEBHOOK_URL: return
    try:
        data = {
            "embeds": [{
                "title": "🦎 Caméléon V26",
                "description": message,
                "color": color_code,
                "footer": {"text": "Adaptive Strategy: Trend & Range"}
            }]
        }
        requests.post(DISCORD_WEBHOOK_URL, json=data)
    except: pass

def get_dynamic_watchlist(all_tickers, limit=25):
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
# 🧠 INDICATEURS TECHNIQUES
# ======================================================
def calculate_all_indicators(symbol):
    time.sleep(1.0) # Pause Anti-Ban
    
    df_1h = get_binance_data(symbol, "1h", limit=500)
    if df_1h is None: return None
    
    time.sleep(0.5)
    df_1d = get_binance_data(symbol, "1d", limit=200)
    if df_1d is None: return None

    # RSI & ATR
    delta = df_1h['close'].diff()
    rs = delta.where(delta>0,0).rolling(14, min_periods=1).mean() / (-delta.where(delta<0,0)).rolling(14, min_periods=1).mean()
    rsi_1h = 100 - (100 / (1 + rs))
    
    tr = pd.concat([df_1h['high']-df_1h['low'], abs(df_1h['high']-df_1h['close'].shift(1)), abs(df_1h['low']-df_1h['close'].shift(1))], axis=1).max(axis=1)
    atr_1h = tr.rolling(14, min_periods=1).mean() 

    # ADX
    atr_safe = atr_1h.replace(0, 1) 
    plus_di = 100 * (df_1h['high'].diff().clip(lower=0).ewm(alpha=1/14, min_periods=1).mean() / atr_safe)
    minus_di = 100 * (abs(df_1h['low'].diff().clip(upper=0)).ewm(alpha=1/14, min_periods=1).mean() / atr_safe)
    adx_1h = (abs(plus_di - minus_di) / abs(plus_di + minus_di) * 100).rolling(14, min_periods=1).mean()

    # MACD
    exp1 = df_1h['close'].ewm(span=12, adjust=False, min_periods=1).mean()
    exp2 = df_1h['close'].ewm(span=26, adjust=False, min_periods=1).mean()
    macd = exp1 - exp2
    signal = macd.ewm(span=9, adjust=False, min_periods=1).mean()

    # Bollinger
    sma20 = df_1h['close'].rolling(window=20, min_periods=1).mean()
    std = df_1h['close'].rolling(window=20, min_periods=1).std()
    bb_upper = sma20 + (2 * std)
    bb_lower = sma20 - (2 * std)
    bb_width = ((sma20 + 2*std) - (sma20 - 2*std)) / sma20
    bb_width = bb_width.fillna(0)

    # Trends
    ema50_1h = df_1h['close'].ewm(span=50, min_periods=1).mean().iloc[-1]
    ema200_1d = df_1d['close'].ewm(span=200, min_periods=1).mean().iloc[-1]
    current_price = df_1h['close'].iloc[-1]
    dist_ma200_pct = ((current_price - ema200_1d) / ema200_1d) * 100

    # Pivot Points
    last_day = df_1d.iloc[-2]
    high_d, low_d, close_d = last_day['high'], last_day['low'], last_day['close']
    pivot = (high_d + low_d + close_d) / 3
    r1, r2 = (2 * pivot) - low_d, pivot + (high_d - low_d)
    s1, s2 = (2 * pivot) - high_d, pivot - (high_d - low_d)

    # Order Book & Volume
    try:
        book = exchange.fetch_order_book(symbol, limit=20)
        bid = sum([b[1] for b in book['bids']])
        ask = sum([a[1] for a in book['asks']])
        ob_ratio = bid / ask if ask > 0 else 1.0
    except: ob_ratio = 1.0

    vol_mean = df_1h['volume'].rolling(20, min_periods=1).mean().iloc[-1]
    vol_cur = df_1h['volume'].iloc[-1]
    vol_ratio = vol_cur / vol_mean if vol_mean > 0 else 0

    return {
        "rsi": rsi_1h.iloc[-1], "adx": adx_1h.iloc[-1], "atr": atr_1h.iloc[-1],
        "macd_line": macd.iloc[-1], "macd_signal": signal.iloc[-1],
        "bb_width": bb_width.iloc[-1], "bb_lower": bb_lower.iloc[-1], "bb_upper": bb_upper.iloc[-1],
        "ema50_1h": ema50_1h, "dist_ma200": dist_ma200_pct,
        "ob_ratio": ob_ratio, "vol_ratio": vol_ratio,
        "pivot_r1": r1, "pivot_r2": r2, "pivot_s1": s1
    }

def analyze_market_and_portfolio():
    print("🧠 Analyse V26 Caméléon...", flush=True)
    
    try: all_tickers = exchange.fetch_tickers()
    except: return

    my_positions, cash_available, total_capital = get_portfolio_data()
    dynamic_list = list(set(CORE_WATCHLIST + list(my_positions.keys()) + get_dynamic_watchlist(all_tickers, 25)))
    history_records = get_all_history()
    
    # --- ANALYSE DE RÉGIME ---
    market_regime = "RANGE" 
    btc_trend = "NEUTRE"
    
    # On regarde si BTC est en tendance ou en range
    try:
        btc_price = float(all_tickers["BTC/USDC"]["last"])
        # On utilise une logique simplifiée pour le régime global sans ré-analyser tout le BTC en lourd
        # Si le prix bouge de plus de 1% en 24h, on peut considérer qu'il y a du mouvement
        change_24h = float(all_tickers["BTC/USDC"]["percentage"])
        if abs(change_24h) > 2.0: market_regime = "TREND"
        if change_24h > 0: btc_trend = "BULL"
        elif change_24h < 0: btc_trend = "BEAR"
    except: pass
    
    try: fng_val = int(requests.get("https://api.alternative.me/fng/?limit=1", timeout=3).json()['data'][0]['value'])
    except: fng_val = 50

    results = []
    
    results.append({
        "Crypto": "💰 TRÉSORERIE", "Prix": "-", "Mon_Bag": smart_format(cash_available), 
        "Conseil": "CAPITAL", "Action": "", "Score": 2000, "Mise ($)": "-", "Frais Est.": "-",
        "Analyse Complète 🧠": f"Capital prêt: {smart_format(cash_available)}"
    })
    
    regime_icon = "🏎️" if market_regime == "TREND" else "🦀"
    results.append({
        "Crypto": "🌍 MACRO", "Prix": "-", "Mon_Bag": "-", 
        "Conseil": "INFO", "Action": "", "Score": 1999, "Mise ($)": "-", "Frais Est.": "-",
        "Analyse Complète 🧠": f"Mode: {market_regime} {regime_icon} | BTC {btc_trend} | Sentiment: {fng_val}"
    })

    for symbol in dynamic_list:
        try:
            live_price = float(all_tickers[symbol]['last']) if symbol in all_tickers else 0
            if live_price == 0: continue
            
            inds = calculate_all_indicators(symbol)
            if inds is None: continue

            # --- DÉCISION HYBRIDE V26 ---
            score = 0
            narrative = []
            advice = "⚪ NEUTRE"
            action = ""
            
            # Paramètres Risk
            atr_val = inds["atr"] if pd.notna(inds["atr"]) and inds["atr"] > 0 else live_price * 0.03
            
            # 1. MODE TREND
            if market_regime == "TREND":
                stop_loss = live_price - (2.0 * atr_val)
                tp_target = inds["pivot_r2"] # On vise haut
                
                if inds["dist_ma200"] > 0: score += 30; narrative.append("Trend OK")
                if inds["adx"] > 25: score += 20; narrative.append("Force OK")
                if inds["vol_ratio"] > 1.5: score += 10; narrative.append("Volume OK")
                
                if btc_trend == "BEAR" and "BTC" not in symbol: 
                    score = -50; narrative.append("⛔ BTC Bear")
                
                if score > 50: advice = "✅ ACHAT (Trend)"

            # 2. MODE RANGE (Le "Ping Pong")
            else:
                narrative.append("Mode Range")
                # On achète le bas du range (Bollinger Low ou Pivot S1)
                support_zone = max(inds["bb_lower"], inds["pivot_s1"])
                dist_to_support = (live_price - support_zone) / live_price
                
                # Si on est proche du support (< 1.5%)
                if abs(dist_to_support) < 0.015:
                    score += 40
                    narrative.append("🟢 Support Zone")
                    stop_loss = support_zone * 0.99 # SL très serré
                    tp_target = inds["ema50_1h"] # TP prudent (milieu du terrain)
                    
                    if inds["rsi"] < 40: score += 20; narrative.append("RSI Bas")
                    if inds["ob_ratio"] > 1.2: score += 10; narrative.append("Buy Wall")
                    
                    if score > 50: advice = "✅ ACHAT (Rebond)"
                else:
                    stop_loss = live_price - (2.0 * atr_val) # Défaut
                    tp_target = inds["pivot_r1"]

            # --- GESTION RISQUE COMMUN ---
            risk_per_share = live_price - stop_loss
            if risk_per_share <= 0: risk_per_share = live_price * 0.01
            
            # Si TP trop proche du prix (Range très serré), on annule
            if tp_target <= live_price: tp_target = live_price + (risk_per_share * 2.0)
            
            real_rr = round((tp_target - live_price) / risk_per_share, 2)

            # Taille Position
            risk_budget = total_equity_usd * RISK_PER_TRADE_PCT 
            pos_size_usd = 0
            forced_msg = ""
            
            if "ACHAT" in advice:
                if real_rr < 1.5:
                    advice = "⚪ NEUTRE"
                    narrative.append(f"Annulé (R:R {real_rr} faible)")
                else:
                    pos_size_usd = (risk_budget / risk_per_share) * live_price
                    if pos_size_usd < MIN_ORDER_SIZE_USD: pos_size_usd = MIN_ORDER_SIZE_USD; forced_msg = " (Min)"
                    if pos_size_usd > cash_usd: pos_size_usd = cash_usd
            
            fees_est = pos_size_usd * 0.001

            # --- SORTIE ---
            value_owned = my_positions.get(symbol, 0) * live_price
            if value_owned > 10:
                if market_regime == "RANGE" and inds["rsi"] > 65:
                    advice = "🚨 VENDRE"; action = "PROFIT"; narrative.append("Haut du Range")
                elif inds["dist_ma200"] < -3 and score < 40:
                    advice = "🚨 VENDRE"; action = "STOP"; narrative.append("Cassure Trend")
                else:
                    advice = "🟢 GARDER"

            # Alerting
            full_narrative = " | ".join(narrative)
            last_signal = "AUCUN"
            relevant = [r for r in history_records if r.get("Crypto") == symbol]
            if relevant: last_signal = relevant[-1].get("Signal", "AUCUN")
            
            full_signal = f"{action} {advice}".strip()
            is_new = False
            if (action != "" and action not in last_signal) or ("ACHAT" in advice and "ACHAT" not in last_signal):
                is_new = True
            
            if is_new:
                append_history_log(symbol, live_price, full_signal, full_narrative)
                msg = f"**{symbol}** : {full_signal}\n💰 {smart_format(live_price)}\n🎯 Mode: {market_regime}\n📝 {full_narrative}"
                send_discord_alert(msg, 0x3498db)

            # --- NOMS EXACTS BINANCE (V24.1) + V26 ---
            stop_loss_limit = stop_loss * 0.995
            
            results.append({
                "Crypto": symbol.replace("/USDC", ""),
                "Prix": smart_format(live_price),
                "Mon_Bag": smart_format(value_owned) if value_owned > 10 else "-",
                "Conseil": advice,
                "Action": action,
                "Mise ($)": f"{smart_format(pos_size_usd)}{forced_msg}" if "ACHAT" in advice else "-",
                "Frais Est.": f"{smart_format(fees_est)}" if "ACHAT" in advice else "-",
                
                "SL Déclenchement": smart_format(stop_loss), 
                "SL Limite": smart_format(stop_loss_limit),         
                "Trailing Stop": smart_format(inds["ema50_1h"]),
                "TP (Cible)": smart_format(tp_target),    
                
                "Score": score,
                "R:R": real_rr,
                "RSI": round(inds["rsi"], 1),
                "ADX": round(inds["adx"], 1),
                "Vol Ratio": round(inds["vol_ratio"], 1),
                "Dist MA200%": round(inds["dist_ma200"], 1),
                "OrderBook": round(inds["ob_ratio"], 2),
                "Update": "",
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
            df_final = pd.concat([df[df["Score"] >= 1999], df[df["Score"] < 1999]])
            
            paris_tz = pytz.timezone('Europe/Paris')
            df_final["Update"] = datetime.now(paris_tz).strftime("%H:%M")
            
            cols = ["Crypto", "Prix", "Mon_Bag", "Conseil", "Action", 
                    "Mise ($)", "Frais Est.", 
                    "SL Déclenchement", "SL Limite", "Trailing Stop", "TP (Cible)", 
                    "Score", "R:R", "RSI", "ADX", "Vol Ratio", "Dist MA200%", "OrderBook",
                    "Update", "Analyse Complète 🧠"]
            
            ws.clear()
            set_with_dataframe(ws, df_final[cols])
            print(f"🚀 Sheet V26 Caméléon mis à jour (Mode {market_regime}) !", flush=True)
        except Exception as e:
            print(f"❌ Erreur Sheet: {e}", flush=True)

# ======================================================
# 🔄 SERVEUR
# ======================================================
def run_bot():
    print("⏳ Démarrage V26...", flush=True)
    analyze_market_and_portfolio()
    while True:
        time.sleep(UPDATE_FREQUENCY)
        analyze_market_and_portfolio()

def keep_alive():
    url = RENDER_EXTERNAL_URL
    if url:
        while True:
            time.sleep(300); requests.get(url)
            except: pass

@app.route("/")
def index(): return "Bot V26 Chameleon Active"

if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))