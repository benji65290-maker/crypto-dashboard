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
import traceback
from datetime import datetime
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from flask import Flask

app = Flask(__name__)

# ======================================================
# ⚙️ CONFIGURATION V30 (ZERO TRUST SAFETY FIX)
# ======================================================
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")

UPDATE_FREQUENCY = 600  # 10 minutes
RISK_PER_TRADE_PCT = 0.02 
MIN_ORDER_SIZE_USD = 11.0 

CORE_WATCHLIST = ["BTC/USDC", "ETH/USDC", "SOL/USDC", "BNB/USDC"]

# ======================================================
# 🔐 CONNEXIONS
# ======================================================
print("🔐 Initialisation V30...", flush=True)

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
            'options': {'defaultType': 'spot'},
            'timeout': 30000 
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
        if val == 0: return "-"
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
                "title": "🛡️ Bot V30",
                "description": message,
                "color": color_code,
                "footer": {"text": "Zero Trust Protocol"}
            }]
        }
        requests.post(DISCORD_WEBHOOK_URL, json=data, timeout=5)
    except: pass

def get_dynamic_watchlist(all_tickers, limit=25):
    try:
        if not all_tickers: return CORE_WATCHLIST
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
        if not ohlcv or len(ohlcv) < limit: return None
        
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
    
    if not exchange: return positions, 0, 10000
    
    try:
        tickers = exchange.fetch_tickers() 
        balance = exchange.fetch_balance()
        
        usdt = float(balance['total'].get('USDT', 0))
        usdc = float(balance['total'].get('USDC', 0))
        cash_usd = usdt + usdc
        total_equity_usd = cash_usd
        
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
        
        return positions, cash_usd, total_equity_usd

    except Exception as e:
        print(f"❌ Erreur CRITIQUE Portfolio: {e}")
        return {}, 0, 0 

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
        now_str = datetime.now(paris_tz).strftime("%d/%m/%Y - %H:%M")
        ws_hist.append_row([now_str, symbol, smart_format(price), full_signal, narrative])
    except: pass

# ======================================================
# 🧠 INDICATEURS TECHNIQUES
# ======================================================
def calculate_all_indicators(symbol):
    time.sleep(1.2) 
    
    df_1h = get_binance_data(symbol, "1h")
    if df_1h is None: return None
    
    if (df_1h['close'] == 0).any(): return None

    # RSI
    delta = df_1h['close'].diff()
    rs = delta.where(delta>0,0).rolling(14).mean() / (-delta.where(delta<0,0)).rolling(14).mean()
    rsi_1h = 100 - (100 / (1 + rs))
    
    # ATR & ADX
    tr = pd.concat([df_1h['high']-df_1h['low'], abs(df_1h['high']-df_1h['close'].shift(1)), abs(df_1h['low']-df_1h['close'].shift(1))], axis=1).max(axis=1)
    atr_1h = tr.rolling(14).mean() 
    
    if atr_1h.iloc[-1] <= 0 or pd.isna(atr_1h.iloc[-1]): return None

    plus_di = 100 * (df_1h['high'].diff().clip(lower=0).ewm(alpha=1/14).mean() / atr_1h)
    minus_di = 100 * (abs(df_1h['low'].diff().clip(upper=0)).ewm(alpha=1/14).mean() / atr_1h)
    adx_1h = (abs(plus_di - minus_di) / abs(plus_di + minus_di) * 100).rolling(14).mean()

    # MACD
    exp1 = df_1h['close'].ewm(span=12, adjust=False).mean()
    exp2 = df_1h['close'].ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    signal = macd.ewm(span=9, adjust=False).mean()

    # Bollinger
    sma20 = df_1h['close'].rolling(window=20).mean()
    std = df_1h['close'].rolling(window=20).std()
    bb_upper = sma20 + (2 * std)
    bb_lower = sma20 - (2 * std)
    bb_width = ((sma20 + 2*std) - (sma20 - 2*std)) / sma20
    
    # Trends
    time.sleep(0.5)
    df_1d = get_binance_data(symbol, "1d")
    if df_1d is None: return None
    
    ema50_1h = df_1h['close'].ewm(span=50).mean().iloc[-1]
    ema200_1d = df_1d['close'].ewm(span=200).mean().iloc[-1]
    current_price = df_1h['close'].iloc[-1]
    
    dist_ma200_pct = 0
    if ema200_1d > 0:
        dist_ma200_pct = ((current_price - ema200_1d) / ema200_1d) * 100

    # Pivot Points
    last_day = df_1d.iloc[-2]
    high_d, low_d, close_d = last_day['high'], last_day['low'], last_day['close']
    pivot = (high_d + low_d + close_d) / 3
    r1, r2 = (2 * pivot) - low_d, pivot + (high_d - low_d)
    s1, s2 = (2 * pivot) - high_d, pivot - (high_d - low_d)

    # Order Book
    try:
        book = exchange.fetch_order_book(symbol, limit=20)
        bid = sum([b[1] for b in book['bids']])
        ask = sum([a[1] for a in book['asks']])
        ob_ratio = bid / ask if ask > 0 else 1.0
    except: ob_ratio = 1.0

    vol_mean = df_1h['volume'].rolling(20).mean().iloc[-1]
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
    print("🧠 Analyse V30 Zero Trust...", flush=True)
    
    all_tickers = {}
    try:
        all_tickers = exchange.fetch_tickers()
        print(f"✅ Tickers OK: {len(all_tickers)}")
    except:
        print(f"❌ Erreur Tickers - Mode dégradé")
    
    my_positions, cash_available, total_capital = get_portfolio_data()
    print(f"💰 Equity: {total_capital} $ | Cash Dispo: {cash_available} $")

    dynamic_list = list(set(CORE_WATCHLIST + list(my_positions.keys()) + get_dynamic_watchlist(all_tickers, 25)))
    history_records = get_all_history()
    
    # --- MACRO ---
    market_regime = "RANGE" 
    btc_trend = "NEUTRE"
    
    try:
        if all_tickers and "BTC/USDC" in all_tickers:
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
        "SL Déclenchement": "-", "SL Limite": "-", "Trailing Stop": "-", "TP (Cible)": "-",
        "R:R": "-", "RSI": "-", "ADX": "-", "Vol Ratio": "-", "Dist MA200%": "-",
        "Analyse Complète 🧠": f"Capital prêt: {smart_format(cash_available)}"
    })
    results.append({
        "Crypto": "🌍 MACRO", "Prix": "-", "Mon_Bag": "-", 
        "Conseil": "INFO", "Action": "", "Score": 1999, "Mise ($)": "-", "Frais Est.": "-",
        "SL Déclenchement": "-", "SL Limite": "-", "Trailing Stop": "-", "TP (Cible)": "-",
        "R:R": "-", "RSI": "-", "ADX": "-", "Vol Ratio": "-", "Dist MA200%": "-",
        "Analyse Complète 🧠": f"Mode: {market_regime} | BTC {btc_trend} | Sentiment: {fng_val}"
    })

    print(f"👉 Scan de {len(dynamic_list)} cryptos...", flush=True)
    count = 0

    for symbol in dynamic_list:
        count += 1
        print(f"🔄 [{count}/{len(dynamic_list)}] {symbol}...", flush=True)
        try:
            live_price = 0
            if all_tickers and symbol in all_tickers:
                live_price = float(all_tickers[symbol]['last'])
            else:
                live_price = get_live_price(symbol)
                
            if live_price is None or live_price == 0: 
                print(f"⚠️ PRIX MANQUANT pour {symbol}")
                continue
            
            inds = calculate_all_indicators(symbol)
            
            if inds is None:
                results.append({
                    "Crypto": symbol.replace("/USDC", ""), "Prix": smart_format(live_price), "Mon_Bag": "-",
                    "Conseil": "⚠️ DATA ERROR", "Action": "", "Score": -9999, "Mise ($)": "-", "Frais Est.": "-",
                    "SL Déclenchement": "-", "SL Limite": "-", "Trailing Stop": "-", "TP (Cible)": "-",
                    "R:R": "-", "RSI": "-", "ADX": "-", "Vol Ratio": "-", "Dist MA200%": "-",
                    "Update": "", "Analyse Complète 🧠": "Données techniques insuffisantes."
                })
                continue

            # --- STRATÉGIE ---
            score = 0
            narrative = []
            advice = "⚪ NEUTRE"
            action = ""
            
            stop_loss = live_price - (2.0 * inds["atr"])
            tp_target = inds["pivot_r1"]

            # 1. TREND
            if market_regime == "TREND":
                if btc_trend == "BEAR" and "BTC" not in symbol:
                    score = -50; narrative.append("⛔ BTC Crash")
                else:
                    if inds["dist_ma200"] > 0: score += 30; narrative.append("Trend OK")
                    if inds["adx"] > 25: score += 20; narrative.append("Force OK")
                    if inds["vol_ratio"] > 1.5: score += 10; narrative.append("Volume OK")
                    if score > 50: advice = "✅ ACHAT (Trend)"

            # 2. RANGE
            else: 
                narrative.append("Mode Range")
                support_zone = max(inds["bb_lower"], inds["pivot_s1"])
                dist_to_support = (live_price - support_zone) / live_price
                
                if abs(dist_to_support) < 0.015: 
                    score += 40; narrative.append("🟢 Support Touché")
                    stop_loss = support_zone * 0.99 
                    tp_target = inds["ema50_1h"]
                    if inds["rsi"] < 40: score += 20; narrative.append("RSI Bas")
                    if score > 50: advice = "✅ ACHAT (Rebond)"
                else:
                    stop_loss = live_price - (2.0 * inds["atr"])
                    tp_target = inds["pivot_r1"]

            risk_per_share = live_price - stop_loss
            if risk_per_share <= 0: risk_per_share = live_price * 0.001 
            
            if tp_target <= live_price: tp_target = live_price + (risk_per_share * 2.0)
            real_rr = round((tp_target - live_price) / risk_per_share, 2)

            pos_size_usd = 0
            forced_msg = ""
            risk_budget = total_capital * RISK_PER_TRADE_PCT 
            
            if "ACHAT" in advice:
                if real_rr < 1.5:
                    advice = "⚪ NEUTRE"; narrative.append(f"Annulé (R:R {real_rr} faible)")
                else:
                    pos_size_usd = (risk_budget / risk_per_share) * live_price
                    if pos_size_usd < MIN_ORDER_SIZE_USD: pos_size_usd = MIN_ORDER_SIZE_USD; forced_msg = " (Min)"
                    if pos_size_usd > cash_usd: pos_size_usd = cash_usd
            
            fees_est = pos_size_usd * 0.001

            value_owned = my_positions.get(symbol, 0) * live_price
            if value_owned > 10:
                if market_regime == "RANGE" and inds["rsi"] > 65:
                    advice = "🚨 VENDRE"; action = "PROFIT"; narrative.append("Haut du Range")
                elif market_regime == "TREND" and inds["dist_ma200"] < -2:
                    advice = "🚨 VENDRE"; action = "STOP"; narrative.append("Cassure Trend")
                else: advice = "🟢 GARDER"

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

            stop_loss_limit = stop_loss * 0.995
            trailing = inds["ema50_1h"] if live_price > inds["ema50_1h"] else stop_loss

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
                "Trailing Stop": smart_format(trailing),
                "TP (Cible)": smart_format(tp_target),    
                
                "Score": score,
                "R:R": real_rr,
                "RSI": round(inds["rsi"], 1),
                "ADX": round(inds["adx"], 1),
                "Vol Ratio": round(inds["vol_ratio"], 1),
                "Dist MA200%": round(inds["dist_ma200"], 1),
                "Update": "",
                "Analyse Complète 🧠": full_narrative
            })

        except Exception as e:
            print(f"⚠️ Erreur {symbol}: {e}")
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
            df_final["Update"] = datetime.now(paris_tz).strftime("%d/%m/%Y - %H:%M")
            
            cols = ["Crypto", "Prix", "Mon_Bag", "Conseil", "Action", 
                    "Mise ($)", "Frais Est.", 
                    "SL Déclenchement", "SL Limite", "Trailing Stop", "TP (Cible)", 
                    "Score", "R:R", "RSI", "ADX", "Vol Ratio", "Dist MA200%", 
                    "Update", "Analyse Complète 🧠"]
            
            ws.clear()
            set_with_dataframe(ws, df_final[cols])
            print(f"🚀 Sheet V30 Safety mis à jour !", flush=True)
        except Exception as e:
            print(f"❌ Erreur Ecriture Sheet: {e}", flush=True)

# ======================================================
# 🔄 SERVEUR
# ======================================================
def run_bot():
    print("⏳ Démarrage V30...", flush=True)
    analyze_market_and_portfolio()
    while True:
        time.sleep(UPDATE_FREQUENCY)
        analyze_market_and_portfolio()

def keep_alive():
    url = RENDER_EXTERNAL_URL
    if url:
        while True:
            time.sleep(300)
            try: requests.get(url)
            except: pass

@app.route("/")
def index(): return "Bot V30 Safety First Active"

if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))