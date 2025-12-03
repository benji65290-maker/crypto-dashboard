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
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")

UPDATE_FREQUENCY = 900  # 15 minutes

# Liste de base (toujours surveillée)
CORE_WATCHLIST = ["BTC/USDC", "ETH/USDC", "SOL/USDC", "BNB/USDC"]

# ======================================================
# 🔐 CONNEXIONS
# ======================================================
print("🔐 Initialisation V15 (Scanner)...", flush=True)

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
# 🛠️ OUTILS & SCANNER
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
                "title": "📡 Scanner V15",
                "description": message,
                "color": color_code,
                "footer": {"text": "Analyse Volatilité & Volume"}
            }]
        }
        requests.post(DISCORD_WEBHOOK_URL, json=data)
    except Exception as e:
        print(f"⚠️ Erreur Discord: {e}")

def get_dynamic_watchlist(limit=25):
    """Récupère les cryptos avec le plus de volume sur 24h"""
    try:
        # On récupère tous les tickers
        tickers = exchange.fetch_tickers()
        # On filtre pour garder uniquement les paires USDC (pour être cohérent)
        # Si peu de volume en USDC, on peut basculer sur USDT et convertir l'affichage
        pairs = []
        for symbol, data in tickers.items():
            if "/USDC" in symbol and "quoteVolume" in data:
                pairs.append((symbol, data['quoteVolume']))
        
        # Tri par volume décroissant
        pairs.sort(key=lambda x: x[1], reverse=True)
        
        # On garde le top X
        top_pairs = [p[0] for p in pairs[:limit]]
        
        # On fusionne avec la liste de base + portefeuille utilisateur (géré plus tard)
        final_list = list(set(CORE_WATCHLIST + top_pairs))
        return final_list
    except Exception as e:
        print(f"⚠️ Erreur Scanner: {e}")
        return CORE_WATCHLIST

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
# 📜 HISTORIQUE
# ======================================================
def check_history_and_alert(symbol, new_action, new_advice, price, reason, entry, rr):
    try:
        sh = gc.open_by_key(SHEET_ID)
        try: ws_hist = sh.worksheet("Journal_Trading")
        except: ws_hist = sh.add_worksheet("Journal_Trading", 1000, 10); ws_hist.append_row(["Date", "Crypto", "Prix", "Signal", "Raison"])

        records = ws_hist.get_all_records()
        last_signal = "AUCUN"
        # Optimisation recherche
        relevant_records = [r for r in records if r.get("Crypto") == symbol]
        if relevant_records:
            last_signal = relevant_records[-1].get("Signal", "AUCUN")
        
        full_signal = f"{new_action} {new_advice}".strip()
        is_alert_worthy = False
        color = 0x95a5a6

        # Alerte si changement significatif
        if new_action == "URGENT" and "URGENT" not in last_signal:
            is_alert_worthy = True; color = 0xe74c3c
        elif "ACHAT FORT" in new_advice and "ACHAT FORT" not in last_signal:
            is_alert_worthy = True; color = 0x2ecc71
            
        if is_alert_worthy:
            paris_tz = pytz.timezone('Europe/Paris')
            now_str = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M")
            ws_hist.append_row([now_str, symbol, smart_format(price), full_signal, reason])
            
            msg = f"**{symbol}** : {full_signal}\n"
            msg += f"💰 Prix: {smart_format(price)}\n"
            msg += f"🎯 Entrée: {entry}\n"
            msg += f"⚖️ R:R: {rr}\n"
            msg += f"🧠 {reason}"
            send_discord_alert(msg, color)
            print(f"🔔 Alerte {symbol}")

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
    
    # ADX & ATR
    tr = pd.concat([df_1h['high']-df_1h['low'], abs(df_1h['high']-df_1h['close'].shift(1)), abs(df_1h['low']-df_1h['close'].shift(1))], axis=1).max(axis=1).rolling(14).mean()
    atr_1h = tr
    plus_di = 100 * (df_1h['high'].diff().clip(lower=0).ewm(alpha=1/14).mean() / atr_1h)
    minus_di = 100 * (abs(df_1h['low'].diff().clip(upper=0)).ewm(alpha=1/14).mean() / atr_1h)
    adx_1h = (abs(plus_di - minus_di) / abs(plus_di + minus_di) * 100).rolling(14).mean()

    # Trends
    ema50_1h = df_1h['close'].ewm(span=50).mean().iloc[-1]
    ema200_1d = df_1d['close'].ewm(span=200).mean().iloc[-1]

    # Order Book
    try:
        book = exchange.fetch_order_book(symbol, limit=20)
        bid = sum([b[1] for b in book['bids']])
        ask = sum([a[1] for a in book['asks']])
        ob_ratio = bid / ask if ask > 0 else 1.0
    except: ob_ratio = 1.0

    return {
        "rsi_1h": rsi_1h.iloc[-1],
        "adx_1h": adx_1h.iloc[-1],
        "atr_1h": atr_1h.iloc[-1],
        "ema50_1h": ema50_1h,
        "ema200_1d": ema200_1d,
        "ob_ratio": ob_ratio
    }

def analyze_market_and_portfolio():
    print("🧠 Analyse V15 Scanner...", flush=True)
    my_positions, cash_available, total_capital = get_portfolio_data()
    
    # Construction Liste Dynamique : Core + Positions + Top Volume
    dynamic_list = list(set(CORE_WATCHLIST + list(my_positions.keys()) + get_dynamic_watchlist(20)))
    
    # Macro
    btc_df = get_binance_data("BTC/USDC", "1d", limit=200)
    btc_trend = "BULL" if btc_df['close'].iloc[-1] > btc_df['close'].ewm(span=200).mean().iloc[-1] else "BEAR"
    
    # Sentiment
    try:
        fng_r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=3).json()
        fng_val = int(fng_r['data'][0]['value'])
    except: fng_val = 50

    results = []
    
    # Header Info
    results.append({
        "Crypto": "💰 TRÉSORERIE", "Prix": "-", "Mon_Bag": smart_format(cash_available), 
        "Conseil": "CAPITAL", "Action": "", "Score": 1000, "R:R": "-",
        "Analyse Complète 🧠": f"Cash: {smart_format(cash_available)}"
    })
    results.append({
        "Crypto": "🌍 MACRO", "Prix": "-", "Mon_Bag": "-", 
        "Conseil": "INFO", "Action": "", "Score": 999, "R:R": "-",
        "Analyse Complète 🧠": f"BTC {'🐻' if btc_trend=='BEAR' else '🐂'} | F&G: {fng_val}"
    })

    for symbol in dynamic_list:
        try:
            live_price = get_live_price(symbol)
            if live_price is None: continue
            inds = calculate_advanced_indicators(symbol)
            if inds is None: continue

            # --- STRATÉGIE SNIPER ---
            # Stop Loss (2 ATR)
            stop_loss = live_price - (2.0 * inds["atr_1h"])
            risk = live_price - stop_loss
            
            # Cible (3 ATR)
            take_profit = live_price + (3.0 * inds["atr_1h"])
            reward = take_profit - live_price
            
            # Ratio Risque/Récompense
            rr_ratio = round(reward / risk, 2) if risk > 0 else 0
            
            # Entrée Optimale (EMA50 1H)
            smart_entry = inds["ema50_1h"]
            entry_str = smart_format(smart_entry) if live_price > smart_entry else "Marché"

            # --- SCORING ---
            score = 0
            details = []
            
            # Trend 1D
            trend_1d = "🔴"
            if live_price > inds["ema200_1d"]:
                trend_1d = "🟢"
                score += 30
            else: details.append("Sous MA200")

            # Pressure
            if inds["ob_ratio"] > 1.5: score += 20; details.append("Buy Wall")
            elif inds["ob_ratio"] < 0.6: score -= 20; details.append("Sell Wall")
            
            # RSI & ADX
            if 45 < inds["rsi_1h"] < 65: score += 10
            elif inds["rsi_1h"] < 30: score += 5; details.append("Oversold")
            
            if inds["adx_1h"] > 25: score += 15
            
            # Filtre R:R (Nouveau !)
            if rr_ratio < 2.0: 
                score -= 10
                details.append(f"Bad R:R ({rr_ratio})")

            if btc_trend == "BEAR" and "BTC" not in symbol: score = max(0, score - 40)

            # --- CONSEIL ---
            value_owned = my_positions.get(symbol, 0) * live_price
            advice = "⚪ NEUTRE"; action = ""

            if value_owned > 10:
                if trend_1d == "🔴" and score < 40: advice = "🚨 VENDRE"; action = "URGENT"
                elif score > 70: advice = "🟢 GARDER"
                else: advice = "🟠 SURVEILLER"
            else:
                if btc_trend == "BEAR":
                    advice = "⛔ ATTENDRE"
                elif score > 80 and inds["adx_1h"] > 25 and rr_ratio >= 2.0:
                    advice = "🔥 ACHAT FORT"
                    details.insert(0, "✅ Sniper")
                elif score > 60: advice = "✅ ACHAT"

            check_history_and_alert(symbol.replace("/USDC", ""), action, advice, live_price, " | ".join(details), entry_str, rr_ratio)

            results.append({
                "Crypto": symbol.replace("/USDC", ""),
                "Prix": smart_format(live_price),
                "Mon_Bag": smart_format(value_owned) if value_owned > 10 else "-",
                "Conseil": advice,
                "Action": action,
                "Entrée Opti": entry_str,
                "TP (Cible)": smart_format(take_profit),
                "R:R": rr_ratio, # Nouvelle Colonne
                "Score": score,
                "Trend": trend_1d,
                "Analyse Complète 🧠": " | ".join(details)
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
                    "Entrée Opti", "TP (Cible)", "R:R", 
                    "Score", "Trend", "Update", "Analyse Complète 🧠"]
            
            ws.clear()
            set_with_dataframe(ws, df_final[cols])
            print("🚀 Sheet V15 mis à jour !", flush=True)
        except Exception as e:
            print(f"❌ Erreur Sheet: {e}", flush=True)

# ======================================================
# 🔄 MAIN
# ======================================================
def run_bot():
    print("⏳ Démarrage V15...", flush=True)
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
def index(): return "Bot V15 Scanner Active"

if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))