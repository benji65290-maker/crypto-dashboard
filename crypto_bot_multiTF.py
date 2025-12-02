import threading
import time
import pandas as pd
import numpy as np
import os
import json
import gspread
import ccxt
import pytz
import requests # Pour Discord
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
print("🔐 Initialisation V11...", flush=True)

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
    """Envoie une notif sur Discord"""
    if not DISCORD_WEBHOOK_URL: return
    try:
        data = {
            "embeds": [{
                "title": "🔔 Alerte Crypto Bot",
                "description": message,
                "color": color_code,
                "footer": {"text": "Trading Automatisé V11"}
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
        
        # 1. Calcul du Cash (USDT + USDC)
        usdt = balance['total'].get('USDT', 0)
        usdc = balance['total'].get('USDC', 0)
        # On considère 1 USDT = 1 USDC = 1 USD pour simplifier l'affichage trésorerie
        cash_usd = usdt + usdc
        
        # 2. Calcul des positions cryptos
        for asset, amount in balance['total'].items():
            if amount > 0:
                price = 0
                if asset in ["USDT", "USDC", "USD"]:
                    price = 1.0
                else:
                    pair_usdc = f"{asset}/USDC"
                    pair_usdt = f"{asset}/USDT"
                    if pair_usdc in tickers: price = tickers[pair_usdc]['last']
                    elif pair_usdt in tickers: price = tickers[pair_usdt]['last']
                
                val_usd = amount * price
                if val_usd > 1:
                    total_equity_usd += val_usd
                    # On stocke pour le matching
                    positions[f"{asset}/USDC"] = amount
                    
        return positions, cash_usd, total_equity_usd
    except Exception as e:
        print(f"⚠️ Erreur Portfolio: {e}")
        return {}, 0, 10000

# ======================================================
# 📜 GESTION HISTORIQUE (Anti-Spam & Journal)
# ======================================================
def check_history_and_alert(symbol, new_action, new_advice, price, reason):
    """
    Vérifie l'historique dans Google Sheet.
    Si le signal a changé depuis la dernière fois => Alerte Discord + Nouvelle Ligne Journal
    """
    try:
        sh = gc.open_by_key(SHEET_ID)
        try: 
            ws_hist = sh.worksheet("Journal_Trading")
        except: 
            ws_hist = sh.add_worksheet("Journal_Trading", 1000, 10)
            ws_hist.append_row(["Date", "Crypto", "Prix", "Ancien_Signal", "Nouveau_Signal", "Raison"])

        # Récupération de l'historique existant
        records = ws_hist.get_all_records()
        last_signal = "AUCUN"
        
        # On cherche la dernière entrée pour cette crypto
        # (On parcourt à l'envers pour trouver le plus récent)
        for row in reversed(records):
            if row.get("Crypto") == symbol:
                last_signal = row.get("Nouveau_Signal")
                break
        
        # Logique de changement de signal
        # On alerte seulement si l'action change (ex: Rien -> URGENT ou Rien -> ACHAT)
        is_alert_worthy = False
        color = 0x95a5a6 # Gris par défaut

        if new_action == "URGENT" and "URGENT" not in last_signal:
            is_alert_worthy = True
            color = 0xe74c3c # Rouge
        elif new_action == "" and new_advice == "🔥 ACHAT FORT" and "ACHAT FORT" not in last_signal:
            is_alert_worthy = True
            color = 0x2ecc71 # Vert
            
        # Si changement d'état important, on enregistre et on notifie
        if is_alert_worthy:
            paris_tz = pytz.timezone('Europe/Paris')
            now_str = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M")
            
            # 1. Ecrire dans le Sheet
            ws_hist.append_row([now_str, symbol, smart_format(price), last_signal, f"{new_action} {new_advice}", reason])
            
            # 2. Envoyer Discord
            msg = f"**{symbol}** : {new_action} {new_advice}\n💰 Prix: {smart_format(price)}\n🧠 Raison: {reason}"
            send_discord_alert(msg, color)
            print(f"🔔 Alerte envoyée pour {symbol}")

    except Exception as e:
        print(f"⚠️ Erreur Journal: {e}")

# ======================================================
# 🧠 INDICATEURS
# ======================================================
def calculate_indicators(df_1h, df_1d):
    delta = df_1h['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    rs = gain.rolling(14).mean() / loss.rolling(14).mean()
    rsi = 100 - (100 / (1 + rs))
    
    ema200_1d = df_1d['close'].ewm(span=200).mean()
    
    tr1 = df_1h['high'] - df_1h['low']
    tr2 = abs(df_1h['high'] - df_1h['close'].shift(1))
    tr3 = abs(df_1h['low'] - df_1h['close'].shift(1))
    atr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1).rolling(14).mean()

    plus_dm = df_1h['high'].diff()
    minus_dm = df_1h['low'].diff()
    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm > 0] = 0
    plus_di = 100 * (plus_dm.ewm(alpha=1/14).mean() / atr)
    minus_di = 100 * (abs(minus_dm).ewm(alpha=1/14).mean() / atr)
    dx = (abs(plus_di - minus_di) / abs(plus_di + minus_di)) * 100
    adx = dx.rolling(14).mean()

    return {
        "rsi": rsi.iloc[-1],
        "adx": adx.iloc[-1],
        "atr": atr.iloc[-1],
        "ema200_1d": ema200_1d.iloc[-1],
        "vol_mean": df_1h['volume'].rolling(20).mean().iloc[-1],
        "vol_cur": df_1h['volume'].iloc[-1]
    }

def analyze_market_and_portfolio():
    print("🧠 Analyse V11...", flush=True)
    my_positions, cash_available, total_capital = get_portfolio_data()
    print(f"💰 Cash: {smart_format(cash_available)} | Total: {smart_format(total_capital)}")
    
    results = []
    
    # Ligne Spéciale TRÉSORERIE (Sera la première ligne du tableau)
    results.append({
        "Crypto": "💰 TRÉSORERIE",
        "Prix": "-",
        "Mon_Bag": smart_format(cash_available),
        "Conseil": "LIQUIDITÉS DISPO",
        "Action": "",
        "Score": 999, # Pour être toujours en haut
        "Trend": "-",
        "RSI": "-",
        "Analyse Complète 🧠": f"Capital prêt à être investi: {smart_format(cash_available)}"
    })

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
            stop_loss = live_price - (2.0 * inds["atr"])
            take_profit = live_price + (3.0 * inds["atr"])
            
            # --- INTELLIGENCE ---
            score = 0
            details = []
            
            # Trend
            trend_fond = "🔴 BAISSE"
            if live_price > inds["ema200_1d"]:
                trend_fond = "🟢 HAUSSE"
                score += 30
                details.append("Fond Haussier")
            else:
                details.append("Sous MA200")

            # RSI & ADX
            if inds["rsi"] > 70: details.append("RSI Surchauffe")
            elif inds["rsi"] < 30: score += 10; details.append("RSI Survente")
            elif 45 < inds["rsi"] < 65: score += 15
            
            if inds["adx"] > 25: score += 15; details.append("Trend Fort")
            
            # Volume
            vol_ratio = inds["vol_cur"] / inds["vol_mean"] if inds["vol_mean"] > 0 else 0
            if vol_ratio > 1.5: score += 20; details.append(f"Vol x{round(vol_ratio,1)}")

            if market_trend == "BEAR" and "BTC" not in symbol: score = max(0, score - 30)

            # --- CONSEIL ---
            amount_owned = my_positions.get(symbol, 0)
            value_owned = amount_owned * live_price
            
            advice = "⚪ NEUTRE"
            action = ""

            if value_owned > 10:
                if trend_fond == "🔴 BAISSE" and score < 45:
                    advice = "🚨 VENDRE"
                    action = "URGENT"
                elif score > 70: advice = "🟢 GARDER"
                else: advice = "🟠 SURVEILLER"
            else:
                if market_trend == "BEAR": advice = "⛔ ATTENDRE"
                elif score > 80 and inds["adx"] > 25:
                    advice = "🔥 ACHAT FORT"
                    details.insert(0, "🎯 Sniper")
                elif score > 60: advice = "✅ ACHAT"

            # --- GESTION HISTORIQUE & ALERTE ---
            # On vérifie si on doit alerter Discord
            check_history_and_alert(
                symbol.replace("/USDC", ""), 
                action, 
                advice, 
                live_price, 
                " | ".join(details)
            )

            # --- OUTPUT ---
            results.append({
                "Crypto": symbol.replace("/USDC", ""),
                "Prix": smart_format(live_price),
                "Mon_Bag": smart_format(value_owned) if value_owned > 10 else "-",
                "Conseil": advice,
                "Action": action,
                "Stop_Loss ($)": smart_format(stop_loss),
                "Take_Profit ($)": smart_format(take_profit),
                "Score": score,
                "Trend": trend_fond,
                "RSI": round(inds["rsi"], 1),
                "Analyse Complète 🧠": " | ".join(details)
            })

        except Exception as e:
            print(f"⚠️ Skip {symbol}: {e}")

    # ECRITURE SHEET
    if results:
        try:
            sh = gc.open_by_key(SHEET_ID)
            try: ws = sh.worksheet("PortfolioManager")
            except: ws = sh.add_worksheet("PortfolioManager", 100, 20)
            
            df = pd.DataFrame(results)
            df = df.sort_values(by=["Score"], ascending=False) # Tri par score
            # On remet la ligne Cash en premier
            df_cash = df[df["Crypto"] == "💰 TRÉSORERIE"]
            df_others = df[df["Crypto"] != "💰 TRÉSORERIE"]
            df_final = pd.concat([df_cash, df_others])
            
            paris_tz = pytz.timezone('Europe/Paris')
            df_final["Mise_à_jour"] = datetime.now(paris_tz).strftime("%H:%M")
            
            cols = ["Crypto", "Prix", "Mon_Bag", "Conseil", "Action", 
                    "Stop_Loss ($)", "Take_Profit ($)", 
                    "Score", "Trend", "RSI", "Mise_à_jour", "Analyse Complète 🧠"]
            
            ws.clear()
            set_with_dataframe(ws, df_final[cols])
            print("🚀 Sheet V11 mis à jour !", flush=True)
        except Exception as e:
            print(f"❌ Erreur Sheet: {e}", flush=True)

# ======================================================
# 🔄 SERVEUR
# ======================================================
def run_bot():
    print("⏳ Démarrage V11...", flush=True)
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
def index(): return "Bot V11 Commander Active"

if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))