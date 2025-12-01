import threading
import time
import requests
import pandas as pd
import numpy as np
import os
import json
import math
import gspread
from datetime import datetime, timezone
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from flask import Flask

app = Flask(__name__)

# ======================================================
# ⚙️ CONFIGURATION & CONSTANTES
# ======================================================
# Capital total fictif pour le calcul de position (à ajuster)
TOTAL_CAPITAL = 500
# Risque max par trade (1% du capital)
RISK_PER_TRADE_PCT = 0.01 

CB_BASE = "https://api.exchange.coinbase.com"
PRODUCTS = {
    "BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD",
    "BNB": "BNB-USD", "ADA": "ADA-USD", "DOGE": "DOGE-USD",
    "AVAX": "AVAX-USD", "XRP": "XRP-USD", "LINK": "LINK-USD",
    "MATIC": "MATIC-USD", "DOT": "DOT-USD", "LTC": "LTC-USD",
    "ATOM": "ATOM-USD", "UNI": "UNI-USD", "NEAR": "NEAR-USD"
}

# ======================================================
# 🔐 AUTHENTIFICATION GOOGLE
# ======================================================
print("🔐 Initialisation des credentials Google...", flush=True)
try:
    info = json.loads(os.getenv("GOOGLE_SERVICE_JSON"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
    print("✅ Credentials Google OK", flush=True)
except Exception as e:
    print(f"❌ Erreur credentials Google : {e}", flush=True)
    raise SystemExit()

# ======================================================
# 🧠 MOTEUR D'ANALYSE TECHNIQUE
# ======================================================

def get_candles(product_id: str, granularity=3600, limit=300):
    """Récupère les bougies avec gestion d'erreur robuste."""
    try:
        url = f"{CB_BASE}/products/{product_id}/candles"
        # Granularité: 3600=1h, 21600=6h, 86400=1d
        r = requests.get(url, params={"granularity": granularity}, timeout=10)
        if r.status_code == 429:
            print(f"⚠️ Rate Limit Coinbase sur {product_id}, pause 2s...", flush=True)
            time.sleep(2)
            return None
        if r.status_code != 200:
            return None
        data = r.json()
        if not data:
            return None
        
        df = pd.DataFrame(data, columns=["ts", "low", "high", "open", "close", "volume"])
        df["ts"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert(None)
        df = df.sort_values("ts").reset_index(drop=True)
        return df.astype(float)
    except Exception as e:
        print(f"⚠️ Erreur get_candles({product_id}): {e}", flush=True)
        return None

# --- Indicateurs Mathématiques ---
def rsi(series, period=14):
    delta = series.diff()
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    roll_up = pd.Series(gain).rolling(period).mean()
    roll_down = pd.Series(loss).rolling(period).mean()
    rs = roll_up / roll_down
    return 100 - (100 / (1 + rs))

def ema(series, span):
    return series.ewm(span=span, adjust=False).mean()

def atr(df, period=14):
    high_low = df["high"] - df["low"]
    high_close = np.abs(df["high"] - df["close"].shift())
    low_close = np.abs(df["low"] - df["close"].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = ranges.max(axis=1)
    return true_range.rolling(period).mean()

def bollinger_squeeze(df, period=20):
    """Détecte si la volatilité est compressée (signe de mouvement imminent)."""
    sma = df["close"].rolling(period).mean()
    std = df["close"].rolling(period).std()
    upper = sma + (2 * std)
    lower = sma - (2 * std)
    bandwidth = (upper - lower) / sma
    # Si la largeur de bande est historiquement basse (< 5% environ pour crypto)
    is_squeeze = bandwidth < 0.10 
    return bandwidth, is_squeeze

def detect_divergence(df, rsi_col="RSI14"):
    """Détection basique de divergence haussière (Prix bas + RSI haut)."""
    if len(df) < 15: return False
    # On regarde les 10 dernières bougies
    price = df["close"].iloc[-10:]
    rsi_vals = df[rsi_col].iloc[-10:]
    
    # Logique simplifiée : Prix fait un plus bas, mais RSI ne fait pas de plus bas
    if price.iloc[-1] < price.iloc[0] and rsi_vals.iloc[-1] > rsi_vals.iloc[0]:
        return True # Divergence Haussière Potentielle
    return False

# ======================================================
# 📊 LOGIQUE DE TRADING & RISQUE
# ======================================================

def analyze_crypto(symbol, pid):
    """Analyse complète multi-timeframe pour une crypto."""
    
    # 1. Récupération des données (1H, 6H, 1D)
    tf_configs = {"1h": 3600, "6h": 21600, "1d": 86400}
    data_store = {}
    
    for tf_name, granularity in tf_configs.items():
        df = get_candles(pid, granularity=granularity)
        if df is None: continue
        
        # Calculs Indicateurs
        df["EMA20"] = ema(df["close"], 20)
        df["EMA50"] = ema(df["close"], 50)
        df["EMA200"] = ema(df["close"], 200)
        df["RSI14"] = rsi(df["close"], 14)
        df["ATR14"] = atr(df, 14)
        
        # Bollinger Squeeze
        df["BB_Width"], df["Squeeze"] = bollinger_squeeze(df)
        
        data_store[tf_name] = df
        time.sleep(0.3) # Petit délai API

    if "1h" not in data_store or "1d" not in data_store:
        return None

    # 2. Analyse Multi-Timeframe
    df_1h = data_store["1h"]
    df_1d = data_store["1d"]
    
    current_price = df_1h["close"].iloc[-1]
    atr_1h = df_1h["ATR14"].iloc[-1]
    rsi_1h = df_1h["RSI14"].iloc[-1]
    
    # --- FILTRES DE TENDANCE (VETO) ---
    # Si on est sous la EMA200 Daily, tendance de fond baissière
    trend_long = "BEAR" if df_1d["close"].iloc[-1] < df_1d["EMA200"].iloc[-1] else "BULL"
    
    # --- CALCUL SCORE (0-100) ---
    score = 0
    
    # Tendance 1D (+30pts)
    if trend_long == "BULL": score += 30
    
    # Tendance 1H (+20pts) - Alignement court terme
    if df_1h["close"].iloc[-1] > df_1h["EMA50"].iloc[-1]: score += 20
    
    # Momentum RSI (+20pts)
    if 45 < rsi_1h < 65: score += 20 # Zone de poussée saine
    
    # Divergence (+15pts)
    has_divergence = detect_divergence(df_1h)
    if has_divergence: score += 15
    
    # Volatilité Squeeze (+15pts)
    is_squeezing = df_1h["Squeeze"].iloc[-1]
    if is_squeezing: score += 15

    # --- GESTION DU RISQUE PRO ---
    # Stop Loss Technique : Sous le dernier creux significatif ou via ATR
    # Ici méthode ATR "Chandelier Exit" : 2.5 x ATR
    sl_dist = atr_1h * 2.5
    stop_loss_price = current_price - sl_dist
    take_profit_price = current_price + (sl_dist * 2) # Ratio 1:2
    
    # Position Sizing : Combien de tokens acheter ?
    # Formule : (Capital * %Risque) / (Prix Entrée - Prix SL)
    risk_amount_usd = TOTAL_CAPITAL * RISK_PER_TRADE_PCT # ex: 100$
    price_diff = current_price - stop_loss_price
    
    if price_diff > 0:
        position_size_tokens = risk_amount_usd / price_diff
        position_size_usd = position_size_tokens * current_price
    else:
        position_size_usd = 0 # Erreur de calcul ou SL > Prix
        
    # Limite de sécurité sur la taille de position (max 20% du capital total sur un trade)
    position_size_usd = min(position_size_usd, TOTAL_CAPITAL * 0.20)

    # --- SIGNAL FINAL ---
    signal = "NEUTRE"
    if score >= 70 and trend_long == "BULL": signal = "ACHAT FORT"
    elif score >= 50 and trend_long == "BULL": signal = "ACHAT FAIBLE"
    elif score < 30: signal = "VENTE"
    if trend_long == "BEAR" and score > 60: signal = "REBOND (RISQUÉ)"

    return {
        "Crypto": symbol,
        "Prix": current_price,
        "Trend_D1": trend_long,
        "Score": score,
        "Signal": signal,
        "RSI_1H": round(rsi_1h, 1),
        "Divergence": "OUI" if has_divergence else "NON",
        "Squeeze": "OUI" if is_squeezing else "NON",
        "Stop_Loss": round(stop_loss_price, 4),
        "Take_Profit": round(take_profit_price, 4),
        "Position_USD": round(position_size_usd, 0), # Le montant à investir
        "Risk_Reward": "1:2"
    }

# ======================================================
# 🔄 BOUCLE PRINCIPALE & EXPORT
# ======================================================
def update_sheet():
    print("🧠 Début analyse marché...", flush=True)
    try:
        sh = gc.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet("MultiTF") # Utilise l'onglet existant
        except:
            ws = sh.add_worksheet(title="MultiTF", rows="100", cols="20")
            
        results = []
        for sym, pid in PRODUCTS.items():
            print(f"👉 Analyse {sym}...", flush=True)
            data = analyze_crypto(sym, pid)
            if data:
                results.append(data)
                print(f"   ✅ {sym} Score: {data['Score']}/100 - Signal: {data['Signal']}")
            
        if not results: return

        df_out = pd.DataFrame(results)
        # Réorganiser les colonnes pour la lisibilité
        cols = ["Crypto", "Prix", "Signal", "Score", "Trend_D1", "Position_USD", 
                "Stop_Loss", "Take_Profit", "RSI_1H", "Divergence", "Squeeze"]
        df_out = df_out[cols]
        
        # Ajout timestamp
        df_out["Update"] = datetime.now(timezone.utc).strftime("%H:%M")

        ws.clear()
        set_with_dataframe(ws, df_out)
        print("✅ Google Sheet mis à jour avec succès.", flush=True)

    except Exception as e:
        print(f"❌ Erreur update_sheet(): {e}", flush=True)

def run_bot():
    print("🚀 Lancement du bot...", flush=True)
    update_sheet()
    while True:
        print("⏳ Pause 1h...", flush=True)
        time.sleep(3600)
        update_sheet()

def keep_alive():
    # Petit serveur web pour que Render ne coupe pas le bot
    url = os.getenv("RENDER_EXTERNAL_URL")
    if url:
        while True:
            time.sleep(600)
            try: requests.get(url)
            except: pass

@app.route("/")
def index(): return "Bot Actif"

if __name__ == "__main__":
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))