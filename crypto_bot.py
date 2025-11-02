import requests
import pandas as pd
import numpy as np
import os
import json
import time
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from datetime import datetime
import telegram

# ==============================
# 🔐 Chargement des variables Render
# ==============================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

try:
    info = json.loads(os.getenv("GOOGLE_SERVICE_JSON"))
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    gc = gspread.authorize(creds)
    SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
    CHAT_ID = os.getenv("CHAT_ID")
    bot = telegram.Bot(token=TELEGRAM_TOKEN)
except Exception as e:
    print(f"❌ Erreur d'initialisation des variables d'environnement : {e}")
    raise

# ==============================
# ⚙️ Fonctions Binance et Analyse
# ==============================
def get_klines(symbol, interval="1h", limit=100):
    """Récupère les données historiques sur Binance"""
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        data = requests.get(url, params=params, timeout=10).json()
        df = pd.DataFrame(data, columns=[
            "timestamp", "open", "high", "low", "close", "volume",
            "_", "__", "___", "____", "_____", "______"
        ])
        df["close"] = df["close"].astype(float)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        return df[["timestamp", "close"]]
    except Exception as e:
        print(f"⚠️ Erreur lors de la récupération de {symbol} : {e}")
        return pd.DataFrame(columns=["timestamp", "close"])

def compute_RSI(series, period=14):
    """Calcule l'indicateur RSI"""
    delta = series.diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(period).mean()
    avg_loss = pd.Series(loss).rolling(period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def signal_RSI(rsi):
    """Génère un signal simple selon le RSI"""
    if rsi.iloc[-1] < 30:
        return "🟢 ACHAT potentiel"
    elif rsi.iloc[-1] > 70:
        return "🔴 VENTE potentielle"
    else:
        return "⚪ Neutre"

# ==============================
# 📊 Mise à jour du Google Sheet
# ==============================
def update_sheet():
    """Mets à jour les onglets MarketData et MarketSignals"""
    sh = gc.open_by_key(SHEET_ID)

    # 🔹 Onglet 1 : MarketData (tous les cours)
    try:
        ws_data = sh.worksheet("MarketData")
    except gspread.exceptions.WorksheetNotFound:
        ws_data = sh.add_worksheet(title="MarketData", rows="1000", cols="10")

    # 🔹 Onglet 2 : MarketSignals (résumé RSI)
    try:
        ws_signals = sh.worksheet("MarketSignals")
    except gspread.exceptions.WorksheetNotFound:
        ws_signals = sh.add_worksheet(title="MarketSignals", rows="100", cols="10")

    cryptos = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", 
               "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "LINKUSDT", "MATICUSDT"]
    
    all_data = []
    signals = []

    for symbol in cryptos:
        df = get_klines(symbol)
        if df.empty:
            continue
        rsi = compute_RSI(df["close"])
        signal = signal_RSI(rsi)
        price = df["close"].iloc[-1]
        timestamp = df["timestamp"].iloc[-1]

        # Stockage complet pour MarketData
        df_symbol = df.copy()
        df_symbol["symbol"] = symbol
        all_data.append(df_symbol)

        # Stockage résumé pour MarketSignals
        signals.append([symbol, price, round(rsi.iloc[-1], 2), signal, timestamp])

        # Envoi Telegram si signal fort
        if "ACHAT" in signal or "VENTE" in signal:
            msg = f"{symbol} → {signal} (RSI: {round(rsi.iloc[-1],2)})"
            bot.send_message(chat_id=CHAT_ID, text=msg)
            print(f"📩 Alerte envoyée : {msg}")

    # 📈 Écriture dans MarketData (toutes les valeurs)
    if all_data:
        df_all = pd.concat(all_data, ignore_index=True)
        ws_data.clear()
        set_with_dataframe(ws_data, df_all)

    # 📊 Écriture dans MarketSignals (résumé)
    df_signals = pd.DataFrame(signals, columns=["Crypto", "Dernier Prix", "RSI", "Signal", "Horodatage"])
    ws_signals.clear()
    set_with_dataframe(ws_signals, df_signals)

    print(f"✅ {datetime.now().strftime('%H:%M:%S')} - Feuilles mises à jour ({len(signals)} cryptos).")

# ==============================
# 🚀 Boucle principale 
# ==============================
if __name__ == "__main__":
    print("🚀 Démarrage du bot crypto Render... (surveillance en continu)")
    while True:
        try:
            update_sheet()
        except Exception as e:
            print(f"❌ Erreur détectée : {e}")
        time.sleep(1800)  # toutes les 30 minutes (modifiable)
