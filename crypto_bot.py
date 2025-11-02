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
from flask import Flask

# --- Flask app pour keep-alive ---
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot actif 🚀"

# --- Google Sheets & Email Bot ---
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

info = json.loads(os.getenv("GOOGLE_SERVICE_JSON"))
creds = Credentials.from_service_account_info(info, scopes=SCOPES)
gc = gspread.authorize(creds)
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

def get_klines(symbol, interval="1h", limit=100):
    url = f"https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    data = requests.get(url, params=params).json()
    df = pd.DataFrame(data, columns=[
        "timestamp", "open", "high", "low", "close", "volume",
        "_", "__", "___", "____", "_____", "______"
    ])
    df["close"] = df["close"].astype(float)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    return df[["timestamp", "close"]]

def compute_RSI(series, period=14):
    delta = series.diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(period).mean()
    avg_loss = pd.Series(loss).rolling(period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def update_sheet():
    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet("MarketData")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title="MarketData", rows="100", cols="10")

    cryptos = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    rows = []
    for symbol in cryptos:
        df = get_klines(symbol)
        rsi = compute_RSI(df["close"])
        price = df["close"].iloc[-1]
        rows.append([symbol, price, round(rsi.iloc[-1], 2)])
    df_out = pd.DataFrame(rows, columns=["Crypto", "Prix", "RSI"])
    ws.clear()
    set_with_dataframe(ws, df_out)
    print(f"✅ {datetime.now().strftime('%H:%M:%S')} - Données mises à jour.")

# --- Boucle d’actualisation ---
def run_bot():
    while True:
        update_sheet()
        time.sleep(3600)  # toutes les heures

if __name__ == "__main__":
    import threading
    threading.Thread(target=run_bot).start()
    app.run(host="0.0.0.0", port=10000)
