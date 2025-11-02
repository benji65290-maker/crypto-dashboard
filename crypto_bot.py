import requests
import pandas as pd
import numpy as np
import os
import json
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

# ==============================
# 🔐 Google Sheets
# ==============================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
info = json.loads(os.getenv("GOOGLE_SERVICE_JSON"))
creds = Credentials.from_service_account_info(info, scopes=SCOPES)
gc = gspread.authorize(creds)
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

# ==============================
# 📧 Email alert settings
# ==============================
EMAIL_SENDER = os.getenv("EMAIL_SENDER")         # ex : "benjamin@gmail.com"
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")     # mot de passe d’application
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")     # destinataire

def send_email(subject, body):
    """Envoie un email via SMTP Gmail."""
    msg = MIMEMultipart()
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECEIVER
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
        print(f"📨 Email envoyé : {subject}")
    except Exception as e:
        print(f"❌ Erreur envoi mail : {e}")

# ==============================
# ⚙️ Binance API + RSI
# ==============================
def get_klines(symbol, interval="1h", limit=100):
    url = "https://api.binance.com/api/v3/klines"
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

def signal_RSI(rsi):
    if rsi.iloc[-1] < 30:
        return "🟢 ACHAT potentiel"
    elif rsi.iloc[-1] > 70:
        return "🔴 VENTE potentielle"
    else:
        return "⚪ Neutre"

# ==============================
# 🧾 Mise à jour Google Sheet
# ==============================
def update_sheet():
    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet("MarketSignals")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title="MarketSignals", rows="100", cols="10")

    cryptos = ["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT",
               "ADAUSDT","AVAXUSDT","DOGEUSDT","LINKUSDT","MATICUSDT"]

    rows = []
    for symbol in cryptos:
        df = get_klines(symbol)
        rsi = compute_RSI(df["close"])
        signal = signal_RSI(rsi)
        price = df["close"].iloc[-1]
        rows.append([symbol, price, round(rsi.iloc[-1],2), signal])
        if "ACHAT" in signal or "VENTE" in signal:
            send_email(
                f"Signal {symbol} : {signal}",
                f"{symbol} → {signal}\nRSI : {round(rsi.iloc[-1],2)}\nPrix actuel : {price}"
            )

    df_out = pd.DataFrame(rows, columns=["Crypto","Dernier Prix","RSI","Signal"])
    ws.clear()
    set_with_dataframe(ws, df_out)
    print(f"✅ {datetime.now().strftime('%H:%M:%S')} - Feuille mise à jour.")

# ==============================
# 🚀 Boucle principale
# ==============================
if __name__ == "__main__":
    print("🚀 Démarrage du bot crypto Render (Email Alerts)...")
    while True:
        try:
            update_sheet()
        except Exception as e:
            print(f"❌ Erreur : {e}")
        time.sleep(3600)
