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
# 🔐 Authentification Google Sheets
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
# ⚙️ Fonctions utilitaires d’analyse crypto
# ======================================================
CB_BASE = "https://api.exchange.coinbase.com"
PRODUCTS = {
    "BTC": "BTC-USD",
    "ETH": "ETH-USD",
    "SOL": "SOL-USD",
    "BNB": "BNB-USD",
    "ADA": "ADA-USD",
    "DOGE": "DOGE-USD",
    "AVAX": "AVAX-USD",
    "XRP": "XRP-USD",
    "LINK": "LINK-USD",
    "MATIC": "MATIC-USD",
}

def get_candles(product_id: str, granularity=3600, limit=300):
    """Récupère les 300 dernières bougies horaires (OHLCV) sur Coinbase."""
    try:
        url = f"{CB_BASE}/products/{product_id}/candles"
        r = requests.get(url, params={"granularity": granularity}, timeout=10)
        if r.status_code != 200:
            print(f"🌐 [{product_id}] Status {r.status_code}", flush=True)
            return None
        data = r.json()
        if not data:
            return None
        df = pd.DataFrame(data, columns=["ts", "low", "high", "open", "close", "volume"])
        df["ts"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert(None)
        df = df.sort_values("ts").reset_index(drop=True)
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = df[c].astype(float)
        return df.tail(limit)
    except Exception as e:
        print(f"⚠️ Erreur get_candles({product_id}): {e}", flush=True)
        return None

# === Calculs d’indicateurs techniques ===
def ema(series, span): return series.ewm(span=span, adjust=False).mean()

def rsi(series, period=14):
    delta = series.diff()
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    roll_up = pd.Series(gain, index=series.index).rolling(period).mean()
    roll_down = pd.Series(loss, index=series.index).rolling(period).mean()
    rs = roll_up / roll_down
    return 100 - (100 / (1 + rs))

def macd(series, fast=12, slow=26, signal=9):
    ema_fast, ema_slow = ema(series, fast), ema(series, slow)
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

def bollinger(series, period=20, stds=2.0):
    ma = series.rolling(period).mean()
    sd = series.rolling(period).std()
    return ma, ma + stds * sd, ma - stds * sd

def atr(df, period=14):
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        (df["high"] - df["low"]).abs(),
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()

# ======================================================
# 📊 Mise à jour Google Sheets
# ======================================================
def update_sheet():
    print("🧠 Début update_sheet()", flush=True)
    try:
        sh = gc.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet("MarketData")
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title="MarketData", rows="200", cols="20")

        rows = []
        now = datetime.now(timezone.utc).astimezone().replace(microsecond=0)

        for sym, pid in PRODUCTS.items():
            df = get_candles(pid)
            if df is None or df.empty:
                print(f"⚠️ Pas de données pour {sym}", flush=True)
                continue

            close = df["close"]
            rsi14 = rsi(close, 14)
            ema20, ema50, ema200 = ema(close, 20), ema(close, 50), ema(close, 200)
            macd_line, macd_sig, macd_hist = macd(close)
            bb_mid, bb_up, bb_lo = bollinger(close, 20, 2)
            atr14 = atr(df, 14)

            # Variation sur 24 dernières bougies (1h = 24h)
            var24 = ((close.iloc[-1] / close.iloc[-24]) - 1) * 100 if len(close) > 24 else np.nan
            trend = "Bull" if ema20.iloc[-1] > ema50.iloc[-1] else "Bear"

            rows.append([
                sym,
                round(close.iloc[-1], 6),
                round(float(rsi14.iloc[-1]), 2),
                round(float(macd_line.iloc[-1]), 6),
                round(float(macd_sig.iloc[-1]), 6),
                round(float(macd_hist.iloc[-1]), 6),
                round(float(ema20.iloc[-1]), 6),
                round(float(ema50.iloc[-1]), 6),
                round(float(ema200.iloc[-1]), 6),
                round(float(bb_mid.iloc[-1]), 6),
                round(float(bb_up.iloc[-1]), 6),
                round(float(bb_lo.iloc[-1]), 6),
                round(float(atr14.iloc[-1]), 6),
                round(float(var24), 2) if not math.isnan(var24) else None,
                trend,
                now.isoformat()
            ])
            print(f"✅ {sym} → OK ({trend})", flush=True)
            time.sleep(1.5)  # anti-rate-limit Coinbase

        if not rows:
            print("⚠️ Aucune donnée récupérée.", flush=True)
            return

        df_out = pd.DataFrame(rows, columns=[
            "Crypto", "Price",
            "RSI14",
            "MACD", "MACD_Signal", "MACD_Hist",
            "EMA20", "EMA50", "EMA200",
            "BB_Mid", "BB_Upper", "BB_Lower",
            "ATR14",
            "Var24h_pct",
            "Trend",
            "LastUpdate"
        ])

        ws.clear()
        set_with_dataframe(ws, df_out)
        print(f"✅ Feuille MarketData mise à jour à {time.strftime('%H:%M:%S')}.", flush=True)

    except Exception as e:
        print(f"❌ Erreur update_sheet(): {e}", flush=True)

# ======================================================
# 🔁 Threads
# ======================================================
def run_bot():
    print("🚀 Lancement du bot principal", flush=True)
    update_sheet()
    while True:
        print("⏳ Attente avant prochaine mise à jour (1h)...", flush=True)
        time.sleep(3600)
        update_sheet()

def keep_alive():
    url = os.getenv("RENDER_EXTERNAL_URL", "https://crypto-dashboard-8tn8.onrender.com")
    while True:
        try:
            requests.get(url, timeout=10)
            print("💤 Ping keep-alive envoyé.", flush=True)
        except Exception as e:
            print(f"⚠️ Erreur keep_alive : {e}", flush=True)
        time.sleep(600)

# ======================================================
# 🌐 Flask routes
# ======================================================
@app.route("/")
def home():
    return "✅ Crypto bot actif avec indicateurs Coinbase + Google Sheets."

@app.route("/run")
def manual_run():
    threading.Thread(target=update_sheet, daemon=True).start()
    return "🧠 Mise à jour manuelle lancée !"

# ======================================================
# 🧠 Lancement
# ======================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=port)
