import threading
import time
import requests
import pandas as pd
import numpy as np
import os
import json
import gspread
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
# ⚙️ Fonctions utilitaires
# ======================================================
def safe_round(x, n=2):
    try:
        if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))):
            return np.nan
        return round(float(x), n)
    except Exception:
        return np.nan

def ensure_numeric(df, cols):
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

# ======================================================
# 🌍 Indicateurs d’émotion (par crypto)
# ======================================================
def get_sentiment_for_symbol(symbol):
    """Récupère un score émotionnel individuel pour chaque crypto."""
    try:
        symbol = symbol.split("-")[0].lower()

        # Fear & Greed Index global
        try:
            fng_data = requests.get("https://api.alternative.me/fng/", timeout=8).json()
            fg_value = int(fng_data["data"][0]["value"])
            if fg_value < 25:
                fg_label = "😱 Extreme Fear"
            elif fg_value < 50:
                fg_label = "😟 Fear"
            elif fg_value < 75:
                fg_label = "😃 Greed"
            else:
                fg_label = "🤑 Extreme Greed"
        except:
            fg_value, fg_label = np.nan, "❌"

        # Social Sentiment (présence dans trending Coingecko)
        try:
            trending = requests.get("https://api.coingecko.com/api/v3/search/trending", timeout=8).json()
            trending_symbols = [c["item"]["symbol"].lower() for c in trending["coins"]]
            score_social = 100 if symbol in trending_symbols else 40
        except:
            score_social = np.nan

        # News Intensity via variation 24h
        try:
            data = requests.get(f"https://api.coingecko.com/api/v3/coins/{symbol}", timeout=8).json()
            change_24h = data["market_data"]["price_change_percentage_24h"] or 0
            news_intensity = min(1.0, abs(change_24h) / 10)
        except:
            news_intensity = np.nan

        # Score synthétique d’émotion
        sentiment_score = np.nanmean([
            fg_value / 100 if not np.isnan(fg_value) else np.nan,
            score_social / 100 if not np.isnan(score_social) else np.nan,
            news_intensity if not np.isnan(news_intensity) else np.nan
        ]) * 100

        return {
            "FearGreed_Index": fg_value,
            "FearGreed_Label": fg_label,
            "Social_Sentiment": score_social,
            "News_Intensity": round(news_intensity, 3),
            "Sentiment_Score": round(sentiment_score, 1)
        }

    except Exception as e:
        print(f"⚠️ Erreur get_sentiment_for_symbol({symbol}) : {e}", flush=True)
        return {
            "FearGreed_Index": np.nan,
            "FearGreed_Label": "❌",
            "Social_Sentiment": np.nan,
            "News_Intensity": np.nan,
            "Sentiment_Score": np.nan
        }

# ======================================================
# ⚙️ Récupération données Coinbase (OHLC)
# ======================================================
def get_candles(symbol_pair, granularity):
    url = f"https://api.exchange.coinbase.com/products/{symbol_pair}/candles"
    params = {"granularity": granularity}
    headers = {"User-Agent": "CryptoBot/1.0"}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=12)
        if r.status_code != 200:
            print(f"🌐 [{symbol_pair}] HTTP {r.status_code} ({granularity}s)", flush=True)
            return None
        data = r.json()
        if not data:
            return None
        df = pd.DataFrame(data, columns=["time", "low", "high", "open", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
        df = df.sort_values("time").reset_index(drop=True)
        df = ensure_numeric(df, ["low", "high", "open", "close", "volume"])
        return df
    except Exception as e:
        print(f"⚠️ Erreur get_candles({symbol_pair}, {granularity}): {e}", flush=True)
        return None

# ======================================================
# 📈 Indicateurs techniques
# ======================================================
def compute_indicators(df):
    if df is None or df.empty:
        return df

    df = df.copy()
    close, high, low, vol = df["close"], df["high"], df["low"], df["volume"]

    # RSI
    delta = close.diff()
    up, down = delta.clip(lower=0), (-delta).clip(lower=0)
    roll_up = up.ewm(alpha=1/14, adjust=False).mean()
    roll_down = down.ewm(alpha=1/14, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    df["RSI14"] = 100 - (100 / (1 + rs))

    # MACD
    ema12, ema26 = close.ewm(span=12, adjust=False).mean(), close.ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()

    # EMA Trend
    df["EMA20"], df["EMA50"] = close.ewm(span=20).mean(), close.ewm(span=50).mean()

    # Bollinger
    bb_mid, bb_std = close.rolling(20).mean(), close.rolling(20).std()
    df["BB_Mid"], df["BB_Upper"], df["BB_Lower"] = bb_mid, bb_mid + 2*bb_std, bb_mid - 2*bb_std

    # Volume & VWAP
    df["Volume_Mean"] = vol.rolling(20).mean()
    df["VWAP"] = (close * vol).cumsum() / vol.replace(0, np.nan).cumsum()

    # ATR
    prev_close = close.shift(1)
    TR = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    df["ATR14"] = TR.ewm(alpha=1/14, adjust=False).mean()

    # ADX simplifié
    up_move, down_move = high.diff(), -low.diff()
    plus_dm = np.where((up_move > 0) & (up_move > down_move), up_move, 0)
    minus_dm = np.where((down_move > 0) & (down_move > up_move), down_move, 0)
    atr14 = df["ATR14"]
    plus_di = 100 * pd.Series(plus_dm).ewm(alpha=1/14).mean() / atr14
    minus_di = 100 * pd.Series(minus_dm).ewm(alpha=1/14).mean() / atr14
    dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan))
    df["ADX"] = dx.ewm(alpha=1/14).mean()

    return df

# ======================================================
# 🧮 Analyse multi-période
# ======================================================
def summarize_last_row(df):
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else last

    trend = "Bull" if last["EMA20"] > last["EMA50"] else "Bear"
    macd_signal = "📈 Bullish" if (prev["MACD"] < prev["MACD_Signal"]) and (last["MACD"] > last["MACD_Signal"]) else \
                  "📉 Bearish" if (prev["MACD"] > prev["MACD_Signal"]) and (last["MACD"] < last["MACD_Signal"]) else "❌ Aucun"
    bb_pos = "⬆️ Surachat" if last["close"] > last["BB_Upper"] else "⬇️ Survente" if last["close"] < last["BB_Lower"] else "〰️ Neutre"

    return {
        "RSI14": safe_round(last["RSI14"]),
        "Trend": trend,
        "MACD_Cross": macd_signal,
        "Bollinger_Pos": bb_pos
    }

def analyze_symbol(symbol_pair):
    results = {}
    for label, gran in {"1h": 3600, "6h": 21600, "1d": 86400}.items():
        df = get_candles(symbol_pair, gran)
        if df is not None and len(df) >= 60:
            df = compute_indicators(df)
            results[label] = summarize_last_row(df)
    if not results:
        return None

    trends = [r["Trend"] for r in results.values()]
    bulls, bears = trends.count("Bull"), trends.count("Bear")
    consensus = "🟢 Achat fort" if bulls >= 2 else "🔴 Vente forte" if bears >= 2 else "⚪ Neutre"

    flat = {"Crypto": symbol_pair.split("-")[0], "Consensus": consensus, "LastUpdate": time.strftime("%Y-%m-%d %H:%M:%S")}
    for tf, vals in results.items():
        for k, v in vals.items():
            flat[f"{k}_{tf}"] = v
    return flat

# ======================================================
# 📊 Mise à jour Google Sheets (avec émotion par crypto)
# ======================================================
def update_sheet():
    try:
        sh = gc.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet("MultiTF")
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title="MultiTF", rows="200", cols="200")

        cryptos = ["BTC-USD", "ETH-USD", "SOL-USD", "BNB-USD", "ADA-USD", "DOGE-USD", "AVAX-USD", "XRP-USD", "LINK-USD", "MATIC-USD"]
        rows = []

        for pair in cryptos:
            res = analyze_symbol(pair)
            if res:
                sentiment = get_sentiment_for_symbol(pair)
                res.update(sentiment)
                rows.append(res)
                print(f"✅ {res['Crypto']} → {res['Consensus']}", flush=True)
            time.sleep(2)

        if not rows:
            print("⚠️ Aucune donnée récupérée", flush=True)
            return

        df_out = pd.DataFrame(rows)
        ws.clear()
        set_with_dataframe(ws, df_out)
        print("✅ Feuille 'MultiTF' mise à jour avec indicateurs émotionnels par crypto !", flush=True)

    except Exception as e:
        print(f"❌ Erreur update_sheet() : {e}", flush=True)

# ======================================================
# 🔁 Threads
# ======================================================
def run_bot():
    print("🚀 Lancement du bot Multi-Timeframe", flush=True)
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
# 🌐 Flask
# ======================================================
@app.route("/")
def home():
    return "✅ Crypto Bot Multi-Timeframe actif (1h / 6h / 1D) — indicateurs avancés et émotionnels intégrés"

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
