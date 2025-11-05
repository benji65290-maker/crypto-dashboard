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
# ⚙️ Utilitaires
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
# 🌍 Indicateurs de Sentiment & Émotion
# ======================================================
        data = r.json()
        if not data:
            return None
        df = pd.DataFrame(data, columns=["time", "low", "high", "open", "close", "volume"])
        df["time"]  = pd.to_datetime(df["time"], unit="s", utc=True)
        df = df.sort_values("time").reset_index(drop=True)
        df = ensure_numeric(df, ["low","high","open","close","volume"])
        return df
    except Exception as e:
        print(f"⚠️ Erreur get_candles({symbol_pair}, {granularity}): {e}", flush=True)
        return None

# ======================================================
# 📈 Calculs d’indicateurs techniques (base + avancés)
# ======================================================
def compute_indicators(df):
    """
    Calcule tous les indicateurs sur une copie du DataFrame (ne modifie pas l’original).
    Les NaN initiales sont normales (périodes de chauffe).
    """
    if df is None or df.empty:
        return df

    df = df.copy()
    close = df["close"]
    high  = df["high"]
    low   = df["low"]
    vol   = df["volume"]

    # ---------- RSI (Wilder) ----------
    delta = close.diff()
    up = delta.clip(lower=0)
    down = (-delta).clip(lower=0)
    # Moyennes exponentielles avec alpha=1/14 => lissage de Wilder
    roll_up = up.ewm(alpha=1/14, adjust=False).mean()
    roll_down = down.ewm(alpha=1/14, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    df["RSI14"] = 100 - (100 / (1 + rs))

    # ---------- MACD ----------
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()

    # ---------- EMA Trend ----------
    df["EMA20"] = close.ewm(span=20, adjust=False).mean()
    df["EMA50"] = close.ewm(span=50, adjust=False).mean()

    # ---------- Bollinger ----------
    bb_mid = close.rolling(20).mean()
    bb_std = close.rolling(20).std()
    df["BB_Mid"]   = bb_mid
    df["BB_Upper"] = bb_mid + 2 * bb_std
    df["BB_Lower"] = bb_mid - 2 * bb_std

    # ---------- Volume / VWAP ----------
    df["Volume_Mean"] = vol.rolling(20).mean()
    df["VWAP"] = (close * vol).cumsum() / vol.replace(0, np.nan).cumsum()

    # ---------- ATR (True Range + Wilder MA) ----------
    prev_close = close.shift(1)
    tr_components = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1)
    TR = tr_components.max(axis=1)
    # ATR Wilder
    df["ATR14"] = TR.ewm(alpha=1/14, adjust=False).mean()
    df["TR"] = TR  # utile SuperTrend/ADX

    # ---------- StochRSI (basé sur prix, rapide & indicatif) ----------
    low14 = low.rolling(14).min()
    high14 = high.rolling(14).max()
    df["StochRSI"] = ( (close - low14) / (high14 - low14) * 100 ).replace([np.inf, -np.inf], np.nan)

    # ---------- ADX (Wilder) ----------
    up_move   = high.diff()
    down_move = -low.diff()
    plus_dm  = np.where((up_move > 0) & (up_move > down_move), up_move, 0.0)
    minus_dm = np.where((down_move > 0) & (down_move > up_move), down_move, 0.0)

    atr14 = df["ATR14"]
    plus_di  = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1/14, adjust=False).mean() / atr14
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1/14, adjust=False).mean() / atr14
    dx = 100 * ( (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan) )
    df["ADX"] = dx.ewm(alpha=1/14, adjust=False).mean()

    # ---------- Ichimoku ----------
    high_9 = high.rolling(9).max()
    low_9  = low.rolling(9).min()
    df["ICH_Tenkan"] = (high_9 + low_9) / 2

    high_26 = high.rolling(26).max()
    low_26  = low.rolling(26).min()
    df["ICH_Kijun"] = (high_26 + low_26) / 2
    df["ICH_SpanA"] = (df["ICH_Tenkan"] + df["ICH_Kijun"]) / 2
    df["ICH_SpanB"] = (high.rolling(52).max() + low.rolling(52).min()) / 2

    # ---------- OBV ----------
    direction = np.sign(close.diff()).fillna(0)
    df["OBV"] = (vol * direction).cumsum()

    # ---------- MFI ----------
    typical_price = (high + low + close) / 3
    money_flow = typical_price * vol
    pos_flow = np.where(typical_price > typical_price.shift(), money_flow, 0.0)
    neg_flow = np.where(typical_price < typical_price.shift(), money_flow, 0.0)
    pos_mf = pd.Series(pos_flow, index=df.index).rolling(14).sum()
    neg_mf = pd.Series(neg_flow, index=df.index).rolling(14).sum().replace(0, np.nan)
    df["MFI"] = 100 - (100 / (1 + (pos_mf / neg_mf)))

    # ---------- SAR (simple proxy) ----------
    df["SAR"] = close.rolling(3).min().shift(1)

    # ---------- CCI ----------
    tp = (high + low + close) / 3
    df["CCI"] = (tp - tp.rolling(20).mean()) / (0.015 * tp.rolling(20).std())

    # ---------- SuperTrend (simple) ----------
    hl2 = (high + low) / 2
    st_atr = df["ATR14"]
    upper_band = hl2 + 3 * st_atr
    lower_band = hl2 - 3 * st_atr
    # Label uniquement (calcul complet de la ligne ST non inclus ici)
    df["SuperTrend"] = np.where(close > lower_band, "Bull", "Bear")

    # ---------- Donchian ----------
    df["Donchian_High"] = high.rolling(20).max()
    df["Donchian_Low"]  = low.rolling(20).min()

    # ---------- MA200 ----------
    df["MA200"] = close.rolling(200).mean()

    # ---------- Pivot / R1 / S1 (classiques) ----------
    df["Pivot"] = (high + low + close) / 3
    df["R1"] = 2 * df["Pivot"] - low
    df["S1"] = 2 * df["Pivot"] - high

    return df

# ======================================================
# 🧮 Analyse multi-période
# ======================================================
ADV_KEYS = [
    "RSI14","MACD","MACD_Signal","EMA20","EMA50",
    "BB_Mid","BB_Upper","BB_Lower","Volume_Mean","VWAP",
    "ATR14","StochRSI","ADX",
    "ICH_Tenkan","ICH_Kijun","ICH_SpanA","ICH_SpanB",
    "OBV","MFI","SAR","CCI","SuperTrend","Donchian_High","Donchian_Low","MA200",
    "Pivot","R1","S1"
]

def summarize_last_row(df):
    """Retourne un dict (valeurs dernière ligne) formaté + signaux lisibles."""
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else last

    # Signaux lisibles
    trend = "Bull" if last["EMA20"] > last["EMA50"] else "Bear"

    if (prev["MACD"] < prev["MACD_Signal"]) and (last["MACD"] > last["MACD_Signal"]):
        macd_signal = "📈 Bullish"
    elif (prev["MACD"] > prev["MACD_Signal"]) and (last["MACD"] < last["MACD_Signal"]):
        macd_signal = "📉 Bearish"
    else:
        macd_signal = "❌ Aucun"

    if last["close"] > last["BB_Upper"]:
        bb_pos = "⬆️ Surachat"
    elif last["close"] < last["BB_Lower"]:
        bb_pos = "⬇️ Survente"
    else:
        bb_pos = "〰️ Neutre"

    vol_trend = "⬆️ Volume haussier" if last["volume"] > last["Volume_Mean"] else "⬇️ Volume baissier"

    out = {
        "RSI": safe_round(last["RSI14"]),
        "Trend": trend,
        "MACD_Cross": macd_signal,
        "Bollinger_Pos": bb_pos,
        "Volume_Sentiment": vol_trend,
    }

    # Ajouter toutes les valeurs numériques clés (arrondies)
    for k in ADV_KEYS:
        v = last.get(k, np.nan)
        out[k] = safe_round(v) if k not in ["SuperTrend"] else (v if isinstance(v, str) else "N/A")

    return out

def analyze_symbol(symbol_pair):
    periods = {
        "1h": 3600,
        "6h": 21600,
        "1d": 86400
    }
    results = {}

    for label, gran in periods.items():
        df = get_candles(symbol_pair, gran)
        if df is None or len(df) < 60:
            # Besoin d’un minimum d’historique pour MA200 / Ichimoku / ADX
            print(f"⚠️ Historique insuffisant pour {symbol_pair} en {label}", flush=True)
            continue
        df = compute_indicators(df)
        results[label] = summarize_last_row(df)

    if not results:
        return None

    # Consensus simple basé sur trend EMA20/50
    trends = [v.get("Trend") for v in results.values()]
    bulls = trends.count("Bull")
    bears = trends.count("Bear")
    consensus = "🟢 Achat fort" if bulls >= 2 else "🔴 Vente forte" if bears >= 2 else "⚪ Neutre"

    # Aplatir le dict
    flat = {
        "Crypto": symbol_pair.split("-")[0],
        "Consensus": consensus,
        "LastUpdate": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    for tf, vals in results.items():
        for k, v in vals.items():
            flat[f"{k}_{tf}"] = v
    return flat


# ======================================================
# 📊 Mise à jour Google Sheets (avec ajout des indicateurs émotionnels)
# ======================================================

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
    return "✅ Crypto Bot Multi-Timeframe actif (1h / 6h / 1D) — indicateurs avancés intégrés"

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
