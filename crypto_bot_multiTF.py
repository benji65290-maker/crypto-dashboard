# crypto_bot_multiTF_V5_1.py
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
from datetime import datetime
import pytz

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
    if not SHEET_ID:
        raise RuntimeError("Env var GOOGLE_SHEET_ID manquante.")
    print("✅ Credentials Google OK", flush=True)
except Exception as e:
    print(f"❌ Erreur credentials Google : {e}", flush=True)
    raise SystemExit()

# ======================================================
# ⚙️ Utilitaires
# ======================================================
def safe_round(x, n=1):
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

def pct(a, b):
    try:
        a = float(a); b = float(b)
        if b == 0: return np.nan
        return (a - b) / b
    except Exception:
        return np.nan

def now_paris_str():
    tz = pytz.timezone("Europe/Paris")
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

# ======================================================
# 🎨 Pastilles (uniquement)
# ======================================================
def dot_green():  return "🟢"
def dot_yellow(): return "🟡"
def dot_white():  return "⚪"
def dot_orange(): return "🟠"
def dot_red():    return "🔴"

# ======================================================
# 🏷️ Labels — UI clean (texte + pastille uniquement)
# ======================================================
def label_rsi(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return f"Insuffisant {dot_white()}"
    v = float(v)
    if v < 30:       return f"{v:.1f} {dot_green()}"
    elif v < 45:     return f"{v:.1f} {dot_yellow()}"
    elif v <= 55:    return f"{v:.1f} {dot_white()}"
    elif v <= 70:    return f"{v:.1f} {dot_orange()}"
    else:            return f"{v:.1f} {dot_red()}"

def label_macd_cross(macd, signal, prev_macd=None, prev_signal=None):
    try:
        macd = float(macd); signal = float(signal)
        if macd > signal:
            # renforce si pente s'améliore vs signal
            if prev_macd is not None and prev_signal is not None:
                if (macd - prev_macd) > (signal - prev_signal):
                    return f"{macd - signal:.2f} {dot_green()}"
            return f"{macd - signal:.2f} {dot_yellow()}"
        if abs(macd - signal) <= 1e-6:
            return f"0.00 {dot_white()}"
        # macd < signal
        if prev_macd is not None and prev_signal is not None:
            if (macd - prev_macd) < (signal - prev_signal):
                return f"{macd - signal:.2f} {dot_red()}"
        return f"{macd - signal:.2f} {dot_orange()}"
    except Exception:
        return f"Insuffisant {dot_white()}"

def label_trend(ema20, ema50):
    try:
        ema20 = float(ema20); ema50 = float(ema50)
        spread = pct(ema20, ema50)
        if np.isnan(spread): return f"Insuffisant {dot_white()}"
        if spread > 0.02:  return f"Haussier {dot_green()}"
        if spread > 0.0:   return f"Haussier {dot_yellow()}"
        if abs(spread) <= 0.005: return f"Neutre {dot_white()}"
        if spread > -0.02: return f"Baissier {dot_orange()}"
        return f"Baissier {dot_red()}"
    except Exception:
        return f"Insuffisant {dot_white()}"

def label_bollinger(close, lower, upper):
    try:
        close = float(close); lower=float(lower); upper=float(upper)
        if close < lower:                 return f"Bas {dot_green()}"
        if close <= lower * 1.02:         return f"Proche bas {dot_yellow()}"
        if lower < close < upper:         return f"Neutre {dot_white()}"
        if close >= upper * 0.98 and close <= upper:
            return f"Proche haut {dot_orange()}"
        if close > upper:                 return f"Haut {dot_red()}"
        return f"Neutre {dot_white()}"
    except Exception:
        return f"Insuffisant {dot_white()}"

def label_volume(vol, vol_mean):
    try:
        vol=float(vol); vm=float(vol_mean)
        if vol >= vm*1.30: return f"{vol:.1f} {dot_green()}"
        if vol >= vm*1.10: return f"{vol:.1f} {dot_yellow()}"
        if abs(vol-vm)/max(vm,1e-9) <= 0.1: return f"{vol:.1f} {dot_white()}"
        if vol > vm*0.70: return f"{vol:.1f} {dot_orange()}"
        return f"{vol:.1f} {dot_red()}"
    except Exception:
        return f"Insuffisant {dot_white()}"

def label_close_vs_ema50(close, ema50):
    try:
        c=float(close); e=float(ema50)
        spread = pct(c,e)
        if np.isnan(spread): return f"Insuffisant {dot_white()}"
        if spread > 0.02:  return f"{c:.1f} {dot_green()}"
        if spread > 0.0:   return f"{c:.1f} {dot_yellow()}"
        if abs(spread) <= 0.005: return f"{c:.1f} {dot_white()}"
        if spread > -0.02: return f"{c:.1f} {dot_orange()}"
        return f"{c:.1f} {dot_red()}"
    except Exception:
        return f"Insuffisant {dot_white()}"

def label_adx(adx):
    try:
        a=float(adx)
        if a > 40:   return f"{a:.1f} {dot_red()}"
        if a >= 25:  return f"{a:.1f} {dot_green()}"
        if a >= 20:  return f"{a:.1f} {dot_yellow()}"
        if a >= 15:  return f"{a:.1f} {dot_white()}"
        return f"{a:.1f} {dot_orange()}"
    except Exception:
        return f"Insuffisant {dot_white()}"

def label_mfi(mfi):
    try:
        m=float(mfi)
        if m < 20:   return f"{m:.1f} {dot_green()}"
        if m < 40:   return f"{m:.1f} {dot_yellow()}"
        if m <= 60:  return f"{m:.1f} {dot_white()}"
        if m <= 80:  return f"{m:.1f} {dot_orange()}"
        return f"{m:.1f} {dot_red()}"
    except Exception:
        return f"Insuffisant {dot_white()}"

def label_cci(cci):
    try:
        c=float(cci)
        if c < -100:  return f"{c:.0f} {dot_green()}"
        if c < 0:     return f"{c:.0f} {dot_yellow()}"
        if c <= 100:  return f"{c:.0f} {dot_white()}"
        if c <= 200:  return f"{c:.0f} {dot_orange()}"
        return f"{c:.0f} {dot_red()}"
    except Exception:
        return f"Insuffisant {dot_white()}"

def label_atr(atr, atr_ma):
    try:
        a=float(atr)
        ma = float(atr_ma) if atr_ma is not None and not pd.isna(atr_ma) else np.nan
        if np.isnan(ma): 
            return f"{a:.2f} {dot_white()}"
        if a >= ma*2.0:   return f"{a:.2f} {dot_red()}"
        if a > ma:        return f"{a:.2f} {dot_orange()}"
        if abs(a-ma)/max(ma,1e-9) <= 0.1: return f"{a:.2f} {dot_white()}"
        return f"{a:.2f} {dot_green()}"
    except Exception:
        return f"Insuffisant {dot_white()}"

def label_obv(delta):
    try:
        d=float(delta)
        if d > 0:   return f"{d:.0f} {dot_green()}"
        if d == 0:  return f"{d:.0f} {dot_white()}"
        return f"{d:.0f} {dot_red()}"
    except Exception:
        return f"Insuffisant {dot_white()}"

def label_supertrend(v):
    s = "" if v is None else str(v)
    if "Bull" in s: return f"Haussier {dot_green()}"
    if "Bear" in s: return f"Baissier {dot_red()}"
    return f"Insuffisant {dot_white()}"

def label_ichimoku(tenkan, kijun):
    try:
        t=float(tenkan); k=float(kijun)
        if t > k:     return f"Haussier {dot_green()}"
        if abs(t-k)/max(abs(k),1e-9) <= 0.005: return f"Neutre {dot_white()}"
        return f"Baissier {dot_red()}"
    except Exception:
        return f"Insuffisant {dot_white()}"

def label_donchian(close, high, low):
    try:
        c=float(close); h=float(high); l=float(low)
        if c > h:  return f"Haut {dot_green()}"
        if c < l:  return f"Bas {dot_red()}"
        return f"Neutre {dot_white()}"
    except Exception:
        return f"Insuffisant {dot_white()}"

def label_pivot(close, r1, s1):
    try:
        c=float(close); r=float(r1); s=float(s1)
        if c > r:  return f"Haut {dot_red()}"
        if c < s:  return f"Bas {dot_green()}"
        return f"Neutre {dot_white()}"
    except Exception:
        return f"Insuffisant {dot_white()}"

def label_ma200(close, ma200):
    try:
        c=float(close); m=float(ma200)
        if c > m:   return f"{c:.1f} {dot_green()}"
        if abs(c-m)/max(m,1e-9) <= 0.005: return f"{c:.1f} {dot_white()}"
        return f"{c:.1f} {dot_red()}"
    except Exception:
        return f"Insuffisant {dot_white()}"

# ======================================================
# ⚖️ Scoring "pro trader" (pondérations) — inchangé
# ======================================================
_W_RSI   = 0.15
_W_TREND = 0.30
_W_MACD  = 0.25
_W_BB    = 0.10
_W_VOL   = 0.10
_W_SENTI = 0.10

_W_TF = {"1h": 0.20, "6h": 0.30, "1d": 0.50}

def _score_from_rsi(v):
    if v is None or (isinstance(v, float) and np.isnan(v)): return 0.5
    try:
        r = float(v)
    except Exception:
        return 0.5
    if r < 30: return 1.0
    if r > 70: return 0.0
    return 1.0 - (r - 30) / 40.0

def _score_from_trend(ema20, ema50):
    try:
        spread = pct(ema20, ema50)
        if np.isnan(spread): return 0.5
        if spread > 0.02:  return 1.0
        if spread > 0.0:   return 0.7
        if abs(spread) <= 0.005: return 0.5
        if spread > -0.02: return 0.3
        return 0.0
    except Exception:
        return 0.5

def _score_from_macd(macd, signal):
    try:
        macd = float(macd); signal=float(signal)
        if macd > signal: return 0.8
        if abs(macd-signal) <= 1e-6: return 0.5
        return 0.2
    except Exception:
        return 0.5

def _score_from_bb(close, lower, upper):
    try:
        c=float(close); l=float(lower); u=float(upper)
        if c < l:  return 1.0
        if c > u:  return 0.0
        return 0.5
    except Exception:
        return 0.5

def _score_from_vol(vol, mean):
    try:
        vol=float(vol); mean=float(mean)
        if vol >= mean*1.3: return 0.8
        if vol >= mean*1.1: return 0.65
        if abs(vol-mean)/max(mean,1e-9) <= 0.1: return 0.5
        if vol > mean*0.7: return 0.35
        return 0.2
    except Exception:
        return 0.5

def _score_from_sentiment(sentiment_dict):
    if not isinstance(sentiment_dict, dict): return 0.5
    val = None
    if "Sentiment_Score" in sentiment_dict and sentiment_dict["Sentiment_Score"] is not None:
        val = sentiment_dict["Sentiment_Score"]
    elif "FearGreed_Index" in sentiment_dict and sentiment_dict["FearGreed_Index"] is not None:
        val = sentiment_dict["FearGreed_Index"]
    if val is None: return 0.5
    try:
        v = float(val)
    except Exception:
        return 0.5
    v = max(0.0, min(100.0, v))
    return v / 100.0

def _signal_global_from_score(score):
    try:
        s = float(score)
    except Exception:
        return "Neutre ⚪"
    if s > 8:  return f"Achat {dot_green()}"
    if s > 6:  return f"Achat {dot_yellow()}"
    if s > 5:  return f"Neutre {dot_white()}"
    if s > 3:  return f"Vente {dot_orange()}"
    return f"Vente {dot_red()}"

def compute_global_score(tfs: dict, senti: dict):
    score_components = []
    for tf, wtf in _W_TF.items():
        vals = tfs.get(tf, {}) if isinstance(tfs, dict) else {}
        s_rsi   = _score_from_rsi(vals.get("RSI"))
        s_trend = _score_from_trend(vals.get("EMA20"), vals.get("EMA50"))
        s_macd  = _score_from_macd(vals.get("MACD"), vals.get("MACD_Signal"))
        s_bb    = _score_from_bb(vals.get("Close"), vals.get("BB_Lower"), vals.get("BB_Upper"))
        s_vol   = _score_from_vol(vals.get("Volume"), vals.get("Volume_Mean"))
        s_tf = (s_rsi * _W_RSI + s_trend * _W_TREND + s_macd * _W_MACD + s_bb * _W_BB + s_vol * _W_VOL)
        score_components.append(s_tf * wtf)
    s_senti = _score_from_sentiment(senti) * _W_SENTI
    total_0_1 = max(0.0, min(1.0, sum(score_components) + s_senti))
    score_0_10 = round(total_0_1 * 10.0, 1)
    signal = _signal_global_from_score(score_0_10)
    return score_0_10, signal

# ======================================================
# 🌍 Sentiments par crypto (inchangé, labels clean)
# ======================================================
def get_sentiment_for_symbol(symbol: str) -> dict:
    try:
        fg_value = np.nan
        try:
            fng = requests.get("https://api.alternative.me/fng/", timeout=10).json()
            fg_value = float(fng["data"][0]["value"])
        except Exception:
            pass

        social_sent = 0.0
        try:
            trending = requests.get("https://api.coingecko.com/api/v3/search/trending", timeout=10).json()
            trending_symbols = [c["item"]["symbol"].upper() for c in trending.get("coins", [])]
            social_sent = 100.0 if symbol.upper() in trending_symbols else max(0.0, 10.0 * len(trending_symbols))
            social_sent = min(100.0, social_sent)
        except Exception:
            pass

        news_intensity = 0.5
        try:
            news_req = requests.get("https://api.coingecko.com/api/v3/global", timeout=10).json()
            mcap_change = float(news_req["data"]["market_cap_change_percentage_24h_usd"])
            news_intensity = min(1.0, abs(mcap_change) / 5.0)
        except Exception:
            pass

        base = []
        if not np.isnan(fg_value): base.append(fg_value)
        base.append(social_sent)
        base.append(news_intensity * 100.0)
        senti_score = float(np.mean(base)) if base else np.nan

        return {
            "FearGreed_Index": round(fg_value, 1) if not np.isnan(fg_value) else np.nan,
            "Social_Sentiment": int(round(social_sent, 0)),
            "News_Intensity": round(news_intensity, 3),
            "Sentiment_Score": round(senti_score, 1) if not np.isnan(senti_score) else np.nan,
        }
    except Exception as e:
        print(f"⚠️ Erreur get_sentiment_for_symbol({symbol}) : {e}", flush=True)
        return {
            "FearGreed_Index": np.nan,
            "Social_Sentiment": np.nan,
            "News_Intensity": np.nan,
            "Sentiment_Score": np.nan,
        }

def _sentiment_global_label(senti: dict) -> str:
    try:
        fg = float(senti.get("FearGreed_Index", np.nan))
        ss = float(senti.get("Social_Sentiment", np.nan))
        ni = float(senti.get("News_Intensity", np.nan))
        vals = []
        if not np.isnan(fg): vals.append(fg / 100.0)
        if not np.isnan(ss): vals.append(ss / 100.0)
        if not np.isnan(ni): vals.append(1.0 - abs(ni - 0.5) * 2.0)
        if not vals:
            return f"Neutre {dot_white()}"
        m = sum(vals) / len(vals)
        if m >= 0.7: return f"Positif {dot_green()}"
        if m >= 0.5: return f"Neutre {dot_white()}"
        return f"Négatif {dot_red()}"
    except Exception:
        return f"Neutre {dot_white()}"

# ======================================================
# 💵 Prix actuel (USD) — cache 5 minutes + fallback
# ======================================================
_COINGECKO_IDS = {
    "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "BNB": "binancecoin",
    "ADA": "cardano", "DOGE": "dogecoin", "AVAX": "avalanche-2", "XRP": "ripple",
    "LINK": "chainlink", "MATIC": "polygon-pos"
}
_price_cache = {"ts": 0.0, "data": {}}

def _refresh_price_cache_usd(symbols):
    ids = [ _COINGECKO_IDS.get(sym, "") for sym in symbols ]
    ids = [i for i in ids if i]
    if not ids:
        return
    try:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {"ids": ",".join(ids), "vs_currencies": "usd"}
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            return
        data = r.json()
        for sym in symbols:
            cid = _COINGECKO_IDS.get(sym)
            if cid and cid in data and "usd" in data[cid]:
                _price_cache["data"][sym] = float(data[cid]["usd"])
        _price_cache["ts"] = time.time()
    except Exception as e:
        print(f"⚠️ Erreur refresh price cache: {e}", flush=True)

def get_price_usd(symbol):
    now = time.time()
    if now - _price_cache["ts"] > 300:
        return _price_cache["data"].get(symbol)
    return _price_cache["data"].get(symbol)

# ======================================================
# ⚙️ API Coinbase – Données OHLC
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
        df = ensure_numeric(df, ["low","high","open","close","volume"])
        return df
    except Exception as e:
        print(f"⚠️ Erreur get_candles({symbol_pair}, {granularity}): {e}", flush=True)
        return None

# ======================================================
# 📈 Calculs d’indicateurs (avec RSI fallback)
# ======================================================
def compute_indicators(df):
    if df is None or df.empty: return df
    df = df.copy()
    close = df["close"]
    high  = df["high"]
    low   = df["low"]
    vol   = df["volume"]

    # --------- RSI (Wilder) avec fallback ---------
    def _rsi_series(series, period):
        delta = series.diff()
        up = delta.clip(lower=0)
        down = (-delta).clip(lower=0)
        roll_up = up.ewm(alpha=1/period, adjust=False).mean()
        roll_down = down.ewm(alpha=1/period, adjust=False).mean()
        rs = roll_up / roll_down.replace(0, np.nan)
        return 100 - (100 / (1 + rs))

    rsi_period = 14
    if len(close) < rsi_period + 1:
        # fallback dynamique
        alt = max(3, len(close)//2)
        df["RSI14"] = _rsi_series(close, alt)
    else:
        df["RSI14"] = _rsi_series(close, rsi_period)

    # --------- MACD ---------
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()

    # --------- EMAs ---------
    df["EMA20"] = close.ewm(span=20, adjust=False).mean()
    df["EMA50"] = close.ewm(span=50, adjust=False).mean()

    # --------- Bollinger (fallback min_periods) ---------
    win = 20 if len(close) >= 20 else max(3, len(close)//2)
    bb_mid = close.rolling(win, min_periods=3).mean()
    bb_std = close.rolling(win, min_periods=3).std()
    df["BB_Mid"]   = bb_mid
    df["BB_Upper"] = bb_mid + 2 * bb_std
    df["BB_Lower"] = bb_mid - 2 * bb_std

    # --------- Volume / VWAP ---------
    df["Volume_Mean"] = vol.rolling(win, min_periods=3).mean()
    df["VWAP"] = (close * vol).cumsum() / vol.replace(0, np.nan).cumsum()

    # --------- ATR / TR + MA20 d'ATR ---------
    prev_close = close.shift(1)
    tr_components = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1)
    TR = tr_components.max(axis=1)
    df["ATR14"] = TR.ewm(alpha=1/14, adjust=False).mean()
    df["ATR14_MA20"] = df["ATR14"].rolling(20, min_periods=3).mean()

    # --------- StochRSI proxy ---------
    low14 = low.rolling(14, min_periods=3).min()
    high14 = high.rolling(14, min_periods=3).max()
    df["StochRSI"] = ((close - low14) / (high14 - low14) * 100).replace([np.inf, -np.inf], np.nan)

    # --------- ADX ---------
    up_move   = high.diff()
    down_move = -low.diff()
    plus_dm  = np.where((up_move > 0) & (up_move > down_move), up_move, 0.0)
    minus_dm = np.where((down_move > 0) & (down_move > up_move), down_move, 0.0)
    atr14 = df["ATR14"]
    plus_di  = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1/14, adjust=False).mean() / atr14
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1/14, adjust=False).mean() / atr14
    dx = 100 * ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan))
    df["ADX"] = dx.ewm(alpha=1/14, adjust=False).mean()

    # --------- Ichimoku (sans décalage) ---------
    high_9 = high.rolling(9, min_periods=3).max(); low_9 = low.rolling(9, min_periods=3).min()
    df["ICH_Tenkan"] = (high_9 + low_9) / 2
    high_26 = high.rolling(26, min_periods=3).max(); low_26 = low.rolling(26, min_periods=3).min()
    df["ICH_Kijun"] = (high_26 + low_26) / 2
    df["ICH_SpanA"] = (df["ICH_Tenkan"] + df["ICH_Kijun"]) / 2
    df["ICH_SpanB"] = (high.rolling(52, min_periods=3).max() + low.rolling(52, min_periods=3).min()) / 2

    # --------- OBV + delta ---------
    direction = np.sign(close.diff()).fillna(0)
    df["OBV"] = (vol * direction).cumsum()
    df["OBV_Delta"] = df["OBV"].diff()

    # --------- MFI ---------
    typical_price = (high + low + close) / 3
    money_flow = typical_price * vol
    pos_flow = np.where(typical_price > typical_price.shift(), money_flow, 0.0)
    neg_flow = np.where(typical_price < typical_price.shift(), money_flow, 0.0)
    pos_mf = pd.Series(pos_flow, index=df.index).rolling(14, min_periods=3).sum()
    neg_mf = pd.Series(neg_flow, index=df.index).rolling(14, min_periods=3).sum().replace(0, np.nan)
    df["MFI"] = 100 - (100 / (1 + (pos_mf / neg_mf)))

    # --------- SAR (proxy) ---------
    df["SAR"] = close.rolling(3, min_periods=2).min().shift(1)

    # --------- CCI ---------
    tp = (high + low + close) / 3
    df["CCI"] = (tp - tp.rolling(20, min_periods=3).mean()) / (0.015 * tp.rolling(20, min_periods=3).std())

    # --------- SuperTrend proxy ---------
    hl2 = (high + low) / 2
    st_atr = df["ATR14"]
    lower_band = hl2 - 3 * st_atr
    df["SuperTrend"] = np.where(close > lower_band, "Bull", "Bear")

    # --------- Donchian ---------
    df["Donchian_High"] = high.rolling(20, min_periods=3).max()
    df["Donchian_Low"]  = low.rolling(20, min_periods=3).min()

    # --------- MA200 ---------
    df["MA200"] = close.rolling(200, min_periods=3).mean()

    # --------- Pivots ---------
    df["Pivot"] = (high + low + close) / 3
    df["R1"] = 2 * df["Pivot"] - low
    df["S1"] = 2 * df["Pivot"] - high

    return df

# ======================================================
# 🧮 Résumé dernière ligne (par timeframe)
# ======================================================
def summarize_last_row(df):
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else last
    out = {
        "Close": safe_round(last["close"]),
        "Volume": safe_round(last["volume"]),
        "RSI": safe_round(last["RSI14"]),
        "MACD": safe_round(last["MACD"]),
        "MACD_Signal": safe_round(last["MACD_Signal"]),
        "EMA20": safe_round(last["EMA20"]),
        "EMA50": safe_round(last["EMA50"]),
        "BB_Lower": safe_round(last["BB_Lower"]),
        "BB_Upper": safe_round(last["BB_Upper"]),
        "Volume_Mean": safe_round(last["Volume_Mean"]),
        "ATR14": safe_round(last["ATR14"], 2),
        "ATR14_MA20": safe_round(last.get("ATR14_MA20", np.nan), 2),
        "ADX": safe_round(last["ADX"]),
        "ICH_Tenkan": safe_round(last["ICH_Tenkan"]),
        "ICH_Kijun": safe_round(last["ICH_Kijun"]),
        "ICH_SpanA": safe_round(last["ICH_SpanA"]),
        "ICH_SpanB": safe_round(last["ICH_SpanB"]),
        "OBV": safe_round(last["OBV"], 0),
        "OBV_Delta": safe_round(last.get("OBV_Delta", np.nan), 0),
        "MFI": safe_round(last["MFI"]),
        "SAR": safe_round(last["SAR"]),
        "CCI": safe_round(last["CCI"], 0),
        "SuperTrend": last["SuperTrend"] if isinstance(last["SuperTrend"], str) else "N/A",
        "Donchian_High": safe_round(last["Donchian_High"]),
        "Donchian_Low": safe_round(last["Donchian_Low"]),
        "MA200": safe_round(last["MA200"]),
        "Pivot": safe_round(last["Pivot"]),
        "R1": safe_round(last["R1"]),
        "S1": safe_round(last["S1"]),
        "_prev_MACD": safe_round(prev["MACD"]) if "MACD" in df.columns else np.nan,
        "_prev_Signal": safe_round(prev["MACD_Signal"]) if "MACD_Signal" in df.columns else np.nan,
    }
    return out

# ======================================================
# 🔎 Analyse d’un symbole (1h/6h/1d)
# ======================================================
def analyze_symbol(symbol_pair):
    periods = {"1h": 3600, "6h": 21600, "1d": 86400}
    data = {}
    for label, gran in periods.items():
        df = get_candles(symbol_pair, gran)
        if df is None or len(df) < 10:
            print(f"⚠️ Historique insuffisant pour {symbol_pair} en {label}", flush=True)
            continue
        df = compute_indicators(df)
        data[label] = summarize_last_row(df)
    if not data:
        return None

    flat = {"Crypto": symbol_pair.split("-")[0]}
    tf_trends = []
    for _, vals in data.items():
        ema20, ema50 = vals.get("EMA20", np.nan), vals.get("EMA50", np.nan)
        tf_trends.append(1 if (ema20 is not None and ema50 is not None and ema20 > ema50) else -1)
    score_trend = tf_trends.count(1) - tf_trends.count(-1)
    consensus = f"Achat {dot_green()}" if score_trend >= 2 else f"Vente {dot_red()}" if score_trend <= -2 else f"Neutre {dot_white()}"
    flat["Consensus"] = consensus

    def add_tf_cols(tf, v):
        flat[f"RSI {tf.upper()}"]           = label_rsi(v.get("RSI"))
        flat[f"Trend {tf.upper()}"]         = label_trend(v.get("EMA20"), v.get("EMA50"))
        flat[f"MACD {tf.upper()}"]          = label_macd_cross(v.get("MACD"), v.get("MACD_Signal"), v.get("_prev_MACD"), v.get("_prev_Signal"))
        flat[f"Bollinger {tf.upper()}"]     = label_bollinger(v.get("Close"), v.get("BB_Lower"), v.get("BB_Upper"))
        flat[f"Volume {tf.upper()}"]        = label_volume(v.get("Volume"), v.get("Volume_Mean"))
        flat[f"Prix {tf.upper()}"]          = label_close_vs_ema50(v.get("Close"), v.get("EMA50"))
        flat[f"ADX {tf.upper()}"]           = label_adx(v.get("ADX"))
        flat[f"ATR {tf.upper()}"]           = label_atr(v.get("ATR14"), v.get("ATR14_MA20"))
        flat[f"MFI {tf.upper()}"]           = label_mfi(v.get("MFI"))
        flat[f"CCI {tf.upper()}"]           = label_cci(v.get("CCI"))
        flat[f"OBV {tf.upper()}"]           = label_obv(v.get("OBV_Delta"))
        flat[f"SuperTrend {tf.upper()}"]    = label_supertrend(v.get("SuperTrend"))
        flat[f"Ichimoku {tf.upper()}"]      = label_ichimoku(v.get("ICH_Tenkan"), v.get("ICH_Kijun"))
        flat[f"Donchian {tf.upper()}"]      = label_donchian(v.get("Close"), v.get("Donchian_High"), v.get("Donchian_Low"))
        flat[f"Pivot {tf.upper()}"]         = label_pivot(v.get("Close"), v.get("R1"), v.get("S1"))
        flat[f"MA200 {tf.upper()}"]         = label_ma200(v.get("Close"), v.get("MA200"))

        # RAW pour scoring
        flat[f"_RAW_RSI_{tf}"]     = v.get("RSI")
        flat[f"_RAW_EMA20_{tf}"]   = v.get("EMA20")
        flat[f"_RAW_EMA50_{tf}"]   = v.get("EMA50")
        flat[f"_RAW_MACD_{tf}"]    = v.get("MACD")
        flat[f"_RAW_SIG_{tf}"]     = v.get("MACD_Signal")
        flat[f"_RAW_BBLOW_{tf}"]   = v.get("BB_Lower")
        flat[f"_RAW_BBUP_{tf}"]    = v.get("BB_Upper")
        flat[f"_RAW_CLOSE_{tf}"]   = v.get("Close")
        flat[f"_RAW_VOL_{tf}"]     = v.get("Volume")
        flat[f"_RAW_VOLMEAN_{tf}"] = v.get("Volume_Mean")

    for tf in ["1h","6h","1d"]:
        if tf in data:
            add_tf_cols(tf, data[tf])

    return flat

# ======================================================
# 📊 Mise à jour Google Sheets
# ======================================================
def update_sheet():
    try:
        sh = gc.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet("MultiTF")
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title="MultiTF", rows="600", cols="260")

        cryptos_pairs = ["BTC-USD","ETH-USD","SOL-USD","BNB-USD","ADA-USD","DOGE-USD","AVAX-USD","XRP-USD","LINK-USD","MATIC-USD"]
        symbols = [c.split("-")[0] for c in cryptos_pairs]

        # 🔄 refresh cache prix (5 min)
        if time.time() - _price_cache["ts"] > 300:
            _refresh_price_cache_usd(symbols)

        rows = []
        for pair in cryptos_pairs:
            res = analyze_symbol(pair)
            if not res:
                print(f"⚠️ Données manquantes pour {pair} — ignoré.", flush=True)
                continue

            symbol = pair.split("-")[0]
            senti = get_sentiment_for_symbol(symbol)
            # Scoring global basé sur _RAW_*
            tfs = {}
            for tf in ["1h","6h","1d"]:
                if f"_RAW_RSI_{tf}" in res:
                    tfs[tf] = {
                        "RSI": res.get(f"_RAW_RSI_{tf}"),
                        "EMA20": res.get(f"_RAW_EMA20_{tf}"),
                        "EMA50": res.get(f"_RAW_EMA50_{tf}"),
                        "MACD": res.get(f"_RAW_MACD_{tf}"),
                        "MACD_Signal": res.get(f"_RAW_SIG_{tf}"),
                        "Close": res.get(f"_RAW_CLOSE_{tf}"),
                        "BB_Lower": res.get(f"_RAW_BBLOW_{tf}"),
                        "BB_Upper": res.get(f"_RAW_BBUP_{tf}"),
                        "Volume": res.get(f"_RAW_VOL_{tf}"),
                        "Volume_Mean": res.get(f"_RAW_VOLMEAN_{tf}"),
                    }

            score_10, signal_global = compute_global_score(tfs, senti)

            # Prix actuel (USD) depuis cache (formaté avec $)
            px = get_price_usd(symbol)
            if px is None:
                price_str = "N/A"
            else:
                price_str = f"{safe_round(px, 2):.2f} $"

            # Ligne exportée (sans _RAW_*)
            flat = {
                "Crypto": res["Crypto"],
                "Prix Actuel (USD)": price_str,   # 2e colonne
                "Global Score (0-10)": score_10,
                "Signal Global": signal_global,
                "Consensus": res.get("Consensus")
            }

            for k, v in res.items():
                if k.startswith("_RAW_"): 
                    continue
                if k in ["Crypto","Consensus"]:
                    continue
                flat[k] = v

            # Sentiments (labels clean)
            flat["Fear & Greed Index"] = senti.get("FearGreed_Index")
            flat["Social Sentiment"]   = senti.get("Social_Sentiment")
            flat["News Intensity"]     = senti.get("News_Intensity")
            flat["Sentiment Score"]    = senti.get("Sentiment_Score")
            flat["Sentiment Global"]   = _sentiment_global_label(senti)
            flat["Last Update"]        = now_paris_str()  # 🇫🇷

            rows.append(flat)
            time.sleep(1.0)

        if not rows:
            print("⚠️ Aucune donnée récupérée", flush=True)
            return

        df_out = pd.DataFrame(rows)

        # Ordre de colonnes lisible
        cols_front = ["Crypto","Prix Actuel (USD)","Global Score (0-10)","Signal Global","Consensus"]

        def _order_by_family(prefix):
            out = []
            for tf in ["1H","6H","1D"]:
                col = f"{prefix} {tf}"
                if col in df_out.columns: out.append(col)
            return out

        ordered = cols_front
        for family in ["RSI","Trend","MACD","Bollinger","Volume","Prix",
                       "ADX","ATR","MFI","CCI","OBV","SuperTrend","Ichimoku","Donchian","Pivot","MA200"]:
            ordered += _order_by_family(family)

        emotion_cols = ["Fear & Greed Index","Social Sentiment","News Intensity","Sentiment Score","Sentiment Global","Last Update"]
        remaining = [c for c in df_out.columns if c not in ordered and c not in emotion_cols]
        ordered += remaining + [c for c in emotion_cols if c in df_out.columns]
        df_out = df_out.reindex(columns=[c for c in ordered if c in df_out.columns])

        ws.clear()
        set_with_dataframe(ws, df_out)
        print("✅ Feuille 'MultiTF' mise à jour : UI clean + RSI fallback + prix USD + heure FR.", flush=True)

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
    url = os.getenv("RENDER_EXTERNAL_URL", "https://crypto-bot-multitf.onrender.com")
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
    return "✅ Crypto Bot Multi-Timeframe — V5.1 UI clean + RSI fallback + Prix USD + Heure FR"

@app.route("/run")
def manual_run():
    threading.Thread(target=update_sheet, daemon=True).start()
    return "Mise à jour manuelle lancée"

# ======================================================
# 🧠 Lancement
# ======================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=port)
