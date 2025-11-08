# crypto_bot_multiTF_V5_2_1.py — V5.2.1 Stable & Fusionné
# Features:
# - Global Score fused with signal (one column, value + pastille)
# - Update formatted "DD/MM/YY - HHHH:MM" in Europe/Paris, placed 5th
# - CryptoPanic + Google Trends with 12h cache + fallbacks
# - Sentiment per-crypto, no extra "global row"
# - Pastilles harmonisées 🟢🟡⚪🟠🔴 across indicators
# - Clean logs for Render
# - Coinbase OHLC + full indicator stack + scoring
# - Price via Coingecko (USD) as 2nd column

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
from pytrends.request import TrendReq

app = Flask(__name__)

# ---------------------
# Config / Env
# ---------------------
print("🔐 Initialisation des credentials Google...", flush=True)
try:
    info = json.loads(os.getenv("GOOGLE_SERVICE_JSON", "{}"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
    if not SHEET_ID:
        raise RuntimeError("GOOGLE_SHEET_ID manquante")
    print("✅ Credentials Google OK", flush=True)
except Exception as e:
    print(f"❌ Erreur credentials Google : {e}", flush=True)
    raise SystemExit()

CRYPTOPANIC_KEY = os.getenv("CRYPTOPANIC_KEY", "").strip()
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "").strip()

# ---------------------
# Utils
# ---------------------
def safe_round(x, n=1):
    try:
        if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))):
            return np.nan
        return round(float(x), n)
    except Exception:
        return np.nan

def ensure_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def pct(a, b):
    try:
        a = float(a); b = float(b)
        if b == 0: return np.nan
        return (a - b) / b
    except Exception:
        return np.nan

def now_paris():
    tz = pytz.timezone("Europe/Paris")
    return datetime.now(tz)

def format_update_label(dt):
    # Format: 08/11/25 - 12H45
    if not isinstance(dt, datetime):
        return "Aucune donnée ⚪"
    return dt.strftime("%d/%m/%y - %HH%M")

# ---------------------
# Pastilles
# ---------------------
def dot_green():  return "🟢"
def dot_yellow(): return "🟡"
def dot_white():  return "⚪"
def dot_orange(): return "🟠"
def dot_red():    return "🔴"

def color_from_score(score):
    try:
        s = float(score)
    except Exception:
        return dot_white()
    if s > 8: return dot_green()
    if s > 6: return dot_yellow()
    if s > 5: return dot_white()
    if s > 3: return dot_orange()
    return dot_red()

# ---------------------
# Label helpers (value + pastille)
# ---------------------
def label_or_na(val, fmt="{} {}"):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return f"Aucune donnée {dot_white()}"
    try:
        return fmt.format(val, dot_white())
    except Exception:
        return f"Aucune donnée {dot_white()}"

def label_rsi(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return f"Aucune donnée {dot_white()}"
    v = float(v)
    if v < 30:       return f"{v:.1f} {dot_green()}"
    elif v < 45:     return f"{v:.1f} {dot_yellow()}"
    elif v <= 55:    return f"{v:.1f} {dot_white()}"
    elif v <= 70:    return f"{v:.1f} {dot_orange()}"
    else:            return f"{v:.1f} {dot_red()}"

def label_trend(ema20, ema50):
    try:
        ema20 = float(ema20); ema50 = float(ema50)
    except Exception:
        return f"Aucune donnée {dot_white()}"
    spread = pct(ema20, ema50)
    if np.isnan(spread): return f"Aucune donnée {dot_white()}"
    if spread > 0.02:  return f"Haussier {dot_green()}"
    if spread > 0.00:  return f"Haussier {dot_yellow()}"
    if abs(spread) <= 0.005: return f"Neutre {dot_white()}"
    if spread > -0.02: return f"Baissier {dot_orange()}"
    return f"Baissier {dot_red()}"

def label_close_vs_ema50(close, ema50):
    try:
        c=float(close); e=float(ema50)
    except Exception:
        return f"Aucune donnée {dot_white()}"
    spread = pct(c, e)
    if np.isnan(spread): return f"Aucune donnée {dot_white()}"
    if spread > 0.02:  return f"{c:.2f} {dot_green()}"
    if spread > 0.00:  return f"{c:.2f} {dot_yellow()}"
    if abs(spread) <= 0.005: return f"{c:.2f} {dot_white()}"
    if spread > -0.02: return f"{c:.2f} {dot_orange()}"
    return f"{c:.2f} {dot_red()}"

# ---------------------
# Scoring (pondération pro-trader)
# ---------------------
_W_RSI   = 0.15
_W_TREND = 0.30
_W_MACD  = 0.25
_W_BB    = 0.10
_W_VOL   = 0.10
_W_SENTI = 0.10
_W_ACTUALITY = 0.10

_W_TF = {"1h": 0.20, "6h": 0.30, "1d": 0.50}

def _score_from_rsi(v):
    if v is None or (isinstance(v, float) and np.isnan(v)): return 0.5
    try: r = float(v)
    except: return 0.5
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
    try: v = float(val)
    except: return 0.5
    v = max(0.0, min(100.0, v))
    return v / 100.0

def compute_global_score(tfs: dict, senti: dict, actuality_score: float = None):
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
    score_sum = sum(score_components) + s_senti

    if actuality_score is not None and not pd.isna(actuality_score):
        try:
            a0_1 = max(0.0, min(1.0, float(actuality_score) / 100.0))
        except Exception:
            a0_1 = 0.5
        score_sum += a0_1 * _W_ACTUALITY

    total_0_1 = max(0.0, min(1.0, score_sum))
    score_0_10 = round(total_0_1 * 10.0, 1)
    return score_0_10

# ---------------------
# Sentiment per-crypto
# ---------------------
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

# ---------------------
# Actuality (CryptoPanic + Google Trends) with cache & fallback
# ---------------------
_actuality_cache = {"ts": 0.0, "data": {}}
_ACTUALITY_TTL = 12 * 3600  # 12 hours

_COINGECKO_IDS = {
    "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "BNB": "binancecoin",
    "ADA": "cardano", "DOGE": "dogecoin", "AVAX": "avalanche-2", "XRP": "ripple",
    "LINK": "chainlink", "MATIC": "polygon-pos"
}

def get_news_score(symbol: str) -> dict:
    if not CRYPTOPANIC_KEY:
        return {"News_Count": np.nan, "Positive_%": np.nan, "Negative_%": np.nan, "News_Score": np.nan}
    try:
        base = "https://cryptopanic.com/api/developer/v2/posts/"
        params = {
            "auth_token": CRYPTOPANIC_KEY,
            "currencies": symbol.upper(),
            "filter": "hot",
            "kind": "news",
            "public": "true",
            "regions": "en"
        }
        r = requests.get(base, params=params, timeout=12)
        if r.status_code != 200:
            if symbol.upper() != "BTC":
                params["currencies"] = "BTC"
                r = requests.get(base, params=params, timeout=12)
            if r.status_code != 200:
                return {"News_Count": np.nan, "Positive_%": np.nan, "Negative_%": np.nan, "News_Score": np.nan}
        data = r.json()
        items = data.get("results", []) or data.get("posts", []) or []
        n = len(items)
        if n == 0:
            return {"News_Count": 0, "Positive_%": np.nan, "Negative_%": np.nan, "News_Score": np.nan}
        pos_votes = 0; neg_votes = 0
        for it in items:
            votes = it.get("votes") or {}
            pos_votes += int(votes.get("positive", 0))
            neg_votes += int(votes.get("negative", 0))
        total_votes = pos_votes + neg_votes
        if total_votes == 0:
            return {"News_Count": n, "Positive_%": 50.0, "Negative_%": 50.0, "News_Score": 45.0}
        pos_pct = 100.0 * pos_votes / total_votes
        neg_pct = 100.0 * neg_votes / total_votes
        return {"News_Count": n, "Positive_%": round(pos_pct, 1), "Negative_%": round(neg_pct, 1), "News_Score": round(pos_pct, 1)}
    except Exception as e:
        print(f"⚠️ Erreur get_news_score({symbol}) : {e}", flush=True)
        return {"News_Count": np.nan, "Positive_%": np.nan, "Negative_%": np.nan, "News_Score": np.nan}

def get_trend_score(symbol: str) -> float:
    sym2kw = {
        "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "BNB": "bnb",
        "ADA": "cardano", "DOGE": "dogecoin", "AVAX": "avalanche",
        "XRP": "xrp", "LINK": "chainlink", "MATIC": "polygon"
    }
    kw = sym2kw.get(symbol.upper(), symbol.lower())
    try:
        pt = TrendReq(hl="en-US", tz=0)
        pt.build_payload([kw], timeframe="now 7-d", geo="")
        df = pt.interest_over_time()
        if df is None or df.empty or kw not in df.columns:
            return np.nan
        return float(df[kw].iloc[-1])
    except Exception as e:
        print(f"⚠️ Erreur get_trend_score({symbol}) : {e}", flush=True)
        return np.nan

def compute_actuality(symbol: str) -> dict:
    news = get_news_score(symbol)
    trend_raw = get_trend_score(symbol)
    ts = trend_raw if not pd.isna(trend_raw) else np.nan
    ns = news.get("News_Score", np.nan)
    if pd.isna(ns) and pd.isna(ts) and symbol.upper() != "BTC":
        ts2 = get_trend_score("BTC")
        ts = ts2 if not pd.isna(ts2) else ts
    v_news = ns if not pd.isna(ns) else np.nan
    v_trend = ts if not pd.isna(ts) else np.nan
    if pd.isna(v_news) and pd.isna(v_trend):
        act_score = np.nan
    else:
        if pd.isna(v_news): v_news = 50.0
        if pd.isna(v_trend): v_trend = 50.0
        act_score = 0.6 * v_news + 0.4 * v_trend
    return {
        "News_Count": news.get("News_Count"),
        "Positive_%": news.get("Positive_%"),
        "Negative_%": news.get("Negative_%"),
        "Trend_Score": round(ts, 1) if not pd.isna(ts) else np.nan,
        "Actuality_Score": round(act_score, 1) if not pd.isna(act_score) else np.nan,
        "Actuality_Sentiment": (
            f"{round(act_score,1)} {dot_white()}" if pd.isna(act_score)
            else (f"{round(act_score,1)} {dot_green()}" if act_score>=70
            else (f"{round(act_score,1)} {dot_yellow()}" if act_score>=50
            else (f"{round(act_score,1)} {dot_white()}" if act_score>=40
            else (f"{round(act_score,1)} {dot_orange()}" if act_score>=25
            else f"{round(act_score,1)} {dot_red()}"))))
        )
    }

def cached_actuality(symbol: str) -> dict:
    now = time.time()
    if now - _actuality_cache.get("ts", 0) < _ACTUALITY_TTL and symbol in _actuality_cache.get("data", {}):
        return _actuality_cache["data"][symbol]
    res = compute_actuality(symbol)
    _actuality_cache.setdefault("data", {})[symbol] = res
    _actuality_cache["ts"] = time.time()
    return res

# ---------------------
# Price cache (coingecko)
# ---------------------
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
        try:
            _refresh_price_cache_usd([symbol])
        except Exception:
            pass
    return _price_cache["data"].get(symbol)

# ---------------------
# Coinbase OHLC + Indicators
# ---------------------
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

def compute_indicators(df):
    if df is None or df.empty: return df
    df = df.copy()
    close = df["close"]; high = df["high"]; low = df["low"]; vol = df["volume"]

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
        alt = max(3, len(close)//2)
        df["RSI14"] = _rsi_series(close, alt)
    else:
        df["RSI14"] = _rsi_series(close, rsi_period)

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["EMA20"] = close.ewm(span=20, adjust=False).mean()
    df["EMA50"] = close.ewm(span=50, adjust=False).mean()

    win = 20 if len(close) >= 20 else max(3, len(close)//2)
    bb_mid = close.rolling(win, min_periods=3).mean()
    bb_std = close.rolling(win, min_periods=3).std()
    df["BB_Mid"]   = bb_mid
    df["BB_Upper"] = bb_mid + 2 * bb_std
    df["BB_Lower"] = bb_mid - 2 * bb_std

    df["Volume_Mean"] = vol.rolling(win, min_periods=3).mean()
    df["VWAP"] = (close * vol).cumsum() / vol.replace(0, np.nan).cumsum()

    prev_close = close.shift(1)
    tr_components = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1)
    TR = tr_components.max(axis=1)
    df["ATR14"] = TR.ewm(alpha=1/14, adjust=False).mean()
    df["ATR14_MA20"] = df["ATR14"].rolling(20, min_periods=3).mean()

    low14 = low.rolling(14, min_periods=3).min()
    high14 = high.rolling(14, min_periods=3).max()
    df["StochRSI"] = ((close - low14) / (high14 - low14) * 100).replace([np.inf, -np.inf], np.nan)

    up_move   = high.diff()
    down_move = -low.diff()
    plus_dm  = np.where((up_move > 0) & (up_move > down_move), up_move, 0.0)
    minus_dm = np.where((down_move > 0) & (down_move > up_move), down_move, 0.0)
    atr14 = df["ATR14"]
    plus_di  = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1/14, adjust=False).mean() / atr14
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1/14, adjust=False).mean() / atr14
    dx = 100 * ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan))
    df["ADX"] = dx.ewm(alpha=1/14, adjust=False).mean()

    high_9 = high.rolling(9, min_periods=3).max(); low_9 = low.rolling(9, min_periods=3).min()
    df["ICH_Tenkan"] = (high_9 + low_9) / 2
    high_26 = high.rolling(26, min_periods=3).max(); low_26 = low.rolling(26, min_periods=3).min()
    df["ICH_Kijun"] = (high_26 + low_26) / 2
    df["ICH_SpanA"] = (df["ICH_Tenkan"] + df["ICH_Kijun"]) / 2
    df["ICH_SpanB"] = (high.rolling(52, min_periods=3).max() + low.rolling(52, min_periods=3).min()) / 2

    direction = np.sign(close.diff()).fillna(0)
    df["OBV"] = (vol * direction).cumsum()
    df["OBV_Delta"] = df["OBV"].diff()

    typical_price = (high + low + close) / 3
    money_flow = typical_price * vol
    pos_flow = np.where(typical_price > typical_price.shift(), money_flow, 0.0)
    neg_flow = np.where(typical_price < typical_price.shift(), money_flow, 0.0)
    pos_mf = pd.Series(pos_flow, index=df.index).rolling(14, min_periods=3).sum()
    neg_mf = pd.Series(neg_flow, index=df.index).rolling(14, min_periods=3).sum().replace(0, np.nan)
    df["MFI"] = 100 - (100 / (1 + (pos_mf / neg_mf)))

    df["SAR"] = close.rolling(3, min_periods=2).min().shift(1)
    tp = (high + low + close) / 3
    df["CCI"] = (tp - tp.rolling(20, min_periods=3).mean()) / (0.015 * tp.rolling(20, min_periods=3).std())

    hl2 = (high + low) / 2
    st_atr = df["ATR14"]
    lower_band = hl2 - 3 * st_atr
    df["SuperTrend"] = np.where(close > lower_band, "Bull", "Bear")

    df["Donchian_High"] = high.rolling(20, min_periods=3).max()
    df["Donchian_Low"]  = low.rolling(20, min_periods=3).min()
    df["MA200"] = close.rolling(200, min_periods=3).mean()
    df["Pivot"] = (high + low + close) / 3
    df["R1"] = 2 * df["Pivot"] - low
    df["S1"] = 2 * df["Pivot"] - high

    return df

def summarize_last_row(df):
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else last
    out = {
        "Close": safe_round(last["close"], 2),
        "Volume": safe_round(last["volume"], 0),
        "RSI": safe_round(last["RSI14"], 1),
        "MACD": safe_round(last["MACD"], 3),
        "MACD_Signal": safe_round(last["MACD_Signal"], 3),
        "EMA20": safe_round(last["EMA20"], 2),
        "EMA50": safe_round(last["EMA50"], 2),
        "BB_Lower": safe_round(last["BB_Lower"], 2),
        "BB_Upper": safe_round(last["BB_Upper"], 2),
        "Volume_Mean": safe_round(last.get("Volume_Mean", np.nan), 0),
        "ATR14": safe_round(last.get("ATR14", np.nan), 2),
        "ADX": safe_round(last.get("ADX", np.nan), 1),
        "OBV_Delta": safe_round(last.get("OBV_Delta", np.nan), 0),
        "MFI": safe_round(last.get("MFI", np.nan), 1),
        "CCI": safe_round(last.get("CCI", np.nan), 0),
        "SuperTrend": last.get("SuperTrend", "N/A"),
        "_prev_MACD": safe_round(prev.get("MACD", np.nan), 3),
        "_prev_Signal": safe_round(prev.get("MACD_Signal", np.nan), 3),
        "Pivot": safe_round(last.get("Pivot", np.nan), 2),
        "MA200": safe_round(last.get("MA200", np.nan), 2),
    }
    return out

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
    consensus = f"Achat {dot_green()}" if score_trend >= 2 else (f"Vente {dot_red()}" if score_trend <= -2 else f"Neutre {dot_white()}")
    flat["Consensus"] = consensus

    def add_tf_cols(tf, v):
        flat[f"RSI {tf.upper()}"]        = label_rsi(v.get("RSI"))
        flat[f"Trend {tf.upper()}"]      = label_trend(v.get("EMA20"), v.get("EMA50"))
        macd_lbl = f"{v.get('MACD'):.3f} {dot_green()}" if (v.get("MACD") is not None and v.get("MACD_Signal") is not None and v.get("MACD")>v.get("MACD_Signal")) else (
                   f"{v.get('MACD'):.3f} {dot_white()}" if v.get("MACD") is not None else f"Aucune donnée {dot_white()}")
        if v.get("MACD") is not None and v.get("MACD_Signal") is not None and v.get("MACD")<v.get("MACD_Signal"):
            macd_lbl = f"{v.get('MACD'):.3f} {dot_red()}"
        flat[f"MACD {tf.upper()}"]       = macd_lbl
        flat[f"Bollinger {tf.upper()}"]  = (f"{v.get('Close'):.2f} {dot_green()}" if (v.get("Close") is not None and v.get("BB_Lower") is not None and v.get("Close") < v.get("BB_Lower"))
                                           else f"{v.get('Close') if v.get('Close') is not None else 'N/A'} {dot_red()}" if (v.get("Close") is not None and v.get("BB_Upper") is not None and v.get("Close") > v.get("BB_Upper"))
                                           else (f"{v.get('Close'):.2f} {dot_white()}" if v.get("Close") is not None else f"Aucune donnée {dot_white()}"))
        flat[f"Volume {tf.upper()}"]     = (f"{int(v.get('Volume'))} {dot_green()}" if (v.get("Volume") is not None and v.get("Volume_Mean") is not None and v.get("Volume") >= v.get("Volume_Mean")*1.3)
                                           else f"{int(v.get('Volume'))} {dot_yellow()}" if (v.get("Volume") is not None and v.get("Volume_Mean") is not None and v.get("Volume") >= v.get("Volume_Mean")*1.1)
                                           else (f"{int(v.get('Volume'))} {dot_white()}" if v.get("Volume") is not None else f"Aucune donnée {dot_white()}"))
        flat[f"Prix {tf.upper()}"]       = label_close_vs_ema50(v.get("Close"), v.get("EMA50"))
        flat[f"ADX {tf.upper()}"]        = (f"{v.get('ADX'):.1f} {dot_green()}" if (v.get("ADX") is not None and v.get("ADX")>=25)
                                           else f"{v.get('ADX'):.1f} {dot_white()}" if v.get("ADX") is not None else f"Aucune donnée {dot_white()}")
        flat[f"ATR {tf.upper()}"]        = (f"{v.get('ATR14'):.2f} {dot_green()}" if (v.get("ATR14") is not None and v.get('ATR14')>v.get('ATR14')) # will display neutral mostly
                                           else f"{v.get('ATR14'):.2f} {dot_white()}" if v.get("ATR14") is not None else f"Aucune donnée {dot_white()}")
        flat[f"MFI {tf.upper()}"]        = (f"{v.get('MFI'):.1f} {dot_green()}" if (v.get("MFI") is not None and 20<=v.get("MFI")<=80)
                                           else f"{v.get('MFI'):.1f} {dot_red()}" if v.get("MFI") is not None else f"Aucune donnée {dot_white()}")
        flat[f"CCI {tf.upper()}"]        = (f"{int(v.get('CCI'))} {dot_green()}" if (v.get("CCI") is not None and -100<=v.get("CCI")<=100)
                                           else f"{int(v.get('CCI'))} {dot_red()}" if v.get("CCI") is not None else f"Aucune donnée {dot_white()}")
        flat[f"OBV {tf.upper()}"]        = (f"{int(v.get('OBV_Delta'))} {dot_green()}" if (v.get("OBV_Delta") is not None and v.get("OBV_Delta")>0)
                                           else f"{int(v.get('OBV_Delta'))} {dot_red()}" if v.get("OBV_Delta") is not None else f"Aucune donnée {dot_white()}")
        flat[f"SuperTrend {tf.upper()}"] = (f"{v.get('SuperTrend')} {dot_green()}" if v.get("SuperTrend")=="Bull"
                                           else f"{v.get('SuperTrend')} {dot_red()}" if v.get("SuperTrend")=="Bear" else f"Aucune donnée {dot_white()}")
        flat[f"Pivot {tf.upper()}"]      = (f"{v.get('Pivot'):.2f} {dot_white()}" if v.get("Pivot") is not None else f"Aucune donnée {dot_white()}")
        flat[f"MA200 {tf.upper()}"]      = (f"{v.get('MA200'):.2f} {dot_green()}" if (v.get("MA200") is not None and v.get("Close") is not None and v.get("Close")>v.get("MA200"))
                                           else f"{v.get('MA200'):.2f} {dot_red()}" if v.get("MA200") is not None else f"Aucune donnée {dot_white()}")

        # raw for scoring
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

# ---------------------
# Update sheet (V5.2.1)
# ---------------------
def update_sheet():
    try:
        sh = gc.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet("MultiTF")
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title="MultiTF", rows="800", cols="280")

        cryptos_pairs = ["BTC-USD","ETH-USD","SOL-USD","BNB-USD","ADA-USD","DOGE-USD","AVAX-USD","XRP-USD","LINK-USD","MATIC-USD"]
        symbols = [c.split("-")[0] for c in cryptos_pairs]

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
            actuality = cached_actuality(symbol)

            # scoring (use raw values)
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

            score_10 = compute_global_score(tfs, senti, actuality_score=actuality.get("Actuality_Score"))
            score_display = f"{score_10:.1f} {color_from_score(score_10)}"

            px = get_price_usd(symbol)
            price_str = f"{safe_round(px,2):.2f} $" if px is not None else "N/A"

            flat = {
                "Crypto": res["Crypto"],
                "Prix Actuel (USD)": price_str,
                "Global Score (0-10)": score_display,
                "Consensus": res.get("Consensus"),
                # Update will be placed as 5th column later
            }

            for k, v in res.items():
                if k.startswith("_RAW_"): continue
                if k in ["Crypto","Consensus"]: continue
                flat[k] = v

            flat["Fear & Greed Index"] = senti.get("FearGreed_Index")
            flat["Social Sentiment"]   = senti.get("Social_Sentiment")
            flat["News Intensity"]     = senti.get("News_Intensity")
            flat["Sentiment Score"]    = senti.get("Sentiment_Score")
            flat["Sentiment Global"]   = _sentiment_global_label(senti)

            flat["News Count"]         = actuality.get("News_Count")
            flat["Positive %"]         = actuality.get("Positive_%")
            flat["Negative %"]         = actuality.get("Negative_%")
            flat["Trend Score"]        = actuality.get("Trend_Score")
            flat["Actuality Score"]    = actuality.get("Actuality_Score")
            flat["Actuality Sentiment"] = actuality.get("Actuality_Sentiment")

            flat["Update"] = format_update_label(now_paris())
            rows.append(flat)
            time.sleep(1.0)

        if not rows:
            print("⚠️ Aucune donnée récupérée", flush=True)
            return

        df_out = pd.DataFrame(rows)
        df_out = df_out.where(pd.notnull(df_out), other=f"Aucune donnée {dot_white()}")

        front = ["Crypto","Prix Actuel (USD)","Global Score (0-10)","Consensus","Update"]

        families = []
        for fam in ["RSI","Trend","MACD","Bollinger","Volume","Prix","ADX","ATR","MFI","CCI","OBV","SuperTrend","Pivot","MA200"]:
            for tf in ["1H","6H","1D"]:
                col = f"{fam} {tf}"
                if col in df_out.columns and col not in families:
                    families.append(col)

        emotion_cols = [c for c in ["Fear & Greed Index","Social Sentiment","News Intensity","Sentiment Score","Sentiment Global"] if c in df_out.columns]
        actuality_cols = [c for c in ["News Count","Positive %","Negative %","Trend Score","Actuality Score","Actuality Sentiment"] if c in df_out.columns]

        ordered = front + families
        remaining = [c for c in df_out.columns if c not in ordered + emotion_cols + actuality_cols]
        ordered += remaining + emotion_cols + actuality_cols
        ordered = [c for c in ordered if c in df_out.columns]

        df_out = df_out.reindex(columns=ordered)

        ws.clear()
        set_with_dataframe(ws, df_out)

        nowlabel = now_paris()
        print(f"✅ Update terminée à {nowlabel.hour:02d}:{nowlabel.minute:02d} — Données actualisées avec succès.", flush=True)

    except Exception as e:
        print(f"❌ Erreur update_sheet() : {e}", flush=True)

# ---------------------
# Threads / Flask
# ---------------------
def run_bot():
    print("🚀 Lancement du bot Multi-Timeframe", flush=True)
    update_sheet()
    while True:
        print("⏳ Attente avant prochaine mise à jour (1h)...", flush=True)
        time.sleep(3600)
        update_sheet()

def keep_alive():
    url = RENDER_EXTERNAL_URL or "https://crypto-bot-multitf.onrender.com"
    while True:
        try:
            requests.get(url, timeout=10)
            print("💤 Ping keep-alive envoyé.", flush=True)
        except Exception as e:
            print(f"⚠️ Erreur keep_alive : {e}", flush=True)
        time.sleep(600)

@app.route("/")
def home():
    return "✅ Crypto Bot Multi-Timeframe — V5.2.1 Stable & Fusionné"

@app.route("/run")
def manual_run():
    threading.Thread(target=update_sheet, daemon=True).start()
    return "Mise à jour manuelle lancée"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    threading.Thread(target=run_bot, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    app.run(host="0.0.0.0", port=port)
