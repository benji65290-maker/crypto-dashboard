
import threading
import time
import requests
import pandas as pd
import numpy as np
import os
import json
import gspread
import re
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
    if not SHEET_ID:
        raise RuntimeError("Env var GOOGLE_SHEET_ID manquante.")
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
# 🎨 Aides de lisibilité: labels/pastilles + renommage colonnes
# ======================================================
def _rsi_label(val):
    try:
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return "N/A ⚪"
        v = float(val)
        if v < 30: return f"{v:.1f} 🟢 Achat"
        if v > 70: return f"{v:.1f} 🔴 Vente"
        return f"{v:.1f} ⚪ Neutre"
    except Exception:
        return "N/A ⚪"

def _trend_label(s):
    s = "" if s is None or (isinstance(s, float) and np.isnan(s)) else str(s)
    if s == "Bull": return "Bull 🟢"
    if s == "Bear": return "Bear 🔴"
    return "N/A ⚪"

def _macd_cross_label(s):
    s = "" if s is None or (isinstance(s, float) and np.isnan(s)) else str(s)
    if "Bullish" in s: return "📈 Bullish 🟢"
    if "Bearish" in s: return "📉 Bearish 🔴"
    return "❌ Aucun ⚪"

def _bollinger_label(s):
    s = "" if s is None or (isinstance(s, float) and np.isnan(s)) else str(s)
    if "Survente" in s: return "⬇️ Survente 🟢"
    if "Surachat" in s: return "⬆️ Surachat 🔴"
    return "〰️ Neutre ⚪"

def _volume_label(s):
    s = "" if s is None or (isinstance(s, float) and np.isnan(s)) else str(s).lower()
    if "haussier" in s: return "⬆️ Haussier 🟢"
    if "baissier" in s: return "⬇️ Baissier 🔴"
    return "〰️ Neutre ⚪"

def _signal_global_from_score(score):
    try:
        s = float(score)
    except Exception:
        return "⚪ Neutre"
    if s > 8:  return "🟢 Achat fort"
    if s > 6:  return "🔵 Achat modéré"
    if s > 5:  return "⚪ Neutre"
    if s > 3:  return "🟠 Vente modérée"
    return "🔴 Vente forte"

def _prettify_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renomme les colonnes en libellés lisibles."""
    rename_map = {}
    for c in df.columns:
        new = str(c)
        # suffixes timeframe
        new = new.replace("_1h", " 1H").replace("_6h", " 6H").replace("_1d", " 1D")
        new = new.replace("_1H", " 1H").replace("_6H", " 6H").replace("_1D", " 1D")
        # remplacements lisibles
        new = new.replace("MACD_Cross", "MACD Cross")\
                 .replace("Bollinger_Pos", "Bollinger Pos")\
                 .replace("Volume_Sentiment", "Volume Sentiment")\
                 .replace("LastUpdate", "Last Update")\
                 .replace("GlobalScore_0_10", "Global Score (0-10)")\
                 .replace("Signal_Global", "Signal Global")\
                 .replace("FearGreed_Index", "Fear & Greed Index")\
                 .replace("FearGreed_Label", "Fear & Greed Label")\
                 .replace("News_Intensity", "News Intensity")\
                 .replace("Sentiment_Score", "Sentiment Score")\
                 .replace("Sentiment_Global", "Sentiment Global")\
                 .replace("Close", "Close Price")
        new = re.sub(r"\s{2,}", " ", new).strip()
        if new != c:
            rename_map[c] = new
    return df.rename(columns=rename_map)

# ======================================================
# ⚖️ Scoring "pro trader" (pondérations)
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
    return 1.0 - (r - 30) / 40.0  # 30->1 ; 70->0

def _score_from_trend(s):
    s = "" if s is None else str(s)
    if s == "Bull": return 1.0
    if s == "Bear": return 0.0
    return 0.5

def _score_from_macd(s):
    s = "" if s is None else str(s)
    if "Bullish" in s: return 1.0
    if "Bearish" in s: return 0.0
    return 0.5

def _score_from_bb(s):
    s = "" if s is None else str(s)
    if "Survente" in s: return 1.0
    if "Surachat" in s: return 0.0
    return 0.5

def _score_from_vol(s):
    s = "" if s is None else str(s).lower()
    if "haussier" in s: return 1.0
    if "baissier" in s: return 0.0
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

def compute_global_score(tfs: dict, senti: dict):
    score_components = []
    for tf, wtf in _W_TF.items():
        vals = tfs.get(tf, {}) if isinstance(tfs, dict) else {}
        s_rsi   = _score_from_rsi(vals.get("RSI"))
        s_trend = _score_from_trend(vals.get("Trend"))
        s_macd  = _score_from_macd(vals.get("MACD_Cross"))
        s_bb    = _score_from_bb(vals.get("Bollinger_Pos"))
        s_vol   = _score_from_vol(vals.get("Volume_Sentiment"))
        s_tf = (s_rsi * _W_RSI + s_trend * _W_TREND + s_macd * _W_MACD + s_bb * _W_BB + s_vol * _W_VOL)
        score_components.append(s_tf * wtf)
    s_senti = _score_from_sentiment(senti) * _W_SENTI
    total_0_1 = max(0.0, min(1.0, sum(score_components) + s_senti))
    score_0_10 = round(total_0_1 * 10.0, 1)  # 1 décimale
    signal = _signal_global_from_score(score_0_10)
    return score_0_10, signal

# ======================================================
# 🌍 Sentiments par crypto
# ======================================================
COINGECKO_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "BNB": "binancecoin",
    "ADA": "cardano",
    "DOGE": "dogecoin",
    "AVAX": "avalanche-2",
    "XRP": "ripple",
    "LINK": "chainlink",
    "MATIC": "matic-network",
}

def get_sentiment_for_symbol(symbol: str) -> dict:
    try:
        # Fear & Greed global
        fg_value = np.nan
        fg_label = "❌"
        try:
            fng = requests.get("https://api.alternative.me/fng/", timeout=10).json()
            fg_value = float(fng["data"][0]["value"])
            if fg_value < 25: fg_label = "😱 Extreme Fear"
            elif fg_value < 50: fg_label = "😟 Fear"
            elif fg_value < 75: fg_label = "😃 Greed"
            else: fg_label = "🤑 Extreme Greed"
        except Exception:
            pass

        # Trending Coingecko
        social_sent = 0.0
        try:
            trending = requests.get("https://api.coingecko.com/api/v3/search/trending", timeout=10).json()
            trending_symbols = [c["item"]["symbol"].upper() for c in trending.get("coins", [])]
            social_sent = 100.0 if symbol.upper() in trending_symbols else max(0.0, 10.0 * len(trending_symbols))
            social_sent = min(100.0, social_sent)
        except Exception:
            pass

        # Intensité "news" via volatilité globale (fallback): mcap change 24h
        news_intensity = 0.5
        try:
            news_req = requests.get("https://api.coingecko.com/api/v3/global", timeout=10).json()
            mcap_change = float(news_req["data"]["market_cap_change_percentage_24h_usd"])
            news_intensity = min(1.0, abs(mcap_change) / 5.0)
        except Exception:
            pass

        # Score synthétique (0-100)
        base = []
        if not np.isnan(fg_value): base.append(fg_value)
        base.append(social_sent)
        base.append(news_intensity * 100.0)
        senti_score = float(np.mean(base)) if base else np.nan

        return {
            "FearGreed_Index": round(fg_value, 1) if not np.isnan(fg_value) else np.nan,
            "FearGreed_Label": fg_label,
            "Social_Sentiment": int(round(social_sent, 0)),
            "News_Intensity": round(news_intensity, 3),
            "Sentiment_Score": round(senti_score, 1) if not np.isnan(senti_score) else np.nan,
        }
    except Exception as e:
        print(f"⚠️ Erreur get_sentiment_for_symbol({symbol}) : {e}", flush=True)
        return {
            "FearGreed_Index": np.nan,
            "FearGreed_Label": "❌",
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
        if not np.isnan(ni): vals.append(1.0 - abs(ni - 0.5) * 2.0)  # max quand ni ≈ 0.5
        if not vals:
            return "⚪ Neutre"
        m = sum(vals) / len(vals)
        if m >= 0.7: return "🟢 Positif"
        if m >= 0.5: return "⚪ Neutre"
        return "🔴 Négatif"
    except Exception:
        return "⚪ Neutre"

# ======================================================
# ⚙️ API Coinbase – Données OHLC
# ======================================================
def get_candles(symbol_pair, granularity):
    """Renvoie un DataFrame trié (time asc) avec colonnes: time, low, high, open, close, volume."""
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
# 📈 Calculs d’indicateurs techniques
# ======================================================
def compute_indicators(df):
    if df is None or df.empty: return df
    df = df.copy()
    close = df["close"]
    high  = df["high"]
    low   = df["low"]
    vol   = df["volume"]

    # RSI (Wilder)
    delta = close.diff()
    up = delta.clip(lower=0)
    down = (-delta).clip(lower=0)
    roll_up = up.ewm(alpha=1/14, adjust=False).mean()
    roll_down = down.ewm(alpha=1/14, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    df["RSI14"] = 100 - (100 / (1 + rs))

    # MACD
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()

    # EMAs
    df["EMA20"] = close.ewm(span=20, adjust=False).mean()
    df["EMA50"] = close.ewm(span=50, adjust=False).mean()

    # Bollinger
    bb_mid = close.rolling(20).mean()
    bb_std = close.rolling(20).std()
    df["BB_Mid"]   = bb_mid
    df["BB_Upper"] = bb_mid + 2 * bb_std
    df["BB_Lower"] = bb_mid - 2 * bb_std

    # Volume / VWAP
    df["Volume_Mean"] = vol.rolling(20).mean()
    df["VWAP"] = (close * vol).cumsum() / vol.replace(0, np.nan).cumsum()

    # ATR / TR
    prev_close = close.shift(1)
    tr_components = pd.concat([
        (high - low), (high - prev_close).abs(), (low - prev_close).abs()
    ], axis=1)
    TR = tr_components.max(axis=1)
    df["ATR14"] = TR.ewm(alpha=1/14, adjust=False).mean()
    df["TR"] = TR

    # StochRSI proxy
    low14 = low.rolling(14).min()
    high14 = high.rolling(14).max()
    df["StochRSI"] = ((close - low14) / (high14 - low14) * 100).replace([np.inf, -np.inf], np.nan)

    # ADX
    up_move   = high.diff()
    down_move = -low.diff()
    plus_dm  = np.where((up_move > 0) & (up_move > down_move), up_move, 0.0)
    minus_dm = np.where((down_move > 0) & (down_move > up_move), down_move, 0.0)
    atr14 = df["ATR14"]
    plus_di  = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1/14, adjust=False).mean() / atr14
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1/14, adjust=False).mean() / atr14
    dx = 100 * ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan))
    df["ADX"] = dx.ewm(alpha=1/14, adjust=False).mean()

    # Ichimoku (sans décalage)
    high_9 = high.rolling(9).max(); low_9 = low.rolling(9).min()
    df["ICH_Tenkan"] = (high_9 + low_9) / 2
    high_26 = high.rolling(26).max(); low_26 = low.rolling(26).min()
    df["ICH_Kijun"] = (high_26 + low_26) / 2
    df["ICH_SpanA"] = (df["ICH_Tenkan"] + df["ICH_Kijun"]) / 2
    df["ICH_SpanB"] = (high.rolling(52).max() + low.rolling(52).min()) / 2

    # OBV
    direction = np.sign(close.diff()).fillna(0)
    df["OBV"] = (vol * direction).cumsum()

    # MFI
    typical_price = (high + low + close) / 3
    money_flow = typical_price * vol
    pos_flow = np.where(typical_price > typical_price.shift(), money_flow, 0.0)
    neg_flow = np.where(typical_price < typical_price.shift(), money_flow, 0.0)
    pos_mf = pd.Series(pos_flow, index=df.index).rolling(14).sum()
    neg_mf = pd.Series(neg_flow, index=df.index).rolling(14).sum().replace(0, np.nan)
    df["MFI"] = 100 - (100 / (1 + (pos_mf / neg_mf)))

    # SAR (proxy)
    df["SAR"] = close.rolling(3).min().shift(1)

    # CCI
    tp = (high + low + close) / 3
    df["CCI"] = (tp - tp.rolling(20).mean()) / (0.015 * tp.rolling(20).std())

    # SuperTrend proxy
    hl2 = (high + low) / 2
    st_atr = df["ATR14"]
    upper_band = hl2 + 3 * st_atr
    lower_band = hl2 - 3 * st_atr
    df["SuperTrend"] = np.where(close > lower_band, "Bull", "Bear")

    # Donchian
    df["Donchian_High"] = high.rolling(20).max()
    df["Donchian_Low"]  = low.rolling(20).min()

    # MA200
    df["MA200"] = close.rolling(200).mean()

    # Pivots
    df["Pivot"] = (high + low + close) / 3
    df["R1"] = 2 * df["Pivot"] - low
    df["S1"] = 2 * df["Pivot"] - high

    return df

ADV_KEYS = [
    "RSI14","MACD","MACD_Signal","EMA20","EMA50",
    "BB_Mid","BB_Upper","BB_Lower","Volume_Mean","VWAP",
    "ATR14","StochRSI","ADX",
    "ICH_Tenkan","ICH_Kijun","ICH_SpanA","ICH_SpanB",
    "OBV","MFI","SAR","CCI","SuperTrend","Donchian_High","Donchian_Low","MA200",
    "Pivot","R1","S1"
]

def summarize_last_row(df):
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else last

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

    vol_trend = "⬆️ Haussier" if last["volume"] > last["Volume_Mean"] else "⬇️ Baissier"

    out = {
        "Close": safe_round(last["close"]),
        "RSI": safe_round(last["RSI14"]),
        "Trend": trend,
        "MACD_Cross": macd_signal,
        "Bollinger_Pos": bb_pos,
        "Volume_Sentiment": vol_trend,
    }
    for k in ADV_KEYS:
        v = last.get(k, np.nan)
        out[k] = safe_round(v) if k != "SuperTrend" else (v if isinstance(v, str) else "N/A")
    return out

def analyze_symbol(symbol_pair):
    periods = {"1h": 3600, "6h": 21600, "1d": 86400}
    results = {}
    for label, gran in periods.items():
        df = get_candles(symbol_pair, gran)
        if df is None or len(df) < 60:
            print(f"⚠️ Historique insuffisant pour {symbol_pair} en {label}", flush=True)
            continue
        df = compute_indicators(df)
        results[label] = summarize_last_row(df)
    if not results:
        return None

    # Consensus simple EMA20/EMA50
    trends = [v.get("Trend") for v in results.values()]
    bulls = trends.count("Bull")
    bears = trends.count("Bear")
    consensus = "🟢 Achat fort" if bulls >= 2 else "🔴 Vente forte" if bears >= 2 else "⚪ Neutre"

    flat = {"Crypto": symbol_pair.split("-")[0], "Consensus": consensus}
    for tf, vals in results.items():
        for k, v in vals.items():
            flat[f"{k}_{tf}"] = v
        # vues colorées
        flat[f"RSI_{tf}_View"] = _rsi_label(vals.get("RSI"))
        flat[f"Trend_{tf}_View"] = _trend_label(vals.get("Trend"))
        flat[f"MACD_Cross_{tf}_View"] = _macd_cross_label(vals.get("MACD_Cross"))
        flat[f"Bollinger_Pos_{tf}_View"] = _bollinger_label(vals.get("Bollinger_Pos"))
        flat[f"Volume_Sentiment_{tf}_View"] = _volume_label(vals.get("Volume_Sentiment"))
    return flat

# ======================================================
# 📊 Mise à jour Google Sheets (par-crypto, sentiments en colonnes)
# ======================================================
def update_sheet():
    try:
        sh = gc.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet("MultiTF")
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title="MultiTF", rows="600", cols="260")

        cryptos = ["BTC-USD","ETH-USD","SOL-USD","BNB-USD","ADA-USD","DOGE-USD","AVAX-USD","XRP-USD","LINK-USD","MATIC-USD"]

        rows = []
        for pair in cryptos:
            res = analyze_symbol(pair)
            if not res:
                print(f"⚠️ Données manquantes pour {pair} — ignoré.", flush=True)
                continue

            symbol = pair.split("-")[0]
            senti = get_sentiment_for_symbol(symbol)

            # reconstruire bloc tfs minimal pour scoring
            tfs = {}
            for tf in ["1h","6h","1d"]:
                tfs[tf] = {
                    "RSI": res.get(f"RSI_{tf}"),
                    "Trend": res.get(f"Trend_{tf}"),
                    "MACD_Cross": res.get(f"MACD_Cross_{tf}"),
                    "Bollinger_Pos": res.get(f"Bollinger_Pos_{tf}"),
                    "Volume_Sentiment": res.get(f"Volume_Sentiment_{tf}"),
                }

            score_10, signal_global = compute_global_score(tfs, senti)

            flat = {"Crypto": res["Crypto"], "GlobalScore_0_10": score_10, "Signal_Global": signal_global, "Consensus": res.get("Consensus")}
            # recopier techniques + vues
            for k, v in res.items():
                if k in ["Crypto","Consensus"]:
                    continue
                flat[k] = v
            # sentiments en colonnes
            flat.update(senti)
            flat["Sentiment_Global"] = _sentiment_global_label(senti)
            flat["LastUpdate"] = time.strftime("%Y-%m-%d %H:%M:%S")

            rows.append(flat)
            time.sleep(1.0)

        if not rows:
            print("⚠️ Aucune donnée récupérée", flush=True)
            return

        df_out = pd.DataFrame(rows)

        # ----- Ordre de colonnes lisible
        cols_front = ["Crypto","GlobalScore_0_10","Signal_Global","Consensus"]

        def _order_block(indic):
            out = []
            for tf in ["1h","6h","1d"]:
                num = f"{indic}_{tf}"
                view = f"{indic}_{tf}_View"
                if num in df_out.columns: out.append(num)
                if view in df_out.columns: out.append(view)
            return out

        ordered_blocks = []
        for indic in ["RSI","Trend","MACD_Cross","Bollinger_Pos","Volume_Sentiment","Close"]:
            ordered_blocks += _order_block(indic)

        used = set(cols_front + ordered_blocks)
        emotion_cols = ["FearGreed_Index","FearGreed_Label","Social_Sentiment","News_Intensity","Sentiment_Score","Sentiment_Global","LastUpdate"]
        remaining = [c for c in df_out.columns if c not in used and c not in emotion_cols]

        ordered = cols_front + ordered_blocks + remaining + emotion_cols
        ordered = [c for c in ordered if c in df_out.columns]
        df_out = df_out.reindex(columns=ordered)

        # Renommer colonnes en version lisible
        df_out = _prettify_columns(df_out)

        ws.clear()
        set_with_dataframe(ws, df_out)
        print("✅ Feuille 'MultiTF' mise à jour (lisible + pastilles + émotions par crypto).", flush=True)

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
    return "✅ Crypto Bot Multi-Timeframe actif (1h / 6h / 1D) — lisible, pastilles, score global, émotions par crypto"

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