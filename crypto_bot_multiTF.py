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
# 🎨 Helpers lisibilité (pastilles + renommage colonnes)
# ======================================================
def _rsi_signal_label(val):
    try:
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return "RSI N/A ⚪"
        v = float(val)
        if v < 30: return "RSI Achat 🟢"
        if v > 70: return "RSI Vente 🔴"
        return "RSI Neutre ⚪"
    except Exception:
        return "RSI N/A ⚪"

def _trend_signal_label(s):
    s = "" if s is None or (isinstance(s, float) and np.isnan(s)) else str(s)
    if s == "Bull": return "Tendance Bull 🟢"
    if s == "Bear": return "Tendance Bear 🔴"
    return "Tendance N/A ⚪"

def _macd_cross_label(s):
    s = "" if s is None or (isinstance(s, float) and np.isnan(s)) else str(s)
    if "Bullish" in s: return "MACD Cross Bullish 🟢"
    if "Bearish" in s: return "MACD Cross Bearish 🔴"
    return "MACD Cross Neutre ⚪"

def _bollinger_label(s):
    s = "" if s is None or (isinstance(s, float) and np.isnan(s)) else str(s)
    if "Survente" in s: return "Bollinger Survente 🟢"
    if "Surachat" in s: return "Bollinger Surachat 🔴"
    return "Bollinger Neutre ⚪"

def _volume_label(s):
    s = "" if s is None or (isinstance(s, float) and np.isnan(s)) else str(s).lower()
    if "haussier" in s: return "Volume Haussier 🟢"
    if "baissier" in s: return "Volume Baissier 🔴"
    return "Volume Neutre ⚪"

def _prettify_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    for c in df.columns:
        new = str(c)
        # timeframe suffixes
        new = new.replace("_1h", " 1H").replace("_6h", " 6H").replace("_1d", " 1D")
        new = new.replace("_1H", " 1H").replace("_6H", " 6H").replace("_1D", " 1D")
        # readable replacements
        new = new.replace("MACD_Cross", "MACD Cross")\
                 .replace("Bollinger_Pos", "Bollinger Pos")\
                 .replace("Volume_Sentiment", "Volume Sentiment")\
                 .replace("LastUpdate", "Last Update")\
                 .replace("GlobalScore_0_10", "Global Score (0-10)")\
                 .replace("Signal_Global", "Signal Global")\
                 .replace("FearGreed_Index", "Fear & Greed Index")\
                 .replace("FearGreed_Label", "Fear & Greed Label")\
                 .replace("News_Intensity", "News Intensity")\
                 .replace("Sentiment_Score", "Sentiment Score")
        new = re.sub(r"\s{2,}", " ", new).strip()
        if new != c:
            rename_map[c] = new
    return df.rename(columns=rename_map)

# ======================================================
# 🌍 Indicateurs de Sentiment & Émotion
# ======================================================
def get_market_sentiment():
    """Récupère les indicateurs de sentiment global (Fear & Greed, actualité, social, volatilité)."""
    try:
        # --- Fear & Greed Index (Alternative.me)
        fng_url = "https://api.alternative.me/fng/"
        fg_data = requests.get(fng_url, timeout=10).json()
        fg_value = int(fg_data["data"][0]["value"])
        if fg_value < 25:
            fg_label = "😱 Extreme Fear"
        elif fg_value < 50:
            fg_label = "😟 Fear"
        elif fg_value < 75:
            fg_label = "😃 Greed"
        else:
            fg_label = "🤑 Extreme Greed"

        # --- Social Sentiment via Coingecko Trending
        try:
            trending = requests.get("https://api.coingecko.com/api/v3/search/trending", timeout=10).json()
            coins = [c["item"]["symbol"].upper() for c in trending["coins"]]
            score_social = min(100, len(coins) * 10)  # proxy euph
        except:
            score_social = 0

        # --- News Intensity (fallback via Coingecko global)
        try:
            news_req = requests.get("https://api.coingecko.com/api/v3/global", timeout=10).json()
            mcap_change = news_req["data"]["market_cap_change_percentage_24h_usd"]
            news_intensity = min(1.0, abs(mcap_change) / 5)
        except:
            news_intensity = 0.5

        # --- BTC Volatility 30d
        try:
            btc_candles = requests.get("https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=86400", timeout=10).json()
            df_btc = pd.DataFrame(btc_candles, columns=["time","low","high","open","close","volume"])
            df_btc["returns"] = pd.Series(df_btc["close"]).pct_change()
            vol_30d = np.std(df_btc["returns"].tail(30)) * np.sqrt(365)
        except:
            vol_30d = np.nan

        return {
            "FearGreed_Index": fg_value,
            "FearGreed_Label": fg_label,
            "Social_Sentiment": score_social,
            "News_Intensity": round(news_intensity, 3),
            "BTC_Volatility_30d": round(vol_30d, 4)
        }

    except Exception as e:
        print(f"⚠️ Erreur get_market_sentiment() : {e}", flush=True)
        return {
            "FearGreed_Index": np.nan,
            "FearGreed_Label": "❌",
            "Social_Sentiment": np.nan,
            "News_Intensity": np.nan,
            "BTC_Volatility_30d": np.nan
        }

# ======================================================
# ⚙️ API Coinbase – Données OHLC
# ======================================================
def get_candles(symbol_pair, granularity):
    """
    Coinbase renvoie des lignes [time, low, high, open, close, volume]
    time est en secondes (UTC). On trie par date croissante.
    """
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
# 🎨 Helpers lisibilité (pastilles + renommage colonnes)
# ======================================================
def _rsi_signal_label(val):
    try:
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return "RSI N/A ⚪"
        v = float(val)
        if v < 30: return "RSI Achat 🟢"
        if v > 70: return "RSI Vente 🔴"
        return "RSI Neutre ⚪"
    except Exception:
        return "RSI N/A ⚪"

def _trend_signal_label(s):
    s = "" if s is None or (isinstance(s, float) and np.isnan(s)) else str(s)
    if s == "Bull": return "Tendance Bull 🟢"
    if s == "Bear": return "Tendance Bear 🔴"
    return "Tendance N/A ⚪"

def _macd_cross_label(s):
    s = "" if s is None or (isinstance(s, float) and np.isnan(s)) else str(s)
    if "Bullish" in s: return "MACD Cross Bullish 🟢"
    if "Bearish" in s: return "MACD Cross Bearish 🔴"
    return "MACD Cross Neutre ⚪"

def _bollinger_label(s):
    s = "" if s is None or (isinstance(s, float) and np.isnan(s)) else str(s)
    if "Survente" in s: return "Bollinger Survente 🟢"
    if "Surachat" in s: return "Bollinger Surachat 🔴"
    return "Bollinger Neutre ⚪"

def _volume_label(s):
    s = "" if s is None or (isinstance(s, float) and np.isnan(s)) else str(s).lower()
    if "haussier" in s: return "Volume Haussier 🟢"
    if "baissier" in s: return "Volume Baissier 🔴"
    return "Volume Neutre ⚪"

def _prettify_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    for c in df.columns:
        new = str(c)
        # timeframe suffixes
        new = new.replace("_1h", " 1H").replace("_6h", " 6H").replace("_1d", " 1D")
        new = new.replace("_1H", " 1H").replace("_6H", " 6H").replace("_1D", " 1D")
        # readable replacements
        new = new.replace("MACD_Cross", "MACD Cross")\
                 .replace("Bollinger_Pos", "Bollinger Pos")\
                 .replace("Volume_Sentiment", "Volume Sentiment")\
                 .replace("LastUpdate", "Last Update")\
                 .replace("GlobalScore_0_10", "Global Score (0-10)")\
                 .replace("Signal_Global", "Signal Global")\
                 .replace("FearGreed_Index", "Fear & Greed Index")\
                 .replace("FearGreed_Label", "Fear & Greed Label")\
                 .replace("News_Intensity", "News Intensity")\
                 .replace("Sentiment_Score", "Sentiment Score")
        new = re.sub(r"\s{2,}", " ", new).strip()
        if new != c:
            rename_map[c] = new
    return df.rename(columns=rename_map)

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
def update_sheet():
    try:
        sh = gc.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet("MultiTF")
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title="MultiTF", rows="200", cols="120")

        cryptos = [
            "BTC-USD", "ETH-USD", "SOL-USD", "BNB-USD",
            "ADA-USD", "DOGE-USD", "AVAX-USD", "XRP-USD",
            "LINK-USD", "MATIC-USD"
        ]

        rows = []

        # --- 1️⃣ Ajout d’une ligne “Sentiment Global”
        sentiment = get_market_sentiment()
        sentiment["Crypto"] = "🌎 Sentiment_Global"
        sentiment["LastUpdate"] = time.strftime("%Y-%m-%d %H:%M:%S")
        rows.append(sentiment)

        # --- 2️⃣ Boucle sur cryptos
        for pair in cryptos:
            res = analyze_symbol(pair)
            if res:
                rows.append(res)
                print(f"✅ {res['Crypto']} → {res['Consensus']}", flush=True)
            time.sleep(1.5)

        if not rows:
            print("⚠️ Aucune donnée récupérée", flush=True)
            return

        df_out = pd.DataFrame(rows)
        ws.clear()
        set_with_dataframe(ws, df_out)
        print("✅ Feuille 'MultiTF' mise à jour avec indicateurs émotionnels !", flush=True)

    except Exception as e:
        print(f"❌ Erreur update_sheet() : {e}", flush=True)

# ======================================================
# 🌍 Indicateurs de Sentiment & Émotion (par crypto)
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

def get_sentiment_for_symbol(symbol: str):
    """
    Récupère les indicateurs de sentiment spécifiques à une crypto.
    - Fear & Greed (global)
    - Social Sentiment (présence dans trending)
    - News Intensity (|var 24h| de la crypto)
    - Sentiment_Score (0-100)
    """
    try:
        # Fear & Greed global
        fng = requests.get("https://api.alternative.me/fng/", timeout=10).json()
        fg_value = int(fng["data"][0]["value"])
        if fg_value < 25: fg_label = "😱 Extreme Fear"
        elif fg_value < 50: fg_label = "😟 Fear"
        elif fg_value < 75: fg_label = "😃 Greed"
        else: fg_label = "🤑 Extreme Greed"

        # Trending (Coingecko)
        trending = requests.get("https://api.coingecko.com/api/v3/search/trending", timeout=10).json()
        trending_symbols = [c["item"]["symbol"].upper() for c in trending.get("coins", [])]
        social_sent = 100 if symbol.upper() in trending_symbols else 30  # 30 par défaut

        # News intensity via var 24h de la crypto
        cg_id = COINGECKO_IDS.get(symbol.upper())
        news_intensity = 0.5
        if cg_id:
            details = requests.get(
                f"https://api.coingecko.com/api/v3/coins/{cg_id}?localization=false&tickers=false&market_data=true",
                timeout=12
            ).json()
            chg = details.get("market_data", {}).get("price_change_percentage_24h")
            if chg is None:
                chg = 0.0
            news_intensity = min(1.0, abs(float(chg)) / 10.0)

        # Score synthétique (0-100) simple non pondéré
        sentiment_score = int(max(0, min(100, (fg_value * 0.4) + (social_sent * 0.2) + ((1.0 - news_intensity) * 100 * 0.4))))

        return {
            "FearGreed_Index": fg_value,
            "FearGreed_Label": fg_label,
            "Social_Sentiment": int(social_sent),
            "News_Intensity": round(news_intensity, 3),
            "Sentiment_Score": int(sentiment_score),
        }
    except Exception as e:
        print(f"⚠️ Erreur sentiment {symbol}: {e}", flush=True)
        return {
            "FearGreed_Index": np.nan,
            "FearGreed_Label": "❌",
            "Social_Sentiment": np.nan,
            "News_Intensity": np.nan,
            "Sentiment_Score": np.nan,
        }

# ======================================================
# 🧮 Scoring & étiquettes couleur
# ======================================================
def _score_from_rsi(rsi):
    if pd.isna(rsi): return 0
    if rsi < 30: return 1
    if rsi > 70: return -1
    return 0

def _score_from_macd_cross(s):
    if not s:
        return 0
    s = str(s)
    if "Bullish" in s: return 1
    if "Bearish" in s: return -1
    return 0

def _score_from_bb(s):
    if not s:
        return 0
    s = str(s)
    if "Survente" in s: return 1
    if "Surachat" in s: return -1
    return 0

def _score_from_trend(trend):
    if trend == "Bull": return 1
    if trend == "Bear": return -1
    return 0

def _score_from_volume(s):
    if not s:
        return 0
    s = str(s)
    if "haussier" in s: return 0.5
    if "baissier" in s: return -0.5
    return 0

def _label_from_score(x):
    if x > 0.3: return "Achat 🟢"
    if x < -0.3: return "Vente 🔴"
    return "Neutre ⚪"

def compute_global_score(results_by_tf, sentiment_info):
    """
    results_by_tf: dict {"1h": {...}, "6h": {...}, "1d": {...}}
    sentiment_info: dict avec Sentiment_Score 0-100
    Retourne (score_decimal_0_10, signal_global_label)
    """
    tf_scores = []
    for tf, vals in results_by_tf.items():
        s = 0.0
        s += _score_from_trend(vals.get("Trend"))
        s += _score_from_macd_cross(vals.get("MACD_Cross", ""))
        s += _score_from_bb(vals.get("Bollinger_Pos", ""))
        s += _score_from_rsi(vals.get("RSI"))
        s += _score_from_volume(vals.get("Volume_Sentiment", ""))
        # pondération légère 1D > 6h > 1h
        w = 1.0 if tf == "1h" else (1.2 if tf == "6h" else 1.5)
        tf_scores.append(s * w)

    raw = np.nanmean(tf_scores) if tf_scores else 0.0
    # ajoute sentiment (0..100 -> -1..+1)
    sent = sentiment_info.get("Sentiment_Score")
    if sent is not None and not pd.isna(sent):
        raw += ((float(sent) - 50.0) / 50.0)  # -1 à +1

    # Clamp & map to 0..10
    raw = max(-5.0, min(5.0, raw))
    score_0_10 = round((raw + 5.0) * (10.0 / 10.0), 2)  # (-5..+5) -> (0..10)
    return score_0_10, _label_from_score(raw)

# ======================================================
# 🧮 Analyse multi-période (ajout Close dans le résumé)
# ======================================================
def summarize_last_row(df):
    """Retourne un dict (valeurs dernière ligne) formaté + signaux lisibles."""
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

    vol_trend = "⬆️ Volume haussier" if last["volume"] > last["Volume_Mean"] else "⬇️ Volume baissier"

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
        out[k] = safe_round(v) if k not in ["SuperTrend"] else (v if isinstance(v, str) else "N/A")

    return out

# ======================================================
# 📊 Mise à jour Google Sheets (corrigée: par-crypto + score + émotions)
# ======================================================
def update_sheet():
    try:
        sh = gc.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet("MultiTF")
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title="MultiTF", rows="500", cols="250")

        cryptos = [
            "BTC-USD", "ETH-USD", "SOL-USD", "BNB-USD",
            "ADA-USD", "DOGE-USD", "AVAX-USD", "XRP-USD",
            "LINK-USD", "MATIC-USD"
        ]

        rows = []

        for pair in cryptos:
            res = analyze_symbol(pair)
            if not res: 
                continue

            symbol = pair.split("-")[0]
            senti = get_sentiment_for_symbol(symbol)

            # reconstruire results_by_tf (limité aux clés "RSI, Trend, MACD_Cross, Bollinger_Pos, Volume_Sentiment")
            tfs = {}
            for tf in ["1h","6h","1d"]:
                vals = {}
                for k in ["RSI","Trend","MACD_Cross","Bollinger_Pos","Volume_Sentiment"]:
                    vals[k] = res.get(f"{k}_{tf}")
                tfs[tf] = vals

            score_10, signal_global = compute_global_score(tfs, senti)

            # Fusionner: placer GlobalScore après Crypto, sentiment avant LastUpdate
            flat = {"Crypto": res["Crypto"], "GlobalScore_0_10": score_10, "Signal_Global": signal_global}
            # Conserver consensus actuel
            flat["Consensus"] = res.get("Consensus")

            # recopier toutes les colonnes techniques déjà présentes
            for k, v in res.items():
                if k in ["Crypto","Consensus","LastUpdate"]: 
                    continue
                flat[k] = v

            # Ajouter sentiments
            flat.update(senti)

            # LastUpdate en dernier
            flat["LastUpdate"] = time.strftime("%Y-%m-%d %H:%M:%S")

            rows.append(flat)
            print(f"✅ {symbol} → Score {score_10}/10 | {signal_global}", flush=True)
            time.sleep(1.2)

        if not rows:
            print("⚠️ Aucune donnée récupérée", flush=True)
            return

        # Harmoniser l'ordre des colonnes: Crypto, GlobalScore, Signal_Global, Consensus, ... tout le reste ..., sentiments, LastUpdate
        cols_front = ["Crypto","GlobalScore_0_10","Signal_Global","Consensus"]
        sentiment_cols = ["FearGreed_Index","FearGreed_Label","Social_Sentiment","News_Intensity","Sentiment_Score","LastUpdate"]

        # Construire DataFrame puis réordonner
        df_out = pd.DataFrame(rows)
        # bouger colonnes si présentes
        remaining = [c for c in df_out.columns if c not in cols_front + sentiment_cols]
        ordered = cols_front + remaining + sentiment_cols
        df_out = df_out.reindex(columns=[c for c in ordered if c in df_out.columns])

        ws.clear()
        set_with_dataframe(ws, df_out)
        print("✅ Feuille 'MultiTF' mise à jour (techniques + émotions par crypto + score global).", flush=True)

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
