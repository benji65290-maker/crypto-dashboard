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
# ⚙️ API Coinbase (sans authentification, gratuite)
# ======================================================
def get_price(symbol_pair):
    """Récupère le dernier prix sur Coinbase (sans limite ni clé API)"""
    try:
        url = f"https://api.exchange.coinbase.com/products/{symbol_pair}/ticker"
        headers = {"User-Agent": "CryptoBot/1.0"}
        r = requests.get(url, headers=headers, timeout=10)
        print(f"🌐 [{symbol_pair}] Status {r.status_code}", flush=True)
        if r.status_code != 200:
            return None
        return float(r.json()["price"])
    except Exception as e:
        print(f"⚠️ Erreur get_price({symbol_pair}): {e}", flush=True)
        return None

# Simule un RSI à partir des variations aléatoires (simple placeholder)
def compute_fake_RSI(price):
    rsi = np.random.uniform(40, 60)  # neutre par défaut
    return round(rsi, 2)

def signal_RSI(rsi):
    if rsi < 30:
        return "🟢 Achat potentiel"
    elif rsi > 70:
        return "🔴 Vente potentielle"
    else:
        return "⚪ Neutre"

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
            ws = sh.add_worksheet(title="MarketData", rows="100", cols="10")

        cryptos = {
            "BTC-USD": "Bitcoin",
            "ETH-USD": "Ethereum",
            "SOL-USD": "Solana",
            "BNB-USD": "BinanceCoin",
            "ADA-USD": "Cardano",
            "DOGE-USD": "Dogecoin",
            "AVAX-USD": "Avalanche",
            "XRP-USD": "XRP",
            "LINK-USD": "Chainlink",
            "MATIC-USD": "Polygon"
        }

        rows = []
        for pair, name in cryptos.items():
            price = get_price(pair)
            if price is None:
                continue
            rsi = compute_fake_RSI(price)
            signal = signal_RSI(rsi)
            rows.append([name, price, rsi, signal])
            print(f"✅ {name} → {price}$ | RSI {rsi} | {signal}", flush=True)
            time.sleep(1)

        if not rows:
            print("⚠️ Aucune donnée récupérée.", flush=True)
            return

        df_out = pd.DataFrame(rows, columns=["Crypto", "Dernier Prix", "RSI (simulé)", "Signal"])
        df_out["Dernière MAJ"] = time.strftime("%Y-%m-%d %H:%M:%S")
        ws.clear()
        set_with_dataframe(ws, df_out)
        print(f"✅ Feuille mise à jour à {time.strftime('%H:%M:%S')}.", flush=True)

    except Exception as e:
        print(f"❌ Erreur update_sheet() : {e}", flush=True)

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
    return "✅ Crypto bot actif via Coinbase et Google Sheets."

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
