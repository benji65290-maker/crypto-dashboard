import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
import pandas as pd
import os
import json

# ======================================================
# 🔐 Gestion des credentials Google via variable Render
# ======================================================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Lecture du JSON depuis la variable d'environnement Render
if os.getenv("GOOGLE_SERVICE_JSON"):
    info = json.loads(os.getenv("GOOGLE_SERVICE_JSON"))
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
else:
    raise Exception("❌ GOOGLE_SERVICE_JSON non défini dans les variables d'environnement.")

gc = gspread.authorize(creds)

# ID du Google Sheet récupéré depuis les variables Render 
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

def open_sheet():
    """Ouvre le Google Sheet via l'ID fourni."""
    if not GOOGLE_SHEET_ID:
        raise Exception("❌ GOOGLE_SHEET_ID non défini dans les variables d'environnement.")
    return gc.open_by_key(GOOGLE_SHEET_ID)

def write_df_to_worksheet(df, worksheet_name, clear=True):
    """Écrit un DataFrame dans un onglet du Google Sheet."""
    sh = open_sheet()
    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows="1000", cols="26")
    if clear:
        ws.clear()
    set_with_dataframe(ws, df, include_index=False, include_column_header=True)

# ======================================================
# 📊 Exemple de test : Écrit un tableau simple dans le Sheet google 
# ======================================================
if __name__ == "__main__":
    df = pd.DataFrame({
        "Crypto": ["BTCUSDC", "SOLUSDC", "ETHUSDC"],
        "Prix": [69500, 197, 3850],
        "RSI": [52, 61, 58]
    })
    write_df_to_worksheet(df, "TestSheet")
    print("✅ Données envoyées dans Google Sheets avec succès !")
