import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
import pandas as pd

# ID de ton Google Sheet (copie-le depuis l’URL)
GOOGLE_SHEET_ID = "1KSc4xYb3m6X4PYozcctMNYJ89A8a0Z8scIg8MPBsXJc"  # ex: 1AbCdEFG123456789xyz

# Fichier de clé JSON (même dossier que ton script)
SERVICE_ACCOUNT_FILE = "service_account.json"

# Portée d’autorisation
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def open_sheet():
    """Ouvre le Google Sheet via service account."""
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    return sh

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

if __name__ == "__main__":
    # Exemple de test : écrire un petit tableau
    df = pd.DataFrame({
        "Crypto": ["BTCUSDC", "SOLUSDC", "ETHUSDC"],
        "Prix": [69500, 197.8, 3850],
        "RSI": [56, 62, 58]
    })
    write_df_to_worksheet(df, "TestSheet")
    print("✅ Données envoyées dans Google Sheets avec succès !")
