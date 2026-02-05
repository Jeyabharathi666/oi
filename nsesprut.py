'''import requests
import pandas as pd
from datetime import datetime
import pytz

# 👇 import your existing helper (NO CHANGES to that file)
from google_sheets import update_google_sheet_by_name, append_footer

# ================= CONFIG =================
SHEET_ID = "1qrpBjK-qBRA85y_kNiRUGQ50U1AmTEX5cPooCPvZ4gw"
WORKSHEET_NAME = "NSPRUT"

URL = "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings"

HEADERS_HTTP = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.nseindia.com/market-data/oi-spurts"
}

# ================= FETCH DATA =================
session = requests.Session()
session.get("https://www.nseindia.com", headers=HEADERS_HTTP)

response = session.get(URL, headers=HEADERS_HTTP)
response.raise_for_status()

json_data = response.json()
df = pd.DataFrame(json_data.get("data", []))

# ================= LIMIT TO FIRST 35 =================
df = df.head(35)

# ================= PREPARE FOR GSHEET =================
headers = list(df.columns)
rows = df.astype(str).values.tolist()  # convert to list of lists

# ================= PUSH TO GOOGLE SHEET =================
update_google_sheet_by_name(
    sheet_id=SHEET_ID,
    worksheet_name=WORKSHEET_NAME,
    headers=headers,
    rows=rows
)

# ================= FOOTER =================
ist = pytz.timezone("Asia/Kolkata")
timestamp = datetime.now(ist).strftime("%d-%m-%Y %H:%M:%S IST")

# Timestamp in FIRST column
footer = [timestamp] + [""] * (len(headers) - 1)

append_footer(
    sheet_id=SHEET_ID,
    worksheet_name=WORKSHEET_NAME,
    footer_row=footer
)



'''

import requests
import pandas as pd
import time
from datetime import datetime
import pytz

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==========================
# CONFIG
# ==========================
SHEET_ID = "1qrpBjK-qBRA85y_kNiRUGQ50U1AmTEX5cPooCPvZ4gw"
WORKSHEET_NAME = "NSPRUT"

SERVICE_ACCOUNT_JSON = "pags-429207-b11860985f46.json"

BASE_URL = "https://www.nseindia.com"
API_URL = "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/market-data/oi-spurts",
    "Connection": "keep-alive"
}

# ==========================
# GOOGLE SHEETS CONNECT
# ==========================
def get_worksheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        SERVICE_ACCOUNT_JSON, scope
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    return sh.worksheet(WORKSHEET_NAME)

# ==========================
# FETCH NSE DATA
# ==========================
def fetch_oi_spurts():
    session = requests.Session()
    session.headers.update(HEADERS)

    session.get(BASE_URL)
    time.sleep(1)

    r = session.get(API_URL)
    r.raise_for_status()

    data = r.json().get("data", [])
    df = pd.DataFrame(data)

    # Optional: limit rows
    df = df.head(35)

    return df

# ==========================
# MAIN
# ==========================
def main():
    print("📡 Fetching NSE OI Spurts data...")
    df = fetch_oi_spurts()

    if df.empty:
        raise Exception("No data received from NSE")

    print("✅ Data fetched")

    ws = get_worksheet()

    # ---- WRITE HEADERS (A1:K1 only once or overwrite safely) ----
    headers = df.columns.tolist()
    ws.update("A1", [headers])

    # ---- WRITE DATA (A2 onward, numbers stay numbers) ----
    ws.update("A2", df.values.tolist())

    # ---- UPDATE TIMESTAMP (Column A, last data row only) ----
    ist = pytz.timezone("Asia/Kolkata")
    timestamp = datetime.now(ist).strftime("Updated %d-%m-%Y %H:%M:%S IST")

    last_row = len(df) + 2  # header + data
    ws.update_cell(last_row, 1, timestamp)

    print("🎉 SUCCESS: Data updated without touching formulas")

# ==========================
if __name__ == "__main__":
    main()
