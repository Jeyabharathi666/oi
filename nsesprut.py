import requests
import pandas as pd
from datetime import datetime

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
timestamp = datetime.now().strftime("Updated on %d-%m-%Y %H:%M:%S")
footer = [""] * (len(headers) - 1) + [timestamp]

append_footer(
    sheet_id=SHEET_ID,
    worksheet_name=WORKSHEET_NAME,
    footer_row=footer
)


