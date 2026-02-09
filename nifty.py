import requests
import json
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------------------
# NSE SESSION SETUP
# ---------------------------
def get_nse_session():
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/"
    }
    session.get("https://www.nseindia.com", headers=headers, timeout=10)
    session.headers.update(headers)
    return session


# ---------------------------
# FETCH OPTION CHAIN TOTALS
# ---------------------------
def fetch_option_totals(session, symbol, expiry):
    url = (
        "https://www.nseindia.com/api/option-chain-v3"
        f"?type=Indices&symbol={symbol}&expiry={expiry}"
    )

    data = session.get(url, timeout=10).json()

    ce = data.get("CE", {})
    pe = data.get("PE", {})

    return {
        "symbol": symbol,
        "expiry": expiry,
        "CE_OI": ce.get("totOI", 0),
        "CE_VOL": ce.get("totVol", 0),
        "PE_OI": pe.get("totOI", 0),
        "PE_VOL": pe.get("totVol", 0),
    }


# ---------------------------
# GOOGLE SHEETS SETUP
# ---------------------------
def get_sheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        json.loads(os.environ["NEW"]),
        scope
    )

    gc = gspread.authorize(creds)
    return gc.open_by_key("1qrpBjK-qBRA85y_kNiRUGQ50U1AmTEX5cPooCPvZ4gw").worksheet("NIFTY")


# ---------------------------
# MAIN
# ---------------------------
def main():
    session = get_nse_session()
    ws = get_sheet()

    rows = []

    rows.append([
        "BANKNIFTY", "24-Feb-2026",
        *fetch_option_totals(session, "BANKNIFTY", "24-Feb-2026").values()
    ])

    rows.append([
        "NIFTY", "10-Feb-2026",
        *fetch_option_totals(session, "NIFTY", "10-Feb-2026").values()
    ])

    rows.append([
        "FINNIFTY", "24-Feb-2026",
        *fetch_option_totals(session, "FINNIFTY", "24-Feb-2026").values()
    ])

    # Clean rows (remove duplicate symbol/expiry)
    clean_rows = []
    for r in rows:
        clean_rows.append([r[0], r[1], r[3], r[4], r[5], r[6]])

    ws.update(
        "A2",
        [["Symbol", "Expiry", "CE_OI", "CE_VOL", "PE_OI", "PE_VOL"]] + clean_rows
    )

    print("✅ NSE Option Chain totals updated successfully")


if __name__ == "__main__":
    main()
