import requests
import json
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------------------
# NSE SESSION
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
# FETCH TOTAL OI / VOLUME
# ---------------------------
def fetch_totals(session, symbol, expiry):
    url = (
        "https://www.nseindia.com/api/option-chain-v3"
        f"?type=Indices&symbol={symbol}&expiry={expiry}"
    )

    data = session.get(url, timeout=10).json()

    ce = data.get("CE", {})
    pe = data.get("PE", {})

    return (
        ce.get("totOI", 0),
        ce.get("totVol", 0),
        pe.get("totOI", 0),
        pe.get("totVol", 0),
    )


# ---------------------------
# GOOGLE SHEETS
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

    rows = [
        ["BANKNIFTY", *fetch_totals(session, "BANKNIFTY", "24-Feb-2026")],
        ["NIFTY", *fetch_totals(session, "NIFTY", "10-Feb-2026")],
        ["FINNIFTY", *fetch_totals(session, "FINNIFTY", "24-Feb-2026")]
    ]

    ws.update(
        "A1",
        [["Symbol", "CE_OI", "CE_VOL", "PE_OI", "PE_VOL"]] + rows
    )

    print("✅ NSE Option Chain totals updated successfully")


if __name__ == "__main__":
    main()
