'''import requests
import json
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from zoneinfo import ZoneInfo


# -------------------------------------------------
# NSE SESSION
# -------------------------------------------------
def get_nse_session():
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/",
        "Connection": "keep-alive"
    }
    session.get("https://www.nseindia.com", headers=headers, timeout=10)
    session.headers.update(headers)
    return session


# -------------------------------------------------
# FETCH TOTAL CE/PE OI & VOLUME
# -------------------------------------------------
def fetch_totals(session, symbol, expiry):
    url = (
        "https://www.nseindia.com/api/option-chain-v3"
        f"?type=Indices&symbol={symbol}&expiry={expiry}"
    )

    data = session.get(url, timeout=10).json()

    if "records" not in data:
        raise Exception("Invalid NSE response")

    ce_oi = ce_vol = pe_oi = pe_vol = 0

    for row in data["records"]["data"]:
        ce = row.get("CE")
        pe = row.get("PE")

        if ce:
            ce_oi += ce.get("openInterest", 0)
            ce_vol += ce.get("totalTradedVolume", 0)

        if pe:
            pe_oi += pe.get("openInterest", 0)
            pe_vol += pe.get("totalTradedVolume", 0)

    return ce_oi, ce_vol, pe_oi, pe_vol


# -------------------------------------------------
# GOOGLE SHEETS
# -------------------------------------------------
def get_worksheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        json.loads(os.environ["NEW"]),
        scope
    )

    gc = gspread.authorize(creds)
    return gc.open_by_key(
        "1qrpBjK-qBRA85y_kNiRUGQ50U1AmTEX5cPooCPvZ4gw"
    ).worksheet("NIFTY")


# -------------------------------------------------
# MAIN
# -------------------------------------------------
def main():
    session = get_nse_session()
    ws = get_worksheet()

    # 🔥 STEP 1: CLEAR OLD DATA (A:E only)
    ws.batch_clear(["A:E"])

    # 🔥 STEP 2: WRITE HEADERS
    headers = [["SYMBOL", "CE_OI", "CE_VOL", "PE_OI", "PE_VOL"]]
    ws.update("A1", headers, value_input_option="USER_ENTERED")

    # 🔥 STEP 3: WRITE FRESH DATA
    data_rows = [
        ["BANKNIFTY", *fetch_totals(session, "BANKNIFTY", "24-Feb-2026")],
        ["NIFTY", *fetch_totals(session, "NIFTY", "10-Feb-2026")],
        ["FINNIFTY", *fetch_totals(session, "FINNIFTY", "24-Feb-2026")]
    ]

    ws.update(
        "A2",
        data_rows,
        value_input_option="USER_ENTERED"
    )

    # 🔥 STEP 4: ADD IST TIMESTAMP FOOTER
    now_ist = datetime.now(ZoneInfo("Asia/Kolkata"))
    timestamp = now_ist.strftime("%d-%m-%Y %H:%M:%S IST")

    ws.update(
        f"A{len(data_rows) + 2}",
        [[timestamp]],
        value_input_option="USER_ENTERED"
    )

    print("✅ Sheet refreshed with headers + NSE data + IST timestamp")


# -------------------------------------------------
# ENTRY POINT
# -------------------------------------------------
if __name__ == "__main__":
    main()'''

import requests
import json
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from zoneinfo import ZoneInfo


# -------------------------------------------------
# 1️⃣ NSE SESSION
# -------------------------------------------------
def get_nse_session():
    session = requests.Session()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/",
        "Connection": "keep-alive"
    }

    session.get("https://www.nseindia.com", headers=headers, timeout=10)
    session.headers.update(headers)

    return session


# -------------------------------------------------
# 2️⃣ GET ALL EXPIRIES FROM NSE
# -------------------------------------------------
def get_all_expiries(session, symbol):
    url = (
        "https://www.nseindia.com/api/option-chain-v3"
        f"?type=Indices&symbol={symbol}"
    )

    data = session.get(url, timeout=10).json()
    return data["records"]["expiryDates"]


# -------------------------------------------------
# 3️⃣ NEAREST WEEKLY EXPIRY (NIFTY)
# -------------------------------------------------
def get_weekly_expiry(session, symbol):
    return get_all_expiries(session, symbol)[0]


# -------------------------------------------------
# 4️⃣ LAST TUESDAY EXPIRY (BANKNIFTY / FINNIFTY)
# -------------------------------------------------
def get_monthly_last_tuesday(session, symbol):
    expiries = get_all_expiries(session, symbol)

    today = datetime.now(ZoneInfo("Asia/Kolkata")).date()

    expiry_dates = [
        datetime.strptime(e, "%d-%b-%Y").date()
        for e in expiries
        if datetime.strptime(e, "%d-%b-%Y").date() >= today
    ]

    # Group by (year, month)
    monthly = {}
    for d in expiry_dates:
        key = (d.year, d.month)
        monthly.setdefault(key, []).append(d)

    # Pick earliest month available
    first_month = sorted(monthly.keys())[0]

    # Last expiry of that month = last Tuesday
    last_expiry = max(monthly[first_month])

    return last_expiry.strftime("%d-%b-%Y")


# -------------------------------------------------
# 5️⃣ FETCH TOTAL OI & VOLUME
# -------------------------------------------------
def fetch_totals(session, symbol, expiry):
    url = (
        "https://www.nseindia.com/api/option-chain-v3"
        f"?type=Indices&symbol={symbol}&expiry={expiry}"
    )

    data = session.get(url, timeout=10).json()

    ce_oi = ce_vol = pe_oi = pe_vol = 0

    for row in data["records"]["data"]:
        if "CE" in row:
            ce_oi += row["CE"].get("openInterest", 0)
            ce_vol += row["CE"].get("totalTradedVolume", 0)

        if "PE" in row:
            pe_oi += row["PE"].get("openInterest", 0)
            pe_vol += row["PE"].get("totalTradedVolume", 0)

    return ce_oi, ce_vol, pe_oi, pe_vol


# -------------------------------------------------
# 6️⃣ GOOGLE SHEET
# -------------------------------------------------
def get_worksheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        json.loads(os.environ["NEW"]),
        scope
    )

    gc = gspread.authorize(creds)

    return gc.open_by_key(
        "1qrpBjK-qBRA85y_kNiRUGQ50U1AmTEX5cPooCPvZ4gw"
    ).worksheet("NIFTY")


# -------------------------------------------------
# 7️⃣ MAIN
# -------------------------------------------------
def main():
    session = get_nse_session()
    ws = get_worksheet()

    ws.batch_clear(["A:E"])

    ws.update("A1", [["SYMBOL", "CE_OI", "CE_VOL", "PE_OI", "PE_VOL"]])

    # 🔥 EXPIRY LOGIC
    nifty_expiry = get_weekly_expiry(session, "NIFTY")
    banknifty_expiry = get_monthly_last_tuesday(session, "BANKNIFTY")
    finnifty_expiry = get_monthly_last_tuesday(session, "FINNIFTY")

    data = [
        ["BANKNIFTY", *fetch_totals(session, "BANKNIFTY", banknifty_expiry)],
        ["NIFTY", *fetch_totals(session, "NIFTY", nifty_expiry)],
        ["FINNIFTY", *fetch_totals(session, "FINNIFTY", finnifty_expiry)]
    ]

    ws.update("A2", data)

    now_ist = datetime.now(ZoneInfo("Asia/Kolkata"))
    ws.update(f"A{len(data) + 2}",
              [[now_ist.strftime("%d-%m-%Y %H:%M:%S IST")]])

    print("✅ Correct Weekly + Monthly Expiry Data Updated")


# -------------------------------------------------
# 8️⃣ ENTRY POINT
# -------------------------------------------------
if __name__ == "__main__":
    main()

