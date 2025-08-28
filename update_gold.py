import requests
import csv
from datetime import date, timedelta, datetime
import os

FILE_PATH = "sjc_gold_prices.csv"

def parse_price(value):
    """Convert '126.000' → 126000 (int)"""
    if value is None:
        return None
    return int(value.replace(".", "").replace(",", ""))

def fetch_day(target_date):
    """Fetch gold prices for one date"""
    url = "https://edge-api.pnj.io/ecom-frontend/v1/get-gold-price-history"
    params = {"date": target_date.strftime("%Y%m%d")}
    rows = []

    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            locations = data.get("locations", [])

            for loc in locations:
                if loc.get("name") == "TPHCM":   # Only TPHCM
                    for gtype in loc.get("gold_type", []):
                        if gtype.get("name") == "SJC":  # Only SJC
                            for record in gtype.get("data", []):
                                updated = record.get("updated_at")
                                gia_mua = parse_price(record.get("gia_mua"))
                                gia_ban = parse_price(record.get("gia_ban"))

                                if updated and gia_mua and gia_ban:
                                    d, t = updated.split(" ")
                                    rows.append((d, t, gia_mua, gia_ban))
    except Exception as e:
        print(f"⚠️ Error {target_date}: {e}")

    if rows:
        rows.sort(key=lambda x: datetime.strptime(x[1], "%H:%M:%S"))
        filtered = []
        last_in_day = None
        for row in rows:
            d, t, mua, ban = row
            current_price = (mua, ban)
            if current_price != last_in_day:
                filtered.append((datetime.strptime(d, "%d/%m/%Y").strftime("%Y-%m-%d"), t, mua, ban))
                last_in_day = current_price
        return filtered
    return None

def main():
    if not os.path.exists(FILE_PATH):
        print("❌ CSV not found! Run the full script first.")
        return

    # Read last date in CSV
    with open(FILE_PATH, "r", encoding="utf-8") as f:
        last_line = None
        for line in f:
            last_line = line.strip()
        last_date = datetime.strptime(last_line.split(",")[0], "%Y-%m-%d").date()

    print(f"📅 Last available date: {last_date}")

    today = date.today()
    current = last_date + timedelta(days=1)

    new_rows = []

    while current <= today:
        fetched = fetch_day(current)
        if fetched:
            new_rows.extend(fetched)
            print(f"✅ {current} added {len(fetched)} rows")
        else:
            # carry forward last price if exists
            if new_rows:
                mua, ban = new_rows[-1][2], new_rows[-1][3]
            else:
                with open(FILE_PATH, "r", encoding="utf-8") as f:
                    last_price_line = f.readlines()[-1].strip().split(",")
                    mua, ban = last_price_line[2], last_price_line[3]
            new_rows.append((current.strftime("%Y-%m-%d"), "00:00:00", mua, ban))
            print(f"ℹ️ {current} no update, carried forward")

        current += timedelta(days=1)

    if new_rows:
        with open(FILE_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for row in new_rows:
                writer.writerow(row)
        print(f"🎉 Appended {len(new_rows)} new rows to {FILE_PATH}")
    else:
        print("No new data found.")

if __name__ == "__main__":
    main()
