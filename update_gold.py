import requests
import csv
from datetime import date, timedelta, datetime
import os
from github import Github

# ==== CONFIG ====
# Read GitHub token from environment (GitHub Actions) or userdata (local Colab)
GITHUB_TOKEN = os.getenv("MY_GITHUB_TOKEN") or userdata.get("MY_GITHUB_TOKEN")
GITHUB_USER = "andy-nguyen-21"
REPO_NAME = "gold-market-data-vietnam"
FILE_PATH = "sjc_gold_prices.csv"

def parse_price(value):
    """Convert '126.000' → 126000 (int)"""
    if value is None:
        return None
    return int(value.replace(".", "").replace(",", ""))

# ==== Connect to GitHub ====
g = Github(GITHUB_TOKEN)
repo = g.get_repo(f"{GITHUB_USER}/{REPO_NAME}")
contents = repo.get_contents(FILE_PATH)
csv_data = contents.decoded_content.decode("utf-8").splitlines()

# Get last date in CSV
reader = csv.reader(csv_data)
header = next(reader)  # skip header
rows = list(reader)
last_date_str = rows[-1][0]  # ISO date string (YYYY-MM-DD)
last_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()

start = last_date + timedelta(days=1)
end = date.today()
delta = timedelta(days=1)

# Open local file for appending
local_file = FILE_PATH
with open(local_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(header)  # keep header
    for row in rows:
        writer.writerow(row)  # write existing rows

    last_price = (int(rows[-1][2]), int(rows[-1][3])) if rows else None

    current = start
    while current <= end:
        url = "https://edge-api.pnj.io/ecom-frontend/v1/get-gold-price-history"
        params = {"date": current.strftime("%Y%m%d")}
        day_rows = []

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
                                        day_rows.append((d, t, gia_mua, gia_ban))

        except Exception as e:
            print(f"Error {current}: {e}")

        if day_rows:
            day_rows.sort(key=lambda x: datetime.strptime(x[1], "%H:%M:%S"))
            filtered = []
            last_in_day = None
            for row in day_rows:
                d, t, mua, ban = row
                current_price = (mua, ban)
                if current_price != last_in_day:
                    filtered.append((datetime.strptime(d, "%d/%m/%Y").strftime("%Y-%m-%d"), t, mua, ban))
                    last_in_day = current_price
            for row in filtered:
                writer.writerow(row)
            last_price = filtered[-1][2:]
        else:
            # No updates → carry forward yesterday’s price
            if last_price:
                mua, ban = last_price
                writer.writerow([current.strftime("%Y-%m-%d"), "00:00:00", mua, ban])

        current += delta

print("✅ Daily update complete, uploading to GitHub...")

# ==== Upload to GitHub ====
with open(local_file, "r", encoding="utf-8") as f:
    content = f.read()

repo.update_file(contents.path, "Daily gold price update", content, contents.sha, branch="main")
print("✅ File updated on GitHub:", FILE_PATH)
