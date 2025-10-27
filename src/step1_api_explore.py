import os, time, json, requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://dish-second-course-gateway-2tximoqc.nw.gateway.dev"
API_KEY = os.getenv("DISH_API_KEY")
HEADERS = {"X-API-Key": API_KEY}
DATA_DIR = Path(__file__).resolve().parents[1] / "data"

def ensure_dir(path):
    path.mkdir(parents=True, exist_ok=True)

def save_json(obj, filename):
    ensure_dir(filename.parent)
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)

def fetch_paginated(endpoint, params=None, limit=100, max_pages=1):
    page = 1
    results = []
    while True:
        query = {"page": page, "limit": limit}
        if params:
            query.update(params)
        url = f"{BASE_URL}/{endpoint}"
        r = requests.get(url, headers=HEADERS, params=query)
        if r.status_code != 200:
            raise RuntimeError(f"Error {r.status_code}: {r.text}")
        data = r.json()
        results.extend(data.get("records", []))
        print(f"{endpoint} page {page} --> {len(data.get('records', []))} rows")
        if not data.get("pagination", {}).get("has_next"):
            break
        if page >= max_pages:
            print("Reached dev page limit.")
            break
        page += 1
        time.sleep(0.2)
    return results

def main():
    if not API_KEY:
        raise RuntimeError("Set DISH_API_KEY first!")

    # 1️⃣ Daily Visits (flat)
    daily = fetch_paginated("daily-visits",
                            params={"start_date":"2016-08-01","end_date":"2016-08-05"})
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    save_json({"records": daily}, DATA_DIR / f"daily_visits_sample_{ts}.json")

    # 2️⃣ GA Sessions (nested)
    ga = fetch_paginated("ga-sessions-data", params={"date":"20160801"})
    save_json({"records": ga}, DATA_DIR / f"ga_sessions_sample_{ts}.json")

    print("Saved both samples in /data")

if __name__ == "__main__":
    main()
