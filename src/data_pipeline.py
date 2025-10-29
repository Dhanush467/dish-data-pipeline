import os
import json
import time
import argparse
import uuid
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery, storage
from pathlib import Path
from dotenv import load_dotenv

# ============================================================
# CONFIGURATION
# ============================================================
load_dotenv()

BASE_URL = "https://dish-second-course-gateway-2tximoqc.nw.gateway.dev"
API_KEY = os.getenv("DISH_API_KEY")
HEADERS = {"X-API-Key": API_KEY}

PROJECT = os.getenv("BIGQUERY_PROJECT")
RAW_DATASET = os.getenv("BIGQUERY_RAW")
GCS_BUCKET = os.getenv("GCS_BUCKET")

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

bq_client = bigquery.Client(project=PROJECT)
gcs_client = storage.Client(project=PROJECT)

# ============================================================
# HELPERS
# ============================================================
def upload_to_gcs(local_path: Path, destination_blob: str):
    """Uploads a file to the specified GCS path."""
    bucket = gcs_client.bucket(GCS_BUCKET)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(local_path)
    logging.info(f"Uploaded to GCS: gs://{GCS_BUCKET}/{destination_blob}")


def fetch_paginated(endpoint: str, params=None, limit: int = 100, max_pages: int = None):
    """Fetch paginated API data."""
    page = 1
    results = []
    while True:
        query = {"page": page, "limit": limit}
        if params:
            query.update(params)
        r = requests.get(f"{BASE_URL}/{endpoint}", headers=HEADERS, params=query)
        if r.status_code != 200:
            raise RuntimeError(f"API call failed: {r.status_code} {r.text}")
        data = r.json()
        records = data.get("records", [])
        results.extend(records)
        logging.info(f"{endpoint}: Page {page} --> {len(records)} records")

        if not data.get("pagination", {}).get("has_next", False):
            break
        if max_pages and page >= max_pages:
            break

        page += 1
        time.sleep(0.3)
    return results


def save_and_upload_json(records, filename_prefix: str, endpoint: str):
    """Saves JSON to local folder and uploads to GCS landing zone."""
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"{filename_prefix}_{ts}.json"
    local_path = DATA_DIR / filename

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump({"records": records}, f, indent=2)
    logging.info(f"Saved local JSON: {local_path}")

    # Upload to GCS landing folder
    today = datetime.utcnow().strftime("%Y/%m/%d")
    gcs_path = f"landing/{endpoint}/{today}/{filename}"
    upload_to_gcs(local_path, gcs_path)
    return f"gs://{GCS_BUCKET}/{gcs_path}"


def load_to_bigquery(df: pd.DataFrame, table_name: str):
    """Loads a DataFrame into BigQuery (truncate + reload)."""
    table_id = f"{PROJECT}.{RAW_DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    bq_client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    logging.info(f"Loaded {len(df)} rows --> {table_id}")


# ============================================================
# EXTRACTION FUNCTIONS
# ============================================================

def extract_to_raw_daily_visits(start_date: str, end_date: str):
    endpoint = "daily-visits"
    logging.info(f"Extracting {endpoint} data for {start_date} --> {end_date}")

    records = fetch_paginated(endpoint, {"start_date": start_date, "end_date": end_date})
    if not records:
        logging.warning("No daily visits data found.")
        return

    gcs_uri = save_and_upload_json(records, "daily_visits_raw", endpoint)

    df = pd.DataFrame([
        {
            "record_id": str(uuid.uuid4()),
            "load_timestamp": datetime.now(timezone.utc),
            "source_endpoint": endpoint,
            "raw_json": json.dumps(rec)
        }
        for rec in records
    ])

    load_to_bigquery(df, "raw_daily_visits")
    logging.info(f"Completed raw load for Daily Visits ({len(df)} records)")


def extract_to_raw_ga_sessions(start_date: str, end_date: str):
    endpoint = "ga-sessions-data"
    logging.info(f"Extracting {endpoint} data for {start_date} --> {end_date}")

    sd, ed = datetime.fromisoformat(start_date), datetime.fromisoformat(end_date)
    all_records = []

    while sd <= ed:
        date_str = sd.strftime("%Y%m%d")
        recs = fetch_paginated(endpoint, {"date": date_str})
        for rec in recs:
            rec["_source_date"] = date_str
            all_records.append(rec)
        sd += timedelta(days=1)

    if not all_records:
        logging.warning("No GA sessions data found.")
        return

    gcs_uri = save_and_upload_json(all_records, "ga_sessions_raw", endpoint)

    df = pd.DataFrame([
        {
            "record_id": str(uuid.uuid4()),
            "load_timestamp": datetime.now(timezone.utc),
            "source_endpoint": endpoint,
            "raw_json": json.dumps(rec)
        }
        for rec in all_records
    ])

    load_to_bigquery(df, "raw_ga_sessions")
    logging.info(f"Completed raw load for GA Sessions ({len(df)} records)")


# ============================================================
# MAIN ORCHESTRATION
# ============================================================
def extract_to_raw(start_date: str, end_date: str):
    logging.info(f"Starting extraction pipeline for {start_date} --> {end_date}")
    extract_to_raw_daily_visits(start_date, end_date)
    extract_to_raw_ga_sessions(start_date, end_date)
    logging.info("Raw layer extraction completed successfully.")


# ============================================================
# EXECUTION
# ============================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    
    parser = argparse.ArgumentParser(description="Extract data from Dish API and load to BigQuery raw layer.")
    parser.add_argument("--start_date", required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end_date", required=True, help="End date in YYYY-MM-DD format")
    args = parser.parse_args()

    extract_to_raw(args.start_date, args.end_date)
