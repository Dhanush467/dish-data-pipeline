## 1 Apache Airflow DAG Development

# Note: Airflow setup is explained in README.md!!

## 1.1 DAG Configuration

* dag_id="dish_etl_data_pipeline_dag" — unique dag name to show in Airflow UI.

* start_date=datetime(2025,10,27) + catchup=False — DAG won’t backfill historic runs automatically; start date is fixed for scheduling.

* schedule="0 6,18 * * 3" — Cron: Runs only on Wednesdays at 06:00 and 18:00.

* default_args:
  retries=2, retry_delay=timedelta(minutes=5) — resilient to transient issues.
  execution_timeout=timedelta(minutes=3) — per-task timeout.

* email_on_failure=True — sends alerts on failures to emails configured. In this case I have given my email-id.

## 1.2 Dynamic execution

* extract_and_load function uses PythonOperator calling run_data_pipeline with op_args:
  {{ dag_run.conf.get('start_date', ds) }} and {{ dag_run.conf.get('end_date', ds) }}

* This allows dynamic runs

* If triggered manually with JSON conf you can pass {"start_date":"2025-10-01","end_date":"2025-10-07"}.

* If triggered by scheduler, ds (execution date string) is used as fallback.

* This pattern enables ad-hoc backfills and scheduled run parameterization.

## 1.3 Tasks and dependencies

## 1.3.1 Design Consideration

* Extract:
    Fetch data from two REST API endpoints —
    /daily-visits
    /ga-sessions-data

    The responses are converted to JSON files and stored both locally and in a GCS landing bucket.

* Load (Raw Layer)
    Raw JSON data is directly ingested into BigQuery raw tables (raw_daily_visits and raw_ga_sessions) using the load_to_bigquery() function inside the Python script.

* Transform (Staging Layer)
    Transformation SQL scripts flatten and validate the JSON structure, producing structured staging tables (stg_*) while pushing bad or duplicate records into reject tables.

* Merge (Mart Layer)
    Final clean and validated data from staging is merged incrementally into mart tables for analytics and reporting.

## 1.3.2 Tasks

# Task 1 — Extract & Load Raw Data

* Operator: PythonOperator
* Task ID: extract_and_load_raw_data

* Description:
    This task triggers the Python script (src/data_pipeline.py) that:

* Calls both API endpoints (daily-visits and ga-sessions-data) using REST requests.

* Writes the responses to local JSON files.

* Uploads these files to the GCS landing zone.

* Loads the same raw JSON data into BigQuery raw tables using the BigQuery client library.

* Parameters passed:
    Dynamic runtime arguments — start_date and end_date — are passed from Airflow at execution time

* Output:
    raw.raw_daily_visits
    raw.raw_ga_sessions

* Purpose:
    Acts as the data ingestion layer for both APIs, ensuring that all source data is safely stored before transformation

# Task 2 — Transform Raw-->Stage (Daily Visits)

* Operator: BigQueryInsertJobOperator
* Task ID: raw2stg_daily_visits
* Description:
    Executes SQL script:
    /opt/airflow/sql/raw_to_stg_daily_visits.sql
* This SQL:
    Flattens simple JSON from raw_daily_visits
    Validates mandatory fields (e.g., visit_date)
    Rejects null or invalid records into stg_reject_daily_visits
    Inserts clean data into stg_daily_visits
* Output Tables:
    stage.stg_daily_visits
    stage.stg_reject_daily_visits

## Task 3 — Transform Raw --> Stage (GA Sessions)

* Operator: BigQueryInsertJobOperator
* Task ID: raw2stg_ga_sessions
* Description:
    Executes:
    /opt/airflow/sql/raw_to_stg_ga_sessions.sql   
* This is a more complex transformation:
    Parses and flattens nested JSON (session-level attributes).
    Validates mandatory keys (fullVisitorId, date, visitId).
    Deduplicates based on primary keys.
    Pushes invalid or duplicate records into stg_reject_ga_sessions.
* Output Tables:
    stage.stg_ga_sessions
    stage.stg_reject_ga_sessions

## Task 4 — Transform Raw --> Stage (GA Hits)

* Operator: BigQueryInsertJobOperator
* Task ID: raw2stg_ga_hits
* Description:
    Executes:
    /opt/airflow/sql/raw_to_stg_ga_hits.sql
* This SQL:
    Unnests the hits_sample array from each session record using BigQuery’s UNNEST() function.
    Flattens hit-level attributes such as pagePath, pageTitle, and type.
    Rejects rows missing fullVisitorId or visitId.
* Output Tables:
    stage.stg_ga_hits
    stage.stg_reject_ga_hits

## Task 5 — Merge Stage --> Mart (Daily Visits)

* Operator: BigQueryInsertJobOperator
* Task ID: merge_mart_daily_visits
* Description:
    Executes:
    /opt/airflow/sql/merge_mart_daily_visits.sql
* This script 
    merges clean data from staging into the mart layer, performing an upsert (MERGE) operation based on unique identifiers.
    This ensures new records are inserted.
    Existing records are updated without duplication.
    Historical data is preserved.
* Target Table: mart.mart_daily_visits

## Task 6 — Merge Stage --> Mart (GA Sessions)

* Operator: BigQueryInsertJobOperator
* Task ID: merge_mart_ga_sessions
* Description:
    Executes:
    /opt/airflow/sql/merge_mart_ga_sessions.sql
* This Script:
    merges clean session-level data into the mart table for historical trend analysis.
* Target Table: mart.mart_ga_sessions

## Task 7 — Merge Stage --> Mart (GA Hits)

* Operator: BigQueryInsertJobOperator
* Task ID: merge_mart_ga_hits
* Description:
    Executes:
    /opt/airflow/sql/merge_mart_ga_hits.sql
* This Script:
    merges flattened hit-level events into the mart table, enabling granular web analytics metrics (e.g., pageviews, interactions, bounce rate).
* Target Table: mart.mart_ga_hits

## 1.3.3 Task Dependencies

```
extract_and_load_raw_data
        ↓
 ┌──────────────────────────────┐
 │  raw2stg_daily_visits        │
 │  raw2stg_ga_sessions         │
 │  raw2stg_ga_hits             │
 └──────────────────────────────┘
        ↓
 ┌──────────────────────────────┐
 │ merge_mart_daily_visits      │
 │ merge_mart_ga_sessions       │
 │ merge_mart_ga_hits           │
 └──────────────────────────────┘
```

## 2 Data Quality & Governance

In this section I'm describing how Data Quality, Monitoring, and Governance are designed and implemented within the Dish Digital ETL Data Pipeline built using Apache Airflow, BigQuery, and Google Cloud Storage (GCS).
The focus is on ensuring data accuracy, completeness, compliance, and observability across the entire data lifecycle

## 2.1 Data Quality Monitoring

## Key Data Quality Metrics Monitored

* Null / Missing Values:
    Ensures mandatory fields like visitId, fullVisitorId, date, and total_visits are not null
* Duplicate Primary Keys:
    Detects duplicates for composite PKs (fullVisitorId, visitId)
* Data Type / Range Validation:
    Checks data conforms to schema and valid types (dates, integers, booleans)
* Parent–Child Consistency:
    Verifies that every ga_hit record has a valid parent ga_session

## Handling Data Quality Failures

* Missing PKs / Required Fields:
    Rejected into dedicated reject tables (stg_reject_*) with reason logged
* Duplicate Primary Keys:
    De-duplicated and duplicates inserted into reject tables with reason "Duplicate PK"
* Invalid Data Types:
    Casted safely using SAFE_CAST in SQL transformations
* Each rejected record contains:
    record_id
    load_timestamp
    source_endpoint
    reject_reason
    This design ensures traceability, auditability, and reprocessability of bad records after data correction

## Alerting & Monitoring Mechanisms

* Airflow Failure Alerts
    Implemented via custom notify_failure() callback in the DAG
    Sends email with DAG name, task ID, and timestamp on failure
* Airflow UI & Logs
    Full transparency for task-level logs, run durations, and retries
* BigQuery Job Monitoring
    All transformation queries are executed via BigQueryInsertJobOperator which automatically tracks job completion and errors in BigQuery Job History

## 2.2 Data Governance & Compliance

## Data Lineage Tracking

* Pipeline Lineage (Airflow DAG)
    Airflow DAG visually documents the full lineage: API --> GCS --> BigQuery Raw --> Stage --> Mart
* Table Lineage (BigQuery)
    Each record carries metadata (record_id, load_timestamp, source_endpoint) enabling full traceability
* Transformation Lineage (SQL)
    Each SQL script is version-controlled under /sql representing logical data flow

## Metadata Management

* record_id --> Unique identifier for traceability
* load_timestamp --> Timestamp of ingestion for freshness validation
* source_endpoint --> Source API endpoint name (e.g. daily-visits, ga-sessions)
* reject_reason --> Documents reject reason for rejected rows
* This metadata ensures end-to-end lineage and supports audit and replay scenarios.

## Data Privacy & Security

* Access Control --> IAM roles for least-privilege access to BigQuery and GCS
* Credential Management --> .env file mounted securely in Docker for this project so no secrets in code
* Encryption --> GCP-managed encryption at rest and in transit (HTTPS/IAM)
* Audit Logging --> BigQuery and GCS access logs enabled for compliance
* PII Handling
    The API extraction layer (data_pipeline.py) only requests required business attributes such as visitId, total_visits, etc
    PII (e.g. names, emails, IP addresses, phone numbers) is never collected unless explicitly required for analytics
    If the source system ever contains PII (e.g. user_id, ip_address), it would be tokenized before loading into any analytical environment

## Documentation & Cataloging Practices

* Version-Controlled SQL Scripts
    All transformations (raw_to_stg_*.sql, merge_mart_*.sql) are tracked in Git
* Airflow DAGs
    DAGs serve as living documentation of workflow dependencies
* README Documentation
    Document in README. Details such as architecture, flow, and governance design
