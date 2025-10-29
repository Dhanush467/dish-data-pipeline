import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.email import send_email
import sys
sys.path.append('/opt/airflow')

# Import data pipeline functions
from src.data_pipeline import extract_to_raw

# Default DAG arguments

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=3),
    "email": ["dhanushh467@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}


# Failure notification callback

def notify_failure(context):
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('ts')
    message = f"""
    <h3>DAG Failed!</h3>
    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Task:</b> {task_id}</p>
    <p><b>Execution Date:</b> {execution_date}</p>
    """
    send_email(to="dhanushh467@gmail.com", subject=f"{dag_id} Failed", html_content=message)


# DAG definition

dag = DAG(
    dag_id="dish_etl_data_pipeline_dag",
    description="Dish ETL pipeline",
    start_date=datetime(2025, 10, 27),
    schedule="0 6,18 * * 3",  # 6 AM & 6 PM every Wednesday
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    on_failure_callback=notify_failure,
    dagrun_timeout=timedelta(minutes=60),
)

# Python callable to extract raw data dynamically based on dates

def run_data_pipeline(start_date, end_date):

    extract_to_raw(start_date, end_date)


# Tasks

# API data extraction and loading into BIgquery

extract_and_load = PythonOperator(
    task_id="extract_and_load_raw_data",
    python_callable=run_data_pipeline,
    op_args=[
        "{{ dag_run.conf.get('start_date', ds) }}",  # dynamic start_date
        "{{ dag_run.conf.get('end_date', ds) }}"     # dynamic end_date
    ],
    dag=dag,
    execution_timeout=timedelta(minutes=3),
)

# SQL transformation tasks (BigQuery)
SQL_BASE_PATH = "/opt/airflow/sql"

raw_to_stg_daily_visits = BigQueryInsertJobOperator(
    task_id="raw2stg_daily_visits",
    configuration={
        "query": {
            "query": open(os.path.join(SQL_BASE_PATH, "raw_to_stg_daily_visits.sql")).read(),
            "useLegacySql": False,
        }
    },
    gcp_conn_id="gcp_conn",
    dag=dag,
)

raw_to_stg_ga_sessions = BigQueryInsertJobOperator(
    task_id="raw2stg_ga_sessions",
    configuration={
        "query": {
            "query": open(os.path.join(SQL_BASE_PATH, "raw_to_stg_ga_sessions.sql")).read(),
            "useLegacySql": False,
        }
    },
    gcp_conn_id="gcp_conn",
    dag=dag,
)

raw_to_stg_ga_hits = BigQueryInsertJobOperator(
    task_id="raw2stg_ga_hits",
    configuration={
        "query": {
            "query": open(os.path.join(SQL_BASE_PATH, "raw_to_stg_ga_hits.sql")).read(),
            "useLegacySql": False,
        }
    },
    gcp_conn_id="gcp_conn",
    dag=dag,
)

merge_mart_daily_visits = BigQueryInsertJobOperator(
    task_id="merge_mart_daily_visits",
    configuration={
        "query": {
            "query": open(os.path.join(SQL_BASE_PATH, "merge_mart_daily_visits.sql")).read(),
            "useLegacySql": False,
        }
    },
    gcp_conn_id="gcp_conn",
    dag=dag,
)

merge_mart_ga_sessions = BigQueryInsertJobOperator(
    task_id="merge_mart_ga_sessions",
    configuration={
        "query": {
            "query": open(os.path.join(SQL_BASE_PATH, "merge_mart_ga_sessions.sql")).read(),
            "useLegacySql": False,
        }
    },
    gcp_conn_id="gcp_conn",
    dag=dag,

)

merge_mart_ga_hits = BigQueryInsertJobOperator(
    task_id="merge_mart_ga_hits",
    configuration={
        "query": {
            "query": open(os.path.join(SQL_BASE_PATH, "merge_mart_ga_hits.sql")).read(),
            "useLegacySql": False,
        }
    },
    gcp_conn_id="gcp_conn",
    dag=dag,

)

# Task dependencies
    
extract_and_load >> [
    raw_to_stg_daily_visits,
    raw_to_stg_ga_sessions,
    raw_to_stg_ga_hits
]

raw_to_stg_daily_visits >> merge_mart_daily_visits
raw_to_stg_ga_sessions >> merge_mart_ga_sessions
raw_to_stg_ga_hits >> merge_mart_ga_hits