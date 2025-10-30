#!/usr/bin/env python3

import os
import json
import textwrap
import argparse
from datetime import datetime
from google.cloud import bigquery
from dotenv import load_dotenv
import openai

# -------------------------------------------------
# Load environment variables
# -------------------------------------------------
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT")
DATASETS = [os.getenv("BIGQUERY_RAW"), os.getenv("BIGQUERY_STAGE"), os.getenv("BIGQUERY_MART")]

if not OPENAI_API_KEY:
    raise SystemExit("Missing OPENAI_API_KEY in your .env file")

openai.api_key = OPENAI_API_KEY


# -------------------------------------------------
# functions
# -------------------------------------------------
def detect_dataset_for_table(client, project, table_name):
    """Check which dataset (raw, stage, mart) contains the given table."""
    for dataset in DATASETS:
        try:
            client.get_table(f"{project}.{dataset}.{table_name}")
            print(f"Found table in dataset: {dataset}")
            return dataset
        except Exception:
            continue
    raise ValueError(f"Table '{table_name}' not found in any dataset: {DATASETS}")


def fetch_schema_and_samples(client, project, dataset, table, num_rows=10, columns=None):
    """Fetch schema and limited sample rows."""
    table_ref = f"{project}.{dataset}.{table}"
    tbl = client.get_table(table_ref)

    schema = [
        {"name": f.name, "type": f.field_type, "mode": f.mode, "description": f.description}
        for f in tbl.schema
        if not columns or f.name in columns
    ]

    # Select only requested columns for samples if provided
    col_str = ", ".join(columns) if columns else "*"
    rows = [dict(r) for r in client.query(f"SELECT {col_str} FROM `{table_ref}` LIMIT {num_rows}").result()]

    return schema, rows


def build_prompt(schema, samples, full_name):
    schema_text = "\n".join([f"- `{c['name']}` ({c['type']}, {c['mode']})" for c in schema])
    samples_json = json.dumps(samples, indent=2, default=str)[:3000]
    return textwrap.dedent(f"""
    Hello Data engineer assistant. Generate a **professional Markdown Data Dictionary** for table **{full_name}**.

    Schema:
    {schema_text}

    Sample data:
    {samples_json}

    Instructions:
    - If multiple columns are provided, describe each one in detail.
    - If only one or few columns are passed, focus deeply on explaining their meaning, context, and validation logic.
    - Output must be in Markdown format.
    - Include a table with columns: Column | Type | Nullable | Description | Example.
    - End with a short 'Suggested Data Quality Checks' section.
    """)


def call_llm(prompt):
    """Call OpenAI model."""
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=900
        )
        return response["choices"][0]["message"]["content"].strip()
    except Exception as e:
        raise RuntimeError(f"LLM call failed: {e}")


def save_output(md_text, dataset, table, columns):
    os.makedirs("docs", exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    suffix = "_".join(columns) if columns else "all_columns"
    path = f"docs/data_dictionary_{dataset}_{table}_{suffix}_{ts}.md"
    with open(path, "w", encoding="utf-8") as f:
        f.write(md_text)
    print(f"Saved documentation to {path}")
    return path


# -------------------------------------------------
# Main
# -------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, help="Table name (e.g. stg_ga_sessions)")
    parser.add_argument("--columns", nargs="*", help="Optional list of column names to document")
    parser.add_argument("--rows", type=int, default=10, help="Number of sample rows to include")
    args = parser.parse_args()

    client = bigquery.Client(project=BIGQUERY_PROJECT)

    # Detect dataset automatically
    dataset = detect_dataset_for_table(client, BIGQUERY_PROJECT, args.table)

    # Fetch schema and sample data
    print(f"Fetching schema and {args.rows} sample rows from {BIGQUERY_PROJECT}.{dataset}.{args.table}")
    schema, samples = fetch_schema_and_samples(
        client, BIGQUERY_PROJECT, dataset, args.table, args.rows, args.columns
    )

    prompt = build_prompt(schema, samples, f"{BIGQUERY_PROJECT}.{dataset}.{args.table}")
    print("Generating Markdown documentation using OpenAI...")
    md = call_llm(prompt)

    save_output(md, dataset, args.table, args.columns or [])


if __name__ == "__main__":
    main()
