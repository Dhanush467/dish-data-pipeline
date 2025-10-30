# Data Dictionary for `dish-digital-476218.mart.mart_daily_visits`

This document provides a detailed description of the schema for the `mart_daily_visits` table, including the data types, nullability, and descriptions of each column.

## Table Schema

| Column            | Type      | Nullable | Description                                                                 | Example                                   |
|-------------------|-----------|----------|-----------------------------------------------------------------------------|-------------------------------------------|
| `visit_date`      | DATE      | NO       | The date of the visit recorded in the table. This field is required and should be in the format `YYYY-MM-DD`. | `2016-08-04`                             |
| `total_visits`    | INTEGER   | YES      | The total number of visits recorded for the specified date. This field can be null if no visits were recorded. | `3176`                                   |
| `record_id`       | STRING    | YES      | A unique identifier for each record in the table, typically a UUID. This field can be null if not provided. | `c4e3fa5f-7db9-4591-a51a-c69896c284ae` |
| `load_timestamp`  | TIMESTAMP | YES      | The timestamp indicating when the record was loaded into the database. This field can be null if not provided. | `2025-10-28 12:01:56.502184+00:00`     |
| `source_endpoint` | STRING    | YES      | The source from which the data was obtained. This field can be null if not provided. | `daily-visits`                           |

## Column Descriptions

### `visit_date`
- **Type**: DATE
- **Nullable**: NO
- **Description**: This column captures the specific date for which the visit data is recorded. It is essential for time-series analysis and reporting. The date must be in the standard format `YYYY-MM-DD`. This field is crucial for aggregating visits over time and identifying trends.

### `total_visits`
- **Type**: INTEGER
- **Nullable**: YES
- **Description**: This column records the total number of visits for the given `visit_date`. It can be null if there were no visits on that date. This metric is vital for understanding user engagement and traffic patterns.

### `record_id`
- **Type**: STRING
- **Nullable**: YES
- **Description**: A unique identifier for each record, typically formatted as a UUID. This ensures that each entry can be distinctly referenced, which is important for data integrity and tracking.

### `load_timestamp`
- **Type**: TIMESTAMP
- **Nullable**: YES
- **Description**: This column indicates the exact time when the record was loaded into the database. It is useful for auditing and tracking data freshness. This field can be null if the timestamp is not recorded at the time of data ingestion.

### `source_endpoint`
- **Type**: STRING
- **Nullable**: YES
- **Description**: This column specifies the source endpoint from which the data was collected. It helps in identifying the origin of the data and can be useful for debugging and data lineage purposes.

## Suggested Data Quality Checks
1. **Visit Date Validation**: Ensure that `visit_date` is always in the `YYYY-MM-DD` format and is a valid date.
2. **Total Visits Check**: Validate that `total_visits` is a non-negative integer or null.
3. **Unique Record ID**: Ensure that `record_id` is unique across the table to prevent duplicate entries.
4. **Load Timestamp Consistency**: Check that `load_timestamp` is always populated when a record is created and follows the correct timestamp format.
5. **Source Endpoint Verification**: Validate that `source_endpoint` contains expected values to maintain data integrity.