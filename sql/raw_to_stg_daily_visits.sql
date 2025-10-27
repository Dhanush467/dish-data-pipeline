-- Step 1: Truncate Stage Table

TRUNCATE TABLE `dish-digital-476218.stage.stg_daily_visits`;

-- Step 2: Flatten JSON and insert valid records into STG

INSERT INTO `dish-digital-476218.stage.stg_daily_visits`
(
  visit_date,
  total_visits,
  record_id,
  load_timestamp,
  source_endpoint
)
SELECT
  PARSE_DATE('%Y-%m-%d', JSON_VALUE(raw_json, '$.visit_date')) AS visit_date,
  SAFE_CAST(JSON_VALUE(raw_json, '$.total_visits') AS INT64) AS total_visits,
  record_id,
  load_timestamp,
  source_endpoint
FROM `dish-digital-476218.raw.raw_daily_visits`
WHERE JSON_VALUE(raw_json, '$.visit_date') IS NOT NULL;


-- Step 3: Insert invalid records into STG_REJECT

INSERT INTO `dish-digital-476218.stage.stg_reject_daily_visits`
(
  visit_date,
  total_visits,
  record_id,
  load_timestamp,
  source_endpoint,
  reject_reason
)
SELECT
  PARSE_DATE('%Y-%m-%d', JSON_VALUE(raw_json, '$.visit_date')) AS visit_date,
  SAFE_CAST(JSON_VALUE(raw_json, '$.total_visits') AS INT64) AS total_visits,
  record_id,
  load_timestamp,
  source_endpoint,
  'Missing or invalid visit_date (PK) in daily visits JSON' AS reject_reason
FROM `dish-digital-476218.raw.raw_daily_visits`
WHERE JSON_VALUE(raw_json, '$.visit_date') IS NULL;
