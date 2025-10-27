-- Step 1: Truncate Hits Stage Table

TRUNCATE TABLE `dish-digital-476218.stage.stg_ga_hits`;

-- Step 2: Insert valid flattened hits

INSERT INTO `dish-digital-476218.stage.stg_ga_hits`
(
  fullVisitorId,
  visitId,
  hitNumber,
  time,
  type,
  isInteraction,
  pagePath,
  pageTitle,
  hostname,
  record_id,
  load_timestamp,
  source_endpoint
)
SELECT
  JSON_VALUE(r.raw_json, '$.fullVisitorId') AS fullVisitorId,
  SAFE_CAST(JSON_VALUE(r.raw_json, '$.visitId') AS INT64) AS visitId,
  SAFE_CAST(JSON_VALUE(hit, '$.hitNumber') AS INT64) AS hitNumber,
  SAFE_CAST(JSON_VALUE(hit, '$.time') AS INT64) AS time,
  JSON_VALUE(hit, '$.type') AS type,
  SAFE_CAST(JSON_VALUE(hit, '$.isInteraction') AS BOOL) AS isInteraction,
  JSON_VALUE(hit, '$.pagePath') AS pagePath,
  JSON_VALUE(hit, '$.pageTitle') AS pageTitle,
  JSON_VALUE(hit, '$.hostname') AS hostname,
  r.record_id,
  r.load_timestamp,
  r.source_endpoint
FROM `dish-digital-476218.raw.raw_ga_sessions` AS r,
UNNEST(JSON_QUERY_ARRAY(r.raw_json, '$.hits_sample')) AS hit
WHERE JSON_VALUE(r.raw_json, '$.fullVisitorId') IS NOT NULL
  AND JSON_VALUE(r.raw_json, '$.visitId') IS NOT NULL;


-- Step 3: Insert invalid (rejected) hits (missing PKs)

INSERT INTO `dish-digital-476218.stage.stg_reject_ga_hits`
(
  fullVisitorId,
  visitId,
  hitNumber,
  time,
  type,
  isInteraction,
  pagePath,
  pageTitle,
  hostname,
  record_id,
  load_timestamp,
  source_endpoint,
  reject_reason
)
SELECT
  JSON_VALUE(r.raw_json, '$.fullVisitorId') AS fullVisitorId,
  SAFE_CAST(JSON_VALUE(r.raw_json, '$.visitId') AS INT64) AS visitId,
  SAFE_CAST(JSON_VALUE(hit, '$.hitNumber') AS INT64) AS hitNumber,
  SAFE_CAST(JSON_VALUE(hit, '$.time') AS INT64) AS time,
  JSON_VALUE(hit, '$.type') AS type,
  SAFE_CAST(JSON_VALUE(hit, '$.isInteraction') AS BOOL) AS isInteraction,
  JSON_VALUE(hit, '$.pagePath') AS pagePath,
  JSON_VALUE(hit, '$.pageTitle') AS pageTitle,
  JSON_VALUE(hit, '$.hostname') AS hostname,
  r.record_id,
  r.load_timestamp,
  r.source_endpoint,
  'Missing fullVisitorId or visitId in GA Hits' AS reject_reason
FROM `dish-digital-476218.raw.raw_ga_sessions` AS r,
UNNEST(JSON_QUERY_ARRAY(r.raw_json, '$.hits_sample')) AS hit
WHERE JSON_VALUE(r.raw_json, '$.fullVisitorId') IS NULL
   OR JSON_VALUE(r.raw_json, '$.visitId') IS NULL;
