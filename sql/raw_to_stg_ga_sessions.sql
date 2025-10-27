
-- Step 1: Truncate stage table

TRUNCATE TABLE `dish-digital-476218.stage.stg_ga_sessions`;


-- Step 2: Insert valid + cleaned + deduped data into stage

INSERT INTO `dish-digital-476218.stage.stg_ga_sessions`
(
  fullVisitorId,
  date,
  channelGrouping,
  visitId,
  visitNumber,
  visitStartTime,
  continent,
  country,
  region,
  city,
  browser,
  operatingSystem,
  isMobile,
  medium,
  source,
  referralPath,
  pageviews,
  hits,
  bounces,
  newVisits,
  record_id,
  load_timestamp,
  source_endpoint
)
WITH cleaned_data AS (
  SELECT
    JSON_VALUE(raw_json, '$.fullVisitorId') AS fullVisitorId,
    PARSE_DATE('%Y%m%d', JSON_VALUE(raw_json, '$.date')) AS date,
    JSON_VALUE(raw_json, '$.channelGrouping') AS channelGrouping,
    SAFE_CAST(JSON_VALUE(raw_json, '$.visitId') AS INT64) AS visitId,
    SAFE_CAST(JSON_VALUE(raw_json, '$.visitNumber') AS INT64) AS visitNumber,
    SAFE_CAST(JSON_VALUE(raw_json, '$.visitStartTime') AS INT64) AS visitStartTime,

    -- Clean "not available" --> NULL
    NULLIF(JSON_VALUE(raw_json, '$.geoNetwork.continent'), 'not available in demo dataset') AS continent,
    NULLIF(JSON_VALUE(raw_json, '$.geoNetwork.country'), 'not available in demo dataset') AS country,
    NULLIF(JSON_VALUE(raw_json, '$.geoNetwork.region'), 'not available in demo dataset') AS region,
    NULLIF(JSON_VALUE(raw_json, '$.geoNetwork.city'), 'not available in demo dataset') AS city,

    JSON_VALUE(raw_json, '$.device.browser') AS browser,
    JSON_VALUE(raw_json, '$.device.operatingSystem') AS operatingSystem,
    SAFE_CAST(JSON_VALUE(raw_json, '$.device.isMobile') AS BOOL) AS isMobile,
    JSON_VALUE(raw_json, '$.trafficSource.medium') AS medium,
    JSON_VALUE(raw_json, '$.trafficSource.source') AS source,
    JSON_VALUE(raw_json, '$.trafficSource.referralPath') AS referralPath,
    SAFE_CAST(JSON_VALUE(raw_json, '$.totals.pageviews') AS INT64) AS pageviews,
    SAFE_CAST(JSON_VALUE(raw_json, '$.totals.hits') AS INT64) AS hits,
    SAFE_CAST(JSON_VALUE(raw_json, '$.totals.bounces') AS INT64) AS bounces,
    SAFE_CAST(JSON_VALUE(raw_json, '$.totals.newVisits') AS INT64) AS newVisits,
    record_id,
    load_timestamp,
    source_endpoint
  FROM `dish-digital-476218.raw.raw_ga_sessions`
),
deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY fullVisitorId, date, visitId
           ORDER BY load_timestamp DESC
         ) AS rn
  FROM cleaned_data
)
SELECT
  fullVisitorId,
  date,
  channelGrouping,
  visitId,
  visitNumber,
  visitStartTime,
  continent,
  country,
  region,
  city,
  browser,
  operatingSystem,
  isMobile,
  medium,
  source,
  referralPath,
  pageviews,
  hits,
  bounces,
  newVisits,
  record_id,
  load_timestamp,
  source_endpoint
FROM deduped
WHERE rn = 1
  AND fullVisitorId IS NOT NULL
  AND date IS NOT NULL
  AND visitId IS NOT NULL;


-- Step 3: Insert rejected records (null PKs or duplicates)

INSERT INTO `dish-digital-476218.stage.stg_reject_ga_sessions`
(
  fullVisitorId,
  date,
  channelGrouping,
  visitId,
  visitNumber,
  visitStartTime,
  continent,
  country,
  region,
  city,
  browser,
  operatingSystem,
  isMobile,
  medium,
  source,
  referralPath,
  pageviews,
  hits,
  bounces,
  newVisits,
  record_id,
  load_timestamp,
  source_endpoint,
  reject_reason
)
WITH cleaned_data AS (
  SELECT
    JSON_VALUE(raw_json, '$.fullVisitorId') AS fullVisitorId,
    PARSE_DATE('%Y%m%d', JSON_VALUE(raw_json, '$.date')) AS date,
    JSON_VALUE(raw_json, '$.channelGrouping') AS channelGrouping,
    SAFE_CAST(JSON_VALUE(raw_json, '$.visitId') AS INT64) AS visitId,
    SAFE_CAST(JSON_VALUE(raw_json, '$.visitNumber') AS INT64) AS visitNumber,
    SAFE_CAST(JSON_VALUE(raw_json, '$.visitStartTime') AS INT64) AS visitStartTime,
    NULLIF(JSON_VALUE(raw_json, '$.geoNetwork.continent'), 'not available in demo dataset') AS continent,
    NULLIF(JSON_VALUE(raw_json, '$.geoNetwork.country'), 'not available in demo dataset') AS country,
    NULLIF(JSON_VALUE(raw_json, '$.geoNetwork.region'), 'not available in demo dataset') AS region,
    NULLIF(JSON_VALUE(raw_json, '$.geoNetwork.city'), 'not available in demo dataset') AS city,
    JSON_VALUE(raw_json, '$.device.browser') AS browser,
    JSON_VALUE(raw_json, '$.device.operatingSystem') AS operatingSystem,
    SAFE_CAST(JSON_VALUE(raw_json, '$.device.isMobile') AS BOOL) AS isMobile,
    JSON_VALUE(raw_json, '$.trafficSource.medium') AS medium,
    JSON_VALUE(raw_json, '$.trafficSource.source') AS source,
    JSON_VALUE(raw_json, '$.trafficSource.referralPath') AS referralPath,
    SAFE_CAST(JSON_VALUE(raw_json, '$.totals.pageviews') AS INT64) AS pageviews,
    SAFE_CAST(JSON_VALUE(raw_json, '$.totals.hits') AS INT64) AS hits,
    SAFE_CAST(JSON_VALUE(raw_json, '$.totals.bounces') AS INT64) AS bounces,
    SAFE_CAST(JSON_VALUE(raw_json, '$.totals.newVisits') AS INT64) AS newVisits,
    record_id,
    load_timestamp,
    source_endpoint
  FROM `dish-digital-476218.raw.raw_ga_sessions`
),
ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY fullVisitorId, date, visitId
           ORDER BY load_timestamp DESC
         ) AS rn
  FROM cleaned_data
)
SELECT
  fullVisitorId,
  date,
  channelGrouping,
  visitId,
  visitNumber,
  visitStartTime,
  continent,
  country,
  region,
  city,
  browser,
  operatingSystem,
  isMobile,
  medium,
  source,
  referralPath,
  pageviews,
  hits,
  bounces,
  newVisits,
  record_id,
  load_timestamp,
  source_endpoint,
  CASE
    WHEN fullVisitorId IS NULL OR date IS NULL OR visitId IS NULL THEN 'Missing PK field'
    WHEN rn > 1 THEN 'Duplicate PK'
  END AS reject_reason
FROM ranked
WHERE (fullVisitorId IS NULL OR date IS NULL OR visitId IS NULL)
   OR rn > 1;
