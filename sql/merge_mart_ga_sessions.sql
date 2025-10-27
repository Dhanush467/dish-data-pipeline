-- Merge Stage to Mart: GA Sessions

MERGE INTO `dish-digital-476218.mart.mart_ga_sessions` AS mart
USING (
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
  FROM `dish-digital-476218.stage.stg_ga_sessions`
) AS stg
ON mart.fullVisitorId = stg.fullVisitorId
   AND mart.date = stg.date
   AND mart.visitId = stg.visitId
WHEN MATCHED THEN
  UPDATE SET
    mart.channelGrouping = stg.channelGrouping,
    mart.visitNumber = stg.visitNumber,
    mart.visitStartTime = stg.visitStartTime,
    mart.continent = stg.continent,
    mart.country = stg.country,
    mart.region = stg.region,
    mart.city = stg.city,
    mart.browser = stg.browser,
    mart.operatingSystem = stg.operatingSystem,
    mart.isMobile = stg.isMobile,
    mart.medium = stg.medium,
    mart.source = stg.source,
    mart.referralPath = stg.referralPath,
    mart.pageviews = stg.pageviews,
    mart.hits = stg.hits,
    mart.bounces = stg.bounces,
    mart.newVisits = stg.newVisits,
    mart.record_id = stg.record_id,
    mart.load_timestamp = stg.load_timestamp,
    mart.source_endpoint = stg.source_endpoint
WHEN NOT MATCHED THEN
  INSERT (
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
  VALUES (
    stg.fullVisitorId,
    stg.date,
    stg.channelGrouping,
    stg.visitId,
    stg.visitNumber,
    stg.visitStartTime,
    stg.continent,
    stg.country,
    stg.region,
    stg.city,
    stg.browser,
    stg.operatingSystem,
    stg.isMobile,
    stg.medium,
    stg.source,
    stg.referralPath,
    stg.pageviews,
    stg.hits,
    stg.bounces,
    stg.newVisits,
    stg.record_id,
    stg.load_timestamp,
    stg.source_endpoint
  );
