-- Merge Stage to Mart: GA Hits

MERGE INTO `dish-digital-476218.mart.mart_ga_hits` AS mart
USING (
  SELECT
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
  FROM `dish-digital-476218.stage.stg_ga_hits`
) AS stg
ON mart.fullVisitorId = stg.fullVisitorId
   AND mart.visitId = stg.visitId
   AND mart.hitNumber = stg.hitNumber
WHEN MATCHED THEN
  UPDATE SET
    mart.time = stg.time,
    mart.type = stg.type,
    mart.isInteraction = stg.isInteraction,
    mart.pagePath = stg.pagePath,
    mart.pageTitle = stg.pageTitle,
    mart.hostname = stg.hostname,
    mart.record_id = stg.record_id,
    mart.load_timestamp = stg.load_timestamp,
    mart.source_endpoint = stg.source_endpoint
WHEN NOT MATCHED THEN
  INSERT (
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
  VALUES (
    stg.fullVisitorId,
    stg.visitId,
    stg.hitNumber,
    stg.time,
    stg.type,
    stg.isInteraction,
    stg.pagePath,
    stg.pageTitle,
    stg.hostname,
    stg.record_id,
    stg.load_timestamp,
    stg.source_endpoint
  );
