
-- Merge Stage → Mart: Daily Visits


MERGE INTO `dish-digital-476218.mart.mart_daily_visits` AS mart
USING (
  SELECT
    visit_date,
    total_visits,
    record_id,
    load_timestamp,
    source_endpoint
  FROM `dish-digital-476218.stage.stg_daily_visits`
) AS stg
ON mart.visit_date = stg.visit_date
WHEN MATCHED THEN
  UPDATE SET
    mart.total_visits = stg.total_visits,
    mart.record_id = stg.record_id,
    mart.load_timestamp = stg.load_timestamp,
    mart.source_endpoint = stg.source_endpoint
WHEN NOT MATCHED THEN
  INSERT (
    visit_date,
    total_visits,
    record_id,
    load_timestamp,
    source_endpoint
  )
  VALUES (
    stg.visit_date,
    stg.total_visits,
    stg.record_id,
    stg.load_timestamp,
    stg.source_endpoint
  );
