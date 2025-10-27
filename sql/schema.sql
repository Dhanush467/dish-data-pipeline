CREATE SCHEMA IF NOT EXISTS `dish-digital-476218.raw`;
CREATE SCHEMA IF NOT EXISTS `dish-digital-476218.stage`;
CREATE SCHEMA IF NOT EXISTS `dish-digital-476218.mart`;

-- Raw Daily Visits
CREATE TABLE IF NOT EXISTS `dish-digital-476218.raw.raw_daily_visits` (
  record_id STRING,
  load_timestamp TIMESTAMP,
  source_endpoint STRING,
  raw_json STRING
);

-- Raw GA Sessions
CREATE TABLE IF NOT EXISTS `dish-digital-476218.raw.raw_ga_sessions` (
  record_id STRING,
  load_timestamp TIMESTAMP,
  source_endpoint STRING,
  raw_json STRING
);

-- Stage Daily Visits
CREATE TABLE IF NOT EXISTS `dish-digital-476218.stage.stg_daily_visits` (
  visit_date DATE NOT NULL,
  total_visits INT64,
  record_id STRING,
  load_timestamp TIMESTAMP,
  source_endpoint STRING
);

-- Stage GA Sessions
CREATE TABLE IF NOT EXISTS `dish-digital-476218.stage.stg_ga_sessions` (
  fullVisitorId STRING NOT NULL,
  date DATE NOT NULL,
  channelGrouping STRING,
  visitId INT64 NOT NULL, 
  visitNumber INT64,
  visitStartTime INT64,
  continent STRING,
  country STRING,
  region STRING,
  city STRING,
  browser STRING,
  operatingSystem STRING,
  isMobile BOOL,
  medium STRING,
  source STRING,
  referralPath STRING,
  pageviews INT64,
  hits INT64,
  bounces INT64,
  newVisits INT64,
  record_id STRING,
  load_timestamp TIMESTAMP,
  source_endpoint STRING
);

-- GA Hits (child table)

CREATE TABLE IF NOT EXISTS `dish-digital-476218.stage.stg_ga_hits` (
  fullVisitorId STRING NOT NULL,
  visitId INT64 NOT NULL,
  hitNumber INT64,
  time INT64,
  type STRING,
  isInteraction BOOL,
  pagePath STRING,
  pageTitle STRING,
  hostname STRING,
  record_id STRING,
  load_timestamp TIMESTAMP,
  source_endpoint STRING
);


-- Reject Daily Visits
CREATE TABLE IF NOT EXISTS `dish-digital-476218.stage.stg_reject_daily_visits` (
  visit_date DATE,
  total_visits INT64,
  record_id STRING,
  load_timestamp TIMESTAMP,
  source_endpoint STRING,
  reject_reason STRING
);

-- Reject GA Sessions
CREATE TABLE IF NOT EXISTS `dish-digital-476218.stage.stg_reject_ga_sessions` (
  fullVisitorId STRING,
  date DATE,
  channelGrouping STRING,
  visitId INT64,
  visitNumber INT64,
  visitStartTime INT64,
  continent STRING,
  country STRING,
  region STRING,
  city STRING,
  browser STRING,
  operatingSystem STRING,
  isMobile BOOL,
  medium STRING,
  source STRING,
  referralPath STRING,
  pageviews INT64,
  hits INT64,
  bounces INT64,
  newVisits INT64,
  record_id STRING,
  load_timestamp TIMESTAMP,
  source_endpoint STRING,
  reject_reason STRING
);

-- Reject GA Hits

CREATE TABLE IF NOT EXISTS `dish-digital-476218.stage.stg_reject_ga_hits` (
  fullVisitorId STRING,
  visitId INT64,
  hitNumber INT64,
  time INT64,
  type STRING,
  isInteraction BOOL,
  pagePath STRING,
  pageTitle STRING,
  hostname STRING,
  record_id STRING,
  load_timestamp TIMESTAMP,
  source_endpoint STRING,
  reject_reason STRING
);

-- Mart Daily Visits
CREATE TABLE IF NOT EXISTS `dish-digital-476218.mart.mart_daily_visits` (
  visit_date DATE NOT NULL,
  total_visits INT64,
  record_id STRING,
  load_timestamp TIMESTAMP,
  source_endpoint STRING
);

-- Mart GA Sessions
CREATE TABLE IF NOT EXISTS `dish-digital-476218.mart.mart_ga_sessions` (
  fullVisitorId STRING NOT NULL,
  date DATE NOT NULL,
  channelGrouping STRING,
  visitId INT64 NOT NULL,
  visitNumber INT64,
  visitStartTime INT64,
  continent STRING,
  country STRING,
  region STRING,
  city STRING,
  browser STRING,
  operatingSystem STRING,
  isMobile BOOL,
  medium STRING,
  source STRING,
  referralPath STRING,
  pageviews INT64,
  hits INT64,
  bounces INT64,
  newVisits INT64,
  record_id STRING,
  load_timestamp TIMESTAMP,
  source_endpoint STRING
);

-- Mart GA hits

CREATE TABLE IF NOT EXISTS `dish-digital-476218.mart.mart_ga_hits` (
  fullVisitorId STRING NOT NULL,
  visitId INT64 NOT NULL,
  hitNumber INT64,
  time INT64,
  type STRING,
  isInteraction BOOL,
  pagePath STRING,
  pageTitle STRING,
  hostname STRING,
  record_id STRING,
  load_timestamp TIMESTAMP,
  source_endpoint STRING
);
