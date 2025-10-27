# Step 1 – API Exploration Findings

## Endpoints tested
- `/daily-visits` --> flat: visit_date, total_visits
- `/ga-sessions-data` --> nested: channelGrouping, date, fullVisitorId, and session identifiers (visitId, visitNumber, visitStartTime)

## Key Observations

## /daily-visits

File generated --> daily_visits_sample_20251025T074203Z.json

The API returns a top-level key called "records", which holds a list of daily summaries.

Each object in the list represents one calendar date with two fields:

visit_date: The date of the visits (in YYYY-MM-DD format).

total_visits: The total number of visits recorded for that day.

The structure is flat and doesn’t include any nested fields, so no transformation or flattening is required before loading to a BigQuery.

The data appears to be sorted in descending order of date — most recent day first.

For the period tested (2016-08-01 to 2016-08-05), the total visits ranged roughly between 1.2K and 3.1K, suggesting typical daily activity metrics.

This endpoint provides clean, aggregated data that can map directly to a simple BigQuery schema with the following columns:

visit_date (DATE)
total_visits (INTEGER)

## Summary

Overall, the daily-visits endpoint is easy to work with. The data is already in a flat, analysis-ready format and ideal for straightforward ingestion into BigQuery or other data warehouses without additional transformation steps.


## /ga-sessions-data

File generated --> ga_sessions_sample_20251025T074203Z.json

While exploring the ga-sessions-data API, I noticed that the response structure is considerably more complex than daily-visits. The endpoint returns session-level Google Analytics data, including information about the user’s device, traffic source, location, and the pages they interacted with during the session

## Key Observations

The top-level structure again contains a "records" array, where each record represents a single user session.

Compared to the daily-visits data, this dataset is nested and hierarchical.

Each session includes several key sections:

Top-level fields such as channelGrouping, date, fullVisitorId, and session identifiers (visitId, visitNumber, visitStartTime).

Nested objects:

device: captures browser, OS, and mobile indicator.

geoNetwork: contains geographic and network details.

trafficSource: describes how the user arrived (medium, source, referral path).

totals: provides aggregated session metrics like pageviews, hits, and bounces.

Nested lists:

customDimensions: contains key-value metadata pairs with index and value.

hits_sample: a list of individual user interactions or page hits within the session.

The nested structure means that flattening is required before loading into BigQuery — particularly for device, geoNetwork, trafficSource, and hits_sample.

Fields like hits_sample can expand into multiple rows per session if we want to analyze data at the “page hit” level.

Dates appear as integers in YYYYMMDD format ("20160801") and may need to be converted into a proper DATE type.

There are occasional null values and "not available in demo dataset" placeholders, which should be handled during transformation.

## Summary

The ga-sessions-data endpoint provides detailed, session-level analytics data. Unlike daily-visits, this structure is multi-level and nested, containing both scalar and array fields. It’s well-suited for deeper behavioral analysis once flattened.
Before loading to BigQuery, the following transformation steps would typically be required:

Flatten nested fields (e.g., device, geoNetwork, trafficSource).

Expand hits_sample if page-level granularity is needed.

Convert date strings to DATE format
