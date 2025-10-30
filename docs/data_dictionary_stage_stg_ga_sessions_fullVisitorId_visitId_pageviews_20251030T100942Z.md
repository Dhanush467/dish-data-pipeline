# Data Dictionary for `dish-digital-476218.stage.stg_ga_sessions`

This document provides a detailed description of the schema for the table `dish-digital-476218.stage.stg_ga_sessions`. This table is designed to capture session data from Google Analytics, specifically focusing on visitor interactions within a defined timeframe.

## Schema Overview

| Column          | Type     | Nullable | Description                                                                 | Example               |
|-----------------|----------|----------|-----------------------------------------------------------------------------|-----------------------|
| `fullVisitorId` | STRING   | NO       | A unique identifier for each visitor. This ID is persistent across sessions and is used to track user behavior over time. It is essential for distinguishing between different users. | "7962255080644646828" |
| `visitId`      | INTEGER  | NO       | A unique identifier for each visit/session. This ID is generated for every new session and helps in tracking the number of visits made by a user. | 1470104488            |
| `pageviews`    | INTEGER  | YES      | The total number of pages viewed during a session. This metric is useful for understanding user engagement and interaction with the website. A value of `NULL` indicates that no pageviews were recorded for that session. | 57                    |

## Detailed Column Descriptions

### `fullVisitorId`
- **Type**: STRING
- **Nullable**: NO
- **Description**: This field serves as a unique identifier for each visitor. It is crucial for tracking user behavior across multiple sessions and visits. The `fullVisitorId` remains constant for a user, allowing for longitudinal analysis of user interactions with the website.
- **Validation Logic**: Must be a non-empty string that uniquely identifies a visitor. The format should conform to the expected structure of a Google Analytics visitor ID.

### `visitId`
- **Type**: INTEGER
- **Nullable**: NO
- **Description**: This field represents a unique identifier for each session. Each time a user visits the website, a new `visitId` is generated. This ID is essential for tracking the number of sessions and understanding user engagement over time.
- **Validation Logic**: Must be a positive integer. Each `visitId` should be unique within the context of a single `fullVisitorId`.

### `pageviews`
- **Type**: INTEGER
- **Nullable**: YES
- **Description**: This field indicates the total number of pages viewed during a session. It is a key metric for assessing user engagement and the effectiveness of the website's content. A `NULL` value indicates that no pageviews were recorded for that session, which may occur in cases where a session was initiated but no pages were loaded.
- **Validation Logic**: Must be a non-negative integer or `NULL`. If present, it should accurately reflect the count of pages viewed during the session.

## Suggested Data Quality Checks
1. **Uniqueness Check**: Ensure that the combination of `fullVisitorId` and `visitId` is unique across the dataset to prevent duplicate session entries.
2. **Non-Null Constraints**: Validate that `fullVisitorId` and `visitId` are never `NULL` to maintain data integrity.
3. **Pageviews Validation**: Check that `pageviews` is either a non-negative integer or `NULL`, ensuring that the data accurately reflects user interactions.
4. **Data Type Validation**: Regularly verify that the data types of each column conform to the specified schema to prevent type-related errors during analysis.