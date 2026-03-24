# API Reference

This document provides a detailed reference for all available API endpoints in the Cohort Analysis backend.

## Core Data Flow

### `POST /upload`
Uploads a CSV file and initializes the raw `events` table.
- **Request**: `multipart/form-data` with a `file` field.
- **Response**: `rows_imported`, `columns`, `detected_types`, `mapping_suggestions`.

### `POST /map-columns`
Maps source CSV columns to canonical fields and creates `events_normalized`.
- **Request Body**:
    - `user_id_column` (string)
    - `event_name_column` (string)
    - `event_time_column` (string)
    - `event_count_column` (string, optional)
    - `revenue_column` (string, optional)
    - `column_types` (object, optional overrides)
- **Response**: `total_users`, `total_events`.

### `POST /apply-filters`
Applies date range and property filters to create `events_scoped`.
- **Request Body**:
    - `filters` (array of filter objects)
    - `from_date` (string, ISO)
    - `to_date` (string, ISO)
- **Response**: `total_rows`, `total_events`.

### `GET /scope`
Retrieves current dataset scope metadata.
- **Response**: `id`, `filters`, `total_rows`, `total_events`, `updated_at`.

## Metadata & Discovery

### `GET /columns`
Lists all columns available in the normalized dataset.

### `GET /column-values`
Retrieves unique values for a specific column, optionally filtered by event.
- **Query Params**: `column`, `event_name` (optional).

### `GET /date-range`
Returns the min and max event timestamps in the normalized dataset.

### `GET /events`
Lists all unique event names in the scoped dataset.

## Cohort Management

### `POST /cohorts`
Creates a new cohort.
- **Request Body**: `name`, `logic_operator` ('AND'|'OR'), `join_type` ('condition_met'|'first_event'), `conditions` (array).

### `GET /cohorts`
Lists all active cohorts for the current dataset.

### `GET /cohorts/{cohort_id}`
Retrieves details for a specific cohort.

### `PUT /cohorts/{cohort_id}`
Updates an existing cohort definition.

### `DELETE /cohorts/{cohort_id}`
Deletes a cohort.

### `PATCH /cohorts/{cohort_id}/hide`
Toggles the visibility of a cohort in analytics results.

### `POST /cohorts/{cohort_id}/random_split`
Splits a cohort into two random groups.

### `POST /cohorts/estimate`
Estimates the size of a cohort without materializing it.

## Saved Cohorts (Global)

### `GET /saved-cohorts`
Lists all globally saved cohort definitions.

### `POST /saved-cohorts`
Saves a cohort definition for reuse.

### `GET /saved-cohorts/{id}`
Retrieves a saved cohort definition.

### `PUT /saved-cohorts/{id}`
Updates a saved cohort definition.

### `DELETE /saved-cohorts/{id}`
Deletes a saved cohort definition.

## Analytics

### `GET /retention`
Computes retention metrics for all active cohorts.
- **Query Params**: `max_day`, `retention_event`, `include_ci`, `confidence`, `retention_type`, `granularity`.

### `GET /usage`
Computes event volume and unique users per cohort/day.
- **Query Params**: `event`, `max_day`, `retention_event`.

### `GET /monetization`
Computes revenue metrics per cohort/day.
- **Query Params**: `max_day`.

### `POST /compare-cohorts`
Performs statistical comparison between two cohorts.

## Revenue Configuration

### `GET /revenue-events`
Lists events with non-zero revenue.

### `GET /revenue-config-events`
Retrieves current revenue inclusion and override configuration.

### `POST /update-revenue-config`
Updates revenue configuration.

## Funnels

### `POST /funnels`
Creates a new funnel.

### `GET /funnels`
Lists all funnels.

### `PUT /funnels/{funnel_id}`
Updates a funnel.

### `DELETE /funnels/{funnel_id}`
Deletes a funnel.

### `POST /funnels/run`
Executes a funnel analysis.

## Flows

### `GET /flow/l1`
Level 1 event flow analysis.

### `GET /flow/l2`
Level 2 expansion for a specific flow path.

### `GET /flow/graph`
Full graph-based flow visualization.

## Debug & Explorer

### `GET /users/search`
Search for individual users.

### `GET /user-explorer`
Retrieve a detailed activity timeline for a specific user.
