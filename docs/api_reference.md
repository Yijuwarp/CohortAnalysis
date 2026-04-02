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
    - `user_id_column`, `event_name_column`, `event_time_column` (Required)
    - `event_count_column`, `revenue_column` (Optional)
    - `column_types` (Optional overrides)
- **Response**: `total_users`, `total_events`, `has_revenue`.

### `POST /apply-filters`
Applies date range and property filters to recreate the `events_scoped` view.
- **Request Body**:
    - `date_range` (start, end)
    - `filters` (array of objects: column, operator, value)
- **Response**: `total_rows`, `total_events`, `filtered_rows`, `percentage`.
- **CRITICAL**: Triggers a full re-materialization of `cohort_membership` and `cohort_activity_snapshot`.

### `GET /scope`
Retrieves current dataset scope metadata, active property filters, and date range.

## Metadata & Discovery

### `GET /columns`
Lists all columns available in the normalized dataset with their detected types.

### `GET /column-values`
Retrieves unique values for a specific column, optionally filtered by event.
- **Query Params**: `column`, `event_name` (optional).

### `GET /date-range`
Returns the min and max event timestamps in the normalized dataset.

### `GET /events`
Lists all unique event names in the scoped dataset.

## Cohort Management

### `POST /cohorts`
Creates a new cohort and materializes its membership and activity snapshots.
- **Request Body**: `name`, `logic_operator` ('AND'|'OR'), `join_type`, `conditions`.

### `GET /cohorts`
Lists all active cohorts for the current dataset.

### `GET /cohorts/{cohort_id}`
Retrieves details for a specific cohort.

### `PUT /cohorts/{cohort_id}`
Updates an existing cohort definition and re-materializes its data.

### `DELETE /cohorts/{cohort_id}`
Deletes a cohort and its associated materialized records.

### `PATCH /cohorts/{cohort_id}/hide`
Toggles visibility for analytics results (e.g., hiding from Retention table).

### `POST /cohorts/{cohort_id}/random_split`
Splits a cohort into two random groups ('Group A' and 'Group B') for Comparison analysis.

### `POST /cohorts/estimate`
Estimates the size of a cohort based on `events_scoped` without performing full materialization.

## Sequence Analysis (Paths)

### `GET /paths`
Lists all saved path (sequence) definitions.

### `POST /paths`
Creates a new path definition.
- **Request Body**: `name`, `steps` (array of `event_name` and optional per-step `filters`).

### `PUT /paths/{path_id}`
Updates an existing path definition.

### `DELETE /paths/{path_id}`
Deletes a path definition.

### `POST /paths/run`
Executes sequence analysis across all active cohorts using **Earliest Greedy Matching**.
- **Source**: Uses `cohort_activity_snapshot` as base; joins `events_scoped` for steps with filters.
- **Response**: `steps`, `results` (per-cohort conversion/drop-off), `global_insights`.

### `POST /paths/create-dropoff-cohort`
Creates a new cohort from users who reached step $N$ but dropped off before step $N+1$.

### `POST /paths/create-reached-cohort`
Creates a new cohort from users who successfully reached a specific step $N$.

## Analytics

### `GET /retention`
Computes retention metrics using `cohort_activity_snapshot`.
- **Query Params**: `max_day`, `retention_event`, `include_ci`, `retention_type` (classic|ever-after).

### `GET /usage`
Computes event volume and unique users per cohort using `events_scoped` aligned with `cohort_membership`.

### `GET /usage-frequency`
Computes distribution of event frequency per user for the scoped dataset.

### `GET /monetization`
Computes revenue metrics using `events_scoped` joined with `cohort_membership` and `cohort_activity_snapshot`.
- **Note**: Respects `join_time` offsets and uses `modified_revenue` fields.

### `GET /flow/l1`
Level 1 event flow (Sankey) analysis using `cohort_activity_snapshot`.

### `GET /flow/l2`
Level 2 expansion for a specific flow path.

## Debug & Explorer

### `GET /users/search`
Search for individual users within the current scope.

### `GET /user-explorer`
Retrieve a detailed activity timeline and properties for a specific user.
