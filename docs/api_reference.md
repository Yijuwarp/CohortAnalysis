# API Reference

This document provides a detailed reference for all available API endpoints in the Cohort Analysis backend. All endpoints require a `user_id` query parameter for multi-user isolation.

## Core Data Flow

### `POST /upload`
Uploads a CSV file and initializes the raw `events` table for a specific user.
- **Query Params**: `user_id` (Required)
- **Request**: `multipart/form-data` with a `file` field.
- **Response**: `rows_imported`, `columns`, `detected_types`, `mapping_suggestions`.

### `POST /map-columns`
Maps source CSV columns to canonical fields and creates `events_normalized`.
- **Query Params**: `user_id` (Required)
- **Request Body**:
    - `user_id_column`, `event_name_column`, `event_time_column` (Required)
    - `event_count_column`, `revenue_column` (Optional)
    - `column_types` (Optional overrides)
- **Response**: `total_users`, `total_events`, `has_revenue`.

### `POST /apply-filters`
Applies date range and property filters to recreate the `events_scoped` view.
- **Query Params**: `user_id` (Required)
- **Request Body**:
    - `date_range`: `{ start: string, end: string }`
    - `filters`: Array of `{ column: string, operator: string, value: any }`
- **Response**: `total_rows`, `total_events`, `filtered_rows`, `percentage`.
- **CRITICAL**: Triggers a full re-materialization of `cohort_membership` and `cohort_activity_snapshot`.

### `GET /scope`
Retrieves current dataset scope metadata, active property filters, and date range.
- **Query Params**: `user_id` (Required)

## Metadata & Discovery

### `GET /columns`
Lists all columns available in the normalized dataset with their detected types.
- **Query Params**: `user_id` (Required)

### `GET /column-values`
Retrieves unique values for a specific column, optionally filtered by event or search term.
- **Query Params**: `user_id` (Required), `column`, `event_name` (optional), `search` (optional), `limit` (optional).

### `GET /date-range`
Returns the min and max event timestamps in the normalized dataset.
- **Query Params**: `user_id` (Required)

### `GET /events`
Lists all unique event names in the scoped dataset.
- **Query Params**: `user_id` (Required)

## Cohort Management

### `POST /cohorts`
Creates a new cohort and materializes its membership and activity snapshots.
- **Query Params**: `user_id` (Required)
- **Request Body**: `name`, `logic_operator` ('AND'|'OR'), `join_type`, `conditions`.

### `GET /cohorts`
Lists all active cohorts for the current dataset.
- **Query Params**: `user_id` (Required)

### `POST /cohorts/estimate`
Estimates the size of a cohort based on `events_scoped` without performing full materialization.
- **Query Params**: `user_id` (Required)
- **Request Body**: Same as `POST /cohorts`.

### `GET /cohorts/{cohort_id}`
Retrieves details for a specific cohort.
- **Query Params**: `user_id` (Required)

### `PUT /cohorts/{cohort_id}`
Updates an existing cohort definition and re-materializes its data.
- **Query Params**: `user_id` (Required)

### `DELETE /cohorts/{cohort_id}`
Deletes a cohort and its associated materialized records.
- **Query Params**: `user_id` (Required)

### `PATCH /cohorts/{cohort_id}/hide`
Toggles visibility for analytics results (e.g., hiding from Retention table).
- **Query Params**: `user_id` (Required)

### `POST /cohorts/{cohort_id}/split`
Splits a parent cohort into multiple groups based on random assignment or property values.
- **Query Params**: `user_id` (Required)
- **Request Body**: `type` ('random'|'property'), `random` (RandomSplitOptions), `property` (PropertySplitOptions).

### `POST /cohorts/{cohort_id}/split/preview`
Returns expected group sizes for a proposed split without persisting any changes.
- **Query Params**: `user_id` (Required)
- **Request Body**: Same as `POST /cohorts/{id}/split`.

## Sequence Analysis (Paths)

### `GET /paths`
Lists all saved path (sequence) definitions.
- **Query Params**: `user_id` (Required)

### `POST /paths`
Creates a new path definition.
- **Query Params**: `user_id` (Required)
- **Request Body**: `name`, `steps` (array of `event_name` and optional per-step `filters`).

### `POST /paths/run`
Executes sequence analysis across all active cohorts using **Earliest Greedy Matching**.
- **Query Params**: `user_id` (Required)
- **Source**: Uses `cohort_activity_snapshot` by default. Falls back to `events_scoped` for steps with property filters.

## Analytics

### `GET /retention`
Computes retention metrics using `cohort_activity_snapshot`.
- **Query Params**: `user_id` (Required), `max_day`, `retention_event`, `include_ci`, `retention_type` (classic|ever-after).

### `GET /usage`
Computes event volume and unique users per cohort using `events_scoped` aligned with `cohort_membership`.
- **Query Params**: `user_id` (Required), `event`, `max_day`.

### `GET /monetization`
Computes revenue metrics using `events_scoped` joined with `cohort_membership`.
- **Query Params**: `user_id` (Required), `max_day`.
- **Note**: Respects `join_time` offsets and uses `modified_revenue` fields from the scoped view.

### `GET /flow/l1`
Level 1 event flow (Sankey) analysis using `cohort_activity_snapshot`.
- **Query Params**: `user_id` (Required), `start_event`, `top_k`.

## Debug & Explorer

### `GET /user-explorer`
Retrieve a detailed activity timeline and properties for a specific user.
- **Query Params**: `user_id` (Required), `target_user_id`.
- **Source**: Directly queries `events_scoped` for full metadata.
