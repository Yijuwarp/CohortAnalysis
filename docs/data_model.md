# Data Model

This document describes the database schema used by the Cohort Analysis backend, implemented in DuckDB.

## Core Event Tables

### `events` (Raw)
The initial table created from the uploaded CSV.
- **Columns**: All source CSV columns, types automatically detected by DuckDB.

### `events_normalized`
The primary source for all analytics, containing canonical and metadata fields. Persistent throughout a workspace session.
- **`user_id`**: Canonical user identifier (TEXT).
- **`event_name`**: Canonical event type (TEXT).
- **`event_time`**: Canonical event timestamp (TIMESTAMP).
- **`event_count`**: The frequency of this event row in the source.
- **`original_revenue`**: The raw revenue value from the source.
- **`modified_revenue`**: Adjusted revenue based on revenue config or overrides.
- **Metadata Columns**: All non-mapped source columns are included as metadata for filtering.

### `events_scoped`
The **Effective Filtered Dataset**. A **Dynamic View** over `events_normalized` created by applying active date range and property filters.
- **CRITICAL**: This is the single source of truth for the active scope. Its change MUST trigger a full rebuild of all downstream materialized tables.

## Cohort Tables

### `cohorts`
Definitions of active cohorts in the current workspace.
- **`cohort_id`**: Primary Key (INTEGER).
- **`name`**: Display name.
- **`is_active`**: Logic flag for system state.
- **`hidden`**: Visibility flag for analytics results.
- **`cohort_origin`**: How the cohort was created (manual, split, paths).

### `cohort_membership` (Materialized)
Identifies users belonging to each cohort and their calculated join time relative to the **current scope**.
- **`user_id`, `cohort_id`**: Composite PK.
- **`join_time`**: The timestamp when the user joined the cohort.

### `cohort_activity_snapshot` (Materialized)
Captured events for cohort members within the active scope.
- **`cohort_id`, `user_id`, `event_time`, `event_name`**: Core fields for analytics joins.
- **INVARIANT**: This table **MUST** always be derived from the same version of `events_scoped` as `cohort_membership`.

## Sequence Analysis (Paths) Tables

### `paths`
- **`id`**: Primary Key (INTEGER).
- **`name`**: Path definition name.
- **`max_step_gap_minutes`**: Optional time constraint between steps.
- **`created_at`**: Creation timestamp.

### `path_steps`
- **`id`**: Primary Key.
- **`path_id`**: Foreign Key to `paths`.
- **`step_order`**: Sequential order index.
- **`group_id`**: Alternative event group index (for `OR` support).
- **`event_name`**: Targeted event for this step.

### `path_step_filters`
- **`id`**: Primary Key.
- **`step_id`**: Foreign Key to `path_steps`.
- **`property_key`, `property_value`, `property_type`**: Filter conditions for specific steps.

## Metadata & Configuration

### `dataset_scope`
Singleton table tracking current filter state (managed in `app/domains/scope/scope_metadata.py`).
- **`id`**: Primary Key (always 1).
- **`filters_json`**: JSON string of active date range and property filters.
- **`total_rows`**: Count in `events_normalized`.
- **`filtered_rows`**: Count in `events_scoped`.
- **`total_events`**: Cumulative `event_count` in `events_scoped`.
- **`updated_at`**: Timestamp of the last scope refresh.

### `revenue_event_selection`
Configuration for which events contribute to revenue calculations.
- **`event_name`**: Primary Key.
- **`is_included`**: Boolean flag.
- **`override_value`**: Optional fixed revenue value to use instead of the source value.
