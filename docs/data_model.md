# Data Model

This document describes the database schema used by the Cohort Analysis backend, implemented in DuckDB.

## Core Event Tables

### `events` (Raw)
The initial table created from the uploaded CSV.
- **Columns**: All source CSV columns, types automatically detected by DuckDB.

### `events_normalized`
The primary source for all analytics, containing canonical and metadata fields.
- **`user_id`**: Canonical user identifier (TEXT).
- **`event_name`**: Canonical event type (TEXT).
- **`event_time`**: Canonical event timestamp (TIMESTAMP).
- **`original_event_count`**: The frequency of this event row in the source.
- **`original_revenue`**: The raw revenue value from the source.
- **`modified_event_count`**: Adjusted event count based on revenue config.
- **`modified_revenue`**: Adjusted revenue based on revenue config.
- **Metadata Columns**: All non-mapped source columns are included as metadata for filtering.

### `events_scoped`
A subset of `events_normalized` created by applying active date range and property filters.

## Cohort Tables

### `cohorts` (Active)
Definitions of active cohorts in the current workspace.
- **`cohort_id`**: Primary Key (UUID).
- **`name`**: Display name.
- **`logic_operator`**: 'AND' or 'OR' for joining conditions.
- **`join_type`**: 'condition_met' (first meeting condition) or 'first_event' (user's first-ever event).
- **`is_active`**: Logic flag for system state.
- **`hidden`**: Visibility flag for analytics results.

### `cohort_conditions`
Individual conditions that define a cohort.
- **`cohort_id`**: Foreign Key to `cohorts`.
- **`event_name`**: The event to match.
- **`min_event_count`**: Minimum cumulative occurrences required.
- **`property_column`, `property_operator`, `property_values`**: Optional filters for the condition.

### `cohort_membership` (Materialized)
Identifies users belonging to each cohort and their calculated join time.
- **`user_id`, `cohort_id`**: Composite PK.
- **`join_time`**: The timestamp when the user joined the cohort.

### `cohort_activity_snapshot`
Captured events for cohort members within the active scope.
- **`cohort_id`, `user_id`, `event_time`, `event_name`**: Core fields for analytics joins.

## Global Saved Cohorts

### `saved_cohorts`
Reusable cohort definitions stored independently of any specific dataset.
- **`id`**: Primary Key (UUID).
- **`name`**: Display name.
- **`definition`**: JSON string containing the full cohort configuration.

## Metadata & Configuration

### `dataset_scope`
Singleton table (ID=1) tracking current filter state.
- **`filters`**: JSON string of active property filters.
- **`total_rows`, `total_events`**: Summary counts for the scoped dataset.
- **`updated_at`**: Timestamp of the last scope refresh.

### `revenue_event_selection`
Configuration for which events contribute to revenue calculations.
- **`event_name`**: Primary Key.
- **`is_included`**: Boolean flag.
- **`override_value`**: Optional fixed revenue value to use instead of the source value.

### `dataset_metadata`
General dataset-level flags, such as `has_revenue_mapping`.
