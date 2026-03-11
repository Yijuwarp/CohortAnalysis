# Ingestion Pipeline

## Step 1: Upload (`POST /upload`)
- Validates filename ends with `.csv`.
- Creates/replaces raw table `events` using DuckDB `read_csv_auto`.
- Returns:
  - `rows_imported`
  - `columns`
  - `detected_types`
  - `mapping_suggestions`

## Step 2: Map columns (`POST /map-columns`)
Request model:
- `user_id_column` (required)
- `event_name_column` (required)
- `event_time_column` (required)
- `event_count_column` (optional)
- `revenue_column` (optional)
- `column_types` (optional type overrides)

Validation highlights:
- Required mapped fields must exist in `events`.
- Required semantic types: user_id TEXT, event_name TEXT, event_time TIMESTAMP.
- Optional `event_count_column` and `revenue_column` must be NUMERIC.
- `event_time` must not be null/empty.
- If `event_count_column` exists, value must be integer >= 1.

Normalization behavior:
- Creates `events_normalized` via grouped aggregation.
- Groups by canonical columns + metadata columns.
- Aggregates:
  - `original_event_count` (sum or count)
  - `original_revenue` (sum or 0)
  - `modified_event_count` initialized from original
  - `modified_revenue` initialized from original

Post-normalization reset/init:
- Clears cohort tables (`cohort_membership`, `cohort_activity_snapshot`, `cohort_conditions`, `cohorts`).
- Initializes scope table/data (`events_scoped`, `dataset_scope`).
- Initializes revenue config table when revenue mapping is provided.
- Creates All Users cohort.

## Step 3: Scope (`POST /apply-filters`)
- Validates filter columns/operators against normalized schema.
- Rebuilds `events_scoped` with SQL `WHERE` from date range + filter list.
- Recomputes modified revenue/count fields for scoped table.
- Updates `dataset_scope` counts and filter JSON.
- Rebuilds cohort memberships and activity snapshots.
