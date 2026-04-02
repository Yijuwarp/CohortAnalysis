# Ingestion Pipeline

The Ingestion Pipeline is responsible for transforming raw CSV data into a canonical, analytical format, while establishing the initial system state and scope.

## Phase 1: Upload (`POST /upload`)

- **Validation**: Ensures the uploaded filename ends with `.csv`.
- **Ingestion**: Uses DuckDB's `read_csv_auto` to create the raw `events` table directly from the file stream.
- **Metadata Detection**:
  - `rows_imported`: Total row count.
  - `columns`: List of all detected column names.
  - `detected_types`: DuckDB-inferred types categorized into semantically relevant classes for the frontend.
  - `mapping_suggestions`: Automatically identifies potential candidates for `user_id`, `event_name`, and `event_time` based on naming patterns.

---

## Phase 2: Mapping (`POST /map-columns`)

The mapping phase initializes the Analytical Workspace by creating a canonical dataset.

### 1. Schema Derivation
The system creates `events_normalized` from the raw `events` table by performing a **Grouped Aggregation**.
- **Grouping**: Data is grouped by its canonical columns (`user_id`, `event_name`, `event_time`) and **all metadata columns** to reduce redundancy while preserving filterable context.
- **Aggregation**:
  - `original_event_count`: The `SUM` of the source `event_count` column or a `COUNT(*)` if no count column was mapped.
  - `original_revenue`: The `SUM` of the source `revenue` column or `0.0`.
  - `modified_revenue`: Initialized as a copy of `original_revenue`, used for downstream value overrides.

### 2. Validation & Quality Rules
- **Non-Nullable**: `user_id`, `event_name`, and `event_time` must not be null or empty string.
- **Type Constraints**: `event_time` must be castable to a `TIMESTAMP`; revenue and count columns must be numeric.
- **Data Integrity**: Rows with `event_count < 1` are filtered or treated as errors depending on strictness settings.

### 3. Workspace Initialization
Successful mapping triggers a comprehensive system reset:
- **Clear Previous State**: Purges existing `cohorts`, `cohort_conditions`, `cohort_membership`, and `cohort_activity_snapshot`.
- **Initial Scope**: Creates the `events_scoped` dataset as a base view over `events_normalized`.
- **Core Population**: Automatically creates and materializes the "All Users" cohort.
- **Revenue Configuration**: If a revenue column was mapped, initializes the `revenue_event_selection` table with all unique events.

---

## Phase 3: Scoping (`POST /apply-filters`)

Scoping allows users to define a refined "Active" dataset for all analytics.

### 1. Target Validation
Validates every active filter (`column`, `operator`, `value`) against the `events_normalized` schema and its detected types.

### 2. Scope Rebuild
Recreates the `events_scoped` view using a dynamic SQL `WHERE` clause built from the user's active filter set and date range.

### 3. Cascading Materialization
> [!IMPORTANT]
> **Materialization Trigger**: Every successful `/apply-filters` call **MUST** trigger a full re-materialization of `cohort_membership` and `cohort_activity_snapshot` for all active cohorts to ensure the entire system operates on a consistent dataset.

---

## Phase 4: Column Type Detection Logic

Column types are semantically categorized to govern UI filter behavior:
- **NUMERIC**: Supports range and equality operators ($>, <, \ge, \le, =, \ne, IN, NOT IN$).
- **TEXT**: Supports equality and set operators ($=, \ne, IN, NOT IN$).
- **TIMESTAMP/DATE**: Supports time-based range filtering and relative offsets.
