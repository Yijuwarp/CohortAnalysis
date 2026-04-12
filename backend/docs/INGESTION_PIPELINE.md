# Ingestion Pipeline

The ingestion pipeline transforms raw CSV data into a structured, optimized format for multi-user cohort analysis.

## Phase 1: Upload (`POST /upload`)

- **Validation**: Ensures the uploaded filename ends with `.csv`.
- **Ingestion**: Uses DuckDB's `read_csv_auto` to create the raw `events` table directly from the file stream.
- **Isolation**: Each user's ingestion process is isolated within their own DuckDB database file (e.g., `backend/data/{user_id}/cohort_analysis.duckdb`).
- **Metadata Detection**:
  - `rows_imported`: Total row count.
  - `columns`: List of all detected column names.
  - `detected_types`: DuckDB-inferred types categorized into semantically relevant classes.
  - `mapping_suggestions`: Automatically identifies potential candidates for `user_id`, `event_name`, and `event_time`.

---

## Phase 2: Mapping (`POST /map-columns`)

The mapping phase initializes the Analytical Workspace by creating a canonical dataset.

### 1. Schema Derivation
The system creates `events_normalized` from the raw `events` table by performing a **Grouped Aggregation**.
- **Grouping**: Data is grouped by its canonical columns (`user_id`, `event_name`, `event_time`) and **all metadata columns** to reduce redundancy while preserving filterable context.
- **Aggregation**:
  - `event_count`: The `SUM` of the source `event_count` column or a `COUNT(*)` if no count column was mapped.
  - `original_revenue`: The `SUM` of the source `revenue` column or `0.0`.
  - `modified_revenue`: Initialized as a copy of `original_revenue`, used for downstream value overrides.

### 2. Validation & Quality Rules
- **Non-Nullable**: `user_id`, `event_name`, and `event_time` must not be null or empty string.
- **Boundary Enforcement**: The system identifies the P99.99 timestamp in the dataset to filter out extreme outliers that could skew analysis.
- **Data Integrity**: Minimum event counts are enforced; revenue must be numeric.

---

## Phase 3: Scoping (`POST /apply-filters`)

Scoping allows users to define a refined "Active" dataset for all analytics.

### 1. Dynamic View definition
Recreates the `events_scoped` view using a dynamic SQL `WHERE` clause built from the user's active filter set and date range.
- **Ground Truth**: `events_scoped` is implemented as a **View** (not a table) over `events_normalized`.

### 2. Cascading Materialization
> [!IMPORTANT]
> **Materialization Trigger**: Every successful `/apply-filters` call **MUST** trigger a full re-materialization of `cohort_membership` and `cohort_activity_snapshot` for all active cohorts to ensure the entire system operates on a consistent dataset.

---

## Persistence Strategy

| Layer | Type | Persistence | Target Use |
| :--- | :--- | :--- | :--- |
| `events` | Table | Temporary | Internal transformation base. |
| `events_normalized` | Table | Persistent | Primary source for all workspace state. |
| `events_scoped` | **View** | Session | Ground truth for active analysis scope. |
| `cohort_membership` | Table | Session | Sub-second user set filtering. |
| `cohort_activity_snapshot` | Table | Session | Pre-filtered event stream for analytics. |

### Why a View for Scoping?
Using a View for `events_scoped` ensures that the "Active Scope" is always a zero-cost definition change until materialization is explicitly required. It prevents data duplication and ensures that property pickers always reflect the literal definition of the current filters.
