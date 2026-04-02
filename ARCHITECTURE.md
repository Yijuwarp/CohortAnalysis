# Architecture

## System Overview
The application is a full-stack cohort analysis platform consisting of a FastAPI backend (`backend/app/main.py`) and a React/Vite frontend (`frontend/src`). It uses DuckDB as an embedded analytical database for high-performance event processing.

## Detailed Architecture
For deeper dives into specific areas, refer to:
- [Backend Overview](docs/backend_overview.md)
- [Frontend Overview](docs/frontend_overview.md)
- [Data Model](docs/data_model.md)
- [API Reference](docs/api_reference.md)

## Data Pipeline
The system processes data through several stages of refinement:
1.  **Raw Ingestion**: `events` (raw CSV) -> initial schema and type detection.
2.  **Normalization**: `events_normalized` -> canonical fields, type casting, and grouping.
3.  **Scoping**: `events_scoped` -> Subset of normalized data based on active filters and date range. Rebuilt on every filter change.
4.  **Cohort Materialization**: `cohort_membership` -> Users satisfying specific logic, derived from `events_scoped`.
5.  **Activity Snapshots**: `cohort_activity_snapshot` -> Materialized stream of events for cohort members, derived from `events_scoped`.
6.  **Analytics Execution**: Final responses generated from `cohort_activity_snapshot` or `events_scoped`.

## Critical System Invariants
- **Source Consistency**: `cohort_membership` and `cohort_activity_snapshot` **MUST** always be derived from the same version of `events_scoped`. Any mismatch leads to duplicated events, incorrect retention, and inflated metrics.
- **Recomputation Cost**: Rebuilding `events_scoped` triggers a full re-materialization of `cohort_membership` and `cohort_activity_snapshot`. This is intentionally expensive and should not be triggered unnecessarily.
- **Allowed Joins**:
    - `snapshot` ↔ `scoped` (for property filters)
    - `snapshot` ↔ `membership`
- **Forbidden Joins**:
    - `normalized` ↔ `analytics` (Prevents stale filter application)
    - `raw events` ↔ `analytics`

## Backend Responsibilities
- **Ingestion**: Column mapping and type inference.
- **Normalization**: Grouping by canonical and metadata columns to reduce row count.
- **Filtering**: Rebuilding `events_scoped` and refreshing dependent cohort memberships.
- **Cohort Engine**: Evaluating frequency-based conditions with property filters.
- **Analytics Models**: Retention, usage, monetization, paths, and flows.
- **Revenue System**: Managing inclusion and value overrides for monetization.

## Frontend Responsibilities
- **Workspace Management**: Coordinate state between mapping, filtering, and analytics.
- **Interactive Analytics**: Tables and visualizations (Retention, Usage, Monetization, Paths, Flows, User Explorer).
- **Cohort Builder**: Interface for defining complex cohort logic.
- **User Detail**: In-depth timeline exploration for individual users (User Explorer).

## Database Tables
Refer to [data_model.md](docs/data_model.md) for detailed table schemas.

## Cohort Logic
- Conditions use cumulative `SUM` of event counts.
- Users satisfy conditions when they reach `min_event_count`.
- Support for `AND`/`OR` logic operators.
- Join types: `condition_met` (trigger time) or `first_event` (global earliest).
- Memberships and activity are materialized to ensure sub-second analytics performance.

## Analytics Systems

### Retention
Dynamic retention analysis using `cohort_activity_snapshot`. Supports Classic and Ever-After algorithms with optional confidence intervals (Wilson score).

### Monetization
Revenue analysis using `events_scoped` joined with `cohort_membership` to respect `join_time` and `modified_revenue` fields. Supports event-level inclusion toggles and value overrides.

### Paths (Sequence Analysis)
Earliest Greedy Matching for multi-step conversion tracking.
- **Logic**: Sequential matching where each step $N$ matches the first valid occurrence after $t_{N-1}$.
- **Source**: Uses `cohort_activity_snapshot` as the base event stream; joins `events_scoped` for per-step property filters.

### Flows
Sankey-style event transition analysis using `cohort_activity_snapshot`. Supports expansion capabilities (L1/L2) and top-K event grouping.

## Persistence
- Analytical data is stored in `backend/cohort_analysis.duckdb`.
- Paths and Revenue configurations are persisted in specialized tables (`paths`, `revenue_event_selection`).
- Workspace UI state (collapsed panes, active tabs, etc.) is persisted in `localStorage`.
