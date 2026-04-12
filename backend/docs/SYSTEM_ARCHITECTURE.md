# Backend System Architecture

## Runtime Composition
The backend is a high-performance analytical server built with FastAPI and DuckDB. It is organized into domain-driven modules to ensure clear separation of concerns.

- **FastAPI Entry Point**: `app/main.py` handles startup logic and router registration.
- **Domain Services**: `app/domains/*` contains the core business logic, partitioned by feature area (Ingestion, Scope, Cohorts, Analytics, etc.).
- **Multi-User Isolation**: A structural isolation layer in `app/db/connection.py` (`run_query`) manages per-user DuckDB connections and enforces thread-safety.
- **Data Persistence**: Analytics data is stored in per-user DuckDB files located at `backend/data/{user_id}/cohort_analysis.duckdb`.

## Initialization Behavior
`app/main.py` registers specialized routers for each functional domain:
- `upload`: CSV reception and schema detection.
- `mapping`: Normalization and canonical field mapping.
- `scope`: Date range and property filter management.
- `cohorts`: Definition, materialization, and splitting of user segments.
- `analytics`: Computation of Retention, Usage, Monetization, and Flows.
- `paths`: Sequence analysis and conversion tracking.
- `revenue`: Management of revenue inclusion rules and value overrides.

## Data Lifecycle
1.  **Ingest**: Raw CSV data is streamed into a temporary `events` table (in-memory or short-lived).
2.  **Normalize**: Data is transformed into the persistent `events_normalized` table with canonical types and grouped aggregation.
3.  **Scope**: An `events_scoped` View is defined over normalized data using active filters.
4.  **Materialize**: Cohort memberships and activity snapshots are built from the current `events_scoped` view.
5.  **Serve**: Analytics services query the materialized snapshots or the scoped view to deliver sub-second responses.

## Core Database Tables

| Table | Purpose | Resilience |
| :--- | :--- | :--- |
| `events_normalized` | Persistent event stream with canonical fields. | Table |
| `events_scoped` | Ground truth for the active analysis scope. | **View** |
| `cohorts` | Metadata and definitions for user segments. | Table |
| `cohort_membership` | Materialized list of (user, cohort, join_time). | Table |
| `cohort_activity_snapshot` | Materialized event stream for cohort members. | Table |
| `dataset_scope` | Current global filter and date range state. | Table |
| `revenue_event_selection` | Active revenue inclusion and override rules. | Table |
| `paths` | Saved sequence analysis definitions. | Table |

## Key Implementation Notes
- **Thread Safety**: On Windows, the system enforces a single-connection, single-worker model (`DUCKDB_SINGLE_WORKER=true`) to avoid database locking crashes.
- **Cascading Recomputation**: Updating the `events_scoped` view or revenue configuration triggers a mandatory re-materialization of all dependent cohort data.
- **Isolation**: DuckDB instances are logically isolated per `user_id`, ensuring no data leakage between concurrent users.
