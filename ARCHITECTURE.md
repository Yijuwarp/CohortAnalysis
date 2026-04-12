# Architecture

## System Overview
The application is a full-stack cohort analysis platform consisting of a FastAPI backend (`backend/app/main.py`) and a React/Vite frontend (`frontend/src`). It uses DuckDB as an embedded analytical database for high-performance event processing. The backend is organized into domain-driven modules to ensure scalability and maintainability.

## Detailed Architecture
For deeper dives into specific areas, refer to:
- [Backend Overview](docs/backend_overview.md)
- [Frontend Overview](docs/frontend_overview.md)
- [Data Model](docs/data_model.md)
- [API Reference](docs/api_reference.md)

## Data Pipeline
The system processes data through several stages of refinement:
1.  **Raw Ingestion**: `events` (raw CSV) -> initial schema and type detection.
2.  **Normalization**: `events_normalized` -> persistent table with canonical fields, type casting, and grouping.
3.  **Scoping**: `events_scoped` -> **Dynamic VIEW** over `events_normalized` based on active filters and date range. Rebuilt on every filter change.
4.  **Cohort Materialization**: `cohort_membership` -> Materialized user set satisfying specific logic, derived from `events_scoped`.
5.  **Activity Snapshots**: `cohort_activity_snapshot` -> Materialized stream of events for cohort members, derived from `events_scoped`.
6.  **Analytics Execution**: Final responses generated from `cohort_activity_snapshot` or `events_scoped` depending on the module.

## Critical System Invariants
- **Source Consistency**: `cohort_membership` and `cohort_activity_snapshot` **MUST** always be derived from the same version of `events_scoped`. Any mismatch leads to duplicated events, incorrect retention, and inflated metrics.
- **Recomputation Cost**: Rebuilding `events_scoped` (updating the view) triggers a full re-materialization of `cohort_membership` and `cohort_activity_snapshot`. This is intentionally expensive and should not be triggered unnecessarily.
- **Allowed Joins**:
    - `snapshot` ↔ `scoped` (for property filters)
    - `snapshot` ↔ `membership`
- **Forbidden Joins**:
    - `normalized` ↔ `analytics` (Prevents stale filter application)
    - `raw events` ↔ `analytics`

## Backend Responsibilities (Domain Modules)
The backend is structured into domain-specific services under `backend/app/domains/`:
- **Ingestion**: Column mapping, type inference, and `events_normalized` creation.
- **Scope**: Managing `events_scoped` view and global filter state.
- **Cohorts**: Evaluating frequency-based logic, materializing memberships and snapshots.
- **Analytics**: Domain-specific logic for Retention, Usage, Monetization, Paths, and Flows.
- **Revenue**: Managing monetization inclusion rules and value overrides via `revenue_event_selection`.

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

## Analytics Systems Source Rules

| Module | Base Dataset | Filtering Layer | Rationale |
| :--- | :--- | :--- | :--- |
| **Retention** | `snapshot` | `scoped` | Only joins `scoped` if property filters are applied per-step. |
| **Monetization** | `scoped` | `membership` | Requires `modified_revenue` from `scoped` joined with `membership` for alignment. |
| **Usage** | `scoped` | `membership` | Directly queries `scoped` to allow granular property filtering. |
| **Paths** | `snapshot` / `scoped` | `scoped` | Uses `snapshot` for speed, but falls back to `scoped` for complex filtering. |
| **Flows** | `snapshot` | `scoped` | Anchored to `snapshot` with optional `EXISTS` check on `scoped` for properties. |

## Persistence
- Analytical data is stored in `backend/cohort_analysis.duckdb`.
- Configurations (Paths, Revenue, Scope) are persisted in specialized tables.
- Workspace UI state is persisted in `localStorage`.
