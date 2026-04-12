# Backend Overview

The backend is a high-performance analytical engine built with FastAPI and DuckDB. It manages structured behavioral data through a domain-driven architecture, ensuring sub-second analysis across large user sets.

## Architecture & Modules

The backend is organized into specialized domain services under `backend/app/domains/`:

- **Ingestion**: Manages CSV uploads, schema detection, and per-user database initialization.
- **Normalization**: Transforms raw data into the persistent `events_normalized` table, performing row-level grouping to reduce volume.
- **Scope**: Manages the "Active Scope" via a dynamic `events_scoped` **View**. Rebuilds the view definition whenever date ranges or property filters change.
- **Cohorts**: Evaluates frequency logic against the scoped view to materialize `cohort_membership` and `cohort_activity_snapshot`.
- **Analytics**: Domain services for Retention, Usage, Monetization, and Flows.
- **Paths**: Deterministic sequence matching service with support for alternative event paths (`OR`) and property filters.
- **Revenue**: Configures inclusion rules and value overrides via `revenue_event_selection`.

## Data Pipeline Invariants

1.  **Normalization**: Raw CSV -> `events` -> `events_normalized` (Persistent).
2.  **Scoping**: `events_normalized` -> `events_scoped` (**Dynamic VIEW**).
3.  **Consistency**: Any definition change to `events_scoped` **MUST** trigger a full re-materialization of `cohort_membership` and `cohort_activity_snapshot`.
4.  **Multi-User Isolation**: Every database operation is performed within a user-specific DuckDB instance, managed by a thread-safe `run_query` wrapper.

## Analytics Source Rules

To ensure performance and correctness, modules follow strict data source patterns:

| Module | Primary Source | Secondary / Join Source | Role |
| :--- | :--- | :--- | :--- |
| **Retention** | `snapshot` | `scoped` | Joins `scoped` only for per-step property filters. |
| **Monetization** | `scoped` | `membership` | Joins `membership` to align events with cohort `join_time`. |
| **Usage** | `scoped` | `membership` | Joins `membership` for alignment and `join_time` filtering. |
| **Paths** | `snapshot` | `scoped` | Uses `snapshot` as stream; joins `scoped` for filtered steps. |
| **Flows** | `snapshot` | `scoped` | `snapshot` for transitions; `EXISTS` check on `scoped` for props. |

## Performance & Persistence
- **DuckDB**: Optimized for OLAP workloads, providing columnar storage and fast aggregations.
- **Materialization**: Heavy joins are computed once during cohort creation to allow fluid interactive exploration.
- **Single-Worker (Windows)**: Enforces `DUCKDB_SINGLE_WORKER=true` to maintain stability in multi-threaded environments.

## Testing
Comprehensive testing is enforced in `backend/tests/`, covering:
- **Domain Logic**: Unit tests for matching algorithms and SQL generators.
- **API Contracts**: Integration tests for all router endpoints.
- **Data Integrity**: Verification of materialization cascades and revenue recomputation.
