# Backend Overview

The backend is a FastAPI application that provides a robust engine for cohort analysis, analytics, and data management using DuckDB as the primary analytical database.

## Architecture & Modules

The backend is structured into domain-driven modules, each responsible for a specific part of the system:

- **Ingestion (`domains/ingestion`)**: Handles CSV file uploads, schema detection, and mapping suggestions.
- **Normalization (`domains/ingestion`)**: Transforms raw data into `events_normalized`, handling column casting and row-level grouping.
- **Scope (`domains/scope`)**: Manages the "Active" subset of data. Rebuilds `events_scoped` based on user-defined filters (date ranges, property filters).
- **Cohort Engine (`domains/cohorts`)**: Handles cohort definition, membership materialization, and activity snapshots.
- **Analytics (`domains/analytics`)**: Implements various analytical models:
    - **Retention**: Periodic active user analysis.
    - **Usage**: Volume and user activity per cohort.
    - **Monetization**: Revenue analysis using inclusion toggles and value overrides.
    - **Flows**: Event-to-event transition analysis (Sankey).
- **Paths (`domains/paths`)**: Implements Sequence Analysis with deterministic greedy matching and property-level filtering.
- **Revenue System (`domains/revenue`)**: Manages monetization configuration.

## Data Pipeline Invariants

1.  **Normalization**: Raw CSV -> `events` -> `events_normalized`.
2.  **Scoping**: `events_normalized` -> `events_scoped`. Rebuilt on every filter change.
3.  **Consistency**: Any change to `events_scoped` **MUST** trigger a full re-materialization of `cohort_membership` and `cohort_activity_snapshot`.
4.  **Analytics Source Rules**:
    - **Retention, Flows, and Paths (Base)**: MUST use `cohort_activity_snapshot`.
    - **Paths (Filtering)**: MUST use `cohort_activity_snapshot` as base, joining `events_scoped` for per-step property filters.
    - **Monetization**: `cohort_activity_snapshot` JOIN `events_scoped` for `modified_revenue` attributes.
    - **Usage**: `events_scoped` aligned with `cohort_membership` using `join_time`.
    - **Forbidden**: Direct joins on `events_normalized` or raw `events` for analytics.

## Recomputation Cost

Rebuilding the scoped dataset and re-materializing cohort data is a heavy operation. Any scope change triggers a cascading rebuild of memberships and snapshots to ensure the system remains internally consistent. This is a deliberate trade-off to ensure sub-second performance during interactive analysis.

## Database Schema

Refer to [data_model.md](data_model.md) for a detailed breakdown of tables and relationships.

## Development & Testing

- **Database**: DuckDB is used for its exceptional performance on analytical queries and local storage capabilities.
- **Testing**: Extensive test suite in `backend/tests/` verifies API contracts, matching logic (Paths, Scope, Cohorts), and data integrity.
