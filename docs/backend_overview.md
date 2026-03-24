# Backend Overview

The backend is a FastAPI application that provides a robust engine for cohort analysis, analytics, and data management using DuckDB as the primary analytical database.

## Architecture & Modules

The backend is structured into several key domains, each responsible for a specific part of the system:

- **Ingestion (`domains/ingestion`)**: Handles CSV file uploads and provides initial schema detection and mapping suggestions.
- **Normalization (`domains/ingestion`)**: Transforms raw uploaded data into a canonical format (`events_normalized`), handling column casting and aggregation of duplicate event rows.
- **Scope (`domains/scope`)**: Manages the "active" subset of data. It builds `events_scoped` based on user-defined filters (date ranges, property filters).
- **Cohort Engine (`domains/cohorts`)**: The core of the system. It handles cohort definition, membership materialization, and activity snapshots.
- **Analytics (`domains/analytics`)**: Implements various analytical models:
    - **Retention**: Periodic active user analysis.
    - **Usage**: Volume and user activity for specific events.
    - **Monetization**: Revenue analysis based on included events and optional overrides.
    - **Funnels**: Multi-step conversion tracking.
    - **Flows**: Event-to-event transition analysis.
- **Revenue System (`domains/revenue`)**: Manages configuration for monetization analytics, including event inclusion and value overrides.

## Data Pipeline

1.  **Upload**: Raw CSV -> `events` table (DuckDB).
2.  **Map Columns**: `events` -> `events_normalized`. This step also initializes the "All Users" cohort and clears any previous state.
3.  **Apply Filters**: `events_normalized` -> `events_scoped`. This triggers a rebuild of cohort memberships and snapshots for the active scope.
4.  **Cohort Materialization**: Users satisfying cohort conditions are registered in `cohort_membership`. Their related events within the scope are captured in `cohort_activity_snapshot`.
5.  **Analytics Execution**: Analytics endpoints query `events_scoped` and `cohort_activity_snapshot` to produce results.

## Database Schema

Refer to [data_model.md](data_model.md) for a detailed breakdown of tables and relationships.

## Implementation Details

- **Database**: DuckDB is used for its exceptional performance on analytical queries and local storage capabilities.
- **Concurrency**: Fast API with standard synchronous and asynchronous handling.
- **Logic**: SQL-heavy implementation for performance, with Python-side aggregation and post-processing where necessary.
