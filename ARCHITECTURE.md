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
3.  **Scoping**: `events_scoped` -> subset of normalized data based on active filters and date range.
4.  **Cohort Materialization**: `cohort_membership` -> users satisfying specific logic.
5.  **Activity Snapshots**: `cohort_activity_snapshot` -> scoped events for cohort members.
6.  **Analytics Execution**: Final responses generated from scoped data and snapshots.

## Backend Responsibilities
- **Ingestion**: Column mapping and type inference.
- **Normalization**: Grouping by canonical and metadata columns to reduce row count.
- **Filtering**: Rebuilding `events_scoped` and refreshing dependent cohort memberships.
- **Cohort Engine**: Evaluating frequency-based conditions with property filters.
- **Analytics Models**: Retention, usage, monetization, funnels, and flows.
- **Revenue System**: Managing inclusion and value overrides for monetization.

## Frontend Responsibilities
- **Workspace Management**: Coordinate state between mapping, filtering, and analytics.
- **Interactive Analytics**: Tables and visualizations for various analytical lenses.
- **Cohort Builder**: Interface for defining complex cohort logic.
- **User Detail**: In-depth timeline exploration for individual users.

## Database Tables
Refer to [data_model.md](docs/data_model.md) for detailed table schemas.

## Cohort Logic
- Conditions use cumulative `SUM` of event counts.
- Users satisfy conditions when they reach `min_event_count`.
- Support for `AND`/`OR` logic operators.
- Join types: `condition_met` (trigger time) or `first_event` (global earliest).
- Memberships are materialized to optimize analytics query performance.

## Analytics Systems

### Retention
Dynamic retention analysis with optional confidence intervals (Wilson score) and multiple algorithms (Classic, 1-Day, etc.).

### Monetization
Revenue analysis based on `modified_revenue` fields, allowing for event-level overrides and inclusion toggles.

### Funnels
Greedy earliest-path matching for multi-step conversion tracking. Supports conversion windows from minutes to lifetime.

### Flows
Sankey-style event transition analysis with expansion capabilities (L1/L2) and top-K event grouping.

## Persistence
- Analytical data is stored in `backend/cohort_analysis.duckdb`.
- Workspace UI state (collapsed panes, active tabs, etc.) is persisted in `localStorage`.
