# Architecture

## System overview
The application is a single FastAPI service (`backend/app/main.py`) plus a React SPA (`frontend/src`).

Backend persistence uses one DuckDB file (`backend/cohort_analysis.duckdb`). Request handlers in `app/routers/*` delegate to functions in `app/domains/legacy_api.py`.

## Data pipeline
`events` (raw CSV) -> `events_normalized` (canonical + aggregated) -> `events_scoped` (active filter scope) -> cohort membership/snapshot tables -> analytics responses.

## Backend responsibilities
- Ingestion: upload CSV and map source columns
- Normalization: parse/cast columns and aggregate duplicate event rows
- Scope: rebuild `events_scoped` and persist scope metadata
- Cohorts: CRUD + membership materialization
- Analytics: retention, usage, monetization
- Revenue config: include/exclude events and optional override value

## Frontend responsibilities
- Upload + mapping wizard
- Left pane for Filters, Analytics Settings, Cohorts
- Analytics tabs: Retention, Usage, Monetization
- Monetization includes projection controls and tune pane
- Persist basic workspace state in `localStorage`

## Database tables

### Core event tables
1. `events`
   - Raw uploaded CSV table with source columns.

2. `events_normalized`
   - Canonical fields:
     - `user_id`
     - `event_name`
     - `event_time`
     - `original_event_count`
     - `original_revenue`
     - `modified_event_count`
     - `modified_revenue`
   - Includes unmapped source columns (cast to selected types).
   - Built via `GROUP BY` across canonical + metadata columns.

3. `events_scoped`
   - Rebuilt from `events_normalized` after scope filters.
   - Same column shape as `events_normalized`.

### Cohort tables
4. `cohorts`
   - `cohort_id`, `name`, `logic_operator`, `join_type`, `is_active`, `hidden`
   - Split metadata: `split_parent_cohort_id`, `split_group_index`, `split_group_total`

5. `cohort_conditions`
   - One row per condition: `event_name`, `min_event_count`
   - Optional property filter fields: `property_column`, `property_operator`, `property_values` (JSON text)

6. `cohort_membership`
   - Materialized members: `user_id`, `cohort_id`, `join_time`

7. `cohort_activity_snapshot`
   - Event snapshot for members: `cohort_id`, `user_id`, `event_time`, `event_name`

### Scope and metadata tables
8. `dataset_scope`
   - Singleton metadata row (`id=1`): filters JSON, row counts, total scoped events, updated timestamp

9. `dataset_metadata`
   - Singleton metadata row (`id=1`) with `has_revenue_mapping`

10. `revenue_event_selection`
   - Event-level monetization config:
     - `event_name` (PK)
     - `is_included`
     - `override_value`

## Cohort logic
- Each condition computes per-user cumulative event count (`SUM(original_event_count)` ordered by event time/name).
- User satisfies condition when cumulative count reaches `min_event_count`.
- `AND` combines users present in all condition CTEs.
- `OR` unions all condition CTEs.
- Join time:
  - `condition_met`: first qualifying condition time
  - `first_event`: user’s earliest event in source table
- Membership and snapshot rebuild after scope updates.

## Analytics logic
- Retention: active users by day offset from `join_time`, optionally filtered to one `retention_event`, optional Wilson CI.
- Usage: per-cohort/day total event volume and distinct users for selected event.
- Monetization: per-cohort/day sums of `modified_revenue` and `modified_event_count` for included revenue events.

## Runtime defaults and limits
- `max_day` default: 7 (retention, usage, monetization)
- Cohort condition limit: 5
- `column-values` sample limit: 100
- CORS is open (`*`)
