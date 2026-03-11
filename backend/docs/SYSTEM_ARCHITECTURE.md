# Backend System Architecture

## Actual runtime composition
- FastAPI app in `app/main.py`
- Routers in `app/routers/*` exposing HTTP contracts
- Core business logic currently centralized in `app/domains/legacy_api.py`
- DuckDB file persistence at `backend/cohort_analysis.duckdb`

## Initialization behavior
`app/main.py` registers these routers:
- upload
- mapping
- filters/scope
- cohorts
- analytics
- revenue
- metadata

The app also re-exports some retention helpers for compatibility.

## Data lifecycle
1. Upload CSV into `events`.
2. Normalize mapped columns into `events_normalized`.
3. Initialize/rebuild `events_scoped` from normalized table.
4. Recompute cohort memberships + activity snapshot.
5. Serve analytics from scoped + cohort tables.

## Data tables
- `events`
- `events_normalized`
- `events_scoped`
- `cohorts`
- `cohort_conditions`
- `cohort_membership`
- `cohort_activity_snapshot`
- `dataset_scope`
- `dataset_metadata`
- `revenue_event_selection`

## Important implementation notes
- `events_normalized` and `events_scoped` both carry original + modified monetization columns.
- Scope and revenue updates trigger recomputation paths so analytics use current settings.
- Hidden cohorts are excluded from analytics outputs.
