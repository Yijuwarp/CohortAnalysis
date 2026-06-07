# Agent Operating Manual (Backend)

## Current reality
Backend logic is structured into domain-specific modules inside `app/domains/*` (e.g., `cohort_service.py`, `paths_service.py`, `retention_service.py`), with FastAPI routers in `app/routers/*` invoking functions from these domain services.

## What to preserve
- Endpoint paths and request/response contracts
- Table names and core data flow:
  - `events` -> `events_normalized` -> `events_scoped`
  - cohort tables (`cohorts`, `cohort_conditions`, `cohort_membership`, `cohort_activity_snapshot`)
  - scope/revenue metadata tables

## Change boundaries for documentation tasks
- Update Markdown docs freely.
- Do not change backend Python behavior unless explicitly requested.

## Validation checklist for backend docs
When documenting behavior, verify against code for:
- Router endpoint list (`app/routers/*`)
- Pydantic request models (defined directly in `app/models/*`)
- Table creation/migration helpers (`ensure_*` functions)
- Analytics SQL in retention/usage/monetization functions
