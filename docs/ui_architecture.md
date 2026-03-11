# UI Architecture

## App structure
The SPA entrypoint is `frontend/src/App.jsx`.

State transitions:
- `empty` -> upload screen
- `mapping` -> column mapping screen
- `workspace` -> analytics workspace

## Workspace layout
- Top bar: dataset summary + navigation actions
- Left pane (collapsible):
  - Filters
  - Analytics Settings
  - Cohorts
- Main area tabs:
  - Retention
  - Usage
  - Monetization

## Data dependencies
Frontend API wrapper: `frontend/src/api.js`.

Core fetch flow in workspace:
- Refresh scope + retention metadata for dataset counters
- Load event list for selectors
- Load tab-specific analytics data

## Persistence
Workspace state is persisted in `localStorage` under `cohort-analysis-workspace-v2`.
Persisted values include app state, mapping context, active tab, settings, and pane section visibility.

## Monetization prediction UI
Monetization tab includes:
- Metric selector
- Prediction horizon selector (30/60/90/180/365 days)
- Projection trigger
- Optional tuning pane for per-cohort power-law params (A/B)
