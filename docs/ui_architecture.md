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
  - Filters (Date range and property filters)
  - Analytics Settings (Retention event, global max day, revenue configuration)
  - Cohorts (Saved cohorts dropdown, Add Cohort, New Cohort modal, and list of materialized dataset cohorts)
- Main area tabs:
  - Retention
  - Usage
  - Monetization
  - Paths (Sequence Analysis)
  - Flows
  - User Explorer

## Data dependencies
Frontend API wrapper: `frontend/src/api.js`.

Core fetch flow in workspace:
- Refresh scope + retention metadata for dataset counters
- Load event list for selectors
- Load tab-specific analytics data

## Persistence
Workspace state is persisted in `localStorage` under `cohort-analysis-workspace-v2`.
Persisted values include app state, mapping context, active tab, settings, and pane section visibility.

## Monetization UI
Monetization tab includes:
- Metric selector
- Prediction controls (if enabled)
- Event inclusion/override configuration interface

## Paths (Sequence Analysis) UI
Paths tab includes:
- Multi-step sequence builder with per-step filters
- Deterministic greedy matching visualization
- Drop-off and Reached cohort creation actions

## Flow analytics UI
Flows tab includes:
- Sankey diagrams for event transitions
- L1/L2 expansion controls
- Forward/Reverse direction toggle
- Property filtering for flow anchoring

## User Explorer UI
User Explorer tab includes:
- User search by ID
- Detailed event timeline with properties
- Event name filtering within timeline
- Extraction of stable user properties
