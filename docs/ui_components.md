# UI Components

## Core pages / containers
- `App.jsx`: orchestrates upload -> mapping -> workspace flow.
- `Upload.jsx`: CSV input and upload handling.
- `Mapping.jsx`: canonical field mapping and type override UI.

## Workspace panels
- `FilterData.jsx`
  - Applies date range and property filters via `/apply-filters`
  - Uses `/columns`, `/column-values`, `/date-range`, `/scope`

- `RevenueConfig.jsx`
  - Loads and updates revenue event inclusion/override config
  - Uses `/revenue-config-events` and `/update-revenue-config`

- `CohortForm.jsx`
  - Cohort create/edit/delete/hide/split UI
  - Uses `/cohorts`, `/cohorts/{id}`, `/cohorts/{id}/hide`, `/cohorts/{id}/random_split`
  - Supports condition-level property filters with type-aware operators

## Analytics views
- `RetentionTable.jsx` + `RetentionGraph.jsx`
  - Uses `/retention`
  - Supports table/graph modes and optional confidence interval rendering

- `UsageTable.jsx`
  - Uses `/usage`
  - Requires selected event and retention-event context

- `MonetizationTable.jsx` + `MonetizationGraph.jsx`
  - Uses `/monetization`
  - Supports multiple monetization metrics and projection features
  - Integrates `TunePredictionPane.jsx` for interactive model tuning

## Shared helpers
- `SearchableSelect.jsx`: reusable searchable dropdown.
- `frontend/src/utils/*`: date, formatting, cohort colors, prediction helpers.
