# UI Component Map

## Workspace shell

- `frontend/src/App.jsx`
  - Application state orchestration.
  - Left pane structure and section controls.
  - Analytics tab selection and view composition.

## Sidebar and configuration

- `frontend/src/components/FilterData.jsx`
  - Dataset filter builder and application.
- `frontend/src/components/RevenueConfig.jsx`
  - Revenue event inclusion/override controls.
- `frontend/src/components/CohortForm.jsx`
  - Cohort create/edit flows, condition builder, cohort management.

## Analytics views

- `frontend/src/components/RetentionTable.jsx`
  - Retention controls and table/graph output.
- `frontend/src/components/UsageTable.jsx`
  - Usage metric controls and dual table outputs.
- `frontend/src/components/MonetizationTable.jsx`
  - Monetization controls, prediction flow, sticky prediction summary, tuning panel host.
- `frontend/src/components/TunePredictionPane.jsx`
  - Temporary right-side tuning panel for projection parameters.

## Shared primitives

- `frontend/src/components/SearchableSelect.jsx`
  - Search-enabled select used across forms and filters.
- `frontend/src/styles.css`
  - Global layout, card, tabs, sidebar, form, button, and table styling.
- `frontend/src/styles/tokens.css`
  - Theme tokens (colors, spacing, typography, radii, shadow).
