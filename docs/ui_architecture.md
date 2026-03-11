# UI Architecture

## Layout system

The application uses a three-region workspace layout:

- **Top bar** for dataset actions (upload/remap/open sections).
- **Left pane** for Filters, Analytics Settings, and Cohorts.
- **Analytics area** for Retention, Usage, and Monetization views.

Core reusable layout classes:

- `ui-section`: vertical section grouping with consistent spacing.
- `ui-card`: card container for subsection blocks.
- `ui-panel`: panel shell for embedded sidebar/pane experiences.
- `ui-tabs`: OneNote-style attached tabs for analytics views.

## Component hierarchy

- `App.jsx`
  - workspace chrome and tab switching
  - left pane section expand/collapse state
  - global settings state (`maxDay`, retention event)
- `RetentionTable.jsx`
  - retention controls, table/graph toggle
- `UsageTable.jsx`
  - usage event controls and two data tables
- `MonetizationTable.jsx`
  - monetization controls, table/graph, sticky prediction summary
  - temporary inline tuning pane via `TunePredictionPane.jsx`

## Sidebar design

- Left pane keeps clear hierarchy:
  - section headers with icon + toggle
  - hints under each section title
  - cardized content for settings and forms
- Collapse button behavior:
  - expanded: right aligned
  - collapsed: centered
  - control remains compact (not full width)

## Monetization tuning layout

The monetization view supports an ephemeral right-side tuning panel:

- Base layout: `| monetization content | sticky prediction summary |`
- Open tuning: `| monetization content | sticky prediction summary | tuning panel |`
- Cancel or update prediction closes panel and restores base layout.
- Tuning state is not persisted across sessions.
