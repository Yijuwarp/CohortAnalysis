# Design System Notes

This project uses lightweight, component-scoped styling via `frontend/src/styles.css` and token definitions in `frontend/src/styles/tokens.css`.

## Principles
- Analytics-first dense tables
- Clear control grouping (filters/settings/cohorts)
- Collapsible navigation to preserve chart/table space
- Reusable card + button patterns

## Common patterns
- Cards for functional sections (`card`, `ui-card`)
- Segmented/tab controls for analytics mode switching
- Sticky cohort/size columns for wide day-based tables
- Inline validation and error text for numeric/config inputs

## Accessibility and behavior
- Controls use native form elements where possible
- Searchable select pattern is used for long option lists (events/columns)
- Loading and empty states are explicitly rendered in analytics components
