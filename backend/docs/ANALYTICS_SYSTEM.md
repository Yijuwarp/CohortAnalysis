# Analytics System

## Retention (`GET /retention`)
Query params:
- `max_day` (default 7)
- `retention_event` (optional; `any` behavior when omitted)
- `include_ci` (default false)
- `confidence` (0.90, 0.95, or 0.99)

Response:
- `max_day`
- `retention_event`
- `retention_table[]` with:
  - `cohort_id`, `cohort_name`, `size`
  - `retention` object keyed by day string
  - optional `retention_ci` object keyed by day string

Computation notes:
- Day number = date diff between `join_time` and activity date.
- Active users are distinct users per cohort/day.
- Hidden cohorts are excluded.
- CI uses Wilson score interval.

## Usage (`GET /usage`)
Query params:
- `event` (required)
- `max_day` (default 7)
- `retention_event` (optional)

Response:
- `usage_volume_table[]` (total event counts by cohort/day)
- `usage_users_table[]` (distinct users by cohort/day)
- `retained_users_table[]` (retention denominator context)

Computation notes:
- Uses `events_scoped` and cohort membership join-time offsets.
- Returns empty tables if scoped data/cohorts/event are unavailable.

## Monetization (`GET /monetization`)
Query params:
- `max_day` (default 7; invalid/non-positive values are normalized to 7 by router)

Response:
- `revenue_table[]`: per cohort/day `revenue` and `event_count`
- `cohort_sizes[]`
- `retained_users_table[]`

Computation notes:
- Includes only events marked `is_included = TRUE` in `revenue_event_selection`.
- Uses `modified_revenue` and `modified_event_count` from `events_scoped`.
