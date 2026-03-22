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

## Flow Analytics

### Endpoints

#### `GET /flow/l1`
Query params:
- `start_event` (required) – the anchor event to compute flows from/to
- `direction` – `forward` (default) or `reverse`

Response:
```json
{
  "rows": [
    {
      "path": ["start_event", "next_event"],
      "values": {
        "<cohort_id>": { "count": 42, "pct": 0.42 }
      },
      "expandable": true
    }
  ]
}
```

#### `GET /flow/l2`
Query params:
- `start_event` (required) – the original anchor event
- `parent_event` (required) – the L1 step to expand (i.e., the event clicked at L1)
- `direction` – `forward` (default) or `reverse`

Response:
```json
{
  "parent_path": ["start_event", "parent_event"],
  "rows": [
    {
      "path": ["start_event", "parent_event", "next_event"],
      "values": {
        "<cohort_id>": { "count": 22, "pct": 0.22 }
      }
    }
  ]
}
```

### Behavior

- **Event-anchored flow analysis**: All flows are anchored to the first occurrence of `start_event` per user per cohort.
- **First-occurrence-per-user**: Only the earliest `start_event` per user is used as the anchor — repeated occurrences are ignored.
- **User-based percentages**: `pct = users_following_path / users_who_performed_start_event` (per cohort).
- **Top-3 + Other**: Only the top-3 events by user count are returned as named rows. The rest are collapsed into a single "Other" row.
- **"Other" row**: Computed in Python (not SQL). Never expandable. Only included when count > 0.
- **Self-loop exclusion**: Transitions from an event to itself are excluded.
- **Forward direction**: `start_event → next_event → second_event`
- **Reverse direction**: `second_event → prev_event → start_event` (looks backward in time)
- **Expandable flag**: `true` for named top-3 rows (potential L2 expansion), always `false` for "Other".
- **Sorting**: Rows sorted by pct of the first visible cohort (descending), secondary sort by count.

### Computation Notes

- Uses `cohort_activity_snapshot (cohort_id, user_id, event_time, event_name)`.
- All non-hidden active cohorts are included; results returned in a single response.
- L2 denominator = users who performed `start_event` (same as L1).
- L2 computation is lazy — only triggered by an explicit `GET /flow/l2` request.
- Window functions (`ROW_NUMBER`) are used instead of `DISTINCT` for correctness on multi-event users.
- All returned numeric types are Python-native `int` / `float` (no numpy).

## Statistical Testing

Primary test: Mann-Whitney U (non-parametric)
Secondary test: Welch’s t-test (diagnostic only)

Rationale:
- Monetization data is skewed
- Mann-Whitney is robust to outliers and non-normal distributions

### Edge Case Behavior

If both cohorts have zero variance:
→ p_value = null
→ comparison not shown as significant
