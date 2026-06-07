# Revenue System

## Revenue configuration table
`revenue_event_selection`:
- `event_name` (primary key)
- `is_included` (boolean)
- `override_revenue` (double, nullable)

## Metadata table
`dataset_metadata` is initialized as a helper placeholder table `(id INTEGER)`. The value of `has_revenue_mapping` is determined dynamically at query-time based on whether the `events_normalized` table exists.

## Initialization behavior
When mapping includes a revenue column:
- system initializes `revenue_event_selection` from events with non-zero total revenue,
- defaults each listed event to included with no override.

## Update API
`POST /update-revenue-config`
- Accepts either:
  - `revenue_config` object keyed by event name, or
  - `events` array payload
- Upserts rows into `revenue_event_selection`
- Recomputes modified revenue columns in normalized and scoped tables
- Refreshes scope totals

## Recompute rules
For each event row in `events_normalized` / `events_scoped`:
- Excluded or missing config => `modified_revenue = 0`
- Included + override => `modified_revenue = event_count * override_revenue`
- Included + no override => `modified_revenue = original_revenue`

## Related endpoints
- `GET /revenue-config-events`
- `GET /revenue-events`
- `POST /update-revenue-config`
- `GET /monetization`
