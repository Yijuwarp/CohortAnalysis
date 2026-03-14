# Cohort Engine

## Cohort schema
- `cohorts`: metadata (`name`, `logic_operator`, `join_type`, active/hidden flags, split metadata)
- `cohort_conditions`: condition rows per cohort
- `cohort_membership`: materialized membership (`user_id`, `cohort_id`, `join_time`)
- `cohort_activity_snapshot`: member event snapshot (`cohort_id`, `user_id`, `event_time`, `event_name`)

## Cohort request model constraints
- `conditions`: max 5
- `min_event_count >= 1`
- `logic_operator`: `AND` or `OR`
- `join_type`: `condition_met` or `first_event`

## Condition evaluation
For each condition, engine builds a CTE that:
1. filters by `event_name` (+ optional property filter),
2. computes cumulative `SUM(event_count)` per user ordered by event time/name,
3. keeps users when cumulative count reaches `min_event_count`.

Combination:
- `AND`: intersect user sets across condition CTEs.
- `OR`: union user sets across condition CTEs.

Join time:
- `condition_met`: earliest matched condition time.
- `first_event`: overwritten to user’s first event time in source table.

## Source table selection
- Membership build can use `events_normalized` or `events_scoped`.
- Global rebuild after scope changes uses `events_scoped`.

## Activity snapshot
After membership build, all matching events for cohort members are inserted into `cohort_activity_snapshot`.
This snapshot is used by retention and monetization-related analytics joins.

## Additional cohort operations
- Hide/unhide (`PATCH /cohorts/{id}/hide`)
- Random split (`POST /cohorts/{id}/random_split`) into two child cohorts
