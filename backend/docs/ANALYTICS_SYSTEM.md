# Analytics System

This document describes the analytical models and data source rules implemented in the backend.

## Data Source Rules

To ensure consistency and performance, all analytics must adhere to the following source rules:

| Module | Base Dataset | Filtering / Metadata Layer | Notes |
| :--- | :--- | :--- | :--- |
| **Retention** | `cohort_activity_snapshot` | `events_scoped` | Only join `scoped` for property-level filtering. |
| **Flows** | `cohort_activity_snapshot` | `events_scoped` | `snapshot` is the transition source; `scoped` for properties. |
| **Paths** | `cohort_activity_snapshot` | `events_scoped` | Uses `snapshot` by default. Falls back to `scoped` for property filters. |
| **Monetization** | `events_scoped` | `cohort_membership` | Mandatory join to align events with cohort `join_time` and access `modified_revenue`. |
| **Usage / Freq** | `events_scoped` | `cohort_membership` | `scoped` as base, aligned with `membership` using `join_time`. |

### Allowed vs Forbidden Joins

*   **ALLOWED**: `snapshot` ↔ `scoped` (filters), `snapshot` ↔ `membership`.
*   **FORBIDDEN**: `normalized` ↔ `analytics`, `raw events` ↔ `analytics`.

---

## Retention (`GET /retention`)
Computes periodic active user counts per cohort.

**Query Params**:
- `max_day` (default 7)
- `retention_event` (optional; `any` behavior when omitted)
- `include_ci` (default false)
- `confidence` (default 0.95; confidence level for Wilson score interval)
- `retention_type` (classic | ever-after, default "classic")
- `granularity` (day | hour, default "day")

**Logic**:
- **Classic**: User is active if they perform an event ON the specific day/interval relative to their `join_time`.
- **Ever-After**: User is active if they perform an event ON or AFTER the specific day/interval.
- Uses `cohort_activity_snapshot` for high performance.
- Confidence intervals use the **Wilson score interval**.

---

## Usage (`GET /usage`)
Analyzes event volume and unique user activity.

**Query Params**:
- `event` (required)
- `max_day` (default 7)
- `retention_event` (optional; custom retention event to align denominator)
- `property` (optional; custom event property to filter by)
- `operator` (default "="; comparison operator for property filter)
- `value` (optional; value to filter by)

**Logic**:
- Uses `events_scoped` directly to support property-level filtering.
- Activity is aligned with `cohort_membership` using `join_time` offsets.
- Provides `usage_volume_table`, `usage_users_table`, `usage_adoption_table`, and `retained_users_table`.

---

## Monetization (`GET /monetization`)
Analyzes revenue generation and user value.

**Query Params**:
- `max_day` (default 7)

**Logic**:
- Uses `events_scoped` directly (joined with `membership`).
- Accesses `modified_revenue` fields which reflect value overrides.
- Includes only events marked `is_included = TRUE` in `revenue_event_selection`.
- Respects cohort `join_time` offsets and filters.
- Uses `cohort_activity_snapshot` (via retention vectors) only for computing denominators (active users).

---

## Sequence Analysis (Paths) (`POST /paths/run`)
Triggers multi-step conversion and drop-off analysis across active cohorts.

**Logic: Earliest Greedy Matching**
- **Sequential**: Matching starts from step 1 and proceeds sequentially to step $N$.
- **Greedy**: For each step, the system finds the **earliest** valid event occurrence after the previous step's match.
- **Deterministic**: Ties in timestamps are broken using internal row identifiers (if present) or event name/time attributes.
- **Constraints**: Each step $N$ must satisfy $t_N > t_{N-1}$ (or higher row ID/rank if $t_N = t_{N-1}$).

**Source**:
- Uses `cohort_activity_snapshot` as the base event stream when steps have no filters.
- Joins `events_scoped` for steps that require per-event property filtering.

---

## Flow Analytics (`GET /flow/l1`, `GET /flow/l2`)
Sankey-style event transition analysis.

**Logic**:
- **Event-anchored**: Flows are anchored to the **first occurrence** of `start_event` per user within the cohort.
- **User-based percentages**: Counts users following a path, not total events.
- **Top-K Grouping**: Top-3 events are named; the rest are collapsed into an "Other" row.
- **Source**: Uses `cohort_activity_snapshot` for transitions. Joins `events_scoped` via `EXISTS` clause for property filters.

---

## User Explorer (`GET /user-explorer`)
Deep-dive into individual user activity.

**Logic**:
- **Timeline View**: Chronological list of all events for a specific user within the active scope.
- **Source**: Queries `events_scoped` directly to provide full metadata for each event.

---

## Statistical Testing (Comparison) (`POST /compare-cohorts`)
Evaluates statistical significance between Cohort A and Cohort B for a specific day and metric.

**Request Payload**:
- `cohort_a` (int; required)
- `cohort_b` (int; required)
- `tab` ("retention" | "usage" | "monetization")
- `metric` (specific metric name depending on tab)
- `day` (target offset day)
- `max_day` (optional max day constraint)
- `event` (optional target event)
- `granularity` ("day" | "hour", default "day")
- `retention_type` ("classic" | "ever_after")
- `property`, `operator`, `value` (optional property filters for usage metric)

**Logic**:
- **Continuous Metrics** (e.g., events per user, revenue per user):
  - **Welch's t-test**: Performs parametric two-sample t-test for unequal variances.
  - **Mann-Whitney U Test (Primary)**: Non-parametric test robust to skewed analytics and revenue distributions.
  - **Edge cases**: If variance is zero for both cohorts, the p-value returns `null`.
- **Proportion Metrics** (e.g., retention rate, unique user percentage):
  - **Two-Proportion z-test**: Standard parametric test for proportion differences.
  - **Fisher's Exact Test**: Non-parametric test computed for smaller combined cohorts (total size $\le 5000$).

---

## Impact Analysis (`POST /impact/run`, `POST /impact/stats`)
Measures conversion, retention, and monetization differences between a baseline cohort and variant cohorts following exposure and interaction events.

**Logic**:
- **Exposure**: Filters for users performing `exposure_events` (e.g. feature flags, A/B variant assignment events).
- **Interaction**: Captures subsequent `interaction_events` indicating user interaction with the variant feature.
- **Outcomes**: Computes retention, monetization, and custom impact metrics relative to the exposure timeline.
- **Stats**: Evaluates significance on outcome metrics lazily via cached run data.
