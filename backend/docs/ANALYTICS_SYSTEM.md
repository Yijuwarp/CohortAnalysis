# Analytics System

This document describes the analytical models and data source rules implemented in the backend.

## Data Source Rules

To ensure consistency and performance, all analytics must adhere to the following source rules:

| Module | Base Dataset | Filtering / Metadata Layer | Notes |
| :--- | :--- | :--- | :--- |
| **Retention** | `cohort_activity_snapshot` | `events_scoped` | Only join for filter verification. |
| **Flows** | `cohort_activity_snapshot` | `events_scoped` | Snapshot is the transition source. |
| **Paths** | `cohort_activity_snapshot` | `events_scoped` | **Snapshot** is the mandatory base. Join **Scoped** for per-step filters. |
| **Monetization** | `cohort_activity_snapshot` | `events_scoped` | Join for `modified_revenue` + `revenue_event_selection`. |
| **Usage / Freq** | `events_scoped` | `cohort_membership` | **Scoped** as base, aligned with **Membership** using `join_time`. |

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
- `retention_type` (classic | ever-after)

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
- `retention_event` (optional)

**Logic**:
- Uses `events_scoped` directly to support property-level filtering.
- Activity is aligned with `cohort_membership` using `join_time` offsets.
- Provides `usage_volume_table`, `usage_users_table`, and `retained_users_table`.

---

## Monetization (`GET /monetization`)
Analyzes revenue generation and user value.

**Query Params**:
- `max_day` (default 7)

**Logic**:
- Uses `cohort_activity_snapshot` joined with `events_scoped` to access `modified_revenue`.
- Includes only events marked `is_included = TRUE` in `revenue_event_selection`.
- Respects cohort `join_time` offsets and filters.

---

## Sequence Analysis (Paths) (`POST /paths/run`)
Triggers multi-step conversion and drop-off analysis across active cohorts.

**Logic: Earliest Greedy Matching**
- **Sequential**: Matching starts from step 1 and proceeds sequentially to step $N$.
- **Greedy**: For each step, the system finds the **earliest** valid event occurrence after the previous step's match.
- **Deterministic**: Ties in timestamps are broken using internal row identifiers (`rn`).
- **Constraints**: Each step $N$ must satisfy $t_N > t_{N-1}$ (or higher row ID if $t_N = t_{N-1}$).

**Source**:
- Uses `cohort_activity_snapshot` as the mandatory base event stream.
- Joins `events_scoped` ONLY when per-step property filters are present.

**Drop-off Calculation**:
- Users who matched step $N$ but did not match step $N+1$ (calculated via ANTI JOIN logic).

---

## Flow Analytics (`GET /flow/l1`, `GET /flow/l2`)
Sankey-style event transition analysis.

**Logic**:
- **Event-anchored**: Flows are anchored to the **first occurrence** of `start_event` per user within the cohort.
- **User-based percentages**: Counts users following a path, not total events.
- **Top-K Grouping**: Top-3 events are named; the rest are collapsed into an "Other" row.
- **Source**: Uses `cohort_activity_snapshot` exclusively.

---

## User Explorer (`GET /user-explorer`)
Deep-dive into individual user activity.

**Logic**:
- **Timeline View**: Chronological list of all events for a specific user within the active scope.
- **Source**: Queries `events_scoped` directly to provide full metadata for each event.

---

## Statistical Testing (Comparison)
- **Primary Test**: Mann-Whitney U (non-parametric).
- **Rationale**: Analytics data (especially revenue) is often skewed; Mann-Whitney is robust to outliers and non-normal distributions.
- **Edge cases**: If variance is zero for either cohort, $p\_value$ returns `null`.
