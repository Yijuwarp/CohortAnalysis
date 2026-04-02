# Cohort Engine

The Cohort Engine is responsible for defining, materializing, and maintaining user cohorts based on event frequency and property filters.

## Core Schema

- **`cohorts`**: Metadata (name, logic_operator, join_type, active/hidden flags, split metadata).
- **`cohort_conditions`**: Individual condition rows per cohort.
- **`cohort_membership`**: Materialized list of users belonging to each cohort and their calculated `join_time`.
- **`cohort_activity_snapshot`**: Materialized stream of events for cohort members within the active scope.

---

## Critical Invariants

> [!IMPORTANT]
> **Same-Source Consistency**: `cohort_membership` and `cohort_activity_snapshot` **MUST** always be derived from the SAME version of `events_scoped`. Any mismatch leads to critical data integrity issues like duplicated events, incorrect retention, or inflated metrics.

> [!CAUTION]
> **Materialization Trigger**: Rebuilding or applying filters to `events_scoped` (e.g., via `/apply-filters`) **MUST** trigger a full re-materialization of both memberships and activity snapshots for all active cohorts.

---

## Condition Evaluation (`membership_builder.py`)

For each condition, the engine builds a SQL CTE that:
1. Filters `events_scoped` by `event_name` and optional property filters.
2. Computes cumulative `SUM(event_count)` per user, ordered by `event_time` and internal row ID (`rn`) to ensure deterministic results.
3. Retains users only when the cumulative count reaches `min_event_count`.

### Logical Combination
- **`AND`**: Logic uses an `INTERSECT` of user sets across all condition CTEs.
- **`OR`**: Logic uses a `UNION` of user sets across all condition CTEs.

### Join Time Calculation
- **`condition_met`**: The earliest timestamp where the user satisfied the cohort's logic conditions.
- **`first_event`**: The globally earliest event timestamp for that user in the `events_scoped` dataset.

---

## Activity Snapshot Rebuild

After a cohort's membership is materialized, the engine identifies all events performed by these members within the current `events_scoped` dataset and inserts them into `cohort_activity_snapshot`.

### Snapshot Purpose
- **Performance**: Provides a pre-filtered, optimized event stream for heavy analytics modules (Retention, Flows, Paths) to avoid expensive joins on the full dataset during interactive analysis.
- **Accuracy**: Ensures that only events relevant to the cohort members and the active scope are analyzed.

---

## Cohort Operations

- **Hide/Unhide**: Toggles visibility in analytics results without re-materializing.
- **Random Split**: Splits a parent cohort into two random child groups ('Group A', 'Group B') with their own materialized memberships and snapshots for Comparison analysis.
- **Estimate**: Predicts the cohort size using `events_scoped` to provide immediate feedback to the user without the cost of full materialization.

---

## Saved Cohorts (Global)

Saved Cohorts are global, reusable definitions stored independently of any specific dataset.
- When imported into a workspace, a materialized `cohort` is created that tracks the `source_saved_id`.
- Editing a globally saved cohort definition automatically triggers an update and rebuild for all active cohorts derived from it.
