# Architecture

## Overview

This repository is a single FastAPI service (`backend/app/main.py`) with a React + Vite frontend (`frontend/src`). The backend persists everything in one DuckDB file at `backend/cohort_analysis.duckdb`. CSV data is uploaded, normalized into a canonical event table, optionally scoped with dataset filters, then used to compute cohort membership, retention, and usage outputs. The frontend is a thin client that calls these HTTP endpoints and renders tables/forms.

## Runtime Components

- **Backend API:** FastAPI app with permissive CORS (`allow_origins=["*"]`).
- **Database:** DuckDB file opened per request via `get_connection()`.
- **Frontend SPA:** React components and a shared `request()` wrapper in `frontend/src/api.js`.

## Persistent / Derived Tables

### 1) `events`
Raw uploaded CSV table (schema exactly matches uploaded columns). Recreated on each `/upload`.

### 2) `events_normalized`
Canonical analytics table created by `/map-columns`.

- Required canonical columns:
  - `user_id` (TEXT)
  - `event_name` (TEXT)
  - `event_time` (TIMESTAMP)
  - `event_count` (INTEGER/BIGINT, NOT NULL)
- Plus every non-mapped source column, with selected types (`TEXT`, `NUMERIC`, `TIMESTAMP`, `BOOLEAN`).
- Deduplication step groups by all columns except `event_count`, summing `event_count`.

### 3) `events_scoped`
Current filtered projection of `events_normalized` (replaced by `/apply-filters`).

### 4) `cohorts`
Metadata:
- `cohort_id` PK (sequence-backed)
- `name`
- `logic_operator`
- `join_type` (`condition_met` or `first_event`)
- `is_active`

### 5) `cohort_conditions`
Condition rows per cohort:
- `condition_id` PK (sequence-backed)
- `cohort_id`
- `event_name`
- `min_event_count`
- `property_column` / `property_operator` / `property_values` (JSON text)

### 6) `cohort_membership`
Materialized membership:
- `user_id`
- `cohort_id`
- `join_time`

### 7) `cohort_activity_snapshot`
Snapshot of events belonging to users who joined each cohort:
- `cohort_id`
- `user_id`
- `event_time`
- `event_name`

### 8) `dataset_scope`
Singleton row (`id = 1`) storing active scope metadata:
- `filters_json`
- `total_rows`
- `filtered_rows`
- `updated_at`

## Data Flow

1. **Upload (`POST /upload`)**
   - Validates `.csv` filename and at least 3 columns.
   - Reads CSV into pandas, writes `events`, returns column names + detected types.

2. **Map columns (`POST /map-columns`)**
   - Validates mapped source columns exist.
   - Validates required mapped semantic types:
     - `user_id` -> TEXT
     - `event_name` -> TEXT
     - `event_time` -> TIMESTAMP
     - optional `event_count` -> NUMERIC
   - Parses each row by selected type.
   - If event_count column is missing, defaults each row to `1`.
   - Requires non-null integer `event_count >= 1` when supplied.
   - Creates `events_normalized`, resets cohort/snapshot tables, initializes `events_scoped`, creates default **All Users** cohort, and refreshes active flags.

3. **Scope filters (`POST /apply-filters`)**
   - Builds a SQL `WHERE` clause from date range + filter rows.
   - Recreates `events_scoped` from `events_normalized`.
   - Persists scope metadata to `dataset_scope`.
   - Rebuilds all cohort memberships/snapshots from scoped data.

4. **Cohort CRUD (`/cohorts`)**
   - Create/update stores metadata + condition rows and materializes membership.
   - Delete removes metadata + conditions + membership + snapshot.
   - `All Users` is protected from update/delete.

5. **Analytics (`GET /retention`, `GET /usage`)**
   - Read active cohorts only (`is_active = TRUE`).
   - Use `cohort_membership` + `cohort_activity_snapshot` + overlay join to `events_scoped` so analytics reflect current scoped dataset.

## Cohort Membership SQL Logic

For each condition, backend builds a CTE over one event stream:

- Filters by `event_name` (+ optional property filter).
- Computes cumulative volume using:
  - `SUM(event_count) OVER (PARTITION BY user_id ORDER BY event_time, event_name ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
- Finds first timestamp where cumulative count reaches threshold (`min_event_count`).

Condition combination:

- **AND:** `INNER JOIN` condition CTEs by `user_id`; join timestamp is `LEAST(c0.event_time, c1.event_time, ...)`.
- **OR:** `UNION ALL` condition CTEs then `MIN(event_time)` per user.

`join_type` handling:

- `condition_met`: keep computed qualifying timestamp.
- `first_event`: overwrite `join_time` with each user’s minimum event time in source table.

After membership insert, snapshot is rebuilt by joining source events to cohort membership on `user_id` + `cohort_id`.

## Retention Logic

`GET /retention?max_day=<int>=7&retention_event=<optional>`:

- Computes day bucket as `DATE_DIFF('day', cm.join_time::DATE, cas.event_time::DATE)`.
- Limits buckets to `0..max_day`.
- Active users per cohort/day are counted distinct.
- If `retention_event` is provided and not `any`, only that event is counted.
- Final value is percentage: `active_users / cohort_size * 100`.

`cohort_size` is computed from currently scoped users (membership left-joined to `events_scoped` and requiring scoped presence).

## Usage Logic

`GET /usage?event=<required>&max_day=<int>=7&retention_event=<optional>` returns three tables:

- `usage_volume_table` -> `COUNT(*)` event rows by cohort/day for selected usage event.
- `usage_users_table` -> `COUNT(DISTINCT user_id)` by cohort/day for selected usage event.
- `retained_users_table` -> same retained-user counts produced by retention query path.

If scoped table does not exist, no active cohorts exist, or selected usage event is absent in scoped data, returns empty tables.

## Column Values Endpoint

`GET /column-values?column=<name>`:

- Validates column exists in `events_normalized`.
- Returns up to 100 distinct non-null values (`ORDER BY 1 LIMIT 100`).
- Also returns true distinct count (`COUNT(DISTINCT column)`).

## Constraints / Limits / Defaults

- Cohort request `conditions`: max 5.
- `min_event_count`: `>= 1`.
- Scope and cohort operators are type-validated.
- `build_where_clause` supports `=`, `!=`, `<`, `>`, `<=`, `>=`, `IN`, `NOT IN`.
- Scope timestamp operators exclude `IN` / `NOT IN`.
- Cohort property filters allow timestamp `IN` / `NOT IN` but normalize strings to `%Y-%m-%d %H:%M:%S`.
- SearchableSelect limits visible options to 100 in frontend.
- Cohort property multi-select caps selected values at 100 in frontend.
- Retention and usage default `max_day` = 7.
- `retention_event` defaults logically to `any` when omitted.

## Frontend Architecture

- `App.jsx` composes workflow: Upload -> Mapping -> Filter Data -> Cohort Form -> Retention -> Usage.
- `api.js` centralizes backend calls and error extraction (`detail`).
- `FilterData.jsx` loads columns/scope/date range, constructs `/apply-filters` payloads, and supports enabled/disabled filter rows.
- `CohortForm.jsx` supports create/edit/delete, join type selection, AND/OR logic, optional typed property filters, and column value lookup.
- `RetentionTable.jsx` controls `maxDay`, retention event, and renders day columns `D0..Dn`.
- `UsageTable.jsx` loads usage by selected event and supports display transforms (`count`, `%`, per-active-user, per-event-firer) on top of backend tables.
- `SearchableSelect.jsx` normalizes options, client-side filters, keyboard navigation, and truncates displayed matches to 100.

## Test-Corroborated Behavior

Backend tests verify:

- Threshold uses cumulative **sum of `event_count`**, not raw row count.
- Join type behavior (`condition_met` vs `first_event`) and normalization of uppercase input.
- Snapshot overlay avoids inflation when different events share the same timestamp.
- Scope operations rebuild memberships and can inactivate cohorts (including All Users).
- `/column-values` enforces the 100-value response limit while returning full distinct cardinality.
