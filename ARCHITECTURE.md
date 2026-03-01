# Cohort Analysis App Architecture

## 1. High-Level Architecture Overview

The application is a single-node analytics system composed of:

- **Frontend client**: React + Vite SPA that handles CSV upload, schema mapping, cohort definition, and analytics visualization.
- **Backend service**: FastAPI API layer that validates requests, executes cohort/retention/usage logic, and returns JSON results.
- **Database layer**: DuckDB file-backed analytical database used for both raw event persistence and derived cohort snapshots.
- **Deployment model**: Split frontend/backend deployment (typically Vercel for static frontend and Render for Python API), connected over HTTP with CORS enabled.

```text
[ React Frontend ]
        ↓
[ FastAPI Backend ]
        ↓
[ DuckDB Database ]
```

### Data Flow Summary

1. Users upload CSV data from the React UI to `POST /upload`.
2. Backend stores uploaded rows in `events` (raw staging table).
3. Users map source columns to canonical fields via `POST /map-columns`.
4. Backend builds `events_normalized` and initializes baseline cohort tables.
5. Users define cohorts via `POST /cohorts`; backend computes cohort membership and `join_time`, then snapshots activity.
6. Retention (`GET /retention`) and usage (`GET /usage`) are computed from snapshots and normalized events.

---

## 2. Backend Architecture

### 2.1 Tech Stack

- **FastAPI**
  - Chosen for typed request/response handling, concise endpoint definitions, and easy local iteration.
  - Enables clean integration with Pydantic models for input validation.
- **DuckDB**
  - Chosen for embedded analytical SQL, low operational overhead, and strong performance for OLAP-style queries on medium datasets.
  - File-backed DB simplifies local development and test isolation.
- **Pydantic**
  - Used to validate payload contracts such as cohort logic operator and condition bounds before query execution.
  - Prevents malformed requests from propagating into SQL logic.
- **Pytest**
  - Used for endpoint and behavioral tests across upload, mapping, cohort creation/deletion, and retention logic.
  - Supports regression testing around snapshot invariants.

### 2.2 Core Modules

The current backend is implemented in a single API module, but behavior is logically separated into service responsibilities:

- **Upload handling (`/upload`)**
  - Parses CSV with Pandas.
  - Validates file type and minimum column count.
  - Replaces `events` table with uploaded dataset.

- **Column mapping (`/map-columns`)**
  - Validates mapped columns exist in `events`.
  - Builds `events_normalized` with canonical fields:
    - `user_id`
    - `event_name`
    - `event_time`
    - `raw_data` JSON for non-mapped columns.
  - Initializes cohort tables and default `All Users` cohort snapshot.

- **Cohort engine (`/cohorts`)**
  - Persists cohort metadata and condition rules.
  - Computes qualifying users and first qualifying timestamp (`join_time`) per cohort.
  - Inserts frozen membership and event activity snapshots.

- **Retention engine (`/retention`)**
  - Computes day-indexed active-user percentages by cohort.
  - Supports optional event filtering (`retention_event`) and variable horizon (`max_day`).

- **Usage engine (`/usage`)**
  - Computes day-indexed event volume and distinct active users for a selected event.
  - Returns two tables for frontend rendering and optional derived metrics.

- **Cohort deletion logic (`DELETE /cohorts/{id}`)**
  - Protects `All Users` from deletion.
  - Explicitly deletes from snapshot and condition tables before deleting cohort metadata.

- **Validation layer**
  - Pydantic models enforce:
    - non-empty cohort name,
    - `AND/OR` logic only,
    - 1..5 conditions,
    - `min_event_count >= 1`.
  - FastAPI query constraints enforce `max_day >= 0`.

### 2.3 Database Design

> Note: the implementation currently uses table names `cohort_membership` (singular) and `cohort_activity_snapshot`. In this document, **`cohort_memberships`** refers to the implemented `cohort_membership` table.

#### `events`

- **Purpose**: Raw uploaded CSV storage.
- **Key columns**: Source-dependent (schema follows uploaded file).
- **Relationships**: Source table for mapping into `events_normalized`.
- **Index assumptions**: No explicit indexes; full scans acceptable for current scale.
- **Design rationale**: Keeps ingestion decoupled from canonical analytics schema.

#### `events_normalized`

- **Purpose**: Canonical event model for analytics queries.
- **Key columns**:
  - `user_id TEXT`
  - `event_name TEXT`
  - `event_time TIMESTAMP`
  - `raw_data JSON`
- **Relationships**:
  - Joined to `cohort_membership` for usage queries.
  - Used to build cohort snapshots.
- **Index assumptions**: No explicit indexes; expected filters on `event_name`, `user_id`, and `event_time`.
- **Design rationale**: Standardizes query surface while preserving unmapped source context in `raw_data`.

#### `cohorts`

- **Purpose**: Cohort definitions and metadata.
- **Key columns**:
  - `cohort_id` (sequence-backed PK)
  - `name`
  - `logic_operator` (`AND` or `OR`)
- **Relationships**:
  - One-to-many with `cohort_conditions`.
  - One-to-many with `cohort_membership`.
- **Index assumptions**: PK lookup by `cohort_id`; small table.
- **Design rationale**: Keeps cohort identity/metadata separate from condition logic and membership data.

#### `cohort_conditions`

- **Purpose**: Persisted condition rows for each cohort.
- **Key columns**:
  - `condition_id` (sequence-backed PK)
  - `cohort_id`
  - `event_name`
  - `min_event_count`
- **Relationships**: Many-to-one into `cohorts`.
- **Index assumptions**: logical lookup by `cohort_id`; no explicit index defined.
- **Design rationale**: Condition normalization avoids denormalized JSON blobs, supports introspection and future editing APIs.

#### `cohort_memberships` (`cohort_membership` in code)

- **Purpose**: Snapshot membership table with immutable cohort join anchor.
- **Key columns**:
  - `user_id`
  - `cohort_id`
  - `join_time TIMESTAMP`
- **Relationships**:
  - Many-to-one to `cohorts`.
  - Join target for both retention and usage calculations.
- **Index assumptions**: frequent joins on `(cohort_id, user_id)`.
- **Design rationale**: Freezes cohort entry population and join anchor for reproducible analytics.

#### Additional implemented snapshot table: `cohort_activity_snapshot`

- **Purpose**: Frozen activity events for each cohort/user at cohort-creation time.
- **Key columns**:
  - `cohort_id`
  - `user_id`
  - `event_time`
- **Relationships**:
  - Joined with `cohort_membership` for retention calculations.
  - Optionally joined with `events_normalized` for event-name filtered retention.
- **Design rationale**: Preserves retention stability even if `events_normalized` is replaced later.

### Why `events_normalized` exists

- Raw CSV schemas vary; analytics logic should not.
- Canonical field naming reduces query complexity and frontend coupling.
- `raw_data` keeps auxiliary attributes without polluting cohort/retention SQL.

### Why snapshot memberships are stored

- Prevents cohort drift when source events change after cohort creation.
- Makes retention reproducible across remaps/uploads.
- Supports auditing and deterministic test behavior.

### Why conditions are in their own table

- Supports multiple conditions per cohort with explicit rows.
- Enables future condition editing/versioning without schema redesign.
- Cleaner relational model vs serialized condition payloads.

---

## 3. Cohort Engine Design

### 3.1 Multi-Condition Logic

- Supports flat boolean composition only:
  - `AND`: user must satisfy all condition CTEs.
  - `OR`: user can satisfy any condition CTE.
- Hard cap: **maximum 5 conditions** enforced in request model.
- No nested logic (e.g., `(A AND B) OR C`) by design.

#### Why no nested logic

- Keeps API payload and SQL generation deterministic.
- Avoids introducing a custom expression parser in current architecture.
- Maintains explainability for non-technical users and faster UI implementation.

### 3.2 Join Time Computation

For each condition `(event_name, min_event_count)`:

1. Filter `events_normalized` to the requested `event_name`.
2. Order each user’s matching events by `event_time`.
3. Assign `ROW_NUMBER()` per user.
4. Select row where `rn = min_event_count`.

Interpretation:

- `min_event_count = 1` → first occurrence timestamp.
- `min_event_count = 3` → timestamp of third occurrence.

Then:

- **AND mode**: combine per-condition qualifying rows per user, compute `LEAST(c0.event_time, c1.event_time, ...)`, then take `MIN(event_time)` grouped by user.
- **OR mode**: union qualifying rows and take `MIN(event_time)` grouped by user.

Final result is inserted as `join_time` into `cohort_membership`.

### 3.3 Snapshot Modeling

#### Why snapshot-based cohorts were chosen

- Cohort membership is persisted at creation time and not recomputed on read.
- Retention is computed against the frozen activity snapshot for that cohort.
- This guarantees historical consistency for business reporting.

#### Tradeoffs vs dynamic cohorts

- **Snapshot advantages**:
  - Stable, reproducible metrics.
  - Faster reads (no full cohort recomputation each query).
  - Clear temporal semantics.
- **Snapshot disadvantages**:
  - Storage duplication in `cohort_activity_snapshot`.
  - Cohorts can become stale relative to newly uploaded data.
  - Requires explicit recalc workflow (currently create new cohort / remap behavior).

#### Impact on retention stability

- Remapping or replacing `events_normalized` does not retroactively alter previously snapshotted cohort retention curves.
- This behavior is explicitly covered by tests.

#### Deletion cascade behavior

`DELETE /cohorts/{id}` executes manual cascade in order:

1. `cohort_activity_snapshot`
2. `cohort_membership`
3. `cohort_conditions`
4. `cohorts`

This keeps orphaned records out of analytical queries and protects referential integrity by convention.

---

## 4. Retention Engine

Retention is calculated as day-based active-user percentage per cohort.

### Core approach

- `join_time` in `cohort_membership` is the day-zero anchor.
- For each activity record, compute:

```sql
DATE_DIFF('day', cm.join_time::DATE, cas.event_time::DATE) AS day_number
```

- Filter to `day_number BETWEEN 0 AND :max_day`.
- Count `COUNT(DISTINCT user_id)` per `(cohort_id, day_number)`.
- Convert to percentage via:

```text
retention% = active_users / cohort_size * 100
```

### `max_day` behavior

- Controls output width (`D0...Dmax_day`).
- Days outside range are excluded at query time.

### `retention_event` behavior

- If omitted (`any`), activity comes from all snapshotted events.
- If provided, retention query joins snapshot rows back to `events_normalized` to filter by `event_name`.

### Pseudo-query example (any event)

```sql
WITH activity_deltas AS (
  SELECT
    cm.cohort_id,
    cm.user_id,
    DATE_DIFF('day', cm.join_time::DATE, cas.event_time::DATE) AS day_number
  FROM cohort_membership cm
  JOIN cohort_activity_snapshot cas
    ON cm.cohort_id = cas.cohort_id
   AND cm.user_id = cas.user_id
  WHERE DATE_DIFF('day', cm.join_time::DATE, cas.event_time::DATE)
        BETWEEN 0 AND :max_day
)
SELECT cohort_id, day_number, COUNT(DISTINCT user_id) AS active_users
FROM activity_deltas
GROUP BY cohort_id, day_number;
```

### Pseudo-query example (filtered by event)

```sql
WITH activity_deltas AS (
  SELECT
    cm.cohort_id,
    cm.user_id,
    DATE_DIFF('day', cm.join_time::DATE, cas.event_time::DATE) AS day_number
  FROM cohort_membership cm
  JOIN cohort_activity_snapshot cas
    ON cm.cohort_id = cas.cohort_id
   AND cm.user_id = cas.user_id
  JOIN events_normalized e
    ON e.user_id = cas.user_id
   AND e.event_time = cas.event_time
  WHERE e.event_name = :retention_event
    AND DATE_DIFF('day', cm.join_time::DATE, cas.event_time::DATE)
        BETWEEN 0 AND :max_day
)
SELECT cohort_id, day_number, COUNT(DISTINCT user_id) AS active_users
FROM activity_deltas
GROUP BY cohort_id, day_number;
```

### Edge cases

- **No cohorts** → returns empty `retention_table`.
- **Zero-sized cohort** → percentage guarded to `0.0` (no divide-by-zero).
- **No matching events in day bucket** → explicit `0.0` value returned for that day.

---

## 5. Usage Engine

Usage answers **"how much activity happened"**, while retention answers **"what share of cohort was active"**.

### Metrics returned

- **Volume computation**: `COUNT(*)` events per cohort/day.
- **Distinct user computation**: `COUNT(DISTINCT user_id)` per cohort/day.
- **Per-active-user metric** (frontend-derived):

```text
per_active_user = total_events / distinct_users
```

(Guarded to `0` when distinct users = 0.)

### Relationship to retention window

- Usage also uses `DATE_DIFF` from cohort `join_time` and `max_day` filtering.
- Unlike retention, usage is always scoped to a single selected event (`event` query parameter is required).

### Conceptual distinction from retention

- **Retention**: normalized percentage metric tied to cohort size.
- **Usage**: absolute or intensity metric tied to event frequency and user concentration.

---

## 6. Frontend Architecture

### React + Vite structure

- Vite provides build tooling and env injection (`VITE_API_BASE_URL`).
- Top-level `App` composes five feature components in workflow order.

### Component breakdown

- `Upload` → CSV selection and `POST /upload`.
- `Mapping` → source-to-canonical column mapping via `POST /map-columns`.
- `CohortForm` → condition builder, cohort creation/deletion.
- `RetentionTable` → day matrix with optional event filtering.
- `UsageTable` → volume + distinct user matrices, with display modes.

### API interaction flow

- Centralized in `src/api.js` through a common `request()` wrapper.
- Handles JSON parsing and error normalization (`detail` fallback).

### State management approach

- Local component state via `useState`.
- Side effects and refresh via `useEffect`.
- Parent-level `retentionRefreshToken` in `App` triggers downstream table reloads after mapping/cohort changes.
- No global state library (Redux/Zustand) is currently needed.

### Form-to-payload mapping

- Mapping form maps directly to:

```json
{
  "user_id_column": "...",
  "event_name_column": "...",
  "event_time_column": "..."
}
```

- Cohort form maps to:

```json
{
  "name": "...",
  "logic_operator": "AND|OR",
  "conditions": [
    { "event_name": "signup", "min_event_count": 1 }
  ]
}
```

### UX flow coverage

- **Upload flow**: file -> backend response -> column options initialized.
- **Mapping flow**: selected columns -> normalized table creation -> analytics refresh.
- **Cohort creation flow**: define conditions -> create cohort -> refresh retention/usage surfaces.
- **Retention visualization**: adjustable day horizon and optional event filter.
- **Usage dashboard**: selected event across day buckets with count/%/per-user display options.

---

## 7. Data Flow (Step-by-Step)

1. **CSV upload**
   - Frontend sends multipart upload.
   - Backend validates and writes raw rows to `events`.
2. **Raw storage in `events`**
   - Table mirrors source columns exactly.
3. **Mapping to canonical model**
   - Backend creates/replaces `events_normalized` using selected user/event/time columns.
   - Non-selected columns are packed into `raw_data` JSON.
4. **Cohort creation + snapshot**
   - Cohort metadata/conditions stored.
   - Membership (`cohort_membership`) computed with `join_time`.
   - Related activity copied to `cohort_activity_snapshot`.
5. **Retention computation**
   - Day deltas computed from `join_time` to snapshot activity.
   - Distinct active users converted to percentages per cohort/day.
6. **Usage computation**
   - For selected event, count total events and distinct users per cohort/day.
   - Frontend optionally derives per-active-user intensity.

---

## 8. Testing Strategy

### Pytest coverage areas

- Upload endpoint:
  - Valid CSV handling.
  - Non-CSV rejection.
  - Minimum column constraints.
- Mapping endpoint:
  - Unknown-column validation.
  - Timestamp coercion behavior.
  - `raw_data` JSON capture for unmapped columns.
- Cohort operations:
  - Creation across logic operators.
  - Min-event-count behavior.
  - Deletion behavior and guardrails.
- Retention:
  - Day bucket correctness.
  - `max_day` behavior.
  - Multiple cohorts.
  - Snapshot stability after remapping.

### Why snapshot behavior is explicitly tested

Snapshot correctness is central to analytics trust. Tests ensure historical cohort metrics stay stable even when base normalized events are replaced.

### Current gaps / not yet covered

- No dedicated tests for `/usage` endpoint behavior.
- No performance/load regression tests.
- No migration/versioning tests for schema evolution.
- Limited frontend automated tests (UI behavior currently validated manually).

### Testing philosophy

- Emphasize deterministic endpoint behavior and data invariants over framework internals.
- Use representative synthetic CSV fixtures to validate analytics semantics.
- Prefer integration-style API tests that exercise SQL paths end-to-end.

---

## 9. Deployment Architecture

### Target deployment topology

- **Backend**: Render web service running FastAPI/uvicorn.
- **Frontend**: Vercel static deployment for Vite build output.

### Runtime integration concerns

- **CORS**:
  - Current backend allows all origins (`*`) for simplicity.
  - Production hardening should restrict to known frontend origins.
- **Environment variables**:
  - Frontend uses `VITE_API_BASE_URL` to target backend environment.
  - Local fallback points to `http://127.0.0.1:8000`.

### Production vs local differences

- Local: both services run on localhost ports (5173 + 8000).
- Production: cross-origin HTTPS calls between separate hosts.
- DuckDB file path persistence semantics differ by host/container lifecycle policy.

---

## 10. Scalability & Limitations

- **DuckDB limitations**:
  - Strong for embedded analytics, weaker for high-concurrency OLTP and distributed workloads.
- **Memory constraints**:
  - Large CSV uploads and wide scans can pressure RAM due to in-process execution.
- **Single-node architecture**:
  - Backend and DB scale vertically, not horizontally.
- **No multi-tenant isolation**:
  - All users/data share same database file and namespace.
- **No authentication/authorization**:
  - API is effectively open in current form.
- **CSV-only ingestion**:
  - No streaming connectors or warehouse sync.
- **No incremental ingestion**:
  - Upload currently replaces `events`; append/merge semantics are absent.

---

## 11. Future Architecture Evolution

Potential evolutions in likely order:

1. **Move to Postgres** for stronger concurrency, indexing, and transactional semantics.
2. **Add background job queue** (e.g., Celery/RQ) for heavy cohort recomputation and imports.
3. **Incremental ingestion** with append + dedup strategies.
4. **Dynamic cohorts** with query-time recomputation for near-real-time segments.
5. **Cache retention/usage results** for repeated dashboard reads.
6. **Multi-tenant isolation** via tenant keys, schemas, or database-per-tenant.
7. **Role-based access control** and API auth boundary.
8. **Partitioned storage** (time/user partitions) for larger event volumes.

Recommended principle: preserve the current clear API contracts while evolving storage and execution internals behind stable endpoints.
