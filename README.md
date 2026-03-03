# Cohort Analysis

A full-stack cohort analysis app backed by FastAPI + DuckDB, with a React/Vite UI for CSV ingestion, schema mapping, cohort definition, retention, and usage analytics.

## Tech Stack

- **Backend:** FastAPI, Pydantic, DuckDB, pandas
- **Frontend:** React, Vite
- **Tests:** pytest + FastAPI TestClient

## What It Actually Does

1. Upload a CSV (`/upload`).
2. Map source columns to canonical fields (`/map-columns`):
   - `user_id` (TEXT)
   - `event_name` (TEXT)
   - `event_time` (TIMESTAMP)
   - optional `event_count` (NUMERIC)
3. Normalize + deduplicate events into `events_normalized` (duplicate keys are merged by summing `event_count`).
4. Apply dataset scope filters (`/apply-filters`) to produce `events_scoped`.
5. Create/edit/delete cohorts (`/cohorts`) with:
   - up to 5 conditions
   - AND/OR logic
   - join type: `condition_met` or `first_event`
   - optional typed property filters
6. Query retention (`/retention`) and usage (`/usage`) from scoped cohort snapshots.

## API Surface

- `GET /` health/status
- `POST /upload`
- `POST /map-columns`
- `POST /apply-filters`
- `GET /scope`
- `GET /columns`
- `GET /column-values?column=...`
- `GET /date-range`
- `POST /cohorts`
- `GET /cohorts`
- `PUT /cohorts/{cohort_id}`
- `DELETE /cohorts/{cohort_id}`
- `GET /events`
- `GET /retention?max_day=...&retention_event=...`
- `GET /usage?event=...&max_day=...&retention_event=...`

## Key Behavioral Details

- Cohort thresholding uses cumulative `SUM(event_count)` over time per user.
- `first_event` join type rewrites cohort join time to each user’s first event in current source table.
- Analytics are scoped through `events_scoped` overlay joins, so filters can hide/inactivate cohorts.
- `/column-values` returns at most 100 sample distinct values plus `total_distinct`.

## Real Constraints and Defaults

- CSV only.
- Upload requires at least 3 columns.
- Cohort conditions: max 5.
- `min_event_count >= 1`.
- `max_day` default is 7 for retention and usage.
- All Users cohort is auto-created on mapping and cannot be edited/deleted.
- No authentication/authorization.
- Single DuckDB file (single-node architecture).

## Local Run

### Backend

```bash
cd backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Default backend URL: `http://127.0.0.1:8000`

### Frontend

```bash
cd frontend
npm install
npm run dev
```

Default frontend URL: `http://localhost:5173`

Frontend API base URL:
- `VITE_API_BASE_URL` if set
- otherwise `http://127.0.0.1:8000`

## Tests

```bash
cd backend
pytest
```
