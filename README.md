# Cohort Analysis

Cohort Analysis is a FastAPI + DuckDB backend with a React/Vite frontend for CSV-based product analytics.

It supports:
- CSV upload and schema mapping
- Event normalization into canonical tables
- Dataset scoping (date + property filters)
- Cohort creation with condition logic
- Retention, usage, and monetization analytics
- Revenue event include/exclude + per-event override config

## Stack
- Backend: FastAPI, DuckDB, pandas, Pydantic
- Frontend: React + Vite
- Tests: pytest, vitest

## End-to-end flow
1. `POST /upload` creates raw `events` from CSV and returns detected types + mapping suggestions.
2. `POST /map-columns` creates `events_normalized`, resets cohort/scoping state, initializes `events_scoped`, and creates the default **All Users** cohort.
3. `POST /apply-filters` rebuilds `events_scoped` and refreshes cohort membership/snapshots.
4. Cohorts are created/updated through `/cohorts*` APIs.
5. Analytics endpoints (`/retention`, `/usage`, `/monetization`) read from scoped data + cohort tables.

## FastAPI endpoints
- `GET /`
- `POST /upload`
- `POST /map-columns`
- `POST /apply-filters`
- `GET /scope`
- `GET /columns`
- `GET /column-values`
- `GET /date-range`
- `POST /cohorts`
- `GET /cohorts`
- `PUT /cohorts/{cohort_id}`
- `DELETE /cohorts/{cohort_id}`
- `PATCH /cohorts/{cohort_id}/hide`
- `POST /cohorts/{cohort_id}/random_split`
- `GET /retention`
- `GET /usage`
- `GET /events`
- `GET /revenue-config-events`
- `GET /revenue-events`
- `POST /update-revenue-config`
- `GET /monetization`

## Key constraints from implementation
- Upload accepts `.csv` only.
- CSV must have at least 3 columns.
- Required mappings:
  - `user_id` => TEXT
  - `event_name` => TEXT
  - `event_time` => TIMESTAMP
- Optional mappings:
  - `event_count` => NUMERIC; values must be integer `>= 1`
  - `revenue_column` => NUMERIC
- Cohorts:
  - `conditions` max length is 5
  - `min_event_count >= 1`
  - `logic_operator` is `AND` or `OR`
  - `join_type` is `condition_met` or `first_event`
- `column-values` returns up to 100 values and `total_distinct`.
- `max_day` defaults to 7 for retention/usage/monetization.

## Run locally
### Backend
```bash
cd backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

### Frontend
```bash
cd frontend
npm install
npm run dev
```

Default API base URL is `http://127.0.0.1:8000` unless `VITE_API_BASE_URL` is set.
