# Cohort Analysis

This repository is licensed under the **Business Source License (BSL) 1.1**.
Commercial use requires a license from the author. See the `LICENSE` file for details.

---

Cohort Analysis is a FastAPI + DuckDB backend with a React/Vite frontend for CSV-based product analytics.

It supports:

* CSV upload and schema mapping
* Event normalization into canonical tables
* Dataset scoping (date + property filters)
* Cohort creation with condition logic
* Retention, usage, and monetization analytics
* Revenue event include/exclude + per-event override config
* Funnels with up to 10 ordered steps, optional conversion window, and drag-drop step ordering in UI

## Stack

* Backend: FastAPI, DuckDB, pandas, Pydantic
* Frontend: React + Vite
* Tests: pytest, vitest

## End-to-end flow

1. `POST /upload` creates raw `events` from CSV and returns detected types + mapping suggestions.
2. `POST /map-columns` creates `events_normalized`, resets cohort/scoping state, initializes `events_scoped`, and creates the default **All Users** cohort.
3. `POST /apply-filters` rebuilds `events_scoped` and refreshes cohort membership/snapshots.
4. Cohorts are created/updated through `/cohorts*` APIs.
5. Analytics endpoints (`/retention`, `/usage`, `/monetization`) read from scoped data + cohort tables.

## FastAPI endpoints

* `GET /`
* `POST /upload`
* `POST /map-columns`
* `POST /apply-filters`
* `GET /scope`
* `GET /columns`
* `GET /column-values`
* `GET /date-range`
* `POST /cohorts`
* `GET /cohorts`
* `POST /cohorts/estimate`
* `GET /saved-cohorts`
* `POST /saved-cohorts`
* `GET /saved-cohorts/{id}`
* `PUT /saved-cohorts/{id}`
* `DELETE /saved-cohorts/{id}`
* `PUT /cohorts/{cohort_id}`
* `DELETE /cohorts/{cohort_id}`
* `PATCH /cohorts/{cohort_id}/hide`
* `POST /cohorts/{cohort_id}/random_split`
* `GET /retention`
* `GET /usage`
* `GET /events`
* `GET /revenue-config-events`
* `GET /revenue-events`
* `POST /update-revenue-config`
* `GET /monetization`
* `POST /funnels`
* `GET /funnels`
* `PUT /funnels/{funnel_id}`
* `DELETE /funnels/{funnel_id}`
* `POST /funnels/run`

## Key constraints from implementation

* Upload accepts `.csv` only.
* CSV must have at least 3 columns.
* Required mappings:

  * `user_id` => TEXT
  * `event_name` => TEXT
  * `event_time` => TIMESTAMP
* Optional mappings:

  * `event_count` => NUMERIC; values must be integer `>= 1`
  * `revenue_column` => NUMERIC
* Cohorts:

  * `conditions` max length is 5
  * `min_event_count >= 1`
  * `logic_operator` is `AND` or `OR`
  * `join_type` is `condition_met` or `first_event`
* `column-values` returns up to 100 values and `total_distinct`.
* `max_day` defaults to 7 for retention/usage/monetization.
* Funnels:
  * step count must be between 2 and 10
  * steps may include optional explicit `step_order` (frontend sends sequential 0-based order)
  * optional `conversion_window` supports minutes (`{"value": <int>, "unit": "minute"}`) or `null` for lifetime

### Funnel Matching Logic

Funnels use greedy earliest-path matching:
- Each step selects the earliest valid event after the previous step
- Does not compute globally optimal paths across all event combinations

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

---

## License

Copyright (c) 2026 Venkat Chaitanya Duggineni

This project is licensed under the **Business Source License (BSL) 1.1**.

You may copy, modify, and run this software for personal use, evaluation, research, or internal business use.

Commercial use, including offering this software as a hosted or managed service, requires a commercial license from the author.

On **2029-03-13**, this project will automatically convert to the **MIT License**.
