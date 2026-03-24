# Cohort Analysis

High-performance, event-based cohort analysis platform with a FastAPI + DuckDB backend and a React/Vite frontend.

## Features

- **CSV Ingestion**: Intelligent schema detection and mapping suggestions.
- **Data Normalization**: Canonical event transformation with grouped aggregation.
- **Dynamic Scoping**: Real-time dataset filtering by date ranges and properties.
- **Cohort Engine**: Powerful frequency-based cohort creation with `AND`/`OR` logic and property filters.
- **Retention Analytics**: Periodic user retention with optional confidence intervals.
- **Usage & Frequency**: Analyze event volume, unique users, and activity frequency.
- **Monetization**: Detailed revenue analysis with inclusion toggles and value overrides.
- **Funnels**: Multi-step conversion tracking with greedy path matching.
- **Event Flows**: Sankey-style transition analysis to understand user journeys.
- **User Explorer**: Deep-dive into individual user activity timelines and properties.

## 📚 Documentation

For detailed information, please refer to:
- [Architecture](ARCHITECTURE.md)
- [Backend Overview](docs/backend_overview.md)
- [Frontend Overview](docs/frontend_overview.md)
- [Data Model](docs/data_model.md)
- [API Reference](docs/api_reference.md)

## Tech Stack

- **Backend**: FastAPI, DuckDB, pandas, Pydantic.
- **Frontend**: React, Vite, Vanilla CSS.
- **Testing**: pytest (backend), vitest (frontend).

## Quick Start

### Backend
1. `cd backend`
2. `python -m venv venv`
3. `source venv/bin/activate` (or `venv\Scripts\activate` on Windows)
4. `pip install -r requirements.txt`
5. `uvicorn app.main:app --reload`

### Frontend
1. `cd frontend`
2. `npm install`
3. `npm run dev`

## Key Implementation Constraints

- **Upload**: Accepts `.csv` only; requires `user_id`, `event_name`, and `event_time`.
- **Cohorts**: Max 5 conditions per cohort; `min_event_count >= 1`.
- **Funnels**: 2-10 steps; optional conversion window.
- **Analytics**: `max_day` defaults to 7.
- **DuckDB**: Uses a local file `backend/cohort_analysis.duckdb` for persistence.

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

No manual alias setup required. Flow graph uses a fallback shim for restricted environments.

Default API base URL is `http://127.0.0.1:8000` unless `VITE_API_BASE_URL` is set.

---

## License

Copyright (c) 2026 Venkat Chaitanya Duggineni

This project is licensed under the **Business Source License (BSL) 1.1**.

You may copy, modify, and run this software for personal use, evaluation, research, or internal business use.

Commercial use, including offering this software as a hosted or managed service, requires a commercial license from the author.

On **2029-03-13**, this project will automatically convert to the **MIT License**.
