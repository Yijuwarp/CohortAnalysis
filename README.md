# CohortAnalysis Backend API

This repository contains the initial FastAPI backend setup for the behavioral cohort analysis tool.

## Project Structure

```text
.
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── models/
│   │   └── __init__.py
│   ├── routers/
│   │   └── __init__.py
│   ├── schemas/
│   │   └── __init__.py
│   └── services/
│       └── __init__.py
├── requirements.txt
└── README.md
```

## Setup

1. (Optional) Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Run the Development Server

Start the API with:

```bash
uvicorn app.main:app --reload
```

The root endpoint is available at `[GET /](http://127.0.0.1:8000/docs)` and returns:

```json
{"status": "ok"}
```

## API Endpoints

- `POST /upload`: Upload raw CSV event data into the `events` table.
- `POST /map-columns`: Map uploaded columns to `user_id`, `event_name`, and `event_time` and create `events_normalized`.
- `POST /cohorts`: Create cohort definitions and persist user membership rows in `cohort_membership`.
- `GET /retention`: Compute dynamic cohort retention from `events_normalized` and `cohort_membership` for day buckets `0..max_day` (default `7`).

Example:

```bash
curl "http://127.0.0.1:8000/retention?max_day=7"
```
