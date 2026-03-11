# Backend Getting Started

## Run locally
```bash
cd backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

API starts at `http://127.0.0.1:8000`.

## Main modules
- `app/main.py`: FastAPI app + router registration
- `app/routers/*.py`: endpoint adapters
- `app/domains/legacy_api.py`: ingestion, scope, cohorts, analytics, revenue logic
- `app/db/connection.py`: DuckDB connection creation

## API workflow to verify quickly
1. `POST /upload` with CSV
2. `POST /map-columns`
3. `GET /events`
4. `GET /retention`
5. `GET /usage?event=<event>&max_day=7`
6. `GET /monetization?max_day=7`

## Tests
```bash
cd backend
pytest
```
