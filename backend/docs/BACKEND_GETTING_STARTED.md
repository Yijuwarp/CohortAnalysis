# Backend Getting Started

The backend is a FastAPI application that uses DuckDB for high-performance analytical queries.

## Run Locally

### 1. Setup Environment
```bash
cd backend
python -m venv venv
source venv/bin/activate  # Or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

### 2. Configure Windows context
If running on Windows, you **must** set the following environment variable to prevent DuckDB locking issues:
```bash
# Windows (PowerShell)
$env:DUCKDB_SINGLE_WORKER="true"

# Windows (CMD)
set DUCKDB_SINGLE_WORKER=true
```

### 3. Start the Server
```bash
uvicorn app.main:app --reload --workers 1
```
The API will be available at `http://127.0.0.1:8000`.

## Main Modules
- **`app/main.py`**: Entry point and router registration.
- **`app/routers/*.py`**: Route handlers and request/response adaptation.
- **`app/domains/*`**: Core business logic partitioned by feature (Ingestion, Scope, Cohorts, Analytics).
- **`app/db/connection.py`**: Thread-safe DuckDB connection management and multi-user isolation.
- **`app/models/*.py`**: Pydantic schemas for data validation.

## Verification Workflow
1.  **Ingest**: `POST /upload` with a valid CSV.
2.  **Map**: `POST /map-columns` with canonical field mappings.
3.  **Validate**: `GET /events` to see unique event names in scope.
4.  **Analyze**: 
    - `GET /retention?max_day=7`
    - `GET /usage?event=Purchase&max_day=7`
    - `POST /paths/run` with a valid sequence.
    - `GET /flow/l1?start_event=SessionStart`

## Testing
Run the comprehensive backend test suite:
```bash
cd backend
pytest
```
To run specific domain tests:
```bash
pytest app/tests/domains/
```
