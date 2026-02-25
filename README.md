# Cohort Analysis (Full Stack)

This repository contains a full-stack cohort analysis app:

- **Backend**: FastAPI + DuckDB analytics engine
- **Frontend**: React + Vite UI for upload, mapping, cohort creation, and retention visualization

## Project Structure

```text
cohort-analysis/
├── backend/
│   ├── app/
│   ├── tests/
│   ├── requirements.txt
│   └── main.py
├── frontend/
│   ├── package.json
│   ├── vite.config.js
│   ├── src/
│   │   ├── App.jsx
│   │   ├── api.js
│   │   └── components/
│   │       ├── Upload.jsx
│   │       ├── Mapping.jsx
│   │       ├── CohortForm.jsx
│   │       └── RetentionTable.jsx
│   └── index.html
└── README.md
```

## Backend Setup

```bash
cd backend
python -m venv venv
# Windows:
venv\Scripts\activate
# macOS/Linux:
# source venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Backend runs on **http://127.0.0.1:8000**.

## Frontend Setup

```bash
cd frontend
npm install
npm run dev
```

Frontend runs on **http://localhost:5173**.

## CORS / Integration

The backend enables CORS for `http://localhost:5173` so the Vite app can call FastAPI endpoints from the browser.

## API Endpoints

- `POST /upload`
- `POST /map-columns`
- `POST /cohorts`
- `GET /retention?max_day=7`

## End-to-End Flow

1. Upload CSV in the frontend.
2. Map CSV columns to user/event/time.
3. Create cohort rules.
4. Load retention table for dynamic day columns (`D0..Dn`).

## Run Backend Tests

```bash
cd backend
pytest
```
