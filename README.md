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

The root endpoint is available at `GET /` and returns:

```json
{"status": "ok"}
```
