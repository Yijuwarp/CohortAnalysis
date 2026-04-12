# Cohort Analysis Platform

A high-performance behavioral analytics platform designed for deep exploration of user retention, monetization, and transition flows. Built with a modern, domain-driven architecture using FastAPI and DuckDB.

## 🚀 Features

*   **Fast Ingestion**: Multi-threaded CSV ingestion with automated column mapping and type detection.
*   **Dynamic Scoping**: Real-time dataset filtering by date range and any metadata property (implemented as a dynamic View).
*   **Complex Cohorts**: Frequency-based cohort definitions with `AND`/`OR` logic and property filters.
*   **Deep Analytics**:
    *   **Retention**: Relative and fixed-date retention with Wilson score confidence intervals.
    *   **Monetization**: Revenue analysis with customizable event inclusion and value overrides.
    *   **Flows**: Interactive Sankey-style transition analysis with multi-level expansion.
    *   **Paths**: Deterministic greedy matching for sequential conversion tracking.
*   **User Explorer**: Detailed chronological event timelines for individual users.

## 📚 Documentation

For detailed architecture and API reference, see:
- [Architecture](ARCHITECTURE.md)
- [Backend Overview](docs/backend_overview.md)
- [Frontend Overview](docs/frontend_overview.md)
- [Data Model](docs/data_model.md)
- [API Reference](docs/api_reference.md)
- [Ingestion Pipeline](backend/docs/INGESTION_PIPELINE.md)

## 🛠 Tech Stack

*   **Backend**: FastAPI, DuckDB, Pydantic.
*   **Frontend**: React, Vite, Vanilla CSS.
*   **State Management**: Materialized analytical tables for sub-second query performance.

## ⚡ Getting Started

### Prerequisites

*   Python 3.10+
*   Node.js 18+

### Backend Setup

1.  Navigate to the `backend` directory.
2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
3.  Set context for Windows:
    **Windows Only**: Create a `.env` file in the `backend/` directory with `DUCKDB_SINGLE_WORKER=true`.
4.  Run the server:
    ```bash
    uvicorn app.main:app --reload --workers 1
    ```

> [!IMPORTANT]
> **Windows Constraint**: DuckDB requires running with exactly **1 worker** on Windows to avoid database locking conflicts.

### Frontend Setup

1.  Navigate to the `frontend` directory.
2.  Install dependencies:
    ```bash
    npm install
    ```
3.  Run the development server:
    ```bash
    npm run dev
    ```

## ⚖️ License

Copyright (c) 2026 Venkat Chaitanya Duggineni

This project is licensed under the **Business Source License (BSL) 1.1**.

You may copy, modify, and run this software for personal use, evaluation, research, or internal business use. Commercial use, including offering this software as a hosted or managed service, requires a commercial license from the author.

On **2029-03-13**, this project will automatically convert to the **MIT License**.
