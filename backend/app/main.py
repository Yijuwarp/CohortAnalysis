import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Load environment variables from .env file
load_dotenv()

from app.routers import (
    analytics_router,
    cohorts_router,
    comparison_router,
    filters_router,
    mapping_router,
    metadata_router,
    revenue_router,
    upload_router,
    saved_cohorts_router,
    paths_router,
    impact_router,
    auth_router,
)
from app.domains.ingestion.type_detection import detect_column_type
from app.utils.math_utils import wilson_ci
from app.domains.analytics.retention_service import get_retention, build_active_cohort_base
from app.domains.cohorts.cohort_service import ensure_cohort_tables
from app.db.connection import get_connection as app_get_connection

DATABASE_PATH = Path(__file__).resolve().parent.parent / "cohort_analysis.duckdb"

def get_connection():
    return app_get_connection()

app = FastAPI(title="Behavioral Cohort Analysis API")

@app.on_event("startup")
async def startup_event():
    # 1. Enforce single worker on Windows
    if os.name == "nt":
        if os.environ.get("DUCKDB_SINGLE_WORKER") != "true":
            print("\n" + "="*60)
            print("CRITICAL ERROR: DuckDB requires a single worker on Windows.")
            print("Please set the environment variable DUCKDB_SINGLE_WORKER=true")
            print("and ensure you are running with --workers 1.")
            print("="*60 + "\n")
            # We don't exit(1) immediately to allow the user to see the message in some console environments,
            # but we could raise a RuntimeError.
            raise RuntimeError("DUCKDB_SINGLE_WORKER=true must be set on Windows")
    
    # 2. Verify path integrity
    from app.db.connection import BASE_USERS_PATH
    try:
        BASE_USERS_PATH.mkdir(parents=True, exist_ok=True)
        # Test write permission
        test_file = BASE_USERS_PATH / ".startup_test"
        test_file.touch()
        test_file.unlink()
    except Exception as e:
        print(f"CRITICAL ERROR: Cannot write to {BASE_USERS_PATH}: {e}")
        sys.exit(1)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

for router in (
    upload_router.router,
    mapping_router.router,
    filters_router.router,
    cohorts_router.router,
    analytics_router.router,
    revenue_router.router,
    metadata_router.router,
    comparison_router.router,
    saved_cohorts_router.router,
    paths_router.router,
    impact_router.router,
    auth_router.router,
):
    app.include_router(router)

__all__ = [
    "app",
    "DATABASE_PATH",
    "get_connection",
    "detect_column_type",
    "wilson_ci",
    "get_retention",
    "ensure_cohort_tables",
    "build_active_cohort_base",
]
