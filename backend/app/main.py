"""
Short summary: creates the FastAPI application and registers routers.
"""
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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
