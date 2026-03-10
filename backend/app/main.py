"""
Short summary: creates the FastAPI application and registers routers.
"""
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import (
    analytics_router,
    cohorts_router,
    filters_router,
    mapping_router,
    metadata_router,
    revenue_router,
    upload_router,
)

DATABASE_PATH = Path(__file__).resolve().parent.parent / "cohort_analysis.duckdb"


def get_connection():
    from app.db.connection import get_connection as _get_connection

    return _get_connection()

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
):
    app.include_router(router)


from app.domains import legacy_api
from app.domains.legacy_api import detect_column_type, wilson_ci

ensure_cohort_tables = legacy_api.ensure_cohort_tables
build_active_cohort_base = legacy_api.build_active_cohort_base
fetch_retention_active_rows = legacy_api.fetch_retention_active_rows


def get_retention(
    max_day: int = 7,
    retention_event: str | None = None,
    include_ci: bool = False,
    confidence: float = 0.95,
):
    originals = (
        legacy_api.get_connection,
        legacy_api.ensure_cohort_tables,
        legacy_api.build_active_cohort_base,
        legacy_api.fetch_retention_active_rows,
    )
    legacy_api.get_connection = get_connection
    legacy_api.ensure_cohort_tables = ensure_cohort_tables
    legacy_api.build_active_cohort_base = build_active_cohort_base
    legacy_api.fetch_retention_active_rows = fetch_retention_active_rows
    try:
        return legacy_api.get_retention(
            max_day=max_day,
            retention_event=retention_event,
            include_ci=include_ci,
            confidence=confidence,
        )
    finally:
        (
            legacy_api.get_connection,
            legacy_api.ensure_cohort_tables,
            legacy_api.build_active_cohort_base,
            legacy_api.fetch_retention_active_rows,
        ) = originals

__all__ = ["app", "DATABASE_PATH", "get_connection", "detect_column_type", "wilson_ci", "get_retention", "ensure_cohort_tables", "build_active_cohort_base", "fetch_retention_active_rows"]
