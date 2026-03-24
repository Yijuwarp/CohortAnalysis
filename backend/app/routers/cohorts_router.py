from fastapi import APIRouter, Depends
import duckdb
from app.db.connection import get_connection
from app.models.cohort_models import CreateCohortRequest
from app.domains.cohorts.cohort_service import (
    create_cohort,
    list_cohorts,
    update_cohort,
    delete_cohort,
    random_split_cohort,
    toggle_cohort_hide,
    get_cohort_detail,
)
from app.models.cohort_models import SavedCohortCreate
from app.domains.cohorts.saved_cohort_service import estimate_cohort

router = APIRouter()

@router.post("/cohorts")
async def create_cohort_endpoint(
    request: CreateCohortRequest,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return create_cohort(conn, request)

@router.post("/cohorts/estimate")
async def estimate_cohort_endpoint(
    request: SavedCohortCreate,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return estimate_cohort(conn, request)

@router.get("/cohorts")
async def list_cohorts_endpoint(
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return list_cohorts(conn)

@router.put("/cohorts/{cohort_id}")
async def update_cohort_endpoint(
    cohort_id: int,
    request: CreateCohortRequest,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return update_cohort(conn, cohort_id, request)

@router.delete("/cohorts/{cohort_id}")
async def delete_cohort_endpoint(
    cohort_id: int,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return delete_cohort(conn, cohort_id)

@router.post("/cohorts/{cohort_id}/random_split")
async def split_cohort_endpoint(
    cohort_id: int,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return random_split_cohort(conn, cohort_id)

@router.patch("/cohorts/{cohort_id}/hide")
async def toggle_hide_endpoint(
    cohort_id: int,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return toggle_cohort_hide(conn, cohort_id)


@router.get("/cohorts/{cohort_id}")
async def get_cohort_detail_endpoint(
    cohort_id: int,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return get_cohort_detail(conn, cohort_id)
