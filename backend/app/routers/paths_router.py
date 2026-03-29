"""
Short summary: FastAPI router for Paths (Sequence Analysis).
"""
from fastapi import APIRouter, Depends
import duckdb
from app.db.connection import get_connection
from app.models.paths_models import RunPathsRequest, CreateDropOffCohortRequest, CreateReachedCohortRequest
from app.domains.paths.paths_service import run_paths, create_paths_dropoff_cohort, create_paths_reached_cohort

router = APIRouter()

@router.post("/paths/run")
async def run_paths_endpoint(
    request: RunPathsRequest,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return run_paths(conn, request.steps)

@router.post("/paths/create-dropoff-cohort")
async def create_dropoff_cohort_endpoint(
    request: CreateDropOffCohortRequest,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return create_paths_dropoff_cohort(
        conn, 
        request.cohort_id, 
        request.step_index, 
        request.steps,
        request.cohort_name
    )

@router.post("/paths/create-reached-cohort")
async def create_reached_cohort_endpoint(
    request: CreateReachedCohortRequest,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return create_paths_reached_cohort(
        conn, 
        request.cohort_id, 
        request.step_index, 
        request.steps,
        request.cohort_name
    )
