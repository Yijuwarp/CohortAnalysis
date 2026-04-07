"""
FastAPI router for Paths (Sequence Analysis) with CRUD and execution.
"""
from fastapi import APIRouter, Depends, HTTPException
import duckdb
from typing import List
from app.db.connection import get_connection, get_db
from app.utils.db_utils import get_user_lock
from app.models.paths_models import (
    RunPathsRequest, CreateDropOffCohortRequest, CreateReachedCohortRequest,
    CreatePathRequest, UpdatePathRequest, PathDetail
)
from app.domains.paths.paths_service import (
    run_paths, create_paths_dropoff_cohort, create_paths_reached_cohort,
    create_path, update_path, list_paths, delete_path
)

router = APIRouter()

@router.get("/paths", response_model=List[PathDetail])
async def list_paths_endpoint(
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
):
    return list_paths(conn)

@router.post("/paths", response_model=PathDetail)
async def create_path_endpoint(
    user_id: str,
    request: CreatePathRequest,
):
    with get_user_lock(user_id):
        with get_connection(user_id) as conn:
            try:
                return create_path(conn, request.name, request.steps, request.max_step_gap_minutes)
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

@router.put("/paths/{path_id}", response_model=PathDetail)
async def update_path_endpoint(
    user_id: str,
    path_id: int,
    request: UpdatePathRequest,
):
    with get_user_lock(user_id):
        with get_connection(user_id) as conn:
            try:
                return update_path(conn, path_id, request.name, request.steps, request.max_step_gap_minutes)
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

@router.delete("/paths/{path_id}")
async def delete_path_endpoint(
    user_id: str,
    path_id: int,
):
    with get_user_lock(user_id):
        with get_connection(user_id) as conn:
            try:
                delete_path(conn, path_id)
                return {"deleted": True, "id": path_id}
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

@router.post("/paths/run")
async def run_paths_endpoint(
    request: RunPathsRequest,
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
):
    return run_paths(conn, request.steps, request.max_step_gap_minutes, request.path_id)

@router.post("/paths/create-dropoff-cohort")
async def create_dropoff_cohort_endpoint(
    user_id: str,
    request: CreateDropOffCohortRequest,
):
    with get_user_lock(user_id):
        with get_connection(user_id) as conn:
            return create_paths_dropoff_cohort(
                conn, 
                request.cohort_id, 
                request.step_index, 
                request.steps,
                request.max_step_gap_minutes,
                request.path_id,
                request.cohort_name
            )

@router.post("/paths/create-reached-cohort")
async def create_reached_cohort_endpoint(
    user_id: str,
    request: CreateReachedCohortRequest,
):
    with get_user_lock(user_id):
        with get_connection(user_id) as conn:
            return create_paths_reached_cohort(
                conn, 
                request.cohort_id, 
                request.step_index, 
                request.steps,
                request.max_step_gap_minutes,
                request.path_id,
                request.cohort_name
            )
