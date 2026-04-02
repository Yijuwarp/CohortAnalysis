"""
FastAPI router for Paths (Sequence Analysis) with CRUD and execution.
"""
from fastapi import APIRouter, Depends, HTTPException
import duckdb
from typing import List
from app.db.connection import get_connection
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
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return list_paths(conn)

@router.post("/paths", response_model=PathDetail)
async def create_path_endpoint(
    request: CreatePathRequest,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    try:
        return create_path(conn, request.name, request.steps, request.max_step_gap_minutes)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/paths/{path_id}", response_model=PathDetail)
async def update_path_endpoint(
    path_id: int,
    request: UpdatePathRequest,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    try:
        return update_path(conn, path_id, request.name, request.steps, request.max_step_gap_minutes)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/paths/{path_id}")
async def delete_path_endpoint(
    path_id: int,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    try:
        delete_path(conn, path_id)
        return {"deleted": True, "id": path_id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/paths/run")
async def run_paths_endpoint(
    request: RunPathsRequest,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return run_paths(conn, request.steps, request.max_step_gap_minutes, request.path_id)

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
        request.max_step_gap_minutes,
        request.path_id,
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
        request.max_step_gap_minutes,
        request.path_id,
        request.cohort_name
    )
