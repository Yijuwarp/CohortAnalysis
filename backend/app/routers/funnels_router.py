"""
Short summary: FastAPI router for funnel CRUD and execution.
"""
from fastapi import APIRouter, Depends
import duckdb
from app.db.connection import get_connection
from app.models.funnel_models import CreateFunnelRequest, RunFunnelRequest
from app.domains.funnels.funnel_service import (
    create_funnel,
    list_funnels,
    delete_funnel,
    run_funnel,
)

router = APIRouter()


@router.post("/funnels")
async def create_funnel_endpoint(
    request: CreateFunnelRequest,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    steps = [
        {
            "event_name": step.event_name,
            "filters": [
                {"property_key": f.property_key, "property_value": f.property_value}
                for f in step.filters
            ],
        }
        for step in request.steps
    ]
    return create_funnel(conn, request.name, steps)


@router.get("/funnels")
async def list_funnels_endpoint(
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return list_funnels(conn)


@router.delete("/funnels/{funnel_id}")
async def delete_funnel_endpoint(
    funnel_id: int,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return delete_funnel(conn, funnel_id)


@router.post("/funnels/run")
async def run_funnel_endpoint(
    request: RunFunnelRequest,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return run_funnel(conn, request.funnel_id)
