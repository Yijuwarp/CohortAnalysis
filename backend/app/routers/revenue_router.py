from fastapi import APIRouter, Depends
import duckdb
from app.db.connection import get_connection
from app.models.revenue_models import UpdateRevenueConfigRequest
from app.domains.revenue.revenue_config_service import (
    get_revenue_config_events,
    get_revenue_events,
    update_revenue_config,
)

router = APIRouter()

@router.get("/revenue-config-events")
async def revenue_config_events_endpoint(
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return get_revenue_config_events(conn)

@router.get("/revenue-events")
async def revenue_events_endpoint(
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return get_revenue_events(conn)

@router.post("/update-revenue-config")
async def update_revenue_config_endpoint(
    request: UpdateRevenueConfigRequest,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return update_revenue_config(conn, request)
