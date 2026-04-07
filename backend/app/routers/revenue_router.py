from fastapi import APIRouter, Depends
import duckdb
from app.db.connection import get_connection, get_db
from app.utils.db_utils import get_user_lock
from app.models.revenue_models import UpdateRevenueConfigRequest
from app.domains.revenue.revenue_config_service import (
    get_revenue_config_events,
    get_revenue_events,
    update_revenue_config,
)

router = APIRouter()

@router.get("/revenue-config-events")
async def revenue_config_events_endpoint(
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
):
    return get_revenue_config_events(conn)

@router.get("/revenue-events")
async def revenue_events_endpoint(
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
):
    return get_revenue_events(conn)

@router.post("/update-revenue-config")
async def update_revenue_config_endpoint(
    user_id: str,
    request: UpdateRevenueConfigRequest,
):
    with get_user_lock(user_id):
        with get_connection(user_id) as conn:
            return update_revenue_config(conn, request)
