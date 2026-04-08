from fastapi import APIRouter
from app.db.connection import run_query
from app.models.revenue_models import UpdateRevenueConfigRequest
from app.domains.revenue.revenue_config_service import (
    get_revenue_config_events,
    get_revenue_events,
    update_revenue_config,
)

router = APIRouter()

@router.get("/revenue-config-events")
async def revenue_config_events_endpoint(user_id: str):
    return run_query(user_id, get_revenue_config_events)

@router.get("/revenue-events")
async def revenue_events_endpoint(user_id: str):
    return run_query(user_id, get_revenue_events)

@router.post("/update-revenue-config")
async def update_revenue_config_endpoint(
    user_id: str,
    request: UpdateRevenueConfigRequest,
):
    return run_query(user_id, lambda conn: update_revenue_config(conn, request))
