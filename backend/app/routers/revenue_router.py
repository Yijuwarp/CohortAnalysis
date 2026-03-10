"""
Short summary: exposes revenue and monetization endpoints.
"""
from fastapi import APIRouter, Query

from app.domains import legacy_api
from app.models.revenue_models import UpdateRevenueConfigRequest

router = APIRouter()


@router.get("/events")
def list_events() -> dict[str, list[str]]:
    return legacy_api.list_events()


@router.get("/revenue-config-events")
def get_revenue_config_events() -> dict[str, object]:
    return legacy_api.get_revenue_config_events()


@router.get("/revenue-events")
def get_revenue_events() -> dict[str, object]:
    return legacy_api.get_revenue_events()


@router.post("/update-revenue-config")
def update_revenue_config(payload: UpdateRevenueConfigRequest) -> dict[str, object]:
    return legacy_api.update_revenue_config(payload)


@router.get("/monetization")
def get_monetization(max_day: int = Query(7, ge=0)) -> dict[str, object]:
    return legacy_api.get_monetization(max_day=max_day)
