"""
Short summary: exposes revenue and monetization endpoints.
"""
from fastapi import APIRouter, Query

from app.domains import legacy_api
from app.models.revenue_models import UpdateRevenueConfigRequest

router = APIRouter()


def normalize_max_day(raw_max_day: str | None) -> int:
    """Normalize raw query input while preserving integer behavior for legacy NaN requests."""
    if raw_max_day is None:
        return 7

    try:
        parsed = int(float(raw_max_day))
    except (TypeError, ValueError):
        return 7

    if parsed <= 0:
        return 7

    return parsed


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
def get_monetization(
    max_day: str | None = Query(
        None,
        description="Raw max_day query value. Parsed to a positive integer; invalid values default to 7.",
    )
) -> dict[str, object]:
    # NOTE: this query is intentionally typed as string to gracefully handle "max_day=NaN"
    # from legacy/front-end callers while still coercing to an integer for domain logic.
    return legacy_api.get_monetization(max_day=normalize_max_day(max_day))
