"""
Short summary: exposes retention and usage analytics endpoints.
"""
from fastapi import APIRouter, Query

from app import main as app_main
from app.domains import legacy_api

router = APIRouter()


@router.get("/retention")
def get_retention(
    max_day: int = Query(7, ge=0),
    retention_event: str | None = Query(None),
    include_ci: bool = Query(False),
    confidence: float = Query(0.95),
) -> dict[str, object]:
    return app_main.get_retention(
        max_day=max_day,
        retention_event=retention_event,
        include_ci=include_ci,
        confidence=confidence,
    )


@router.get("/usage")
def get_usage(
    event: str = Query(...),
    max_day: int = Query(7, ge=0),
    retention_event: str | None = Query(None),
    property: str | None = Query(None),
    operator: str = Query("="),
    value: str | None = Query(None),
) -> dict[str, object]:
    return legacy_api.get_usage(
        event=event,
        max_day=max_day,
        retention_event=retention_event,
        property=property,
        operator=operator,
        value=value,
    )


@router.get("/usage-frequency")
def get_usage_frequency(
    event: str = Query(...),
    property: str | None = Query(None),
    operator: str = Query("="),
    value: str | None = Query(None),
) -> dict[str, object]:
    return legacy_api.get_usage_frequency(event=event, property=property, operator=operator, value=value)


@router.get("/events/{event_name}/properties")
def get_event_properties(event_name: str) -> dict[str, list[str]]:
    return legacy_api.get_event_properties(event_name=event_name)


@router.get("/events/{event_name}/properties/{property}/values")
def get_event_property_values(event_name: str, property: str, limit: int = Query(25, ge=1, le=100)) -> dict[str, object]:
    return legacy_api.get_event_property_values(event_name=event_name, property=property, limit=limit)
