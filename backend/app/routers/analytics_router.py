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
def get_usage(event: str = Query(...), max_day: int = Query(7, ge=0), retention_event: str | None = Query(None)) -> dict[str, object]:
    return legacy_api.get_usage(event=event, max_day=max_day, retention_event=retention_event)


@router.get("/usage-frequency")
def get_usage_frequency(event: str = Query(...)) -> dict[str, object]:
    return legacy_api.get_usage_frequency(event=event)
