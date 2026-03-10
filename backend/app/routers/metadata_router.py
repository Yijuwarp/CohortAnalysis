"""
Short summary: exposes dataset metadata endpoints.
"""
from fastapi import APIRouter, Query

from app.domains import legacy_api

router = APIRouter()


@router.get("/columns")
def get_columns() -> dict[str, list[dict[str, str | None]]]:
    return legacy_api.get_columns()


@router.get("/column-values")
def get_column_values(column: str = Query(..., min_length=1), event_name: str | None = Query(default=None, min_length=1)) -> dict[str, list[str] | int]:
    return legacy_api.get_column_values(column=column, event_name=event_name)


@router.get("/date-range")
def get_date_range() -> dict[str, str | None]:
    return legacy_api.get_date_range()
