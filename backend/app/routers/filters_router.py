"""
Short summary: exposes scope and filtering endpoints.
"""
from fastapi import APIRouter

from app.domains import legacy_api
from app.models.filter_models import ApplyFiltersRequest

router = APIRouter()


@router.post("/apply-filters")
def apply_filters(payload: ApplyFiltersRequest) -> dict[str, object]:
    return legacy_api.apply_filters(payload)


@router.get("/scope")
def get_scope() -> dict[str, object]:
    return legacy_api.get_scope()
