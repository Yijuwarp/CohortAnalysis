"""
Short summary: exposes cohort management endpoints.
"""
from fastapi import APIRouter

from app.domains import legacy_api
from app.models.cohort_models import CreateCohortRequest

router = APIRouter()


@router.post("/cohorts")
def create_cohort(payload: CreateCohortRequest) -> dict[str, int]:
    return legacy_api.create_cohort(payload)


@router.get("/cohorts")
def list_cohorts() -> dict[str, list[dict[str, object]]]:
    return legacy_api.list_cohorts()


@router.put("/cohorts/{cohort_id}")
def update_cohort(cohort_id: int, payload: CreateCohortRequest) -> dict[str, int]:
    return legacy_api.update_cohort(cohort_id, payload)


@router.delete("/cohorts/{cohort_id}")
def delete_cohort(cohort_id: int) -> dict[str, int | bool]:
    return legacy_api.delete_cohort(cohort_id)


@router.patch("/cohorts/{cohort_id}/hide")
def toggle_cohort_hide(cohort_id: int) -> dict[str, object]:
    return legacy_api.toggle_cohort_hide(cohort_id)
