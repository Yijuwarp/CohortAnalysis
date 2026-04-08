from fastapi import APIRouter
from app.db.connection import run_query
from app.models.cohort_models import CreateCohortRequest, SplitRequest, SavedCohortCreate
from app.domains.cohorts.cohort_service import (
    create_cohort,
    list_cohorts,
    update_cohort,
    delete_cohort,
    random_split_cohort,
    split_cohort,
    preview_split,
    toggle_cohort_hide,
    get_cohort_detail,
)
from app.domains.cohorts.saved_cohort_service import estimate_cohort

router = APIRouter()

@router.post("/cohorts")
async def create_cohort_endpoint(
    user_id: str,
    request: CreateCohortRequest,
):
    return run_query(user_id, lambda conn: create_cohort(conn, request))

@router.post("/cohorts/estimate")
async def estimate_cohort_endpoint(
    user_id: str,
    request: SavedCohortCreate,
):
    """
    Computes cohort size estimates based on input conditions.
    Even for read-only analytical queries, we wrap in run_query
    to ensure structural thread-safety on Windows.
    """
    return run_query(user_id, lambda conn: estimate_cohort(conn, request))

@router.get("/cohorts")
async def list_cohorts_endpoint(user_id: str):
    return run_query(user_id, list_cohorts)

@router.put("/cohorts/{cohort_id}")
async def update_cohort_endpoint(
    user_id: str,
    cohort_id: int,
    request: CreateCohortRequest,
):
    return run_query(user_id, lambda conn: update_cohort(conn, cohort_id, request))

@router.delete("/cohorts/{cohort_id}")
async def delete_cohort_endpoint(
    user_id: str,
    cohort_id: int,
):
    return run_query(user_id, lambda conn: delete_cohort(conn, cohort_id))

# Unified split endpoint
@router.post("/cohorts/{cohort_id}/split")
async def split_cohort_endpoint(
    user_id: str,
    cohort_id: int,
    request: SplitRequest,
):
    return run_query(user_id, lambda conn: split_cohort(conn, cohort_id, request))

# Preview endpoint (no persistence from the split itself)
@router.post("/cohorts/{cohort_id}/split/preview")
async def preview_split_endpoint(
    user_id: str,
    request: SplitRequest,
    cohort_id: int,
):
    return run_query(user_id, lambda conn: preview_split(conn, cohort_id, request))

# Backward-compat legacy endpoint
@router.post("/cohorts/{cohort_id}/random_split")
async def legacy_random_split_endpoint(
    user_id: str,
    cohort_id: int,
):
    return run_query(user_id, lambda conn: random_split_cohort(conn, cohort_id))

@router.patch("/cohorts/{cohort_id}/hide")
async def toggle_hide_endpoint(
    user_id: str,
    cohort_id: int,
):
    return run_query(user_id, lambda conn: toggle_cohort_hide(conn, cohort_id))


@router.get("/cohorts/{cohort_id}")
async def get_cohort_detail_endpoint(
    user_id: str,
    cohort_id: int,
):
    return run_query(user_id, lambda conn: get_cohort_detail(conn, cohort_id))
