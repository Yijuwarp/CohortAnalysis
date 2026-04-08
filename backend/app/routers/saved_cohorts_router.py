from fastapi import APIRouter
from app.db.connection import run_query
from app.models.cohort_models import SavedCohortCreate
from app.domains.cohorts.saved_cohort_service import (
    create_saved_cohort,
    get_all_saved_cohorts,
    get_saved_cohort,
    update_saved_cohort,
    delete_saved_cohort
)

router = APIRouter()

@router.post("/saved-cohorts")
async def create_saved_cohort_endpoint(
    user_id: str,
    request: SavedCohortCreate,
):
    return run_query(user_id, lambda conn: create_saved_cohort(conn, request))

@router.get("/saved-cohorts")
async def get_all_saved_cohorts_endpoint(user_id: str):
    return run_query(user_id, get_all_saved_cohorts)

@router.get("/saved-cohorts/{cohort_id}")
async def get_saved_cohort_endpoint(
    user_id: str,
    cohort_id: str,
):
    return run_query(user_id, lambda conn: get_saved_cohort(conn, cohort_id))

@router.put("/saved-cohorts/{cohort_id}")
async def update_saved_cohort_endpoint(
    user_id: str,
    cohort_id: str,
    request: SavedCohortCreate,
):
    return run_query(user_id, lambda conn: update_saved_cohort(conn, cohort_id, request))

@router.delete("/saved-cohorts/{cohort_id}")
async def delete_saved_cohort_endpoint(
    user_id: str,
    cohort_id: str,
):
    return run_query(user_id, lambda conn: delete_saved_cohort(conn, cohort_id))
