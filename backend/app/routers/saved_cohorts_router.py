from fastapi import APIRouter, Depends
import duckdb
from app.db.connection import get_db, get_connection
from app.utils.db_utils import get_user_lock
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
    with get_user_lock(user_id):
        with get_connection(user_id) as conn:
            return create_saved_cohort(conn, request)

@router.get("/saved-cohorts")
async def get_all_saved_cohorts_endpoint(
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
):
    return get_all_saved_cohorts(conn)

@router.get("/saved-cohorts/{cohort_id}")
async def get_saved_cohort_endpoint(
    cohort_id: str,
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
):
    return get_saved_cohort(conn, cohort_id)

@router.put("/saved-cohorts/{cohort_id}")
async def update_saved_cohort_endpoint(
    user_id: str,
    cohort_id: str,
    request: SavedCohortCreate,
):
    with get_user_lock(user_id):
        with get_connection(user_id) as conn:
            return update_saved_cohort(conn, cohort_id, request)

@router.delete("/saved-cohorts/{cohort_id}")
async def delete_saved_cohort_endpoint(
    user_id: str,
    cohort_id: str,
):
    with get_user_lock(user_id):
        with get_connection(user_id) as conn:
            return delete_saved_cohort(conn, cohort_id)
