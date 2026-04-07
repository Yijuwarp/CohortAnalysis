from fastapi import APIRouter, Depends
import duckdb
from app.db.connection import get_connection, get_db
from app.utils.db_utils import get_user_lock
from app.models.cohort_models import CreateCohortRequest, SplitRequest
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
from app.models.cohort_models import SavedCohortCreate
from app.domains.cohorts.saved_cohort_service import estimate_cohort

router = APIRouter()

@router.post("/cohorts")
async def create_cohort_endpoint(
    user_id: str,
    request: CreateCohortRequest,
):
    with get_user_lock(user_id):
        with get_connection(user_id) as conn:
            return create_cohort(conn, request)

@router.post("/cohorts/estimate")
async def estimate_cohort_endpoint(
    request: SavedCohortCreate,
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
):
    """
    Computes cohort size estimates based on input conditions.
    READ ONLY: This service does not mutate the database, allowing
    for concurrent execution across multiple analytical requests.
    """
    return estimate_cohort(conn, request)

@router.get("/cohorts")
async def list_cohorts_endpoint(
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
):
    return list_cohorts(conn)

@router.put("/cohorts/{cohort_id}")
async def update_cohort_endpoint(
    user_id: str,
    cohort_id: int,
    request: CreateCohortRequest,
):
    with get_user_lock(user_id):
        with get_connection(user_id) as conn:
            return update_cohort(conn, cohort_id, request)

@router.delete("/cohorts/{cohort_id}")
async def delete_cohort_endpoint(
    user_id: str,
    cohort_id: int,
):
    with get_user_lock(user_id):
        with get_connection(user_id) as conn:
            return delete_cohort(conn, cohort_id)

# Unified split endpoint
@router.post("/cohorts/{cohort_id}/split")
async def split_cohort_endpoint(
    user_id: str,
    cohort_id: int,
    request: SplitRequest,
):
    with get_user_lock(user_id):
        with get_connection(user_id) as conn:
            return split_cohort(conn, cohort_id, request)

# Preview endpoint (no persistence)
@router.post("/cohorts/{cohort_id}/split/preview")
async def preview_split_endpoint(
    request: SplitRequest,
    cohort_id: int,
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
):
    return preview_split(conn, cohort_id, request)

# Backward-compat legacy endpoint
@router.post("/cohorts/{cohort_id}/random_split")
async def legacy_random_split_endpoint(
    user_id: str,
    cohort_id: int,
):
    with get_user_lock(user_id):
        with get_connection(user_id) as conn:
            return random_split_cohort(conn, cohort_id)

@router.patch("/cohorts/{cohort_id}/hide")
async def toggle_hide_endpoint(
    user_id: str,
    cohort_id: int,
):
    with get_user_lock(user_id):
        with get_connection(user_id) as conn:
            return toggle_cohort_hide(conn, cohort_id)


@router.get("/cohorts/{cohort_id}")
async def get_cohort_detail_endpoint(
    cohort_id: int,
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
):
    return get_cohort_detail(conn, cohort_id)
