from fastapi import APIRouter
from app.db.connection import run_query
from app.models.filter_models import ApplyFiltersRequest
from app.domains.scope.filter_service import apply_filters

router = APIRouter()

@router.post("/apply-filters")
async def apply_filters_endpoint(
    user_id: str,
    request: ApplyFiltersRequest,
):
    return run_query(user_id, lambda conn: apply_filters(conn, request))
