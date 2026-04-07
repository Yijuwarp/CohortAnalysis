from fastapi import APIRouter
from app.db.connection import get_connection
from app.utils.db_utils import get_user_lock
from app.models.filter_models import ApplyFiltersRequest
from app.domains.scope.filter_service import apply_filters

router = APIRouter()

@router.post("/apply-filters")
async def apply_filters_endpoint(
    user_id: str,
    request: ApplyFiltersRequest,
):
    with get_user_lock(user_id):
        with get_connection(user_id) as conn:
            return apply_filters(conn, request)
