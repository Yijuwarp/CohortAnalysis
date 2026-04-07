from fastapi import APIRouter
from app.db.connection import get_connection
from app.utils.db_utils import get_user_lock
from app.models.ingestion_models import ColumnMappingRequest
from app.domains.ingestion.mapping_service import map_columns

router = APIRouter()

@router.post("/map-columns")
async def map_columns_endpoint(
    user_id: str,
    request: ColumnMappingRequest,
):
    with get_user_lock(user_id):
        with get_connection(user_id) as conn:
            return map_columns(conn, request)
