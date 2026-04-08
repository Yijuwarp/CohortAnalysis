from fastapi import APIRouter
from app.db.connection import run_query
from app.models.ingestion_models import ColumnMappingRequest
from app.domains.ingestion.mapping_service import map_columns

router = APIRouter()

@router.post("/map-columns")
async def map_columns_endpoint(
    user_id: str,
    request: ColumnMappingRequest,
):
    return run_query(user_id, lambda conn: map_columns(conn, request))
