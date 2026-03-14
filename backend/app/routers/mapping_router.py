from fastapi import APIRouter, Depends
import duckdb
from app.db.connection import get_connection
from app.models.ingestion_models import ColumnMappingRequest
from app.domains.ingestion.mapping_service import map_columns

router = APIRouter()

@router.post("/map-columns")
async def map_columns_endpoint(
    request: ColumnMappingRequest,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return map_columns(conn, request)
