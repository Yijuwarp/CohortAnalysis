from fastapi import APIRouter, Depends
import duckdb
from app.db.connection import get_connection
from app.models.filter_models import ApplyFiltersRequest
from app.domains.scope.filter_service import apply_filters

router = APIRouter()

@router.post("/apply-filters")
async def apply_filters_endpoint(
    request: ApplyFiltersRequest,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return apply_filters(conn, request)
