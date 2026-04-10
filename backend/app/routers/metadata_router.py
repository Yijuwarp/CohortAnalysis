from fastapi import APIRouter
from app.db.connection import run_query
from app.domains.scope.scope_metadata import (
    get_scope,
    get_columns,
    get_column_values,
    get_date_range,
)

router = APIRouter()

@router.get("/scope")
async def get_scope_endpoint(user_id: str):
    return run_query(user_id, get_scope)

@router.get("/columns")
async def get_columns_endpoint(user_id: str):
    return run_query(user_id, get_columns)

@router.get("/column-values")
async def get_column_values_endpoint(
    user_id: str,
    column: str,
    event_name: str | None = None,
    search: str | None = None,
    limit: int = 100,
):
    # Strict limit enforcement
    safe_limit = min(limit, 100)
    return run_query(user_id, lambda conn: get_column_values(conn, column, event_name, search, safe_limit))

@router.get("/date-range")
async def get_date_range_endpoint(user_id: str):
    return run_query(user_id, get_date_range)
