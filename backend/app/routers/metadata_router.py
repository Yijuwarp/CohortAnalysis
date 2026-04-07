from fastapi import APIRouter, Depends
import duckdb
from app.db.connection import get_db
from app.domains.scope.scope_metadata import (
    get_scope,
    get_columns,
    get_column_values,
    get_date_range,
)

router = APIRouter()

@router.get("/scope")
async def get_scope_endpoint(
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
):
    return get_scope(conn)

@router.get("/columns")
async def get_columns_endpoint(
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
):
    return get_columns(conn)

@router.get("/column-values")
async def get_column_values_endpoint(
    column: str,
    event_name: str | None = None,
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
):
    return get_column_values(conn, column, event_name)

@router.get("/date-range")
async def get_date_range_endpoint(
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
):
    return get_date_range(conn)
