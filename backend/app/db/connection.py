import re
from pathlib import Path
import os
import duckdb
from contextlib import contextmanager
from fastapi import Query, HTTPException

# Base path for user-specific databases
BASE_USERS_PATH = Path("backend/data/users")

def get_user_db_path(user_id: str) -> Path:
    """Returns the absolute path to a user's DuckDB file."""
    if not user_id or not re.match(r"^[a-f0-9]{8}$", user_id):
        raise HTTPException(status_code=400, detail="Invalid user_id format")
    # Ensure the directory exists
    BASE_USERS_PATH.mkdir(parents=True, exist_ok=True)
    return BASE_USERS_PATH / f"user_{user_id}.duckdb"

# Per-process cache for initialized user databases to skip metadata checks
_INITIALIZED_DBS: set[str] = set()

@contextmanager
def get_connection(user_id: str):
    """
    Context manager for multi-user DuckDB connection.
    Caches initialization status to avoid redundant SQL checks on every request.
    """
    path = get_user_db_path(user_id)
    path_str = str(path)
    
    conn = duckdb.connect(path_str)
    try:
        if path_str not in _INITIALIZED_DBS:
            from app.db.schema_init import ensure_base_schema
            ensure_base_schema(conn)
            _INITIALIZED_DBS.add(path_str)
        yield conn
    finally:
        conn.close()

def get_db(user_id: str = Query(...)):
    """
    FastAPI dependency for read operations.
    Standardized 'conn' injection for routers.
    """
    if not user_id or not re.match(r"^[a-f0-9]{8}$", user_id):
        raise HTTPException(status_code=400, detail="Invalid user_id format")
    with get_connection(user_id) as conn:
        yield conn
