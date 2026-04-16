"""
Short summary: provides utilities for converting database results to named formats.
"""
from typing import Any, Iterable
import threading

def to_dict(cursor: Any, row: Any) -> dict[str, Any]:
    """
    Converts a single cursor row tuple to a dictionary using cursor description.
    """
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    if cursor.description is None:
        return {}
    column_names = [d[0] for d in cursor.description]
    return dict(zip(column_names, row))

def to_dicts(cursor: Any, rows: Iterable[Any]) -> list[dict[str, Any]]:
    """
    Converts multiple cursor row tuples to a list of dictionaries.
    """
    if cursor.description is None:
        return []
    column_names = [d[0] for d in cursor.description]
    result = []
    for row in rows:
        if isinstance(row, dict):
            result.append(row)
        else:
            result.append(dict(zip(column_names, row)))
    return result

_user_locks = {}
_locks_lock = threading.Lock()

_table_existence_cache = {}
_cache_lock = threading.Lock()

def get_user_lock(user_id: str) -> threading.RLock:
    """Provides a thread-safe recursive lock for a specific user_id."""
    with _locks_lock:
        if user_id not in _user_locks:
            _user_locks[user_id] = threading.RLock()
        return _user_locks[user_id]

def check_table_exists(connection: Any, table_name: str) -> bool:
    """Checks if a table exists, using a process-local cache to reduce catalog overhead."""
    # Note: DuckDB connections are per-thread/request in our app, so we key by id(connection)
    # and table_name. This cache should be cleared on mapping/filtering changes.
    cache_key = (id(connection), table_name)
    with _cache_lock:
        if cache_key in _table_existence_cache:
            return _table_existence_cache[cache_key]
    
    res = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ? AND table_schema = 'main'",
        [table_name]
    ).fetchone()[0]
    exists = bool(res > 0)
    
    with _cache_lock:
        _table_existence_cache[cache_key] = exists
        
    return exists

def clear_schema_cache():
    """Clears the table existence cache. Call this after DDL operations."""
    with _cache_lock:
        _table_existence_cache.clear()
