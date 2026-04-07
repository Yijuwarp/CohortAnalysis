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

def get_user_lock(user_id: str) -> threading.RLock:
    """Provides a thread-safe recursive lock for a specific user_id."""
    with _locks_lock:
        if user_id not in _user_locks:
            _user_locks[user_id] = threading.RLock()
        return _user_locks[user_id]
