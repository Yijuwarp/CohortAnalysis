import logging
import os
import re
import shutil
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from fastapi import HTTPException, Query

# ---------------- CONFIGURATION & PATHS ----------------

# Base path for user-specific databases - resolve relative to this file to avoid nesting issues
_ROOT = Path(__file__).resolve().parent.parent.parent
BASE_USERS_PATH = _ROOT / "data" / "users"
MIGRATION_MARKER = BASE_USERS_PATH / ".migration_complete"
MIGRATION_IN_PROGRESS = BASE_USERS_PATH / ".migration_in_progress"

logger = logging.getLogger(__name__)

def get_user_db_path(user_id: str) -> Path:
    """Returns the absolute path to a user's DuckDB file."""
    if not user_id or not re.match(r"^[a-f0-9]{8}$", user_id):
        raise HTTPException(status_code=400, detail="Invalid user_id format")
    # Ensure the directory exists
    BASE_USERS_PATH.mkdir(parents=True, exist_ok=True)
    return BASE_USERS_PATH / f"user_{user_id}.duckdb"

def _ensure_data_moved():
    """
    Atomic migration from legacy nested backend/backend/data to clean backend/data.
    Uses copy-then-delete for Windows safety.
    """
    legacy_path = _ROOT / "backend" / "data" / "users"
    if not legacy_path.exists() or MIGRATION_MARKER.exists():
        return

    # Create in-progress marker
    BASE_USERS_PATH.mkdir(parents=True, exist_ok=True)
    MIGRATION_IN_PROGRESS.touch()

    try:
        logger.info(f"Starting data migration from {legacy_path} to {BASE_USERS_PATH}")
        for db_file in legacy_path.glob("user_*.duckdb"):
            dest = BASE_USERS_PATH / db_file.name
            if not dest.exists() or dest.stat().st_size < db_file.stat().st_size:
                logger.info(f"Copying {db_file.name} to {BASE_USERS_PATH}")
                shutil.copy2(db_file, dest)
                # Verify copy
                if dest.stat().st_size != db_file.stat().st_size:
                    raise RuntimeError(f"Migration failed: file size mismatch for {db_file.name}")
                os.remove(db_file)
        
        # Cleanup legacy directories if empty
        try:
            if not any(legacy_path.iterdir()):
                shutil.rmtree(legacy_path)
                # Try to cleanup parent 'backend' if empty
                parent_legacy = legacy_path.parent
                if not any(parent_legacy.iterdir()):
                    shutil.rmtree(parent_legacy)
        except Exception as e:
            logger.warning(f"Failed to cleanup legacy directories: {e}")

        MIGRATION_MARKER.touch()
        logger.info("Data migration completed successfully.")
    finally:
        if MIGRATION_IN_PROGRESS.exists():
            os.remove(MIGRATION_IN_PROGRESS)

# Run migration once on module import
_ensure_data_moved()

# ---------------- CONNECTION REGISTRY ----------------

# Per-process registry for thread-safe persistent user connections
_USER_CONNECTIONS: dict[str, duckdb.DuckDBPyConnection] = {}
_USER_LOCKS: dict[str, threading.RLock] = {}
_INITIALIZED_DBS: set[str] = set()
_LAST_ACCESS: dict[str, float] = {}

_REGISTRY_LOCK = threading.Lock()

def _cleanup_idle_connections():
    """Background thread to close connections idle for >30m."""
    while True:
        time.sleep(300) # Check every 5 minutes
        now = time.time()
        to_close = []
        
        with _REGISTRY_LOCK:
            for user_id, last_time in _LAST_ACCESS.items():
                if now - last_time > 1800: # 30 minutes
                    to_close.append(user_id)
        
        for user_id in to_close:
            lock = _USER_LOCKS.get(user_id)
            if lock and lock.acquire(blocking=False):
                try:
                    conn = _USER_CONNECTIONS.pop(user_id, None)
                    if conn:
                        logger.info(f"Closing idle connection for user {user_id}")
                        conn.close()
                    _USER_LOCKS.pop(user_id, None)
                    _INITIALIZED_DBS.discard(user_id)
                    with _REGISTRY_LOCK:
                        _LAST_ACCESS.pop(user_id, None)
                finally:
                    lock.release()

# Start background cleanup thread
threading.Thread(target=_cleanup_idle_connections, daemon=True).start()

def get_connection(user_id: str) -> tuple[duckdb.DuckDBPyConnection, threading.RLock]:
    """
    Retrieves or creates a persistent connection and lock for a user.
    """
    with _REGISTRY_LOCK:
        if user_id not in _USER_LOCKS:
            _USER_LOCKS[user_id] = threading.RLock()
        
        lock = _USER_LOCKS[user_id]
        
        if user_id not in _USER_CONNECTIONS:
            path = get_user_db_path(user_id)
            if not path.exists():
                logger.info(f"Creating new database file for user {user_id}")
            
            try:
                conn = duckdb.connect(str(path))
                _USER_CONNECTIONS[user_id] = conn
                logger.info(f"Connected to database for user {user_id}")
            except Exception as e:
                logger.error(f"Failed to connect to database for user {user_id}: {e}")
                raise HTTPException(status_code=500, detail=f"Database connection failed: {e}")
                
        _LAST_ACCESS[user_id] = time.time()
        return _USER_CONNECTIONS[user_id], lock

def run_query(user_id: str, fn):
    """
    Structural API to enforce thread-safety for synchronous functions.
    Handles double-checked initialization and exception-safe connection resets.
    """
    conn, lock = get_connection(user_id)
    
    with lock:
        try:
            # Double-checked initialization
            if user_id not in _INITIALIZED_DBS:
                logger.info(f"Initializing schema for user {user_id}")
                from app.db.schema_init import ensure_base_schema
                ensure_base_schema(conn)
                _INITIALIZED_DBS.add(user_id)
            
            _LAST_ACCESS[user_id] = time.time()
            return fn(conn)
            
        except Exception as e:
            logger.exception(f"Database operation failed for user {user_id}")
            # Reset poisoned connection
            with _REGISTRY_LOCK:
                _USER_CONNECTIONS.pop(user_id, None)
                _USER_LOCKS.pop(user_id, None)
                _INITIALIZED_DBS.discard(user_id)
                _LAST_ACCESS.pop(user_id, None)
            try:
                conn.close()
            except:
                pass
            raise

async def async_run_query(user_id: str, fn):
    """
    Structural API to enforce thread-safety for asynchronous functions.
    """
    conn, lock = get_connection(user_id)
    
    with lock:
        try:
            # Double-checked initialization
            if user_id not in _INITIALIZED_DBS:
                logger.info(f"Initializing schema for user {user_id}")
                from app.db.schema_init import ensure_base_schema
                ensure_base_schema(conn)
                _INITIALIZED_DBS.add(user_id)
            
            _LAST_ACCESS[user_id] = time.time()
            return await fn(conn)
            
        except Exception as e:
            logger.exception(f"Async database operation failed for user {user_id}")
            # Reset poisoned connection
            with _REGISTRY_LOCK:
                _USER_CONNECTIONS.pop(user_id, None)
                _USER_LOCKS.pop(user_id, None)
                _INITIALIZED_DBS.discard(user_id)
                _LAST_ACCESS.pop(user_id, None)
            try:
                conn.close()
            except:
                pass
            raise

def get_db(user_id: str = Query(...)):
    """
    FastAPI dependency.
    YIELDS the connection but the router MUST use it within the functional API
    for secondary calls. For standard usage, we provide a thread-safe wrapper.
    """
    if not user_id or not re.match(r"^[a-f0-9]{8}$", user_id):
        raise HTTPException(status_code=400, detail="Invalid user_id format")
    
    # We yield the connection, but we NO LONGER hold the lock across the yield
    # to avoid "cannot release un-acquired lock" on Windows thread-pools.
    # All routers SHOULD use run_query(user_id, fn) for thread-safety.
    conn, lock = get_connection(user_id)
    
    # Ensure initialized (using lock briefly)
    if user_id not in _INITIALIZED_DBS:
        with lock:
            if user_id not in _INITIALIZED_DBS:
                from app.db.schema_init import ensure_base_schema
                ensure_base_schema(conn)
                _INITIALIZED_DBS.add(user_id)
    yield conn
