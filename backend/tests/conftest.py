from pathlib import Path
import duckdb
import pytest
from tests.utils import DeterministicTestClient, DETERMINISTIC_USER_ID
from app.db import connection as db_conn
from app import main

@pytest.fixture()
def test_users_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolates user databases to a temporary directory for each test."""
    # CLEAR REGISTRY to prevent state leakage between tests
    with db_conn._REGISTRY_LOCK:
        for conn in db_conn._USER_CONNECTIONS.values():
            try:
                conn.close()
            except:
                pass
        db_conn._USER_CONNECTIONS.clear()
        db_conn._USER_LOCKS.clear()
        db_conn._INITIALIZED_DBS.clear()
        db_conn._LAST_ACCESS.clear()

    users_path = tmp_path / "data" / "users"
    users_path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(db_conn, "BASE_USERS_PATH", users_path)
    return users_path


@pytest.fixture()
def client(test_users_dir: Path) -> DeterministicTestClient:
    del test_users_dir
    return DeterministicTestClient(main.app)

@pytest.fixture()
def db_connection(test_users_dir: Path) -> duckdb.DuckDBPyConnection:
    from app.db.connection import get_user_db_path
    db_path = get_user_db_path(DETERMINISTIC_USER_ID)
    connection = duckdb.connect(str(db_path))
    try:
        yield connection
    finally:
        connection.close()

