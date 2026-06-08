from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse, parse_qs
from typing import Any
import duckdb
import pytest
from fastapi.testclient import TestClient

from app import main
import app.db.connection

DEFAULT_TEST_USER_ID = "00000000"

class DefaultUserTestClient(TestClient):
    def request(self, method: str, url: str, *args: Any, **kwargs: Any) -> Any:
        params = kwargs.get("params")
        has_user_id = False
        if params is not None:
            if isinstance(params, dict):
                has_user_id = "user_id" in params
            elif isinstance(params, (list, tuple)):
                has_user_id = any(k == "user_id" for k, v in params)
            
        if not has_user_id:
            parsed = urlparse(url)
            query_params = parse_qs(parsed.query)
            if "user_id" not in query_params:
                if params is not None:
                    if isinstance(params, dict):
                        params["user_id"] = DEFAULT_TEST_USER_ID
                    elif isinstance(params, list):
                        params.append(("user_id", DEFAULT_TEST_USER_ID))
                else:
                    separator = "&" if parsed.query else "?"
                    url = f"{url}{separator}user_id={DEFAULT_TEST_USER_ID}"
        return super().request(method, url, *args, **kwargs)


@pytest.fixture(autouse=True)
def isolate_database_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # 1. Close and clean up any connections from the previous test to prevent reuse
    with app.db.connection._REGISTRY_LOCK:
        for conn in list(app.db.connection._USER_CONNECTIONS.values()):
            try:
                conn.close()
            except Exception:
                pass
        app.db.connection._USER_CONNECTIONS.clear()
        app.db.connection._USER_LOCKS.clear()
        app.db.connection._INITIALIZED_DBS.clear()
        app.db.connection._LAST_ACCESS.clear()
        
    # 2. Redirect BASE_USERS_PATH to a temporary test directory
    test_users_path = tmp_path / "data" / "users"
    monkeypatch.setattr(app.db.connection, "BASE_USERS_PATH", test_users_path)


@pytest.fixture()
def client() -> TestClient:
    return DefaultUserTestClient(main.app)


@pytest.fixture()
def db_connection(isolate_database_directory: Any) -> duckdb.DuckDBPyConnection:
    # Always return a connection to the default test user's isolated DB
    conn, _ = app.db.connection.get_connection(DEFAULT_TEST_USER_ID)
    return conn
