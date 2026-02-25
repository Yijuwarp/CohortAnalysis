from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from app import main


@pytest.fixture()
def test_db_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "test.duckdb"
    monkeypatch.setattr(main, "DATABASE_PATH", db_path)
    return db_path


@pytest.fixture()
def client(test_db_path: Path) -> TestClient:
    del test_db_path
    return TestClient(main.app)


@pytest.fixture()
def db_connection(test_db_path: Path) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(str(test_db_path))
    try:
        yield connection
    finally:
        connection.close()

