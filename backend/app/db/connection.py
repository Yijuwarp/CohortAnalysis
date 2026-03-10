"""
Short summary: provides DuckDB connections using the runtime database path.
"""
from pathlib import Path

import duckdb


_DEFAULT_PATH = Path(__file__).resolve().parents[2] / "cohort_analysis.duckdb"


def get_database_path() -> Path:
    try:
        from app import main as app_main

        return Path(getattr(app_main, "DATABASE_PATH", _DEFAULT_PATH))
    except Exception:
        return _DEFAULT_PATH


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(get_database_path()))
