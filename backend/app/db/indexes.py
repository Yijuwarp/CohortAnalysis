"""
Short summary: creates indexes used by scoped and analytics queries.
"""
import duckdb

def create_scoped_indexes(connection: duckdb.DuckDBPyConnection) -> None:
    # Since events_scoped is a VIEW, we cannot create indexes on it directly.
    # DuckDB handles performance via vectorized execution and column storage.
    pass

__all__ = ["create_scoped_indexes"]
