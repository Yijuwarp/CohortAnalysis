"""
Short summary: handles revenue configuration table setup.
"""
import duckdb

def ensure_revenue_event_selection_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS revenue_event_selection (
            event_name VARCHAR PRIMARY KEY,
            is_included BOOLEAN NOT NULL DEFAULT FALSE,
            override_revenue DOUBLE
        )
        """
    )
