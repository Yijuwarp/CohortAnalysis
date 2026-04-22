"""
Short summary: logic for recomputing modified revenue based on config.
"""
import duckdb
from app.utils.perf import time_block

def recompute_modified_revenue_columns(connection: duckdb.DuckDBPyConnection, table_name: str) -> None:
    # DuckDB doesn't support UPDATE on VIEWs.
    # We check the table type and skip if it's a view.
    rows = connection.execute(
        """
        SELECT table_type 
        FROM information_schema.tables 
        WHERE table_name = ?
          AND table_schema = 'main'
        """,
        [table_name]
    ).fetchall()
    if rows and rows[0][0] == 'VIEW':
        return

    allowed = {"events_normalized", "events_scoped", "events_raw", "events_scoped_raw", "cohort_activity_snapshot", "events_base"}
    if table_name not in allowed:
        raise ValueError(f"Unsupported table for revenue recomputation: {table_name}")

    end_timer = time_block(f"revenue_recomputation_{table_name}")

    # Use High-Performance CTAS (Create Table As Select) instead of UPDATE.
    # DuckDB is significantly faster at replacing a table than updating large fractions of it.
    
    # 1. Fetch existing table structure to preserve precise column order
    cursor = connection.execute(f"PRAGMA table_info('{table_name}')")
    table_info = cursor.fetchall()
    
    select_parts = []
    for row in table_info:
        col_name = row[1]
        if col_name == 'modified_revenue':
            # Compute the new value in the original position
            expr = f"""
                CASE
                    WHEN res.is_included = TRUE AND res.override_revenue IS NOT NULL THEN CAST(res.override_revenue * {table_name}.event_count AS DOUBLE)
                    WHEN res.is_included = TRUE THEN CAST({table_name}.original_revenue AS DOUBLE)
                    ELSE 0.0
                END AS modified_revenue
            """
            select_parts.append(expr)
        else:
            select_parts.append(f'{table_name}."{col_name}"')

    # 2. Perform atomic replacement with preserved order
    connection.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT 
            {", ".join(select_parts)}
        FROM {table_name}
        LEFT JOIN (
            SELECT event_name, is_included, override_revenue
            FROM revenue_event_selection
        ) res ON {table_name}.event_name = res.event_name
    """)

    # 3. Restore critical indexes if this is events_base
    if table_name == "events_base":
        connection.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_user ON {table_name} (user_id)")
        connection.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_row ON {table_name} (row_id)")

    end_timer()
