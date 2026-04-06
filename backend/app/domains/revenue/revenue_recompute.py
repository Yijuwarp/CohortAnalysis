"""
Short summary: logic for recomputing modified revenue based on config.
"""
import duckdb
from app.utils.perf import time_block

def recompute_modified_revenue_columns(connection: duckdb.DuckDBPyConnection, table_name: str) -> None:
    # DuckDB 0.x - 1.x doesn't support UPDATE on VIEWs.
    # Since events_scoped is usually a VIEW selecting from events_normalized,
    # we only need to update the base table. Any request to update a VIEW
    # is ignored to avoid Binder Errors.
    
    if table_name == "events_scoped":
        rows = connection.execute(
            """
            SELECT table_type 
            FROM information_schema.tables 
            WHERE table_name = 'events_scoped' 
              AND table_schema = 'main'
            """
        ).fetchall()
        if rows and rows[0][0] == 'VIEW':
            return

    if table_name not in {"events_normalized", "events_scoped"}:
        raise ValueError("Unsupported table for revenue recomputation")

    end_timer = time_block("revenue_recomputation")

    # Fixed ambiguous column reference by using explicit table alias for event_count
    connection.execute(
        f"""
        UPDATE {table_name}
        SET modified_revenue = CASE
            WHEN res.is_included = TRUE AND res.override_revenue IS NOT NULL THEN res.override_revenue * t_base.event_count
            WHEN res.is_included = TRUE THEN t_base.original_revenue
            ELSE 0.0
        END
        FROM {table_name} AS t_base
        LEFT JOIN revenue_event_selection res ON t_base.event_name = res.event_name
        WHERE {table_name}.user_id = t_base.user_id 
          AND {table_name}.event_name = t_base.event_name 
          AND {table_name}.event_time = t_base.event_time
        """
    )

    end_timer()
