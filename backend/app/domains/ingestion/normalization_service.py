"""
Short summary: normalizes uploaded events into canonical event tables.
"""
import duckdb
from app.utils.sql import get_column_type_map

def ensure_normalized_events_revenue_columns(connection: duckdb.DuckDBPyConnection, table_name: str = "events_normalized") -> None:
    table_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()[0]
    if not table_exists:
        return

    # Drop dependent views that might block ALTER TABLE operations
    connection.execute("DROP VIEW IF EXISTS cohort_activity_snapshot")
    connection.execute("DROP VIEW IF EXISTS events_scoped")
    connection.execute("DROP VIEW IF EXISTS events_scoped_raw")
    connection.execute("DROP VIEW IF EXISTS events_normalized")

    existing_columns = {
        row[0]
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = ?
            """,
            [table_name],
        ).fetchall()
    }

    if "event_count" not in existing_columns:
        if "original_event_count" in existing_columns:
            connection.execute(f"ALTER TABLE {table_name} RENAME COLUMN original_event_count TO event_count")
            existing_columns.discard("original_event_count")
            existing_columns.add("event_count")
        else:
            connection.execute(f"ALTER TABLE {table_name} ADD COLUMN event_count DOUBLE")

    if "original_revenue" not in existing_columns and "revenue_amount" in existing_columns:
        connection.execute(f"ALTER TABLE {table_name} RENAME COLUMN revenue_amount TO original_revenue")
        existing_columns.discard("revenue_amount")
        existing_columns.add("original_revenue")

    if "original_revenue" not in existing_columns:
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN original_revenue DOUBLE DEFAULT 0.0")
    if "modified_revenue" not in existing_columns:
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN modified_revenue DOUBLE DEFAULT 0.0")

    if "original_event_count" in existing_columns:
        connection.execute(f"ALTER TABLE {table_name} DROP COLUMN original_event_count")
    if "modified_event_count" in existing_columns:
        connection.execute(f"ALTER TABLE {table_name} DROP COLUMN modified_event_count")

    # Restore default constraints if they exist but were removed
    if "original_revenue" in existing_columns:
        connection.execute(f"ALTER TABLE {table_name} ALTER COLUMN original_revenue SET DEFAULT 0.0")
    if "modified_revenue" in existing_columns:
        connection.execute(f"ALTER TABLE {table_name} ALTER COLUMN modified_revenue SET DEFAULT 0.0")

    # Ensure any pre-existing revenue columns with DECIMAL precision are widened to DOUBLE
    column_types = get_column_type_map(connection, table_name)
    for col in ("original_revenue", "modified_revenue"):
        col_type = column_types.get(col, "").upper()
        if col_type and col_type != "DOUBLE" and not col_type.startswith("FLOAT"):
            connection.execute(f"ALTER TABLE {table_name} ALTER COLUMN {col} TYPE DOUBLE")

    connection.execute(
        f"""
        UPDATE {table_name}
        SET modified_revenue = COALESCE(modified_revenue, original_revenue)
        """
    )
