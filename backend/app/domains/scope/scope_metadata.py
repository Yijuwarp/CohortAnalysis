"""
Short summary: reads and writes scoped dataset metadata.
"""
import json
import duckdb
from datetime import datetime, timezone
from fastapi import HTTPException, Query
from app.utils.sql import quote_identifier, classify_column

def ensure_scope_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dataset_scope (
            id INTEGER PRIMARY KEY,
            filters_json TEXT,
            total_rows INTEGER,
            filtered_rows INTEGER,
            total_events BIGINT,
            total_users INTEGER DEFAULT 0,
            updated_at TIMESTAMP
        )
        """
    )

    existing_columns = {
        row[0]
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'dataset_scope' AND table_schema = 'main'
            """
        ).fetchall()
    }
    if "total_events" not in existing_columns:
        connection.execute("ALTER TABLE dataset_scope ADD COLUMN total_events BIGINT")
    if "total_users" not in existing_columns:
        connection.execute("ALTER TABLE dataset_scope ADD COLUMN total_users INTEGER DEFAULT 0")

    connection.execute("UPDATE dataset_scope SET total_events = 0 WHERE total_events IS NULL")
    connection.execute("UPDATE dataset_scope SET total_users = 0 WHERE total_users IS NULL")


def upsert_dataset_scope(connection: duckdb.DuckDBPyConnection, payload: dict[str, object]) -> dict[str, int]:
    ensure_scope_tables(connection)
    # Consolidate aggregation queries for better performance
    all_metrics = connection.execute("""
        SELECT 
            (SELECT COUNT(*) FROM events_normalized),
            COUNT(*), 
            COALESCE(SUM(event_count), 0),
            COUNT(DISTINCT user_id)
        FROM events_scoped
    """).fetchone()
    
    total_rows = int(all_metrics[0] or 0)
    filtered_rows = int(all_metrics[1] or 0)
    total_events = int(all_metrics[2] or 0)
    total_users = int(all_metrics[3] or 0)

    connection.execute(
        """
        INSERT INTO dataset_scope (id, filters_json, total_rows, filtered_rows, total_events, total_users, updated_at)
        VALUES (1, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET
            filters_json = excluded.filters_json,
            total_rows = excluded.total_rows,
            filtered_rows = excluded.filtered_rows,
            total_events = excluded.total_events,
            total_users = excluded.total_users,
            updated_at = excluded.updated_at
        """,
        [
            json.dumps(payload),
            total_rows,
            filtered_rows,
            total_events,
            total_users,
            datetime.now(timezone.utc),
        ],
    )
    return {"total_rows": total_rows, "filtered_rows": filtered_rows, "total_events": total_events, "total_users": total_users}


def get_scope(connection: duckdb.DuckDBPyConnection) -> dict[str, object]:
    ensure_scope_tables(connection)
    row = connection.execute(
        "SELECT filters_json, total_rows, filtered_rows, total_events, total_users, updated_at FROM dataset_scope WHERE id = 1"
    ).fetchone()
    if row is None:
        return {
            "filters_json": {"date_range": None, "filters": []},
            "total_rows": 0,
            "filtered_rows": 0,
            "total_events": 0,
            "total_users": 0,
            "updated_at": None,
        }

    return {
        "filters_json": json.loads(row[0]) if row[0] else {"date_range": None, "filters": []},
        "total_rows": int(row[1] or 0),
        "filtered_rows": int(row[2] or 0),
        "total_events": int(row[3] or 0),
        "total_users": int(row[4] or 0),
        "updated_at": row[5].isoformat() if row[5] else None,
    }

def get_columns(connection: duckdb.DuckDBPyConnection) -> dict[str, list[dict[str, str | None]]]:
    try:
        exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized' AND table_schema = 'main'"
        ).fetchone()[0]
        if not exists:
            return {"columns": []}

        rows = connection.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'events_normalized'
            ORDER BY ordinal_position
            """
        ).fetchall()
        role_map = {
            "user_id": "user id",
            "event_name": "event name",
            "event_time": "event time",
            "event_count": "event count",
        }
        payload = [
            {
                "name": str(name),
                "role": role_map.get(str(name)),
                "data_type": "TIMESTAMP" if "TIMESTAMP" in str(data_type).upper() else str(data_type).upper(),
                "category": classify_column(str(name)),
            }
            for name, data_type in rows
            if str(name) != "row_id"
        ]
        return {"columns": payload}
    finally:
        pass

def get_column_values(
    connection: duckdb.DuckDBPyConnection,
    column: str,
    event_name: str | None = None,
    search: str | None = None,
    limit: int = 100,
) -> dict[str, list[str] | int]:
    normalized_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized' AND table_schema = 'main'"
    ).fetchone()[0]
    if not normalized_exists:
        return {"values": [], "total_distinct": 0}

    # High cardinality columns bypass GROUP BY/SUM for performance
    HIGH_CARDINALITY_COLUMNS = ["user_id", "device_id", "anonymous_id", "session_id"]
    is_high_cardinality = column.lower() in HIGH_CARDINALITY_COLUMNS

    # Ensure table/column is known
    # Determine source table (fallback to events_normalized if events_scoped is missing)
    source_table = "events_scoped"
    exists_scoped = connection.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT table_name FROM information_schema.tables 
            UNION 
            SELECT table_name FROM information_schema.views
        ) WHERE table_name = 'events_scoped'
        """
    ).fetchone()[0]
    if not exists_scoped:
        source_table = "events_normalized"

    # Use PRAGMA table_info which is robust for both tables and views
    column_metadata = {
        row[1]: row[2].upper()
        for row in connection.execute(f"PRAGMA table_info('{source_table}')").fetchall()
    }


    if column not in column_metadata:
        raise HTTPException(status_code=400, detail=f"Unknown column: {column}")

    column_type = column_metadata[column]
    column_ref = quote_identifier(column)
    where_clauses = [f"{column_ref} IS NOT NULL"]
    
    # Only filter out empty strings for string-like columns (fixes Conversion Error for numerics)
    if any(t in column_type for t in ["VARCHAR", "TEXT", "STRING"]):
        where_clauses.append(f"{column_ref} != ''")
    query_params = []

    # Scoping conflict guard: if searching 'event_name', ignore the event_name parameter
    if event_name and column != "event_name":
        where_clauses.append("event_name = ?")
        query_params.append(event_name)

    if search:
        # Hybrid search: prefix for short terms, substring for longer ones
        if len(search) <= 2:
            where_clauses.append(f"{column_ref} ILIKE ?")
            query_params.append(f"{search}%")
        else:
            where_clauses.append(f"{column_ref} ILIKE ?")
            query_params.append(f"%{search}%")

    where_sql = f"WHERE {' AND '.join(where_clauses)}"

    if is_high_cardinality:
        # Simple distinct for fast performance on massive cardinalities
        rows = connection.execute(
            f"""
            SELECT DISTINCT {column_ref}
            FROM {source_table}
            {where_sql}
            LIMIT ?
            """,
            query_params + [limit]
        ).fetchall()
    else:
        # Quality-weighted results for standard columns
        rows = connection.execute(
            f"""
            SELECT {column_ref}, SUM(event_count) as freq
            FROM {source_table}
            {where_sql}
            GROUP BY 1
            ORDER BY freq DESC, {column_ref} ASC
            LIMIT ?
            """,
            query_params + [limit]
        ).fetchall()


    # Calculate total distinct for UX (optional but kept for alignment with existing interface)
    # Note: total_distinct also respects the search/filter parameters
    total_distinct = int(
        connection.execute(
            f"""
            SELECT COUNT(DISTINCT {column_ref})
            FROM {source_table}
            {where_sql}
            """,
            query_params
        ).fetchone()[0]

    )

    return {
        "values": [str(value) for (value, *_) in rows],
        "total_distinct": total_distinct,
    }

def get_date_range(connection: duckdb.DuckDBPyConnection) -> dict[str, str | None]:
    normalized_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized' AND table_schema = 'main'"
    ).fetchone()[0]
    if not normalized_exists:
        return {"min_date": None, "max_date": None}

    min_event_time, max_event_time = connection.execute(
        "SELECT MIN(event_time), MAX(event_time) FROM events_normalized"
    ).fetchone()

    return {
        "min_date": min_event_time.date().isoformat() if min_event_time else None,
        "max_date": max_event_time.date().isoformat() if max_event_time else None,
    }
