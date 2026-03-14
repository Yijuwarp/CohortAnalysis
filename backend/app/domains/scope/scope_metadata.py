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

    connection.execute("UPDATE dataset_scope SET total_events = 0 WHERE total_events IS NULL")


def upsert_dataset_scope(connection: duckdb.DuckDBPyConnection, payload: dict[str, object]) -> dict[str, int]:
    ensure_scope_tables(connection)
    total_rows = int(connection.execute("SELECT COUNT(*) FROM events_normalized").fetchone()[0])
    filtered_rows = int(connection.execute("SELECT COUNT(*) FROM events_scoped").fetchone()[0])
    total_events = int(connection.execute("SELECT COALESCE(SUM(event_count), 0) FROM events_scoped").fetchone()[0] or 0)

    connection.execute(
        """
        INSERT INTO dataset_scope (id, filters_json, total_rows, filtered_rows, total_events, updated_at)
        VALUES (1, ?, ?, ?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET
            filters_json = excluded.filters_json,
            total_rows = excluded.total_rows,
            filtered_rows = excluded.filtered_rows,
            total_events = excluded.total_events,
            updated_at = excluded.updated_at
        """,
        [
            json.dumps(payload),
            total_rows,
            filtered_rows,
            total_events,
            datetime.now(timezone.utc),
        ],
    )
    return {"total_rows": total_rows, "filtered_rows": filtered_rows, "total_events": total_events}


def get_scope(connection: duckdb.DuckDBPyConnection) -> dict[str, object]:
    ensure_scope_tables(connection)
    row = connection.execute(
        "SELECT filters_json, total_rows, filtered_rows, total_events, updated_at FROM dataset_scope WHERE id = 1"
    ).fetchone()
    if row is None:
        return {
            "filters_json": {"date_range": None, "filters": []},
            "total_rows": 0,
            "filtered_rows": 0,
            "total_events": 0,
            "updated_at": None,
        }

    return {
        "filters_json": json.loads(row[0]) if row[0] else {"date_range": None, "filters": []},
        "total_rows": int(row[1] or 0),
        "filtered_rows": int(row[2] or 0),
        "total_events": int(row[3] or 0),
        "updated_at": row[4].isoformat() if row[4] else None,
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
        ]
        return {"columns": payload}
    finally:
        pass

def get_column_values(
    connection: duckdb.DuckDBPyConnection,
    column: str,
    event_name: str | None = None,
) -> dict[str, list[str] | int]:
    normalized_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized' AND table_schema = 'main'"
    ).fetchone()[0]
    if not normalized_exists:
        return {"values": [], "total_distinct": 0}

    table_name = "events_scoped" if event_name is not None else "events_normalized"
    known_columns = {
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
    if column not in known_columns:
        raise HTTPException(status_code=400, detail=f"Unknown column: {column}")

    column_ref = quote_identifier(column)

    if event_name is not None:
        event_exists = connection.execute(
            "SELECT COUNT(*) FROM events_scoped WHERE event_name = ?",
            [event_name],
        ).fetchone()[0]
        if not event_exists:
            raise HTTPException(status_code=400, detail=f"Unknown event_name: {event_name}")

        rows = connection.execute(
            f"""
            SELECT DISTINCT {column_ref}
            FROM events_scoped
            WHERE {column_ref} IS NOT NULL AND event_name = ?
            ORDER BY 1
            LIMIT 100
            """,
            [event_name],
        ).fetchall()
        total_distinct = int(
            connection.execute(
                f"""
                SELECT COUNT(DISTINCT {column_ref})
                FROM events_scoped
                WHERE {column_ref} IS NOT NULL AND event_name = ?
                """,
                [event_name],
            ).fetchone()[0]
        )
    else:
        rows = connection.execute(
            f"""
            SELECT DISTINCT {column_ref}
            FROM events_normalized
            WHERE {column_ref} IS NOT NULL
            ORDER BY 1
            LIMIT 100
            """
        ).fetchall()
        total_distinct = int(
            connection.execute(
                f"SELECT COUNT(DISTINCT {column_ref}) FROM events_normalized WHERE {column_ref} IS NOT NULL"
            ).fetchone()[0]
        )
    return {
        "values": [str(value) for (value,) in rows],
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
