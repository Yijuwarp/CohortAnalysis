"""
Short summary: service for user-level event exploration and navigation.
"""
from __future__ import annotations

import math
from datetime import datetime, time

import duckdb
from fastapi import HTTPException

from app.utils.sql import classify_column, quote_identifier


DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200


def _scoped_exists(connection: duckdb.DuckDBPyConnection) -> bool:
    return bool(
        connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
        ).fetchone()[0]
    )


def _cohort_tables_exist(connection: duckdb.DuckDBPyConnection) -> bool:
    return bool(
        connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'cohort_membership' AND table_schema = 'main'"
        ).fetchone()[0]
    )


def _parse_jump_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    parsed = value.strip()
    if not parsed:
        return None

    try:
        if "T" not in parsed and " " not in parsed:
            return datetime.combine(datetime.fromisoformat(parsed).date(), time(0, 0, 0))
        value = datetime.fromisoformat(parsed.replace("Z", "+00:00"))
        if value.tzinfo is not None:
            value = value.astimezone().replace(tzinfo=None)
        return value
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid jump_datetime format. Use ISO-8601 datetime or YYYY-MM-DD") from exc


def search_users(connection: duckdb.DuckDBPyConnection, query: str = "", limit: int = 20, cohort_id: int | None = None) -> list[dict[str, str]]:
    if not _scoped_exists(connection):
        return []

    safe_limit = max(1, min(int(limit), 100))
    
    if cohort_id is not None:
        sql = """
            SELECT DISTINCT es.user_id
            FROM events_scoped es
            JOIN cohort_membership cm ON es.user_id = cm.user_id
            WHERE es.user_id IS NOT NULL
              AND cm.cohort_id = ?
              AND CAST(es.user_id AS VARCHAR) ILIKE ?
            ORDER BY es.user_id
            LIMIT ?
        """
        params = [cohort_id, f"%{query}%", safe_limit]
    else:
        sql = """
            SELECT DISTINCT user_id
            FROM events_scoped
            WHERE user_id IS NOT NULL
              AND CAST(user_id AS VARCHAR) ILIKE ?
            ORDER BY user_id
            LIMIT ?
        """
        params = [f"%{query}%", safe_limit]

    rows = connection.execute(sql, params).fetchall()

    return [{"user_id": str(user_id)} for (user_id,) in rows]


def _get_property_columns(connection: duckdb.DuckDBPyConnection) -> list[str]:
    return [
        str(column)
        for (column,) in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'events_scoped'
            ORDER BY ordinal_position
            """
        ).fetchall()
        if classify_column(str(column)) == "property"
    ]


def _build_summary(connection: duckdb.DuckDBPyConnection, user_id: str, property_columns: list[str]) -> dict[str, object]:
    aggregate = connection.execute(
        """
        SELECT
            MIN(event_time) AS first_event_time,
            MAX(event_time) AS last_event_time,
            COALESCE(SUM(event_count), 0) AS total_events
        FROM events_scoped
        WHERE user_id = ?
        """,
        [user_id],
    ).fetchone()

    first_event_time, last_event_time, total_events = aggregate
    latest_row: dict[str, object] = {}
    if property_columns:
        column_list = ", ".join(quote_identifier(column) for column in property_columns)
        latest_result = connection.execute(
            f"""
            SELECT {column_list}
            FROM events_scoped
            WHERE user_id = ?
            ORDER BY event_time DESC, event_name DESC
            LIMIT 1
            """,
            [user_id],
        ).fetchone()
        if latest_result is not None:
            latest_row = {
                property_columns[index]: latest_result[index]
                for index in range(len(property_columns))
            }

    return {
        "first_event_time": first_event_time.isoformat() if first_event_time else None,
        "last_event_time": last_event_time.isoformat() if last_event_time else None,
        "total_events": int(total_events or 0),
        "properties": latest_row,
    }


def _count_events(connection: duckdb.DuckDBPyConnection, user_id: str) -> int:
    return int(
        connection.execute("SELECT COUNT(*) FROM events_scoped WHERE user_id = ?", [user_id]).fetchone()[0] or 0
    )


def _resolve_target_row(
    connection: duckdb.DuckDBPyConnection,
    user_id: str,
    event_search: str | None,
    direction: str | None,
    from_event_time: datetime | None,
    jump_datetime: datetime | None,
) -> tuple[datetime, str] | None:
    if jump_datetime is not None:
        return connection.execute(
            """
            SELECT event_time, event_name
            FROM events_scoped
            WHERE user_id = ?
              AND event_time >= ?
            ORDER BY event_time ASC, event_name ASC
            LIMIT 1
            """,
            [user_id, jump_datetime],
        ).fetchone()

    if event_search and direction in {"next", "prev"} and from_event_time is not None:
        if direction == "next":
            return connection.execute(
                """
                SELECT event_time, event_name
                FROM events_scoped
                WHERE user_id = ?
                  AND event_name = ?
                  AND event_time > ?
                ORDER BY event_time ASC, event_name ASC
                LIMIT 1
                """,
                [user_id, event_search, from_event_time],
            ).fetchone()

        return connection.execute(
            """
            SELECT event_time, event_name
            FROM events_scoped
            WHERE user_id = ?
              AND event_name = ?
              AND event_time < ?
            ORDER BY event_time DESC, event_name DESC
            LIMIT 1
            """,
            [user_id, event_search, from_event_time],
        ).fetchone()

    return None


def _compute_page_for_row(
    connection: duckdb.DuckDBPyConnection,
    user_id: str,
    event_time: datetime,
    event_name: str,
    page_size: int,
) -> int:
    offset = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM events_scoped
            WHERE user_id = ?
              AND (
                event_time < ?
                OR (event_time = ? AND event_name < ?)
              )
            """,
            [user_id, event_time, event_time, event_name],
        ).fetchone()[0]
        or 0
    )
    return max(1, (offset // page_size) + 1)


def _fetch_events_page(connection: duckdb.DuckDBPyConnection, user_id: str, page: int, page_size: int) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT *
        FROM events_scoped
        WHERE user_id = ?
        ORDER BY event_time ASC, event_name ASC
        LIMIT ? OFFSET ?
        """,
        [user_id, page_size, (page - 1) * page_size],
    ).fetchall()

    columns = [desc[0] for desc in connection.description]

    events = []
    for row in rows:
        row_obj = dict(zip(columns, row))
        event_time = row_obj.get("event_time")
        event_name = row_obj.get("event_name")
        properties = {
            key: value
            for key, value in row_obj.items()
            if classify_column(str(key)) == "property"
        }
        events.append(
            {
                "event_time": event_time.isoformat() if event_time else None,
                "event_name": str(event_name) if event_name is not None else "",
                "properties": properties,
                "cohort_joins": [],
            }
        )

    return events


def _attach_cohort_joins(connection: duckdb.DuckDBPyConnection, user_id: str, events: list[dict[str, object]]) -> None:
    if not events or not _cohort_tables_exist(connection):
        return

    joins = connection.execute(
        """
        SELECT cm.join_time, c.name
        FROM cohort_membership cm
        JOIN cohorts c ON c.cohort_id = cm.cohort_id
        WHERE cm.user_id = ?
          AND cm.join_time IS NOT NULL
          AND c.hidden = FALSE
          AND c.name IS NOT NULL
          AND c.name <> 'All Users'
        ORDER BY cm.join_time ASC, c.name ASC
        """,
        [user_id],
    ).fetchall()

    if not joins:
        return

    event_times: list[datetime] = []
    for event in events:
        raw_time = event.get("event_time")
        if raw_time is None:
            event_times.append(datetime.max)
            continue
        event_times.append(datetime.fromisoformat(str(raw_time)))

    for join_time, cohort_name in joins:
        attached = False
        for idx, event_time in enumerate(event_times):
            if event_time == join_time or event_time >= join_time:
                events[idx]["cohort_joins"].append(str(cohort_name))
                attached = True
                break
        if not attached:
            continue


def get_user_explorer(
    connection: duckdb.DuckDBPyConnection,
    user_id: str,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    event_search: str | None = None,
    direction: str | None = None,
    from_event_time: datetime | None = None,
    jump_datetime_raw: str | None = None,
) -> dict[str, object]:
    if not user_id:
        raise HTTPException(status_code=400, detail="user_id is required")

    if not _scoped_exists(connection):
        return {
            "summary": {
                "first_event_time": None,
                "last_event_time": None,
                "total_events": 0,
                "properties": {},
            },
            "events": [],
            "pagination": {"page": 1, "total_pages": 1, "total_events": 0},
            "cursor": {"current_event_time": None},
        }

    safe_page_size = max(1, min(int(page_size), MAX_PAGE_SIZE))
    jump_datetime = _parse_jump_datetime(jump_datetime_raw)

    total_events = _count_events(connection, user_id)
    total_pages = max(1, math.ceil(total_events / safe_page_size))

    resolved_page = max(1, int(page))
    current_event_time = from_event_time.isoformat() if from_event_time else None

    matched_event = None
    target = _resolve_target_row(connection, user_id, event_search, direction, from_event_time, jump_datetime)
    if target is not None:
        target_time, target_event_name = target
        matched_event = {
            "event_time": target_time.isoformat() if target_time else None,
            "event_name": target_event_name,
        }
        resolved_page = _compute_page_for_row(connection, user_id, target_time, target_event_name, safe_page_size)
        current_event_time = target_time.isoformat() if target_time else current_event_time

    resolved_page = min(resolved_page, total_pages)

    property_columns = _get_property_columns(connection)
    summary = _build_summary(connection, user_id, property_columns)
    events = _fetch_events_page(connection, user_id, resolved_page, safe_page_size)

    if current_event_time is None and events:
        current_event_time = events[0].get("event_time")

    _attach_cohort_joins(connection, user_id, events)

    return {
        "summary": summary,
        "events": events,
        "pagination": {
            "page": resolved_page,
            "total_pages": total_pages,
            "total_events": total_events,
        },
        "cursor": {
            "current_event_time": current_event_time,
        },
        "matched_event": matched_event,
    }
