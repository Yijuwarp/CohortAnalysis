"""
Short summary: service for computing event usage and frequency.
"""
import duckdb
from fastapi import HTTPException
from app.utils.perf import time_block
from app.utils.sql import quote_identifier, classify_column
from app.domains.cohorts.cohort_service import ensure_cohort_tables
from app.domains.analytics.retention_service import build_active_cohort_base
from app.queries.retention_queries import fetch_retention_active_rows
from app.queries.usage_queries import build_usage_property_filter_clause

def list_events(connection: duckdb.DuckDBPyConnection) -> dict[str, list[str]]:
    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
    ).fetchone()[0]
    if not scoped_exists:
        return {"events": []}

    rows = connection.execute("SELECT DISTINCT event_name FROM events_scoped ORDER BY event_name").fetchall()
    return {"events": [str(row[0]) for row in rows]}


def get_event_properties(connection: duckdb.DuckDBPyConnection, event_name: str) -> dict[str, list[str]]:
    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
    ).fetchone()[0]
    if not scoped_exists:
        return {"properties": []}

    event_exists = connection.execute("SELECT 1 FROM events_scoped WHERE event_name = ? LIMIT 1", [event_name]).fetchone()
    if event_exists is None:
        raise HTTPException(status_code=404, detail=f"Unknown event: {event_name}")

    columns = [
        str(row[0])
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'events_scoped'
            ORDER BY ordinal_position
            """
        ).fetchall()
    ]
    properties = [column for column in columns if classify_column(column) == "property"]

    available = []
    for column in properties:
        column_ref = quote_identifier(column)
        has_values = connection.execute(
            f"""
            SELECT 1
            FROM events_scoped
            WHERE event_name = ?
              AND {column_ref} IS NOT NULL
            LIMIT 1
            """,
            [event_name],
        ).fetchone()
        if has_values is not None:
            available.append(column)

    return {"properties": available}


def get_event_property_values(
    connection: duckdb.DuckDBPyConnection,
    event_name: str,
    property: str,
    limit: int = 25,
) -> dict[str, object]:
    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
    ).fetchone()[0]
    if not scoped_exists:
        return {"values": [], "total_distinct": 0}

    event_exists = connection.execute("SELECT 1 FROM events_scoped WHERE event_name = ? LIMIT 1", [event_name]).fetchone()
    if event_exists is None:
        raise HTTPException(status_code=404, detail=f"Unknown event: {event_name}")

    columns = {
        str(row[0])
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'events_scoped'
            """
        ).fetchall()
    }
    if property not in columns or classify_column(property) != "property":
        raise HTTPException(status_code=400, detail=f"Unknown property: {property}")

    property_ref = quote_identifier(property)
    rows = connection.execute(
        f"""
        SELECT CAST({property_ref} AS VARCHAR) AS property_value, COUNT(*) AS frequency
        FROM events_scoped
        WHERE event_name = ?
          AND {property_ref} IS NOT NULL
        GROUP BY property_value
        ORDER BY frequency DESC, property_value ASC
        LIMIT ?
        """,
        [event_name, limit],
    ).fetchall()

    total_distinct = int(connection.execute(
        f"""
        SELECT COUNT(DISTINCT CAST({property_ref} AS VARCHAR))
        FROM events_scoped
        WHERE event_name = ?
          AND {property_ref} IS NOT NULL
        """,
        [event_name],
    ).fetchone()[0] or 0)

    return {"values": [str(value) for value, _ in rows], "total_distinct": total_distinct}


def get_usage(
    connection: duckdb.DuckDBPyConnection,
    event: str,
    max_day: int = 7,
    retention_event: str | None = None,
    property: str | None = None,
    operator: str = "=",
    value: str | None = None,
) -> dict[str, object]:
    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
    ).fetchone()[0]
    ensure_cohort_tables(connection)

    empty_response = {
        "max_day": int(max_day),
        "event": event,
        "retention_event": retention_event or "any",
        "property_filter": {"property": property, "operator": operator, "value": value} if property else None,
        "usage_volume_table": [],
        "usage_users_table": [],
        "usage_adoption_table": [],
        "retained_users_table": [],
    }
    if not scoped_exists:
        return empty_response

    end_timer = time_block("usage_query")
    cohorts, cohort_sizes = build_active_cohort_base(connection)
    if not cohorts:
        end_timer(event=event, max_day=max_day, retention_event=retention_event, cohort_count=0)
        return empty_response

    event_exists = connection.execute("SELECT 1 FROM events_scoped WHERE event_name = ? LIMIT 1", [event]).fetchone()
    if event_exists is None:
        end_timer(event=event, max_day=max_day, retention_event=retention_event, error="event_not_found")
        return empty_response

    count = connection.execute("SELECT COUNT(*) FROM events_scoped").fetchone()[0]
    print("USAGE: using events_scoped row count:", count)

    known_columns = {
        row[0]
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'events_scoped'
            """
        ).fetchall()
    }
    if property and (
        property not in known_columns
        or classify_column(property) != "property"
    ):
        raise HTTPException(status_code=400, detail=f"Unknown property: {property}")

    property_clause, property_params = build_usage_property_filter_clause(
        property=property,
        operator=operator,
        value=value,
    )

    usage_rows = connection.execute(
        """
        WITH usage_deltas AS (
            SELECT
                cm.cohort_id,
                cm.user_id,
                DATE_DIFF('day', cm.join_time::DATE, es.event_time::DATE) AS day_number,
                es.event_count AS event_count
            FROM cohort_membership cm
            JOIN cohorts c ON c.cohort_id = cm.cohort_id
            JOIN events_scoped es ON es.user_id = cm.user_id
            WHERE c.hidden = FALSE
              AND es.event_name = ?
              AND DATE_DIFF('day', cm.join_time::DATE, es.event_time::DATE) BETWEEN 0 AND ?{property_clause}
        )
        SELECT
            cohort_id,
            day_number,
            SUM(event_count) AS total_events,
            COUNT(DISTINCT user_id) AS distinct_users
        FROM usage_deltas
        GROUP BY cohort_id, day_number
        """.format(property_clause=property_clause),
        [event, max_day, *property_params],
    ).fetchall()

    adoption_rows = connection.execute(
        """
        WITH user_first_event_day AS (
            SELECT
                cm.cohort_id,
                cm.user_id,
                MIN(DATE_DIFF('day', cm.join_time::DATE, es.event_time::DATE)) AS first_event_day
            FROM cohort_membership cm
            JOIN cohorts c ON c.cohort_id = cm.cohort_id
            JOIN events_scoped es ON es.user_id = cm.user_id
            WHERE c.hidden = FALSE
              AND es.event_name = ?
              AND DATE_DIFF('day', cm.join_time::DATE, es.event_time::DATE) BETWEEN 0 AND ?{property_clause}
            GROUP BY cm.cohort_id, cm.user_id
        )
        SELECT cohort_id, first_event_day, COUNT(DISTINCT user_id) AS first_event_users
        FROM user_first_event_day
        GROUP BY cohort_id, first_event_day
        """.format(property_clause=property_clause),
        [event, max_day, *property_params],
    ).fetchall()

    usage_by_day = {
        (int(cohort_id), int(day_number)): {"total_events": int(total_events), "distinct_users": int(distinct_users)}
        for cohort_id, day_number, total_events, distinct_users in usage_rows
    }
    adoption_by_first_day = {
        (int(cohort_id), int(day_number)): int(first_event_users)
        for cohort_id, day_number, first_event_users in adoption_rows
    }

    retention_rows = fetch_retention_active_rows(connection, max_day, retention_event)
    retained_by_day = {(int(c), int(d)): int(a) for c, d, a in retention_rows}

    usage_volume_table = []
    usage_users_table = []
    usage_adoption_table = []
    retained_users_table = []
    for cohort_id, cohort_name in cohorts:
        cohort_id = int(cohort_id)
        cohort_size = cohort_sizes.get(cohort_id, 0)
        volume_values = {}
        user_values = {}
        adoption_values = {}
        retained_values = {}
        cumulative_adoption = 0
        for day_number in range(max_day + 1):
            bucket = usage_by_day.get((cohort_id, day_number), {})
            volume_values[str(day_number)] = int(bucket.get("total_events", 0))
            user_values[str(day_number)] = int(bucket.get("distinct_users", 0))
            cumulative_adoption += int(adoption_by_first_day.get((cohort_id, day_number), 0))
            adoption_values[str(day_number)] = cumulative_adoption
            retained_values[str(day_number)] = int(retained_by_day.get((cohort_id, day_number), 0))

        common_metadata = {"cohort_id": cohort_id, "cohort_name": str(cohort_name), "size": int(cohort_size)}
        usage_volume_table.append({**common_metadata, "values": volume_values})
        usage_users_table.append({**common_metadata, "values": user_values})
        usage_adoption_table.append({**common_metadata, "values": adoption_values})
        retained_users_table.append({**common_metadata, "values": retained_values})

    end_timer(
        event=event,
        max_day=max_day,
        retention_event=retention_event,
        cohort_count=len(cohorts)
    )

    return {
        "max_day": int(max_day),
        "event": event,
        "retention_event": retention_event or "any",
        "property_filter": {"property": property, "operator": operator, "value": value} if property else None,
        "usage_volume_table": usage_volume_table,
        "usage_users_table": usage_users_table,
        "usage_adoption_table": usage_adoption_table,
        "retained_users_table": retained_users_table,
    }


def get_usage_frequency(
    connection: duckdb.DuckDBPyConnection,
    event: str,
    property: str | None = None,
    operator: str = "=",
    value: str | None = None,
) -> dict[str, object]:
    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
    ).fetchone()[0]
    ensure_cohort_tables(connection)

    if not scoped_exists:
        return {"buckets": [], "cohort_sizes": []}

    cohorts, cohort_sizes_map = build_active_cohort_base(connection)
    if not cohorts:
        return {"buckets": [], "cohort_sizes": []}

    cohort_sizes = [{"cohort_id": c[0], "name": str(c[1]), "size": cohort_sizes_map.get(c[0], 0)} for c in cohorts]

    known_columns = {
        row[0]
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'events_scoped'
            """
        ).fetchall()
    }
    if property and (
        property not in known_columns
        or classify_column(property) != "property"
    ):
        raise HTTPException(status_code=400, detail=f"Unknown property: {property}")

    property_clause, property_params = build_usage_property_filter_clause(
        property=property,
        operator=operator,
        value=value,
        table_alias="e",
    )

    rows = connection.execute(
        """
        WITH user_event_counts AS (
            SELECT
                cm.cohort_id,
                cm.user_id,
                COALESCE(SUM(e.event_count), 0) AS event_count
            FROM cohort_membership cm
            LEFT JOIN events_scoped e
                ON e.user_id = cm.user_id
                AND e.event_name = ?
                AND e.event_time >= cm.join_time{property_clause}
            GROUP BY cm.cohort_id, cm.user_id
        )
        SELECT
            cohort_id,
            CASE
                WHEN event_count = 0 THEN '0'
                WHEN event_count = 1 THEN '1'
                WHEN event_count BETWEEN 2 AND 5 THEN '2-5'
                WHEN event_count BETWEEN 6 AND 10 THEN '6-10'
                WHEN event_count BETWEEN 11 AND 20 THEN '11-20'
                ELSE '20+'
            END AS bucket,
            COUNT(*) AS users
        FROM user_event_counts
        GROUP BY cohort_id, bucket
        ORDER BY
            cohort_id,
            CASE
                WHEN bucket = '0' THEN 0
                WHEN bucket = '1' THEN 1
                WHEN bucket = '2-5' THEN 2
                WHEN bucket = '6-10' THEN 3
                WHEN bucket = '11-20' THEN 4
                ELSE 5
            END
        """.format(property_clause=property_clause),
        [event, *property_params]
    ).fetchall()

    bucket_order = ["0", "1", "2-5", "6-10", "11-20", "20+"]
    
    bucket_data = {b: {c[0]: 0 for c in cohorts} for b in bucket_order}
    
    for cohort_id, bucket, users in rows:
        if bucket in bucket_data:
            bucket_data[bucket][cohort_id] = users
            
    buckets = []
    for b in bucket_order:
        cohorts_list = [{"cohort_id": cid, "users": count} for cid, count in bucket_data[b].items()]
        cohorts_list.sort(key=lambda x: x["cohort_id"])
        buckets.append({
            "bucket": b,
            "cohorts": cohorts_list
        })

    return {
        "buckets": buckets,
        "cohort_sizes": cohort_sizes
    }
