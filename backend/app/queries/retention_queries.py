"""
Short summary: contains raw SQL queries for retention analytics.
"""
import duckdb

def fetch_retention_active_rows(
    connection: duckdb.DuckDBPyConnection,
    max_day: int,
    retention_event: str | None,
    granularity: str = "day",
    retention_type: str = "classic",
) -> list[tuple[int, int, int]]:
    unit = "day" if granularity == "day" else "hour"
    total_buckets = max_day + 1 if granularity == "day" else (max_day * 24)
    
    event_filter = ""
    params = [total_buckets]
    if retention_event and retention_event != "any":
        event_filter = "AND es.event_name = ?"
        params = [retention_event, total_buckets]

    date_expr_join = "cm.join_time::DATE" if unit == "day" else f"DATE_TRUNC('{unit}', cm.join_time)"
    date_expr_event = "cas.event_time::DATE" if unit == "day" else f"DATE_TRUNC('{unit}', cas.event_time)"

    # Choose CTE and join condition based on retention type
    # Classic: active ON bucket
    # Ever-After: active AT OR AFTER bucket
    if retention_type == "classic":
        query = f"""
        WITH user_activity AS (
          SELECT
            cm.cohort_id,
            cm.user_id,
            DATE_DIFF('{unit}', {date_expr_join}, {date_expr_event}) AS bucket_index
          FROM cohort_membership cm
          JOIN cohort_activity_snapshot cas
            ON cm.cohort_id = cas.cohort_id AND cm.user_id = cas.user_id
          JOIN cohorts c
            ON c.cohort_id = cm.cohort_id
          JOIN events_scoped es
            ON es.user_id = cas.user_id
           AND es.event_time = cas.event_time
           AND es.event_name = cas.event_name
          WHERE c.hidden = FALSE
            AND DATE_DIFF('{unit}', {date_expr_join}, {date_expr_event}) >= 0
            {event_filter}
          GROUP BY 1, 2, 3
        ),
        buckets AS (
          SELECT range AS bucket_number
          FROM range(0, ?)
        )
        SELECT
          ua.cohort_id,
          b.bucket_number,
          COUNT(DISTINCT ua.user_id) AS active_users
        FROM buckets b
        JOIN user_activity ua
          ON ua.bucket_index = b.bucket_number
        GROUP BY 1, 2
        ORDER BY 1, 2
        """
    else:
        # Ever-After logic
        query = f"""
        WITH user_last_activity AS (
          SELECT
            cm.cohort_id,
            cm.user_id,
            MAX(DATE_DIFF('{unit}', {date_expr_join}, {date_expr_event})) AS last_bucket
          FROM cohort_membership cm
          JOIN cohort_activity_snapshot cas
            ON cm.cohort_id = cas.cohort_id AND cm.user_id = cas.user_id
          JOIN cohorts c
            ON c.cohort_id = cm.cohort_id
          JOIN events_scoped es
            ON es.user_id = cas.user_id
           AND es.event_time = cas.event_time
           AND es.event_name = cas.event_name
          WHERE c.hidden = FALSE
            AND DATE_DIFF('{unit}', {date_expr_join}, {date_expr_event}) >= 0
            {event_filter}
          GROUP BY 1, 2
        ),
        buckets AS (
          SELECT range AS bucket_number
          FROM range(0, ?)
        )
        SELECT
          ula.cohort_id,
          b.bucket_number,
          COUNT(DISTINCT ula.user_id) AS active_users
        FROM buckets b
        JOIN user_last_activity ula
          ON ula.last_bucket >= b.bucket_number
        GROUP BY 1, 2
        ORDER BY 1, 2
        """


    return connection.execute(query, params).fetchall()

def fetch_eligibility_rows(
    connection: duckdb.DuckDBPyConnection,
    max_day: int,
    granularity: str = "day",
) -> list[tuple[int, int, int]]:
    unit = "day" if granularity == "day" else "hour"
    total_buckets = max_day + 1 if granularity == "day" else (max_day * 24)
    
    # Get robust observation end time
    from app.utils.time_boundary import get_observation_end_time
    max_time = get_observation_end_time(connection)
    if not max_time:
        return []

    date_expr_join = "cm.join_time::DATE" if unit == "day" else f"DATE_TRUNC('{unit}', cm.join_time)"
    
    # max_time_expr needs to be formatted for DuckDB
    from datetime import datetime
    if isinstance(max_time, datetime):
        max_time_str = max_time.strftime('%Y-%m-%d %H:%M:%S')
    else:
        max_time_str = str(max_time)
        
    max_time_expr = f"'{max_time_str}'::TIMESTAMP"
    if unit == "day":
        if isinstance(max_time, datetime):
            max_time_expr = f"'{max_time.date().isoformat()}'::DATE"
        else:
            max_time_expr = f"CAST('{max_time_str}' AS DATE)"

    query = f"""
    WITH buckets AS (
      SELECT range AS bucket_number
      FROM range(0, ?)
    )
    SELECT
      cm.cohort_id,
      b.bucket_number,
      COUNT(DISTINCT cm.user_id) AS eligible_users
    FROM cohort_membership cm
    CROSS JOIN buckets b
    WHERE DATE_DIFF('{unit}', {date_expr_join}, {max_time_expr}) >= b.bucket_number
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    return connection.execute(query, [total_buckets]).fetchall()
