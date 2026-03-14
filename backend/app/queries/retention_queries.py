"""
Short summary: contains raw SQL queries for retention analytics.
"""
import duckdb

def fetch_retention_active_rows(
    connection: duckdb.DuckDBPyConnection,
    max_day: int,
    retention_event: str | None,
) -> list[tuple[int, int, int]]:
    if retention_event and retention_event != "any":
        return connection.execute(
            """
            WITH activity_deltas AS (
                SELECT
                    cm.cohort_id,
                    cm.user_id,
                    DATE_DIFF('day', cm.join_time::DATE, cas.event_time::DATE) AS day_number
                FROM cohort_membership cm
                JOIN cohorts c ON c.cohort_id = cm.cohort_id
                JOIN cohort_activity_snapshot cas
                  ON cm.cohort_id = cas.cohort_id
                 AND cm.user_id = cas.user_id
                JOIN events_scoped es
                  ON es.user_id = cas.user_id
                 AND es.event_time = cas.event_time
                 AND es.event_name = cas.event_name
                WHERE c.hidden = FALSE
                  AND es.event_name = ?
                  AND DATE_DIFF('day', cm.join_time::DATE, cas.event_time::DATE) BETWEEN 0 AND ?
            )
            SELECT cohort_id, day_number, COUNT(DISTINCT user_id) AS active_users
            FROM activity_deltas
            GROUP BY cohort_id, day_number
            """,
            [retention_event, max_day],
        ).fetchall()

    return connection.execute(
        """
        WITH activity_deltas AS (
            SELECT
                cm.cohort_id,
                cm.user_id,
                DATE_DIFF('day', cm.join_time::DATE, cas.event_time::DATE) AS day_number
            FROM cohort_membership cm
            JOIN cohorts c ON c.cohort_id = cm.cohort_id
            JOIN cohort_activity_snapshot cas
              ON cm.cohort_id = cas.cohort_id
             AND cm.user_id = cas.user_id
            JOIN events_scoped es
              ON es.user_id = cas.user_id
             AND es.event_time = cas.event_time
             AND es.event_name = cas.event_name
            WHERE c.hidden = FALSE
              AND DATE_DIFF('day', cm.join_time::DATE, cas.event_time::DATE) BETWEEN 0 AND ?
        )
        SELECT cohort_id, day_number, COUNT(DISTINCT user_id) AS active_users
        FROM activity_deltas
        GROUP BY cohort_id, day_number
        """,
        [max_day],
    ).fetchall()
