"""
Short summary: contains raw SQL queries for monetization analytics.
"""
import duckdb

def fetch_monetization_revenue_rows(
    connection: duckdb.DuckDBPyConnection,
    max_day: int,
) -> list[tuple[int, int, float, int]]:
    return connection.execute(
        """
        WITH revenue_events AS (
            SELECT event_name
            FROM revenue_event_selection
            WHERE is_included = TRUE
        ),
        revenue_by_day AS (
            SELECT
                cm.cohort_id,
                DATE_DIFF('day', cm.join_time::DATE, es.event_time::DATE) AS day_number,
                SUM(es.modified_revenue) AS revenue,
                SUM(es.event_count) AS event_volume
            FROM cohort_membership cm
            JOIN cohorts c ON c.cohort_id = cm.cohort_id
            JOIN events_scoped es
              ON cm.user_id = es.user_id
            WHERE c.hidden = FALSE
              AND es.event_name IN (SELECT event_name FROM revenue_events)
            GROUP BY cm.cohort_id, day_number
        )
        SELECT cohort_id, day_number, revenue, event_volume
        FROM revenue_by_day
        WHERE day_number BETWEEN 0 AND ?
        ORDER BY cohort_id, day_number
        """,
        [max_day],
    ).fetchall()
