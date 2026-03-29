"""
Short summary: service for computing revenue and monetization across cohorts.
"""
import duckdb
from app.utils.perf import time_block
from app.domains.cohorts.cohort_service import ensure_cohort_tables
from app.domains.analytics.retention_service import build_active_cohort_base, fetch_retention_active_rows
from app.queries.retention_queries import fetch_eligibility_rows
from app.utils.time_boundary import get_observation_end_time

def get_monetization(
    connection: duckdb.DuckDBPyConnection,
    max_day: int,
) -> dict[str, int | list[dict[str, object]]]:
    ensure_cohort_tables(connection)
    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
    ).fetchone()[0]

    empty_response = {
        "max_day": int(max_day),
        "revenue_table": [],
        "cohort_sizes": [],
        "retained_users_table": [],
    }
    if not scoped_exists:
        return empty_response

    end_timer = time_block("monetization_query")
    cohorts, cohort_sizes = build_active_cohort_base(connection)
    if not cohorts:
        end_timer(metric="cumulative_revenue_per_acquired_user", max_day=max_day, cohort_count=0)
        return empty_response

    revenue_rows = connection.execute(
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

    retained_rows = fetch_retention_active_rows(connection, max_day, None)
    eligibility_rows = fetch_eligibility_rows(connection, max_day)
    eligible_by_bucket = {(int(c), int(b)): int(a) for c, b, a in eligibility_rows}

    cohort_name_by_id = {int(cohort_id): str(cohort_name) for cohort_id, cohort_name in cohorts}
    revenue_table = [
        {
            "cohort_id": int(cohort_id),
            "cohort_name": cohort_name_by_id.get(int(cohort_id), "Unknown"),
            "day_number": int(day_number),
            "revenue": float(revenue or 0.0),
            "event_count": int(event_volume or 0),
            "availability": {
                "eligible_users": int(eligible_by_bucket.get((int(cohort_id), int(day_number)), 0)),
                "cohort_size": int(cohort_sizes.get(int(cohort_id), 0))
            }
        }
        for cohort_id, day_number, revenue, event_volume in revenue_rows
    ]
    cohort_size_table = [
        {"cohort_id": int(cohort_id), "cohort_name": str(cohort_name), "size": int(cohort_sizes.get(int(cohort_id), 0))}
        for cohort_id, cohort_name in cohorts
    ]
    retained_users_table = [
        {
            "cohort_id": int(cohort_id),
            "day_number": int(day_number),
            "retained_users": int(active_users),
            "availability": (int(active_users) / cohort_sizes.get(int(cohort_id), 0)) if cohort_sizes.get(int(cohort_id), 0) > 0 else 0
        }
        for cohort_id, day_number, active_users in retained_rows
    ]

    end_timer(
        metric="cumulative_revenue_per_acquired_user",
        max_day=max_day,
        cohort_count=len(cohorts)
    )

    eligibility_table = [
        {"cohort_id": int(cohort_id), "day_number": int(day_number), "eligible_users": int(eligible_users)}
        for cohort_id, day_number, eligible_users in eligibility_rows
    ]

    return {
        "max_day": int(max_day),
        "revenue_table": revenue_table,
        "cohort_sizes": cohort_size_table,
        "retained_users_table": retained_users_table,
        "eligibility_table": eligibility_table,
        "observation_end_time": get_observation_end_time(connection).isoformat() if get_observation_end_time(connection) else None
    }
