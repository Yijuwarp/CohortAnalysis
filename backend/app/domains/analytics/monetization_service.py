"""
Short summary: service for computing revenue and monetization across cohorts.
"""
import duckdb
from app.utils.perf import time_block
from app.domains.cohorts.cohort_service import ensure_cohort_tables
from app.domains.analytics.retention_service import build_active_cohort_base
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

    from app.domains.analytics.metric_builders.revenue_vectors import build_revenue_vector_sql
    from app.domains.analytics.metric_builders.retention_vectors import build_retention_vector_sql
    observation_end_time = get_observation_end_time(connection)

    revenue_table = []
    retained_users_table = []
    eligibility_table = []
    
    for cohort_id, cohort_name in cohorts:
        cohort_id = int(cohort_id)
        cohort_size = cohort_sizes.get(cohort_id, 0)
        
        # 1. Revenue Vectors
        rev_sql, rev_params = build_revenue_vector_sql(
            cohort_id=cohort_id,
            max_day=max_day,
            observation_end_time=observation_end_time
        )
        # Aggregate per day: (day_offset, sum_revenue, sum_events, eligible_users)
        agg_sql = f"""
            SELECT day_offset, SUM(value), SUM(event_count), SUM(is_eligible::INTEGER)
            FROM ({rev_sql})
            GROUP BY 1
            ORDER BY 1
        """
        rows = connection.execute(agg_sql, rev_params).fetchall()
        
        for d, rev, count, eligible in rows:
            revenue_table.append({
                "cohort_id": cohort_id,
                "cohort_name": str(cohort_name),
                "day_number": int(d),
                "revenue": float(rev or 0.0),
                "event_count": int(count or 0),
                "availability": {
                    "eligible_users": int(eligible),
                    "cohort_size": int(cohort_size)
                }
            })
            eligibility_table.append({
                "cohort_id": cohort_id,
                "day_number": int(d),
                "eligible_users": int(eligible),
                "cohort_size": int(cohort_size)
            })
            
        # 2. Retained Users
        ret_sql, ret_params = build_retention_vector_sql(
            cohort_id=cohort_id,
            max_day=max_day,
            retention_event=None,
            observation_end_time=observation_end_time
        )
        ret_agg_sql = f"SELECT day_offset, SUM(value::INTEGER), SUM(is_eligible::INTEGER) FROM ({ret_sql}) GROUP BY 1 ORDER BY 1"
        ret_rows = connection.execute(ret_agg_sql, ret_params).fetchall()
        
        for d, active_users, eligible in ret_rows:
            retained_users_table.append({
                "cohort_id": cohort_id,
                "day_number": int(d),
                "retained_users": int(active_users),
                "availability": {
                    "eligible_users": int(eligible),
                    "cohort_size": int(cohort_size)
                }
            })

    end_timer(
        metric="cumulative_revenue_per_acquired_user",
        max_day=max_day,
        cohort_count=len(cohorts)
    )

    return {
        "max_day": int(max_day),
        "revenue_table": revenue_table,
        "cohort_sizes": [
            {"cohort_id": int(cid), "cohort_name": str(name), "size": int(cohort_sizes.get(cid, 0))}
            for cid, name in cohorts
        ],
        "retained_users_table": retained_users_table,
        "eligibility_table": eligibility_table,
        "observation_end_time": get_observation_end_time(connection).isoformat() if get_observation_end_time(connection) else None
    }
