from datetime import datetime
from typing import Optional, List, Any, Tuple
from app.utils.sql import build_bucket_expr, build_eligibility_expr

def build_retention_vector_sql(
    cohort_id: int,
    max_day: int,
    join_type: str,
    retention_event: Optional[str] = None,
    retention_type: str = "classic",
    granularity: str = "day",
    property_clause: str = "",
    property_params: List[Any] = None,
    observation_end_time: Optional[Any] = None
) -> Tuple[str, List[Any]]:
    """
    Returns (SQL, params) producing (cohort_id, user_id, day_offset, value, is_eligible) for a specific cohort.
    
    Source: cohort_activity_snapshot (Full Path Layer)
    Identity: cohort_membership
    """
    # NOTE: property_clause and property_params are ignored for retention in V9 to reduce fragility.
    # If property filtering is needed in the future, it should join back to events_scoped or use a property-enriched snapshot.

    if granularity == "day":
        total_buckets = max_day
    else:
        total_buckets = max_day * 24

    bucket_expr = build_bucket_expr("cm.join_time", "e.event_time", granularity)
    
    if observation_end_time:
        if isinstance(observation_end_time, datetime):
            obs_time_str = observation_end_time.strftime('%Y-%m-%d %H:%M:%S')
        else:
            obs_time_str = str(observation_end_time)
        eligibility_expr = build_eligibility_expr("cm.join_time", "dg.day_offset", granularity, obs_time_str)
    else:
        eligibility_expr = "TRUE"

    # Event filter for retention
    eo_filter = ""
    if retention_event and retention_event != "any":
        eo_filter = f"AND e.event_name = '{retention_event}'"

    # BUILDER LOGIC:
    # 1. ug (User-Grid): All users in cohort crossed with all relevant day offsets.
    # 2. eo (Event-Occurrences): Distinct (user, day_offset) pairs from the SNAPSHOT where the event occurred.
    
    final_sql = f"""
    SELECT 
        ug.cohort_id,
        ug.user_id,
        ug.day_offset,
        MAX(CASE WHEN eo.user_id IS NOT NULL THEN 1 ELSE 0 END * ug.is_eligible::INTEGER)::INTEGER AS value,
        MAX(ug.is_eligible::INTEGER) AS is_eligible
    FROM (
        SELECT cm.user_id, cm.cohort_id, cm.join_time, dg.day_offset, ({eligibility_expr}) as is_eligible
        FROM cohort_membership cm
        CROSS JOIN (SELECT i AS day_offset FROM generate_series(0, {total_buckets}) t(i)) dg
        WHERE cm.cohort_id = ?
    ) ug
    LEFT JOIN (
        SELECT 
            e.user_id,
            {bucket_expr} AS day_offset
        FROM cohort_activity_snapshot e
        JOIN cohort_membership cm ON e.user_id = cm.user_id AND e.cohort_id = cm.cohort_id
        WHERE e.cohort_id = ?
          AND e.event_time >= cm.join_time
          AND {bucket_expr} <= {total_buckets}
          {eo_filter}
    ) eo ON ug.user_id = eo.user_id AND ug.day_offset = eo.day_offset
    GROUP BY 1, 2, 3
    """
    
    params = [cohort_id, cohort_id]
    
    if retention_type == "classic":
        final_sql = f"""
        SELECT cohort_id, user_id, day_offset, value, is_eligible
        FROM ({final_sql})
        ORDER BY user_id, day_offset
        """
    else:
        # Ever-after retention
        final_sql = f"""
        SELECT 
            cohort_id,
            user_id, 
            day_offset,
            MAX(value::BOOLEAN) OVER (
                PARTITION BY user_id 
                ORDER BY day_offset 
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            )::INTEGER AS value,
            is_eligible
        FROM ({final_sql})
        ORDER BY user_id, day_offset
        """
        
    return final_sql, params
