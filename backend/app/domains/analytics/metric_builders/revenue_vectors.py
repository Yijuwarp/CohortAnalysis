from datetime import datetime
from typing import List, Any, Tuple, Optional
from app.utils.sql import build_bucket_expr, build_eligibility_expr

def build_revenue_vector_sql(
    cohort_id: int,
    max_day: int,
    join_type: str,
    granularity: str = "day",
    property_clause: str = "",
    property_params: List[Any] = None,
    observation_end_time: Optional[Any] = None
) -> Tuple[str, List[Any]]:
    """
    Returns (SQL, params) producing (cohort_id, user_id, day_offset, value, event_count, is_eligible) for a specific cohort.
    
    Source: cohort_activity_snapshot (Full Path Layer)
    Identity: cohort_membership
    Properties: Joined from events_scoped if property_clause is provided.
    """
    if property_params is None:
        property_params = []
        
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

    # Property Filter Join
    prop_join = ""
    if property_clause.strip():
        # Joins snapshot row to events_scoped to verify properties
        prop_join = f"""
        JOIN (
            SELECT DISTINCT user_id, event_time, event_name
            FROM events_scoped es
            WHERE 1=1 {property_clause}
        ) prop ON e.user_id = prop.user_id 
              AND e.event_time = prop.event_time 
              AND e.event_name = prop.event_name
        """

    final_sql = f"""
    SELECT 
        ug.cohort_id,
        ug.user_id,
        ug.day_offset,
        SUM(COALESCE(eo.modified_revenue, 0.0) * ug.is_eligible::INTEGER)::NUMERIC AS value,
        SUM(COALESCE(eo.event_count, 0) * ug.is_eligible::INTEGER)::INTEGER AS event_count,
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
            e.modified_revenue,
            e.event_count,
            {bucket_expr} AS day_offset
        FROM cohort_activity_snapshot e
        JOIN cohort_membership cm ON e.user_id = cm.user_id AND e.cohort_id = cm.cohort_id
        {prop_join}
        WHERE e.cohort_id = ?
          AND e.event_time >= cm.join_time
          AND {bucket_expr} <= {total_buckets}
        GROUP BY 1, 2, 3, 4
    ) eo ON ug.user_id = eo.user_id AND ug.day_offset = eo.day_offset
    GROUP BY 1, 2, 3
    ORDER BY 2, 3
    """
    
    # Correct Parameter order: ug (cid) -> prop_join (params) -> eo (cid)
    params = [cohort_id, *property_params, cohort_id]
    return final_sql, params
