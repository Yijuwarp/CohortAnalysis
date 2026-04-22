from datetime import datetime
from typing import Optional, List, Any, Tuple
from app.utils.sql import build_bucket_expr, build_eligibility_expr

def build_usage_vector_sql(
    cohort_id: int,
    max_day: int,
    join_type: str,
    event_name: str,
    metric: str = "volume",
    granularity: str = "day",
    property_clause: str = "",
    property_params: List[Any] = None,
    observation_end_time: Optional[Any] = None
) -> Tuple[str, List[Any]]:
    """
    Returns (SQL, params) producing (cohort_id, user_id, day_offset, value, is_eligible) for a specific cohort.
    
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

    # Aggregation logic per user/day
    if metric == "volume":
        val_expr = "SUM(e.event_count)" # Correctly sums pre-aggregated counts mapped during ingestion
    else:
        val_expr = "1" # For uniques, any match is 1

    # Property Filter Join
    # If property_clause is present, we must join back to events_scoped to check properties.
    # Note: We use DISTINCT in the property subquery to avoid inflating counts if 
    # somehow one snapshot row maps to multiple property-matching rows (unlikely but safe).
    prop_join = ""
    if property_clause.strip():
        # property_clause usually looks like "AND es.column = ?"
        # We need to make sure 'es' alias works.
        prop_join = f"""
        JOIN (
            SELECT DISTINCT user_id, event_time, event_name
            FROM events_scoped es
            WHERE 1=1 {property_clause}
        ) prop ON e.user_id = prop.user_id 
              AND e.event_time = prop.event_time 
              AND e.event_name = prop.event_name
        """

    sql = f"""
    SELECT 
        ug.cohort_id,
        ug.user_id,
        ug.day_offset,
        (COALESCE(eo.val, 0) * ug.is_eligible::INTEGER)::INTEGER AS value,
        ug.is_eligible
    FROM (
        SELECT cm.user_id, cm.cohort_id, cm.join_time, dg.day_offset, ({eligibility_expr}) as is_eligible
        FROM cohort_membership cm
        CROSS JOIN (SELECT i AS day_offset FROM generate_series(0, {total_buckets}) t(i)) dg
        WHERE cm.cohort_id = ?
    ) ug
    LEFT JOIN (
        SELECT 
            e.user_id,
            {bucket_expr} AS day_offset,
            {val_expr} AS val
        FROM cohort_activity_snapshot e
        JOIN cohort_membership cm ON e.user_id = cm.user_id AND e.cohort_id = cm.cohort_id
        {prop_join}
        WHERE e.cohort_id = ?
          AND e.event_name = ?
          AND e.event_time >= cm.join_time
          AND {bucket_expr} <= {total_buckets}
        GROUP BY 1, 2
    ) eo ON ug.user_id = eo.user_id AND ug.day_offset = eo.day_offset
    ORDER BY 2, 3
    """
    
    params = [cohort_id, *property_params, cohort_id, event_name]
    return sql, params
