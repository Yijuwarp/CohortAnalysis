from datetime import datetime
from typing import Optional, List, Any, Tuple

def build_retention_vector_sql(
    cohort_id: int,
    max_day: int,
    retention_event: Optional[str] = None,
    retention_type: str = "classic",
    granularity: str = "day",
    property_clause: str = "",
    property_params: List[Any] = None,
    observation_end_time: Optional[Any] = None
) -> Tuple[str, List[Any]]:
    """
    Returns (SQL, params) producing (cohort_id, user_id, day_offset, value, is_eligible) for a specific cohort.
    Uses 24h relative windows: FLOOR(EXTRACT(EPOCH FROM (event_time - join_time)) / 86400)
    """
    if property_params is None:
        property_params = []

    # Bucketing and pushdown expressions
    if granularity == "day":
        bucket_val = 86400
        total_buckets = max_day
    else:
        bucket_val = 3600
        total_buckets = max_day * 24

    bucket_expr = f"FLOOR(EXTRACT(EPOCH FROM (cas.event_time - cm.join_time)) / {bucket_val})"
    pushdown_clause = f"AND {bucket_expr} <= {total_buckets}"
    
    params = [cohort_id]
    
    # Eligibility expression
    if observation_end_time:
        if isinstance(observation_end_time, datetime):
            obs_time_str = observation_end_time.strftime('%Y-%m-%d %H:%M:%S')
        else:
            obs_time_str = str(observation_end_time)
        eligibility_expr = f"cm.join_time + (dg.day_offset * INTERVAL '1 {granularity}') <= '{obs_time_str}'::TIMESTAMP"
    else:
        eligibility_expr = "TRUE"

    # Event filter for retention
    eo_filter = ""
    if retention_event and retention_event != "any":
        eo_filter = f"AND cas.event_name = '{retention_event}'"

    # Property filter join logic (Fix 2, 4)
    if property_clause:
        event_offsets_cte = f"""
        filtered_events AS (
            SELECT user_id, event_time, event_name
            FROM events_scoped es
            WHERE 1=1 {property_clause}
        ),
        event_offsets AS (
            SELECT 
                cas.user_id,
                {bucket_expr} AS day_offset
            FROM cohort_activity_snapshot cas
            JOIN cohort_membership cm 
              ON cas.user_id = cm.user_id 
             AND cas.cohort_id = cm.cohort_id
            JOIN filtered_events fe
              ON cas.user_id = fe.user_id
             AND cas.event_time = fe.event_time
             AND cas.event_name = fe.event_name
            WHERE cas.cohort_id = ?
              AND cas.event_time >= cm.join_time
              {eo_filter}
              {pushdown_clause}
        )
        """
        # params for event_offsets: [*property_params, cohort_id]
        cte_params = [*property_params, cohort_id]
    else:
        event_offsets_cte = f"""
        event_offsets AS (
            SELECT 
                cas.user_id,
                {bucket_expr} AS day_offset
            FROM cohort_activity_snapshot cas
            JOIN cohort_membership cm 
              ON cas.user_id = cm.user_id 
             AND cas.cohort_id = cm.cohort_id
            WHERE cas.cohort_id = ?
              AND cas.event_time >= cm.join_time
              {eo_filter}
              {pushdown_clause}
        )
        """
        cte_params = [cohort_id]

    final_sql = f"""
    WITH day_grid AS (
        SELECT generate_series AS day_offset
        FROM generate_series(0, {total_buckets})
    ),
    user_grid AS (
        SELECT 
            cm.user_id, 
            cm.cohort_id, 
            cm.join_time, 
            dg.day_offset,
            ({eligibility_expr}) AS is_eligible
        FROM cohort_membership cm
        CROSS JOIN day_grid dg
        WHERE cm.cohort_id = ?
    ),
    {event_offsets_cte},
    daily_activity AS (
        SELECT 
            ug.cohort_id,
            ug.user_id,
            ug.day_offset,
            MAX(CASE WHEN eo.user_id IS NOT NULL THEN 1 ELSE 0 END) AS has_activity,
            MAX(ug.is_eligible::INTEGER) AS is_eligible
        FROM user_grid ug
        LEFT JOIN event_offsets eo
          ON ug.user_id = eo.user_id
         AND ug.day_offset = eo.day_offset
        GROUP BY 1, 2, 3
    )
    """
    params.extend(cte_params)
    
    if retention_type == "classic":
        final_sql += """
        SELECT cohort_id, user_id, day_offset, (has_activity AND is_eligible::BOOLEAN)::INTEGER AS value, is_eligible
        FROM daily_activity
        ORDER BY user_id, day_offset
        """
    else:
        # Ever-after retention: 1 if activity exists on OR after day_offset
        final_sql += """
        SELECT 
            cohort_id,
            user_id, 
            day_offset,
            MAX(has_activity AND is_eligible::BOOLEAN) OVER (
                PARTITION BY user_id 
                ORDER BY day_offset 
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            )::INTEGER AS value,
            is_eligible
        FROM daily_activity
        ORDER BY user_id, day_offset
        """
        
    return final_sql, params
