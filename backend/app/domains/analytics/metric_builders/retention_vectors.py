from datetime import datetime
from typing import Optional, List, Any, Tuple

def build_retention_vector_sql(
    cohort_id: int,
    max_day: int,
    retention_event: Optional[str] = None,
    retention_type: str = "classic",
    granularity: str = "day",
    observation_end_time: Optional[Any] = None
) -> Tuple[str, List[Any]]:
    """
    Returns (SQL, params) producing (user_id, day_offset, value, is_eligible) for a specific cohort.
    Uses 24h relative windows: FLOOR(EXTRACT(EPOCH FROM (event_time - join_time)) / 86400)
    """
    unit_seconds = 86400 if granularity == "day" else 3600
    params = [cohort_id]
    
    # Eligibility expression
    if observation_end_time:
        if isinstance(observation_end_time, datetime):
            obs_time_str = observation_end_time.strftime('%Y-%m-%d %H:%M:%S')
        else:
            obs_time_str = str(observation_end_time)
        eligibility_expr = f"(EXTRACT(EPOCH FROM ('{obs_time_str}'::TIMESTAMP - cm.join_time)) / {unit_seconds}) >= dg.day_offset"
    else:
        eligibility_expr = "TRUE"

    # 1. Day Grid (0..max_day)
    # 2. User Grid (all users x all days)
    # 3. Event Offsets (pre-calculated day_offset for events)
    # 4. Daily Activity (LEFT JOIN grid to offsets)
    
    # Event filter for the offsets CTE
    eo_filter = ""
    if retention_event and retention_event != "any":
        eo_filter = f"AND cas.event_name = '{retention_event}'"

    final_sql = f"""
    WITH day_grid AS (
        SELECT range AS day_offset FROM range(0, {max_day} + 1)
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
    event_offsets AS (
        SELECT 
            cas.user_id,
            FLOOR(EXTRACT(EPOCH FROM (cas.event_time - cm.join_time)) / {unit_seconds}) AS day_offset
        FROM cohort_activity_snapshot cas
        JOIN cohort_membership cm 
          ON cas.user_id = cm.user_id 
         AND cas.cohort_id = cm.cohort_id
        WHERE cas.cohort_id = ?
          AND cas.event_time >= cm.join_time
          {eo_filter}
    ),
    daily_activity AS (
        SELECT 
            ug.user_id,
            ug.day_offset,
            MAX(CASE WHEN eo.user_id IS NOT NULL THEN 1 ELSE 0 END) AS has_activity,
            MAX(ug.is_eligible::INTEGER) AS is_eligible
        FROM user_grid ug
        LEFT JOIN event_offsets eo
          ON ug.user_id = eo.user_id
         AND ug.day_offset = eo.day_offset
        GROUP BY 1, 2
    )
    """
    params.extend([cohort_id])
    
    if retention_type == "classic":
        final_sql += """
        SELECT user_id, day_offset, has_activity AS value, is_eligible
        FROM daily_activity
        """
    else:
        # Ever-after retention: 1 if activity exists on OR after day_offset
        final_sql += """
        SELECT 
            user_id, 
            day_offset,
            MAX(has_activity) OVER (
                PARTITION BY user_id 
                ORDER BY day_offset 
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            ) AS value,
            is_eligible
        FROM daily_activity
        """
        
    return final_sql, params
