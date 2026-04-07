from datetime import datetime
from typing import Optional, List, Any, Tuple

def build_usage_vector_sql(
    cohort_id: int,
    max_day: int,
    event_name: str,
    metric: str = "volume",
    granularity: str = "day",
    property_clause: str = "",
    property_params: List[Any] = None,
    observation_end_time: Optional[Any] = None
) -> Tuple[str, List[Any]]:
    """
    Returns (SQL, params) producing (user_id, day_offset, value, is_eligible) for a specific cohort.
    Windowing: FLOOR(EXTRACT(EPOCH FROM (event_time - join_time)) / 86400)
    """
    if property_params is None:
        property_params = []
        
    unit_seconds = 86400 if granularity == "day" else 3600
    val_expr = "COALESCE(SUM(eo.event_count), 0)" if metric == "volume" else "MAX(CASE WHEN eo.user_id IS NOT NULL THEN 1 ELSE 0 END)"

    # Eligibility expression
    if observation_end_time:
        if isinstance(observation_end_time, datetime):
            obs_time_str = observation_end_time.strftime('%Y-%m-%d %H:%M:%S')
        else:
            obs_time_str = str(observation_end_time)
        eligibility_expr = f"(EXTRACT(EPOCH FROM ('{obs_time_str}'::TIMESTAMP - cm.join_time)) / {unit_seconds}) >= dg.day_offset"
    else:
        eligibility_expr = "TRUE"

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
            es.user_id,
            es.event_count,
            FLOOR(EXTRACT(EPOCH FROM (es.event_time - cm.join_time)) / {unit_seconds}) AS day_offset
        FROM events_scoped es
        JOIN cohort_membership cm 
          ON es.user_id = cm.user_id 
         AND cm.cohort_id = ?
        WHERE cm.cohort_id = ?
          AND es.event_name = ?
          AND es.event_time >= cm.join_time
          {property_clause}
    ),
    daily_activity AS (
        SELECT 
            ug.user_id,
            ug.day_offset,
            {val_expr} AS value,
            MAX(ug.is_eligible::INTEGER) AS is_eligible
        FROM user_grid ug
        LEFT JOIN event_offsets eo
          ON ug.user_id = eo.user_id
         AND ug.day_offset = eo.day_offset
        GROUP BY 1, 2
    )
    SELECT user_id, day_offset, (value * is_eligible)::INTEGER AS value, is_eligible
    FROM daily_activity
    """
    
    # params: [cohort_id, cohort_id, cohort_id, event_name, *property_params]
    params = [cohort_id, cohort_id, cohort_id, event_name, *property_params]
    
    return final_sql, params
