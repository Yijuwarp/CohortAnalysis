from datetime import datetime
from typing import List, Any, Tuple, Optional

def build_revenue_vector_sql(
    cohort_id: int,
    max_day: int,
    granularity: str = "day",
    observation_end_time: Optional[Any] = None
) -> Tuple[str, List[Any]]:
    """
    Returns (SQL, params) producing (user_id, day_offset, value, event_count, is_eligible) for a specific cohort.
    Uses modified_revenue which already encodes inclusion and override logic.
    """
    unit_seconds = 86400 if granularity == "day" else 3600

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
            es.modified_revenue,
            es.event_count,
            FLOOR(EXTRACT(EPOCH FROM (es.event_time - cm.join_time)) / {unit_seconds}) AS day_offset
        FROM events_scoped es
        JOIN cohort_membership cm 
          ON es.user_id = cm.user_id 
         AND cm.cohort_id = ?
        WHERE cm.cohort_id = ?
          AND es.event_time >= cm.join_time
          AND es.modified_revenue > 0
    ),
    daily_revenue AS (
        SELECT 
            ug.user_id,
            ug.day_offset,
            COALESCE(SUM(eo.modified_revenue), 0.0) AS value,
            COALESCE(SUM(eo.event_count), 0) AS event_count,
            MAX(ug.is_eligible::INTEGER) AS is_eligible
        FROM user_grid ug
        LEFT JOIN event_offsets eo
          ON ug.user_id = eo.user_id
         AND ug.day_offset = eo.day_offset
        GROUP BY 1, 2
    )
    SELECT 
        user_id, 
        day_offset, 
        (value * is_eligible)::NUMERIC AS value, 
        (event_count * is_eligible)::INTEGER AS event_count, 
        is_eligible
    FROM daily_revenue
    """
    
    # params: [cohort_id, cohort_id, cohort_id]
    params = [cohort_id, cohort_id, cohort_id]
    
    return final_sql, params
