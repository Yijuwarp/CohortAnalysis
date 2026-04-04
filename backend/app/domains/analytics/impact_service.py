"""
Short summary: service for computing experiment impact metrics.
"""
import duckdb
from fastapi import HTTPException
from typing import Any, List, Optional, Tuple, Union
from pydantic import BaseModel
from app.utils.sql import quote_identifier

class ImpactMetricValue(BaseModel):
    value: Optional[float]
    delta: Optional[float]

class ImpactMetric(BaseModel):
    metric: str
    values: dict[str, ImpactMetricValue]

class ImpactCohort(BaseModel):
    id: int
    name: str
    size: int

class ImpactResponse(BaseModel):
    cohorts: List[ImpactCohort]
    metrics: List[ImpactMetric]

from app.domains.cohorts.cohort_service import get_events_source_table

def build_event_filter_sql(event_configs: List[Union[dict, Any]]) -> Tuple[str, List[Any]]:
    """
    Builds a WHERE clause fragment for a list of event configurations.
    Returns (sql_fragment, list_of_params).
    e.g., "( (event_name = ? AND prop1 = ?) OR (event_name = ? AND prop2 = ?) )"
    """
    if not event_configs:
        return "1=1", [] # Should not happen if validated, but safe default
    
    # Limit max events per type for performance
    event_configs = event_configs[:10]
    
    parts = []
    params = []
    for config in event_configs:
        # Handle both Pydantic objects and dicts safely
        if isinstance(config, dict):
            event_name = config.get('event_name')
            filters = config.get('filters', [])
        else:
            event_name = getattr(config, 'event_name', None)
            filters = getattr(config, 'filters', [])
        
        if not event_name:
            continue

        event_part = "(event_name = ?"
        event_params = [event_name]
        
        for f in filters:
            if isinstance(f, dict):
                prop = f.get('property')
                op = f.get('operator', '=')
                val = f.get('value')
            else:
                prop = getattr(f, 'property', None)
                op = getattr(f, 'operator', '=')
                val = getattr(f, 'value', None)
            
            # V1 only supports "="
            if str(op) == "=" and prop and val is not None:
                event_part += f" AND {quote_identifier(str(prop))} = ?"
                event_params.append(val)
        
        event_part += ")"
        parts.append(event_part)
        params.extend(event_params)
        
    return f"( {' OR '.join(parts)} )", params

def run_impact_analysis(
    connection: duckdb.DuckDBPyConnection,
    baseline_cohort_id: int,
    variant_cohort_ids: List[int],
    start_day: int,
    end_day: int,
    exposure_events: List[Any],
    interaction_events: List[Any],
    impact_events: List[Any] = []
) -> dict:
    all_cohort_ids = [baseline_cohort_id] + variant_cohort_ids
    source_table = get_events_source_table(connection)
    
    # 1. Fetch cohort details and total sizes (including users with no events)
    cohort_data = connection.execute(
        """
        SELECT cohort_id, name, COUNT(DISTINCT user_id) as total_users
        FROM cohorts
        LEFT JOIN cohort_membership USING (cohort_id)
        WHERE cohort_id IN ({})
        GROUP BY cohort_id, name
        """.format(", ".join(["?"] * len(all_cohort_ids))),
        all_cohort_ids
    ).fetchall()
    
    cohort_info = {row[0]: {"name": row[1], "size": row[2]} for row in cohort_data}
    
    # 2. Base CTE for scoped events joined with membership and time window
    ids_str = ", ".join(map(str, all_cohort_ids))
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW impact_base AS
        SELECT
            cm.cohort_id,
            cm.user_id,
            es.* EXCLUDE (user_id)
        FROM cohort_membership cm
        JOIN {source_table} es ON cm.user_id = es.user_id
        WHERE cm.cohort_id IN ({ids_str})
          AND es.event_time >= cm.join_time + {int(start_day)} * INTERVAL 1 DAY
          AND es.event_time <= cm.join_time + {int(end_day)} * INTERVAL 1 DAY
        """
    )

    results = {}
    
    # Helper to compute core metrics for each cohort
    for cid in all_cohort_ids:
        total_users = cohort_info[cid]["size"]
        
        # Exposure Users
        exposure_users = 0
        if exposure_events:
            event_sql, event_params = build_event_filter_sql(exposure_events)
            exposure_users = connection.execute(
                f"SELECT COUNT(DISTINCT user_id) FROM impact_base WHERE impact_base.cohort_id = ? AND {event_sql}",
                [cid, *event_params]
            ).fetchone()[0]
            
        # Interaction Users and Counts
        interaction_users = 0
        interaction_counts = 0
        if interaction_events:
            event_sql, event_params = build_event_filter_sql(interaction_events)
            row = connection.execute(
                f"""
                SELECT COUNT(DISTINCT user_id), SUM(event_count)
                FROM impact_base
                WHERE impact_base.cohort_id = ? AND {event_sql}
                """,
                [cid, *event_params]
            ).fetchone()
            interaction_users = row[0] or 0
            interaction_counts = row[1] or 0
            
        # Reach and Intensity for each impact event
        impact_metrics = []
        for config in impact_events:
            event_sql, event_params = build_event_filter_sql([config])
            row = connection.execute(
                f"""
                SELECT COUNT(DISTINCT user_id), SUM(event_count)
                FROM impact_base
                WHERE impact_base.cohort_id = ? AND {event_sql}
                """,
                [cid, *event_params]
            ).fetchone()
            event_name = getattr(config, 'event_name', None) or config.get('event_name')
            reach = (row[0] or 0) / total_users if total_users > 0 else 0
            intensity = (row[1] or 0) / total_users if total_users > 0 else 0
            impact_metrics.append({"event": event_name, "reach": reach, "intensity": intensity})

        results[cid] = {
            "exposure_rate": exposure_users / total_users if total_users > 0 else 0,
            "ctr": interaction_users / exposure_users if exposure_users > 0 else None,
            "engagement": interaction_counts / total_users if total_users > 0 else 0,
            "impact_metrics": impact_metrics
        }

    # 3. Format response and calculate deltas
    response_cohorts = [
        {"id": cid, "name": cohort_info[cid]["name"], "size": cohort_info[cid]["size"]}
        for cid in all_cohort_ids
    ]
    
    baseline = results[baseline_cohort_id]
    
    def get_value_with_delta(cid, metric_key, impact_event_idx=None, sub_key=None):
        val = results[cid][metric_key]
        if impact_event_idx is not None:
             val = results[cid][metric_key][impact_event_idx][sub_key]
             base_val = baseline[metric_key][impact_event_idx][sub_key]
        else:
             base_val = baseline[metric_key]
             
        delta = None
        if cid != baseline_cohort_id and base_val is not None and base_val != 0 and val is not None:
            delta = (val - base_val) / base_val
            
        return {"value": val, "delta": delta}

    metrics_list = []
    
    # Section 1: Exposure & Interaction
    metrics_list.append({
        "metric": "Exposure Rate",
        "values": {str(cid): get_value_with_delta(cid, "exposure_rate") for cid in all_cohort_ids}
    })
    metrics_list.append({
        "metric": "CTR",
        "values": {str(cid): get_value_with_delta(cid, "ctr") for cid in all_cohort_ids}
    })
    metrics_list.append({
        "metric": "Engagement",
        "values": {str(cid): get_value_with_delta(cid, "engagement") for cid in all_cohort_ids}
    })
    
    # Section 2: Impact
    for i, config in enumerate(impact_events):
        event_name = getattr(config, 'event_name', None) or config.get('event_name')
        metrics_list.append({
            "metric": f"{event_name} → Reach",
            "values": {str(cid): get_value_with_delta(cid, "impact_metrics", i, "reach") for cid in all_cohort_ids}
        })
        metrics_list.append({
            "metric": f"{event_name} → Intensity",
            "values": {str(cid): get_value_with_delta(cid, "impact_metrics", i, "intensity") for cid in all_cohort_ids}
        })

    return {
        "cohorts": response_cohorts,
        "metrics": metrics_list
    }
