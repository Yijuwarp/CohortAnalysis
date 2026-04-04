"""
Short summary: service for computing experiment impact metrics.
"""
import duckdb
from fastapi import HTTPException
from typing import Any, List, Optional
from pydantic import BaseModel

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

def run_impact_analysis(
    connection: duckdb.DuckDBPyConnection,
    baseline_cohort_id: int,
    variant_cohort_ids: List[int],
    start_day: int,
    end_day: int,
    exposure_events: List[str],
    interaction_events: List[str],
    impact_events: List[str] = []
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
            es.event_name,
            es.event_count
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
            exposure_users = connection.execute(
                "SELECT COUNT(DISTINCT user_id) FROM impact_base WHERE cohort_id = ? AND event_name IN ({})".format(
                    ", ".join(["?"] * len(exposure_events))
                ),
                [cid, *exposure_events]
            ).fetchone()[0]
            
        # Interaction Users and Counts
        interaction_users = 0
        interaction_counts = 0
        if interaction_events:
            row = connection.execute(
                """
                SELECT COUNT(DISTINCT user_id), SUM(event_count)
                FROM impact_base
                WHERE cohort_id = ? AND event_name IN ({})
                """.format(", ".join(["?"] * len(interaction_events))),
                [cid, *interaction_events]
            ).fetchone()
            interaction_users = row[0] or 0
            interaction_counts = row[1] or 0
            
        # Reach and Intensity for each impact event
        impact_metrics = []
        for event in impact_events:
            row = connection.execute(
                """
                SELECT COUNT(DISTINCT user_id), SUM(event_count)
                FROM impact_base
                WHERE cohort_id = ? AND event_name = ?
                """,
                [cid, event]
            ).fetchone()
            reach = (row[0] or 0) / total_users if total_users > 0 else 0
            intensity = (row[1] or 0) / total_users if total_users > 0 else 0
            impact_metrics.append({"event": event, "reach": reach, "intensity": intensity})

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
    for i, event in enumerate(impact_events):
        metrics_list.append({
            "metric": f"{event} → Reach",
            "values": {str(cid): get_value_with_delta(cid, "impact_metrics", i, "reach") for cid in all_cohort_ids}
        })
        metrics_list.append({
            "metric": f"{event} → Intensity",
            "values": {str(cid): get_value_with_delta(cid, "impact_metrics", i, "intensity") for cid in all_cohort_ids}
        })

    return {
        "cohorts": response_cohorts,
        "metrics": metrics_list
    }
