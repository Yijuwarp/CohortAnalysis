"""
Short summary: service for computing experiment impact metrics.
"""
import duckdb
import time
import uuid
from fastapi import HTTPException
from typing import Any, List, Optional, Tuple, Union
from pydantic import BaseModel
from app.utils.sql import quote_identifier

# ---------------------------------------------------------------------------
# Run cache for lazy stats endpoint
# ---------------------------------------------------------------------------
IMPACT_RUN_CACHE: dict[str, dict] = {}
CACHE_TTL_SECONDS = 600   # 10 minutes
MAX_CACHE_SIZE = 100


def _cleanup_cache() -> None:
    """Evict expired entries and enforce size cap."""
    now = time.time()
    expired = [k for k, v in IMPACT_RUN_CACHE.items()
               if now - v["created_at"] > CACHE_TTL_SECONDS]
    for k in expired:
        del IMPACT_RUN_CACHE[k]
    # Hard cap: evict oldest
    while len(IMPACT_RUN_CACHE) > MAX_CACHE_SIZE:
        oldest = min(IMPACT_RUN_CACHE, key=lambda k: IMPACT_RUN_CACHE[k]["created_at"])
        del IMPACT_RUN_CACHE[oldest]

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
        
        standard_cols = {'user_id', 'event_name', 'event_time', 'original_revenue', 'modified_revenue'}
        
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
    impact_events: List[Any] = [],
    monetization_events: List[Any] = [],
    retention_event: Optional[str] = None
) -> dict:
    _cleanup_cache()
    run_id = str(uuid.uuid4())

    all_cohort_ids = [baseline_cohort_id] + variant_cohort_ids
    source_table = get_events_source_table(connection)

    if not monetization_events:
        # Skip monetization if none selected
        monetized_cohort_ids = []
    else:
        monetized_cohort_ids = all_cohort_ids
    
    # 1. Fetch cohort details and total sizes
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
    
    # 2. Base CTE for scoped events
    ids_str = ", ".join(map(str, all_cohort_ids))
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW impact_base AS
        SELECT
            cm.cohort_id,
            cm.user_id,
            cm.join_time,
            es.* EXCLUDE (user_id)
        FROM cohort_membership cm
        JOIN {source_table} es ON cm.user_id = es.user_id
        WHERE cm.cohort_id IN ({ids_str})
          AND es.event_time >= cm.join_time + {int(start_day)} * INTERVAL 1 DAY
          AND es.event_time < cm.join_time + ({int(end_day)} + 1) * INTERVAL 1 DAY
        """
    )

    results = {}
    
    # 3. Monetization Distributions (per user)
    monetization_data = {}
    if monetization_events:
        rev_sql, rev_params = build_event_filter_sql(monetization_events)
        for cid in all_cohort_ids:
            # Mandatory pattern: LEFT JOIN with filters in the ON clause to preserve denominator
            # Sum COALESCE(modified_revenue, 0) to handle users with no events
            sql = f"""
                SELECT
                    cm.user_id,
                    SUM(COALESCE(es.modified_revenue, 0.0)) as total_rev
                FROM cohort_membership cm
                LEFT JOIN {source_table} es
                    ON cm.user_id = es.user_id
                    AND es.event_time >= cm.join_time + ? * INTERVAL 1 DAY
                    AND es.event_time < cm.join_time + (? + 1) * INTERVAL 1 DAY
                    AND {rev_sql}
                WHERE cm.cohort_id = ?
                GROUP BY cm.user_id
            """
            params = [start_day, end_day, *rev_params, cid]
            rows = connection.execute(sql, params).fetchall()
            monetization_data[cid] = [float(r[1]) for r in rows]
            

    # Helper to compute core metrics for each cohort
    for cid in all_cohort_ids:
        total_users = cohort_info[cid]["size"]
        
        # Exposure Users and Counts
        exposure_users = 0
        exposure_counts = 0
        if exposure_events:
            event_sql, event_params = build_event_filter_sql(exposure_events)
            row = connection.execute(
                f"""
                SELECT COUNT(DISTINCT user_id), SUM(event_count)
                FROM impact_base
                WHERE impact_base.cohort_id = ? AND {event_sql}
                """,
                [cid, *event_params]
            ).fetchone()
            exposure_users = row[0] or 0
            exposure_counts = row[1] or 0
            
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
            
        # Reuse Users (users with >=2 interactions)
        reuse_users = 0
        if interaction_events:
            event_sql, event_params = build_event_filter_sql(interaction_events)
            reuse_users = connection.execute(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT user_id FROM impact_base
                    WHERE impact_base.cohort_id = ? AND {event_sql}
                    GROUP BY user_id
                    HAVING SUM(event_count) >= 2
                ) as sub
                """,
                [cid, *event_params]
            ).fetchone()[0]

        # Time to First Interaction (Median)
        median_time_to_int = None
        time_to_int_distribution = []
        if exposure_events and interaction_events:
            exp_sql, exp_params = build_event_filter_sql(exposure_events)
            int_sql, int_params = build_event_filter_sql(interaction_events)
            
            # Optimized SQL from V5 Plan
            sql = f"""
            WITH first_exposures AS (
                SELECT user_id, MIN(event_time) AS first_exp
                FROM impact_base 
                WHERE impact_base.cohort_id = ? AND {exp_sql} 
                GROUP BY user_id
            ),
            interactions AS (
                SELECT i.user_id, MIN(i.event_time) AS first_int
                FROM impact_base i
                JOIN first_exposures fe ON i.user_id = fe.user_id
                WHERE i.cohort_id = ? AND {int_sql} AND i.event_time > fe.first_exp
                GROUP BY i.user_id
            )
            SELECT EXTRACT(EPOCH FROM (first_int - first_exp))
            FROM interactions JOIN first_exposures USING (user_id)
            """
            rows = connection.execute(sql, [cid, *exp_params, cid, *int_params]).fetchall()
            time_to_int_distribution = [float(r[0]) for r in rows]
            if time_to_int_distribution:
                import numpy as np
                median_time_to_int = float(np.median(time_to_int_distribution))

        # Daily Average Metrics (Engagement & Revenue)
        daily_eng_avg = None
        daily_rev_avg = None
        eng_sparkline = []
        rev_sparkline = []
        
        # Calculate daily stats
        if retention_event:
            if retention_event == 'any':
                ret_sql, ret_params = "1=1", []
            else:
                ret_sql, ret_params = build_event_filter_sql([{"event_name": retention_event}])
            int_sql, int_params = build_event_filter_sql(interaction_events) if interaction_events else ("1=0", [])
            mon_sql, mon_params = build_event_filter_sql(monetization_events) if monetization_events else ("1=0", [])
            
            # Query active users, interactions, and revenue per day
            # We use local time differences (EPOCH) to handle partial days accurately if needed, 
            # but FLOOR(/86400) is standard for "Day N"
            sql = f"""
            SELECT 
                FLOOR(EXTRACT(EPOCH FROM (event_time - join_time)) / 86400) AS rel_day,
                COUNT(DISTINCT CASE WHEN {ret_sql} THEN user_id END) AS active_users,
                SUM(CASE WHEN {int_sql} THEN event_count ELSE 0 END) AS interactions,
                SUM(CASE WHEN {mon_sql} THEN COALESCE(modified_revenue, 0.0) ELSE 0 END) AS revenue
            FROM impact_base
            WHERE cohort_id = ?
            GROUP BY rel_day
            ORDER BY rel_day
            """
            all_params = [*ret_params, *int_params, *mon_params, cid]
            day_rows = connection.execute(sql, all_params).fetchall()
            day_lookup = {int(r[0]): {"active": r[1], "eng": r[2], "rev": r[3]} for r in day_rows}
            
            eng_vals = []
            rev_vals = []
            for d in range(int(start_day), int(end_day) + 1):
                stats = day_lookup.get(d, {"active": 0, "eng": 0, "rev": 0})
                active = stats["active"]
                eng_vals.append(stats["eng"] / active if active > 0 else 0.0)
                rev_vals.append(stats["rev"] / active if active > 0 else 0.0)
            
            eng_sparkline = eng_vals
            rev_sparkline = rev_vals
            if eng_vals:
                daily_eng_avg = sum(eng_vals) / len(eng_vals)
            if rev_vals:
                daily_rev_avg = sum(rev_vals) / len(rev_vals)

        # Handle empty/None sparkline case (Unify to None if no data across all days)
        if not any(eng_sparkline): eng_sparkline = []
        if not any(rev_sparkline): rev_sparkline = []
        if not eng_sparkline: daily_eng_avg = None
        if not rev_sparkline: daily_rev_avg = None
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
            event_users = row[0] or 0
            reach = event_users / total_users if total_users > 0 else 0
            intensity = (row[1] or 0) / total_users if total_users > 0 else 0
            impact_metrics.append({
                "event": event_name,
                "reach": reach,
                "intensity": intensity,
                "event_users": event_users,
                "total_users": total_users,
            })

        # Monetization Metrics
        m_metrics = {}
        if cid in monetization_data:
            dist = monetization_data[cid]
            total_rev = sum(dist)
            # Define paying_users as total_user_revenue > 0
            paying_users = sum(1 for v in dist if v > 0.0001)
            m_metrics = {
                "revenue_per_user": total_rev / total_users if total_users > 0 else 0,
                "revenue_conversion": paying_users / total_users if total_users > 0 else 0,
                "revenue_intensity": total_rev / paying_users if paying_users > 0 else None,
                "total_revenue": total_rev,
                "paying_users": paying_users,
                "distribution": dist
            }

        results[cid] = {
            "exposure_rate": exposure_users / total_users if total_users > 0 else 0,
            "usage_rate": interaction_users / exposure_users if exposure_users > 0 else 0,
            "ctr": interaction_counts / exposure_counts if exposure_counts > 0 else None,
            "reuse_rate": reuse_users / interaction_users if interaction_users > 0 else 0,
            "time_to_first_interaction": median_time_to_int,
            "engagement": interaction_counts / total_users if total_users > 0 else 0,
            "engagement_daily_avg": daily_eng_avg,
            "revenue_daily_avg": daily_rev_avg,
            "eng_sparkline": eng_sparkline,
            "rev_sparkline": rev_sparkline,
            "impact_metrics": impact_metrics,
            "monetization": m_metrics,
            # Raw aggregates for stats endpoint
            "exposure_users": exposure_users,
            "exposure_counts": exposure_counts,
            "total_users": total_users,
            "interaction_users": interaction_users,
            "interaction_counts": interaction_counts,
            "reuse_users": reuse_users,
            "time_to_int_distribution": time_to_int_distribution,
            "eng_daily_distribution": eng_sparkline, # simplified for now
            "rev_daily_distribution": rev_sparkline,
        }

    # 3. Format response and calculate deltas
    response_cohorts = [
        {"id": cid, "name": cohort_info[cid]["name"], "size": cohort_info[cid]["size"]}
        for cid in all_cohort_ids
    ]
    
    baseline = results[baseline_cohort_id]
    
    def get_value_with_delta(cid, metric_key, impact_event_idx=None, sub_key=None):
        if metric_key == "monetization":
             val = results[cid][metric_key][sub_key]
             base_val = baseline[metric_key][sub_key]
        elif impact_event_idx is not None:
             val = results[cid][metric_key][impact_event_idx][sub_key]
             base_val = baseline[metric_key][impact_event_idx][sub_key]
        else:
             val = results[cid][metric_key]
             base_val = baseline[metric_key]
             
        delta = None
        if cid != baseline_cohort_id and base_val is not None and base_val != 0 and val is not None:
            delta = (val - base_val) / base_val
            
        return {"value": val, "delta": delta}

    metrics_list = []
    
    # Section 1: Exposure & Interaction
    metrics_list.append({
        "metric_key": "exposure_rate",
        "metric": "Exposure Rate",
        "values": {str(cid): get_value_with_delta(cid, "exposure_rate") for cid in all_cohort_ids}
    })
    metrics_list.append({
        "metric_key": "usage_rate",
        "metric": "Usage Rate",
        "values": {str(cid): get_value_with_delta(cid, "usage_rate") for cid in all_cohort_ids}
    })
    metrics_list.append({
        "metric_key": "ctr",
        "metric": "CTR",
        "values": {str(cid): get_value_with_delta(cid, "ctr") for cid in all_cohort_ids}
    })
    metrics_list.append({
        "metric_key": "reuse_rate",
        "metric": "Reuse Rate",
        "values": {str(cid): get_value_with_delta(cid, "reuse_rate") for cid in all_cohort_ids}
    })
    metrics_list.append({
        "metric_key": "time_to_first_interaction",
        "metric": "Time to First Interaction (Median)",
        "values": {str(cid): get_value_with_delta(cid, "time_to_first_interaction") for cid in all_cohort_ids}
    })
    metrics_list.append({
        "metric_key": "engagement",
        "metric": "Engagement (Total)",
        "values": {str(cid): get_value_with_delta(cid, "engagement") for cid in all_cohort_ids}
    })
    metrics_list.append({
        "metric_key": "engagement_daily_avg",
        "metric": "Engagement (Retained Daily Avg)",
        "values": {str(cid): {**get_value_with_delta(cid, "engagement_daily_avg"), "sparkline": results[cid]["eng_sparkline"]} for cid in all_cohort_ids}
    })

    # Section 1.5: Monetization
    if monetization_events:
        metrics_list.append({
            "metric_key": "revenue_per_user",
            "metric": "Revenue / User",
            "values": {str(cid): get_value_with_delta(cid, "monetization", sub_key="revenue_per_user") for cid in all_cohort_ids}
        })
        metrics_list.append({
            "metric_key": "revenue_conversion",
            "metric": "Revenue Conversion",
            "values": {str(cid): get_value_with_delta(cid, "monetization", sub_key="revenue_conversion") for cid in all_cohort_ids}
        })
        metrics_list.append({
            "metric_key": "revenue_intensity",
            "metric": "Revenue / User (Active)",
            "values": {str(cid): get_value_with_delta(cid, "monetization", sub_key="revenue_intensity") for cid in all_cohort_ids}
        })
        metrics_list.append({
            "metric_key": "revenue_daily_avg",
            "metric": "Revenue / User (Retained Daily Avg)",
            "values": {str(cid): {**get_value_with_delta(cid, "revenue_daily_avg"), "sparkline": results[cid]["rev_sparkline"]} for cid in all_cohort_ids}
        })
    
    # Section 2: Impact
    for i, config in enumerate(impact_events):
        event_name = getattr(config, 'event_name', None) or config.get('event_name')
        metrics_list.append({
            "metric_key": f"{event_name}_reach",
            "metric": f"{event_name} → Reach",
            "values": {str(cid): get_value_with_delta(cid, "impact_metrics", i, "reach") for cid in all_cohort_ids}
        })
        metrics_list.append({
            "metric_key": f"{event_name}_intensity",
            "metric": f"{event_name} → Intensity",
            "values": {str(cid): get_value_with_delta(cid, "impact_metrics", i, "intensity") for cid in all_cohort_ids}
        })

    # Serialize request for stats endpoint cache
    def _serialize_events(evts):
        out = []
        for e in evts:
            if isinstance(e, dict):
                out.append(e)
            else:
                out.append({"event_name": getattr(e, 'event_name', str(e)),
                            "filters": [{"property": f.property, "operator": f.operator, "value": f.value}
                                        for f in getattr(e, 'filters', [])]})
        return out

    IMPACT_RUN_CACHE[run_id] = {
        "created_at": time.time(),
        "request": {
            "start_day": start_day,
            "end_day": end_day,
            "exposure_events": _serialize_events(exposure_events),
            "interaction_events": _serialize_events(interaction_events),
            "impact_events": _serialize_events(impact_events),
            "monetization_events": _serialize_events(monetization_events),
            "retention_event": retention_event
        },
        "results": results,
        "baseline_cohort_id": baseline_cohort_id,
        "all_cohort_ids": all_cohort_ids,
        "cohort_info": cohort_info,
    }

    return {
        "run_id": run_id,
        "cohorts": response_cohorts,
        "metrics": metrics_list
    }
