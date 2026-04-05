"""
Isolated statistical significance service for Experiment Impact.

Computes Z-test (proportions) and Mann-Whitney U (distributions) per metric.
Reuses statistical functions from comparison_service.py.

No correction for multiple comparisons in V1.
"""
from __future__ import annotations

import logging
import random
from typing import Any, Optional

import duckdb

from app.domains.analytics.comparison_service import (
    _two_proportion_z_test,
    _mann_whitney_u,
)
from app.domains.analytics.impact_service import build_event_filter_sql
from app.domains.cohorts.cohort_service import get_events_source_table

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

METRIC_TEST_MAP = {
    "exposure_rate": "z_test",
    "usage_rate": "z_test",
    "revenue_conversion": "z_test",
    "reuse_rate": "z_test",
    "ctr": "mwu",
    "engagement": "mwu",
    "intensity": "mwu",
    "time_to_first_interaction": "mwu",
    "engagement_daily_avg": "mwu",
    "revenue_daily_avg": "mwu",
    "revenue_per_user": "mwu",
    "revenue_intensity": "mwu",
    "reach": "z_test",
}

TEST_LABELS = {
    "z_test": "Z-test",
    "mwu": "Mann-Whitney U",
}

MAX_MWU_SAMPLE = 50_000
MIN_SAMPLE_SIZE = 30


# ---------------------------------------------------------------------------
# Core stat function
# ---------------------------------------------------------------------------

def compute_stat_test(
    metric_key: str,
    baseline_data: dict,
    variant_data: dict,
) -> dict:
    """
    Compute a statistical test for a single metric comparison.

    For z_test metrics: data = {"x": success_count, "n": total_count}
    For mwu metrics:    data = {"values": [per_user_event_counts]}

    Returns:
        {
            "p_value": float | None,
            "is_significant": bool,
            "test_label": str | None,
            "skip_reason": str | None,
            "sampled": bool,
        }
    """
    # Resolve which test to use
    raw_key = metric_key
    # Handle dynamic impact-event keys like "purchase_reach" → "reach"
    for suffix in ("_reach", "_intensity"):
        if raw_key.endswith(suffix):
            raw_key = suffix.lstrip("_")
            break

    test_type = METRIC_TEST_MAP.get(raw_key)
    if test_type is None:
        return _skip("insufficient_data", None)

    test_label = TEST_LABELS[test_type]

    if test_type == "z_test":
        return _compute_z_test(baseline_data, variant_data, test_label)
    else:
        return _compute_mwu(baseline_data, variant_data, test_label)


def _skip(reason: str, test_label: Optional[str]) -> dict:
    return {
        "p_value": None,
        "is_significant": False,
        "test_label": test_label,
        "skip_reason": reason,
        "sampled": False,
    }


def _compute_z_test(baseline: dict, variant: dict, test_label: str) -> dict:
    x_b, n_b = baseline.get("x", 0), baseline.get("n", 0)
    x_v, n_v = variant.get("x", 0), variant.get("n", 0)

    # Edge: zero denominator
    if n_b == 0 or n_v == 0:
        return _skip("insufficient_data", test_label)

    # Edge: low sample
    if n_b < MIN_SAMPLE_SIZE or n_v < MIN_SAMPLE_SIZE:
        return _skip("low_sample", test_label)

    # Edge: delta == 0
    p_b = x_b / n_b
    p_v = x_v / n_v
    if p_b == p_v:
        return _skip("no_difference", test_label)

    p_value = _two_proportion_z_test(x_b, n_b, x_v, n_v)

    return {
        "p_value": round(p_value, 6),
        "is_significant": p_value < 0.05,
        "test_label": test_label,
        "skip_reason": None,
        "sampled": False,
    }


def _compute_mwu(baseline: dict, variant: dict, test_label: str) -> dict:
    vec_b = baseline.get("values", [])
    vec_v = variant.get("values", [])

    # Edge: empty
    if len(vec_b) == 0 or len(vec_v) == 0:
        return _skip("insufficient_data", test_label)

    # Edge: low sample
    if len(vec_b) < MIN_SAMPLE_SIZE or len(vec_v) < MIN_SAMPLE_SIZE:
        return _skip("low_sample", test_label)

    # Edge: delta == 0 (compare means as proxy)
    mean_b = sum(vec_b) / len(vec_b)
    mean_v = sum(vec_v) / len(vec_v)
    if mean_b == mean_v:
        return _skip("no_difference", test_label)

    # Sampling guard
    sampled = False
    if len(vec_b) > MAX_MWU_SAMPLE:
        vec_b = random.sample(vec_b, MAX_MWU_SAMPLE)
        sampled = True
    if len(vec_v) > MAX_MWU_SAMPLE:
        vec_v = random.sample(vec_v, MAX_MWU_SAMPLE)
        sampled = True

    p_value = _mann_whitney_u(vec_b, vec_v)

    # Edge: zero variance returns 1.0 from _mann_whitney_u,
    # but if both groups are constant and equal we already caught it above.
    if p_value is None:
        return _skip("insufficient_data", test_label)

    return {
        "p_value": round(p_value, 6),
        "is_significant": p_value < 0.05,
        "test_label": test_label,
        "skip_reason": None,
        "sampled": sampled,
    }


# ---------------------------------------------------------------------------
# Full stats computation (called from /impact/stats endpoint)
# ---------------------------------------------------------------------------

def compute_all_stats(
    conn: duckdb.DuckDBPyConnection,
    cached: dict,
) -> dict[str, dict[str, dict]]:
    """
    Compute statistical significance for all metrics × variant cohorts.

    Uses cached request to rebuild impact_base (DuckDB temp views are
    connection-scoped, so the view from /impact/run doesn't exist here).

    Returns:
        {
            "<metric_key>": {
                "<cohort_id>": {
                    "p_value": ...,
                    "is_significant": ...,
                    "test_label": ...,
                    "skip_reason": ...,
                    "sampled": ...
                }
            }
        }
    """
    request_data = cached["request"]
    results = cached["results"]
    baseline_id = cached["baseline_cohort_id"]
    all_cohort_ids = cached["all_cohort_ids"]
    variant_ids = [cid for cid in all_cohort_ids if cid != baseline_id]

    # Rebuild impact_base view (connection-scoped, won't exist from /impact/run)
    _rebuild_impact_base(conn, request_data, all_cohort_ids)

    stats: dict[str, dict[str, dict]] = {}
    baseline_results = results[baseline_id]

    # Rate/Proportion Metrics (Z-test)
    rate_metrics = [
        ("exposure_rate", "exposure_users", "total_users"),
        ("usage_rate", "interaction_users", "exposure_users"),
        ("revenue_conversion", "monetization.paying_users", "total_users"),
        ("reuse_rate", "reuse_users", "interaction_users"),
    ]
    
    for key, num_key, den_key in rate_metrics:
        stats[key] = {}
        for vid in variant_ids:
            # Helper to get nested keys like "monetization.paying_users"
            def get_val(res, path):
                parts = path.split(".")
                curr = res
                for p in parts:
                    if not isinstance(curr, dict): return 0
                    curr = curr.get(p, 0)
                return curr

            stat = compute_stat_test(
                key,
                {"x": get_val(baseline_results, num_key), "n": get_val(baseline_results, den_key)},
                {"x": get_val(results[vid], num_key), "n": get_val(results[vid], den_key)},
            )
            _log_stat(key, vid, stat)
            stats[key][str(vid)] = stat

    # CTR (MWU)
    stats["ctr"] = {}
    exp_events = request_data.get("exposure_events", [])
    int_events = request_data.get("interaction_events", [])
    if exp_events and int_events:
        for vid in variant_ids:
            # CTR Low-Exposure Guard: Skip if total exposure count in either cohort < 30
            b_exp_count = baseline_results.get("exposure_counts", 0)
            v_exp_count = results[vid].get("exposure_counts", 0)
            
            if b_exp_count < MIN_SAMPLE_SIZE or v_exp_count < MIN_SAMPLE_SIZE:
                stat = _skip("low_sample", TEST_LABELS["mwu"])
            else:
                vec_b = _query_per_user_ctr(conn, baseline_id, exp_events, int_events)
                vec_v = _query_per_user_ctr(conn, vid, exp_events, int_events)
                stat = compute_stat_test("ctr", {"values": vec_b}, {"values": vec_v})
                if len(vec_b) >= MAX_MWU_SAMPLE or len(vec_v) >= MAX_MWU_SAMPLE:
                    stat["sampled"] = True

            _log_stat("ctr", vid, stat)
            stats["ctr"][str(vid)] = stat

    # --- MWU metrics ---

    # Engagement: per-user interaction event counts (zero-filled)
    interaction_events = request_data.get("interaction_events", [])
    retention_event = request_data.get("retention_event")
    
    if interaction_events:
        stats["engagement"] = {}
        stats["engagement_daily_avg"] = {}
        stats["time_to_first_interaction"] = {}

        for vid in variant_ids:
            # Engagement (Total)
            vec_b = _query_per_user_counts(conn, baseline_id, interaction_events)
            vec_v = _query_per_user_counts(conn, vid, interaction_events)
            stat = compute_stat_test("engagement", {"values": vec_b}, {"values": vec_v})
            if len(vec_b) >= MAX_MWU_SAMPLE or len(vec_v) >= MAX_MWU_SAMPLE:
                stat["sampled"] = True
            _log_stat("engagement", vid, stat)
            stats["engagement"][str(vid)] = stat

            # Engagement Daily Avg (Retained Daily Avg per user)
            if retention_event:
                vec_b_da = _query_per_user_daily_avg(conn, baseline_id, interaction_events, retention_event)
                vec_v_da = _query_per_user_daily_avg(conn, vid, interaction_events, retention_event)
                stat_da = compute_stat_test("engagement_daily_avg", {"values": vec_b_da}, {"values": vec_v_da})
                if len(vec_b_da) >= MAX_MWU_SAMPLE or len(vec_v_da) >= MAX_MWU_SAMPLE:
                    stat_da["sampled"] = True
                stats["engagement_daily_avg"][str(vid)] = stat_da

            # Time to First Interaction
            vec_b_time = _query_per_user_time_to_int(conn, baseline_id, exp_events, interaction_events)
            vec_v_time = _query_per_user_time_to_int(conn, vid, exp_events, interaction_events)
            
            stat_time = compute_stat_test("time_to_first_interaction", {"values": vec_b_time}, {"values": vec_v_time})
            if len(vec_b_time) >= MAX_MWU_SAMPLE or len(vec_v_time) >= MAX_MWU_SAMPLE:
                stat_time["sampled"] = True
            stats["time_to_first_interaction"][str(vid)] = stat_time

    # --- Monetization metrics ---
    mon_events = request_data.get("monetization_events", [])
    if mon_events:
        stats["revenue_per_user"] = {}
        stats["revenue_conversion"] = {}
        stats["revenue_intensity"] = {}
        stats["revenue_daily_avg"] = {}

        for vid in variant_ids:
            # Revenue / User: MWU on full cohort distributions (sampled)
            vec_b = _query_per_user_monetization(conn, baseline_id, mon_events)
            vec_v = _query_per_user_monetization(conn, vid, mon_events)
            stat_rpu = compute_stat_test("revenue_per_user", {"values": vec_b}, {"values": vec_v})
            if len(vec_b) >= MAX_MWU_SAMPLE or len(vec_v) >= MAX_MWU_SAMPLE:
                stat_rpu["sampled"] = True
            _log_stat("revenue_per_user", vid, stat_rpu)
            stats["revenue_per_user"][str(vid)] = stat_rpu

            # Revenue Conversion: Z-test on (paying_users, total_users)
            stat_conv = compute_stat_test(
                "revenue_conversion",
                {"x": get_val(baseline_results, "monetization.paying_users"), "n": baseline_results["total_users"]},
                {"x": get_val(results[vid], "monetization.paying_users"), "n": results[vid]["total_users"]}
            )
            _log_stat("revenue_conversion", vid, stat_conv)
            stats["revenue_conversion"][str(vid)] = stat_conv

            # Revenue / User (Active): MWU on distribution of PAYING users (sampled)
            dist_b_active = [v for v in vec_b if v > 0]
            dist_v_active = [v for v in vec_v if v > 0]
            stat_int = compute_stat_test("revenue_intensity", {"values": dist_b_active}, {"values": dist_v_active})
            if len(dist_b_active) >= MAX_MWU_SAMPLE or len(dist_v_active) >= MAX_MWU_SAMPLE:
                stat_int["sampled"] = True
            _log_stat("revenue_intensity", vid, stat_int)
            stats["revenue_intensity"][str(vid)] = stat_int

            # Revenue Daily Avg (MWU)
            if retention_event:
                vec_b_da = _query_per_user_daily_avg(conn, baseline_id, mon_events, retention_event, is_revenue=True)
                vec_v_da = _query_per_user_daily_avg(conn, vid, mon_events, retention_event, is_revenue=True)
                stat_da = compute_stat_test("revenue_daily_avg", {"values": vec_b_da}, {"values": vec_v_da})
                if len(vec_b_da) >= MAX_MWU_SAMPLE or len(vec_v_da) >= MAX_MWU_SAMPLE:
                    stat_da["sampled"] = True
                stats["revenue_daily_avg"][str(vid)] = stat_da

    # --- Per-impact-event metrics ---

    impact_events = request_data.get("impact_events", [])
    for ie_config in impact_events:
        if isinstance(ie_config, dict):
            event_name = ie_config.get("event_name", "")
        else:
            event_name = getattr(ie_config, "event_name", str(ie_config))

        reach_key = f"{event_name}_reach"
        intensity_key = f"{event_name}_intensity"

        # Find matching impact_metrics entry in cached results
        b_impact = _find_impact_metric(baseline_results, event_name)
        stats[reach_key] = {}
        stats[intensity_key] = {}

        for vid in variant_ids:
            v_impact = _find_impact_metric(results[vid], event_name)

            # Reach: z-test on (event_users, total_users)
            stat = compute_stat_test(
                reach_key,
                {"x": b_impact["event_users"], "n": b_impact["total_users"]},
                {"x": v_impact["event_users"], "n": v_impact["total_users"]},
            )
            _log_stat(reach_key, vid, stat)
            stats[reach_key][str(vid)] = stat

            # Intensity: MWU on per-user event counts (zero-filled)
            vec_b = _query_per_user_counts(conn, baseline_id, [ie_config])
            vec_v = _query_per_user_counts(conn, vid, [ie_config])
            stat = compute_stat_test(intensity_key, {"values": vec_b}, {"values": vec_v})
            _log_stat(intensity_key, vid, stat)
            stats[intensity_key][str(vid)] = stat

    return stats


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rebuild_impact_base(
    conn: duckdb.DuckDBPyConnection,
    request_data: dict,
    all_cohort_ids: list[int],
) -> None:
    """Rebuild the impact_base temp view using the cached request parameters."""
    source_table = get_events_source_table(conn)
    ids_str = ", ".join(map(str, all_cohort_ids))
    start_day = request_data.get("start_day", 0)
    end_day = request_data.get("end_day", 7)

    conn.execute(
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


def _query_per_user_counts(
    conn: duckdb.DuckDBPyConnection,
    cohort_id: int,
    event_configs: list,
    limit: int = MAX_MWU_SAMPLE
) -> list[float]:
    """
    Query per-user event counts for MWU, zero-filled via LEFT JOIN.
    Uses reservoir sampling in SQL for scalability and determinism.
    """
    event_sql, event_params = build_event_filter_sql(event_configs)
    
    # Reservoir sampling in DuckDB with repeatable seed
    sql = f"""
    SELECT user_id, value FROM (
        SELECT cm.user_id, COALESCE(sub.event_count, 0) AS value
        FROM cohort_membership cm
        LEFT JOIN (
            SELECT user_id, SUM(event_count) AS event_count
            FROM impact_base
            WHERE cohort_id = ? AND {event_sql}
            GROUP BY user_id
        ) sub ON cm.user_id = sub.user_id
        WHERE cm.cohort_id = ?
    ) AS core
    USING SAMPLE reservoir({limit} ROWS) REPEATABLE (42)
    """
    rows = conn.execute(sql, [cohort_id, *event_params, cohort_id]).fetchall()
    return [float(r[1]) for r in rows]

def _query_per_user_ctr(
    conn: duckdb.DuckDBPyConnection,
    cohort_id: int,
    exp_configs: list,
    int_configs: list,
    limit: int = MAX_MWU_SAMPLE
) -> list[float]:
    """
    Query per-user CTR (interactions/exposures) for MWU.
    Aligns with UI population by including all users (COALESCE exposures to 0).
    """
    exp_sql, exp_params = build_event_filter_sql(exp_configs)
    int_sql, int_params = build_event_filter_sql(int_configs)
    
    # We JOIN with cohort_membership to get the full population (unbiased CTR MWU)
    sql = f"""
    SELECT user_id, user_ctr FROM (
        SELECT 
            cm.user_id,
            COALESCE(
                SUM(CASE WHEN {int_sql} THEN event_count ELSE 0 END) / 
                NULLIF(CAST(SUM(CASE WHEN {exp_sql} THEN event_count ELSE 0 END) AS FLOAT), 0),
                0.0
            ) as user_ctr
        FROM cohort_membership cm
        LEFT JOIN impact_base ib ON cm.user_id = ib.user_id AND ib.cohort_id = cm.cohort_id
        WHERE cm.cohort_id = ?
        GROUP BY cm.user_id
    ) AS core
    USING SAMPLE reservoir({limit} ROWS) REPEATABLE (42)
    """
    rows = conn.execute(sql, [*int_params, *exp_params, cohort_id]).fetchall()
    return [float(r[1]) for r in rows]

def _query_per_user_daily_avg(
    conn: duckdb.DuckDBPyConnection,
    cohort_id: int,
    event_configs: list,
    retention_event: str,
    limit: int = MAX_MWU_SAMPLE,
    is_revenue: bool = False
) -> list[float]:
    """
    Query per-user 'Retained Daily Avg' (Total events on retained days / Count of retained days).
    Correctly implements the per-user behavioral distribution.
    """
    event_sql, event_params = build_event_filter_sql(event_configs)
    val_col = "COALESCE(modified_revenue, 0.0)" if is_revenue else "event_count"

    sql = f"""
    SELECT user_id, user_avg FROM (
        WITH user_day_stats AS (
            SELECT 
                user_id,
                FLOOR(EXTRACT(EPOCH FROM (event_time - join_time)) / 86400) AS rel_day,
                SUM(CASE WHEN {event_sql} THEN {val_col} ELSE 0 END) AS day_val,
                MAX(CASE WHEN event_name = ? THEN 1 ELSE 0 END) AS is_retained
            FROM impact_base
            WHERE cohort_id = ?
            GROUP BY user_id, rel_day
        )
        SELECT 
            cm.user_id,
            COALESCE(
                SUM(CASE WHEN is_retained = 1 THEN day_val ELSE 0 END) / 
                NULLIF(CAST(SUM(is_retained) AS FLOAT), 0),
                0.0
            ) AS user_avg
        FROM cohort_membership cm
        LEFT JOIN user_day_stats uds ON cm.user_id = uds.user_id
        WHERE cm.cohort_id = ?
        GROUP BY cm.user_id
    ) AS core
    USING SAMPLE reservoir({limit} ROWS) REPEATABLE (42)
    """
    rows = conn.execute(sql, [*event_params, retention_event, cohort_id, cohort_id]).fetchall()
    return [float(r[1]) for r in rows]

def _query_per_user_time_to_int(
    conn: duckdb.DuckDBPyConnection,
    cohort_id: int,
    exp_configs: list,
    int_configs: list,
    limit: int = MAX_MWU_SAMPLE
) -> list[float]:
    """Query per-user time-to-first-interaction with reservoir sampling."""
    exp_sql, exp_params = build_event_filter_sql(exp_configs)
    int_sql, int_params = build_event_filter_sql(int_configs)
    
    sql = f"""
    SELECT user_id, time_to_int FROM (
        WITH first_exposures AS (
            SELECT user_id, MIN(event_time) AS first_exp
            FROM impact_base 
            WHERE cohort_id = ? AND {exp_sql} 
            GROUP BY user_id
        ),
        interactions AS (
            SELECT i.user_id, MIN(i.event_time) AS first_int
            FROM impact_base i
            JOIN first_exposures fe ON i.user_id = fe.user_id
            WHERE i.cohort_id = ? AND {int_sql} AND i.event_time > fe.first_exp
            GROUP BY i.user_id
        )
        SELECT fe.user_id, EXTRACT(EPOCH FROM (first_int - first_exp)) AS time_to_int
        FROM first_exposures fe
        JOIN interactions i ON fe.user_id = i.user_id
    ) AS core
    USING SAMPLE reservoir({limit} ROWS) REPEATABLE (42)
    """
    rows = conn.execute(sql, [cohort_id, *exp_params, cohort_id, *int_params]).fetchall()
    return [float(r[1]) for r in rows]

def _query_per_user_monetization(
    conn: duckdb.DuckDBPyConnection,
    cohort_id: int,
    mon_configs: list,
    limit: int = MAX_MWU_SAMPLE
) -> list[float]:
    """Query per-user total revenue with reservoir sampling."""
    rev_sql, rev_params = build_event_filter_sql(mon_configs)
    
    sql = f"""
    SELECT user_id, total_rev FROM (
        SELECT 
            cm.user_id,
            COALESCE(SUM(es.modified_revenue), 0.0) as total_rev
        FROM cohort_membership cm
        LEFT JOIN impact_base es ON cm.user_id = es.user_id AND es.cohort_id = cm.cohort_id AND {rev_sql}
        WHERE cm.cohort_id = ?
        GROUP BY cm.user_id
    ) AS core
    USING SAMPLE reservoir({limit} ROWS) REPEATABLE (42)
    """
    rows = conn.execute(sql, [*rev_params, cohort_id]).fetchall()
    return [float(r[1]) for r in rows]


def _find_impact_metric(cohort_results: dict, event_name: str) -> dict:
    """Find the cached impact metric entry for a given event name."""
    for im in cohort_results.get("impact_metrics", []):
        if im["event"] == event_name:
            return im
    # Fallback: no data
    return {"event_users": 0, "total_users": 0, "reach": 0, "intensity": 0}


def _log_stat(metric_key: str, cohort_id: int, stat: dict) -> None:
    logger.info(
        "stat_computed: metric=%s cohort=%s test=%s p_value=%s sampled=%s",
        metric_key,
        cohort_id,
        stat.get("test_label"),
        stat.get("p_value"),
        stat.get("sampled"),
    )
