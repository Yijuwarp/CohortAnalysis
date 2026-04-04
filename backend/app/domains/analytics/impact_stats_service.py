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
    "ctr":           "z_test",
    "reach":         "z_test",
    "engagement":    "mwu",
    "intensity":     "mwu",
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

    # Edge: delta == 0
    p_b = x_b / n_b
    p_v = x_v / n_v
    if p_b == p_v:
        return _skip("no_difference", test_label)

    # Edge: low sample
    if n_b < MIN_SAMPLE_SIZE or n_v < MIN_SAMPLE_SIZE:
        return _skip("low_sample", test_label)

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

    # --- Z-test metrics ---

    # Exposure Rate: z-test on (exposure_users, total_users)
    stats["exposure_rate"] = {}
    for vid in variant_ids:
        stat = compute_stat_test(
            "exposure_rate",
            {"x": baseline_results["exposure_users"], "n": baseline_results["total_users"]},
            {"x": results[vid]["exposure_users"], "n": results[vid]["total_users"]},
        )
        _log_stat("exposure_rate", vid, stat)
        stats["exposure_rate"][str(vid)] = stat

    # CTR: z-test on (interaction_users, exposure_users)
    stats["ctr"] = {}
    for vid in variant_ids:
        b_exposure = baseline_results["exposure_users"]
        v_exposure = results[vid]["exposure_users"]
        stat = compute_stat_test(
            "ctr",
            {"x": baseline_results["interaction_users"], "n": b_exposure},
            {"x": results[vid]["interaction_users"], "n": v_exposure},
        )
        _log_stat("ctr", vid, stat)
        stats["ctr"][str(vid)] = stat

    # --- MWU metrics ---

    # Engagement: per-user interaction event counts (zero-filled)
    interaction_events = request_data.get("interaction_events", [])
    if interaction_events:
        stats["engagement"] = {}
        for vid in variant_ids:
            vec_b = _query_per_user_counts(conn, baseline_id, interaction_events)
            vec_v = _query_per_user_counts(conn, vid, interaction_events)
            stat = compute_stat_test("engagement", {"values": vec_b}, {"values": vec_v})
            _log_stat("engagement", vid, stat)
            stats["engagement"][str(vid)] = stat

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
) -> list[float]:
    """
    Query per-user event counts for MWU, zero-filled via LEFT JOIN.
    Includes ALL cohort members (even those with zero events).
    """
    event_sql, event_params = build_event_filter_sql(event_configs)

    rows = conn.execute(
        f"""
        SELECT cm.user_id, COALESCE(sub.event_count, 0) AS value
        FROM cohort_membership cm
        LEFT JOIN (
            SELECT user_id, SUM(event_count) AS event_count
            FROM impact_base
            WHERE cohort_id = ? AND {event_sql}
            GROUP BY user_id
        ) sub ON cm.user_id = sub.user_id
        WHERE cm.cohort_id = ?
        """,
        [cohort_id, *event_params, cohort_id],
    ).fetchall()

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
