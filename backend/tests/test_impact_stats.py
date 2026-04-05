"""
Tests for impact statistical significance computation.
Covers: Z-test, Mann-Whitney U, edge cases, skip rules, sampling,
metric key consistency, run_id caching flow, and integration.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from tests.utils import csv_upload

# ---------------------------------------------------------------------------
# Unit tests — pure function, no DB
# ---------------------------------------------------------------------------

from app.domains.analytics.impact_stats_service import (
    compute_stat_test,
    METRIC_TEST_MAP,
)


class TestZTest:
    """Z-test proportion tests."""

    def test_z_test_significant(self):
        """baseline: 50/100, variant: 70/100 → p < 0.05"""
        result = compute_stat_test(
            "exposure_rate",
            {"x": 50, "n": 100},
            {"x": 70, "n": 100},
        )
        assert result["p_value"] is not None
        assert result["p_value"] < 0.05
        assert result["is_significant"] is True
        assert result["test_label"] == "Z-test"

    def test_z_test_not_significant(self):
        """baseline: 50/100, variant: 52/100 → p > 0.05"""
        result = compute_stat_test(
            "ctr",
            {"x": 50, "n": 100},
            {"x": 52, "n": 100},
        )
        assert result["p_value"] is not None
        assert result["p_value"] > 0.05
        assert result["is_significant"] is False

    def test_z_test_zero_variance(self):
        """baseline: 0/100, variant: 0/100 → insufficient data"""
        result = compute_stat_test(
            "exposure_rate",
            {"x": 0, "n": 100},
            {"x": 0, "n": 100},
        )
        assert result["p_value"] is None
        assert result["skip_reason"] == "no_difference"


class TestMWU:
    """Mann-Whitney U tests."""

    def test_mwu_significant(self):
        """Clearly different distributions → significant"""
        result = compute_stat_test(
            "engagement",
            {"values": [1, 1, 1, 1] * 10},
            {"values": [5, 5, 5, 5] * 10},
        )
        assert result["p_value"] is not None
        assert result["p_value"] < 0.05
        assert result["is_significant"] is True
        assert result["test_label"] == "Mann-Whitney U"

    def test_mwu_not_significant(self):
        """Overlapping distributions → not significant"""
        result = compute_stat_test(
            "engagement",
            {"values": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] * 3},
            {"values": [2, 3, 4, 5, 6, 7, 8, 9, 10, 11] * 3},
        )
        assert result["p_value"] is not None
        assert result["p_value"] > 0.05
        assert result["is_significant"] is False

    def test_mwu_unequal_sizes(self):
        """Unequal group sizes → valid p_value"""
        result = compute_stat_test(
            "intensity",
            {"values": [1, 2] * 15},
            {"values": [1, 2, 3, 4, 5] * 10},
        )
        assert result["p_value"] is not None
        assert isinstance(result["p_value"], float)

    def test_mwu_zero_users_included(self):
        """Zero-fill users present → valid result"""
        result = compute_stat_test(
            "engagement",
            {"values": [0, 0, 1, 2] * 10},
            {"values": [0, 3, 4, 5] * 10},
        )
        assert result["p_value"] is not None
        assert isinstance(result["p_value"], float)


class TestEdgeCases:
    """Edge cases and skip rules."""

    def test_empty_values(self):
        """Empty arrays → insufficient data"""
        result = compute_stat_test(
            "engagement",
            {"values": []},
            {"values": []},
        )
        assert result["p_value"] is None
        assert result["is_significant"] is False
        assert result["skip_reason"] == "insufficient_data"

    def test_zero_denominator(self):
        """n=0 → insufficient data"""
        result = compute_stat_test(
            "exposure_rate",
            {"x": 0, "n": 0},
            {"x": 0, "n": 0},
        )
        assert result["p_value"] is None
        assert result["skip_reason"] == "insufficient_data"

    def test_delta_zero_skips_test(self):
        """Same proportions → no_difference skip"""
        result = compute_stat_test(
            "exposure_rate",
            {"x": 50, "n": 100},
            {"x": 50, "n": 100},
        )
        assert result["p_value"] is None
        assert result["skip_reason"] == "no_difference"

    def test_low_sample_skips_test(self):
        """n < 30 → low_sample skip"""
        result = compute_stat_test(
            "engagement",
            {"values": [1, 2, 3]},       # n=3 < 30
            {"values": [4, 5, 6]},       # n=3 < 30
        )
        assert result["p_value"] is None
        assert result["skip_reason"] == "low_sample"

    def test_sampling_guard(self):
        """Large input → sampled=True"""
        big_baseline = {"values": list(range(60_000))}
        big_variant = {"values": list(range(1, 60_001))}
        result = compute_stat_test("engagement", big_baseline, big_variant)
        assert result["sampled"] is True


class TestMetricKeyConsistency:
    """Metric key mapping is complete and correct."""

    def test_all_metric_keys_mapped(self):
        expected_keys = {"exposure_rate", "ctr", "reach", "engagement", "intensity"}
        assert set(METRIC_TEST_MAP.keys()) == expected_keys

    def test_z_test_metrics(self):
        for k in ["exposure_rate", "ctr", "reach"]:
            assert METRIC_TEST_MAP[k] == "z_test"

    def test_mwu_metrics(self):
        for k in ["engagement", "intensity"]:
            assert METRIC_TEST_MAP[k] == "mwu"


# ---------------------------------------------------------------------------
# Integration tests — DB + client
# ---------------------------------------------------------------------------

def _setup_stats_dataset(client: TestClient):
    """Create dataset with enough users for statistical tests (n >= 30 per cohort)."""
    lines = ["user_id,event_name,event_time,revenue"]

    # Baseline: 100 users
    for i in range(1, 101):
        lines.append(f"b{i},signup,2024-01-01 00:00:00,0")

    # Variant: 100 users
    for i in range(1, 101):
        lines.append(f"v{i},sig_v,2024-01-01 00:00:00,0")

    # Baseline exposure: 50 users
    for i in range(1, 51):
        lines.append(f"b{i},exposure,2024-01-02 00:00:00,0")

    # Variant exposure: 80 users (significant difference)
    for i in range(1, 81):
        lines.append(f"v{i},exposure,2024-01-02 00:00:00,0")

    # Baseline interaction: 30 users, 2 events each
    for i in range(1, 31):
        lines.append(f"b{i},interaction,2024-01-02 01:00:00,0")
        lines.append(f"b{i},interaction,2024-01-02 02:00:00,0")

    # Variant interaction: 60 users, 3 events each
    for i in range(1, 61):
        lines.append(f"v{i},interaction,2024-01-02 01:00:00,0")
        lines.append(f"v{i},interaction,2024-01-02 02:00:00,0")
        lines.append(f"v{i},interaction,2024-01-02 03:00:00,0")

    # Impact event: baseline 20 users, variant 50 users
    for i in range(1, 21):
        lines.append(f"b{i},impact_ev,2024-01-02 05:00:00,0")
    for i in range(1, 51):
        lines.append(f"v{i},impact_ev,2024-01-02 05:00:00,0")

    csv_text = "\n".join(lines)
    upload = csv_upload(client, csv_text=csv_text)
    assert upload.status_code == 200

    client.post("/map-columns", json={
        "user_id_column": "user_id",
        "event_name_column": "event_name",
        "event_time_column": "event_time",
        "revenue_column": "revenue",
    })

    b = client.post("/cohorts", json={
        "name": "Baseline",
        "logic_operator": "AND",
        "conditions": [{"event_name": "signup", "min_event_count": 1}],
    })
    v = client.post("/cohorts", json={
        "name": "Variant",
        "logic_operator": "AND",
        "conditions": [{"event_name": "sig_v", "min_event_count": 1}],
    })
    return b.json()["cohort_id"], v.json()["cohort_id"]


def test_run_id_flow(client):
    """POST /impact/run → run_id → POST /impact/stats → valid stats."""
    b_id, v_id = _setup_stats_dataset(client)

    # Phase 1: run
    run_res = client.post("/impact/run", json={
        "baseline_cohort_id": b_id,
        "variant_cohort_ids": [v_id],
        "exposure_events": [{"event_name": "exposure"}],
        "interaction_events": [{"event_name": "interaction"}],
        "impact_events": [{"event_name": "impact_ev"}],
    })
    assert run_res.status_code == 200, run_res.text
    run_data = run_res.json()
    assert "run_id" in run_data
    run_id = run_data["run_id"]

    # Phase 2: stats
    stats_res = client.post("/impact/stats", json={"run_id": run_id})
    assert stats_res.status_code == 200, stats_res.text
    stats_data = stats_res.json()
    assert "stats" in stats_data

    # Verify structure: stats[metric_key][cohort_id]
    stats = stats_data["stats"]
    assert "exposure_rate" in stats
    cohort_stat = stats["exposure_rate"][str(v_id)]
    assert "p_value" in cohort_stat
    assert "is_significant" in cohort_stat
    assert "test_label" in cohort_stat


def test_run_id_expired(client):
    """POST /impact/stats with invalid run_id → 404."""
    res = client.post("/impact/stats", json={"run_id": "nonexistent-id"})
    assert res.status_code == 404


def test_stats_keys_match_metrics(client):
    """metric_keys in stats response match metric_keys in run response."""
    b_id, v_id = _setup_stats_dataset(client)

    run_res = client.post("/impact/run", json={
        "baseline_cohort_id": b_id,
        "variant_cohort_ids": [v_id],
        "exposure_events": [{"event_name": "exposure"}],
        "interaction_events": [{"event_name": "interaction"}],
        "impact_events": [{"event_name": "impact_ev"}],
    })
    run_data = run_res.json()
    run_id = run_data["run_id"]
    metric_keys_from_run = {m["metric_key"] for m in run_data["metrics"]}

    stats_res = client.post("/impact/stats", json={"run_id": run_id})
    stats_keys = set(stats_res.json()["stats"].keys())

    # stats keys should be a subset of run metric keys (baseline has no stats)
    assert stats_keys <= metric_keys_from_run
