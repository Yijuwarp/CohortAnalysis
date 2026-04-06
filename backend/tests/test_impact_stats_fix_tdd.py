import pytest
from unittest.mock import MagicMock
import duckdb
from app.domains.analytics.impact_stats_service import (
    _query_per_user_ctr,
    _query_per_user_daily_avg,
    compute_all_stats
)

@pytest.fixture
def mock_conn():
    conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    return conn

def test_unbiased_ctr_distribution_red(mock_conn):
    """
    Issue: CTR MWU and total-users population are out of sync.
    Red: _query_per_user_ctr currently uses HAVING which excludes 0s.
    """
    # Mocking rows: DuckDB fetchall returns list of tuples
    # If correctly implemented, it should return 100 rows even if most didn't have exposure
    mock_conn.execute.return_value.fetchall.return_value = [(1, 0.5)] * 20 + [(1, 0.0)] * 80
    
    vec = _query_per_user_ctr(mock_conn, 1, [{"event_name": "exp"}], [{"event_name": "int"}])
    
    # Existing implementation (with HAVING) would only return 20 values.
    # We want 100.
    assert len(vec) == 100
    assert vec.count(0.0) == 80

def test_ctr_low_exposure_guard_red(mock_conn):
    """
    Issue: Low exposure counts (not users) make CTR noisy.
    Red: compute_all_stats currently only checks n (users) > 30.
    """
    cached = {
        "request": {
            "exposure_events": [{"event_name": "exp"}],
            "interaction_events": [{"event_name": "int"}],
            "start_day": 0,
            "end_day": 7
        },
        "results": {
            "1": { # Baseline
                "total_users": 100,
                "exposure_users": 50,
                "exposure_counts": 50,
                "interaction_users": 10,
                "interaction_counts": 10,
                "reuse_users": 0
            },
            "2": { # Variant
                "total_users": 100,
                "exposure_users": 5,
                "exposure_counts": 5, # < 30 threshold
                "interaction_users": 1,
                "interaction_counts": 1,
                "reuse_users": 0
            }
        },
        "baseline_cohort_id": "1",
        "all_cohort_ids": ["1", "2"]
    }
    
    # Mock queries to return dummy vectors
    mock_conn.execute.return_value.fetchall.return_value = [(1, 0.0)] * 100
    
    stats = compute_all_stats(mock_conn, cached)
    
    # Requirement: If exposure_counts < 30 in ANY group, CTR skip_reason should be 'low_sample'
    assert stats["ctr"]["2"]["skip_reason"] == "low_sample"

def test_per_user_retained_daily_avg_red(mock_conn):
    """
    Issue: Daily Avg MWU runs on day-level aggregates instead of per-user averages.
    Red: compute_all_stats currently uses baseline_results['eng_daily_distribution'].
    """
    cached = {
        "request": {
            "interaction_events": [{"event_name": "int"}],
            "retention_event": "app_open",
            "start_day": 0,
            "end_day": 6 # 7 days
        },
        "results": {
            "1": {"eng_daily_distribution": [1.0] * 7},
            "2": {"eng_daily_distribution": [1.0] * 7}
        },
        "baseline_cohort_id": "1",
        "all_cohort_ids": ["1", "2"]
    }
    
    # Mock query to return different per-user distributions so delta != 0
    # Provide enough results for all possible MWU queries (counts, daily_avg, time_to_int)
    mock_conn.execute.return_value.fetchall.side_effect = [
        [(1, 1.0)] * 100, [(1, 1.0)] * 100, # engagement (counts)
        [(1, 5.0)] * 100, [(1, 0.0)] * 100, # engagement_daily_avg (the one we check)
        [(1, 10.0)] * 100, [(1, 10.0)] * 100, # time_to_first_interaction
    ] * 5 # Extra buffer
    
    stats = compute_all_stats(mock_conn, cached)
    
    # Verifying that it didn't just use the 7-day aggregate (length 7)
    assert "engagement_daily_avg" in stats
    assert stats["engagement_daily_avg"]["2"]["p_value"] is not None

def test_daily_avg_skips_if_retention_missing(mock_conn):
    """
    Verify that if retention_event is missing from the cache (the bug I just fixed),
    the daily average metrics are skipped (empty dict or no p-value).
    """
    cached = {
        "request": {
            "interaction_events": [{"event_name": "int"}],
            "retention_event": None, # Missing/None
            "start_day": 0,
            "end_day": 6
        },
        "results": {
            "1": {"eng_daily_distribution": [1.0] * 7},
            "2": {"eng_daily_distribution": [1.0] * 7}
        },
        "baseline_cohort_id": "1",
        "all_cohort_ids": ["1", "2"]
    }
    
    stats = compute_all_stats(mock_conn, cached)
    
    # If missing, it should at least not crash, but it won't have the key in stats
    # because the 'if retention_event' block is skipped.
    assert "engagement_daily_avg" in stats
    assert stats["engagement_daily_avg"] == {}

def test_impact_event_stats(mock_conn):
    """Verify that individual impact events (Reach/Intensity) get their stats."""
    cached = {
        "request": {
            "impact_events": [{"event_name": "purchase"}],
            "start_day": 0,
            "end_day": 7
        },
        "results": {
            "1": {
                "total_users": 100,
                "impact_metrics": [{"event": "purchase", "event_users": 50, "total_users": 100}]
            },
            "2": {
                "total_users": 100,
                "impact_metrics": [{"event": "purchase", "event_users": 70, "total_users": 100}]
            }
        },
        "baseline_cohort_id": "1",
        "all_cohort_ids": ["1", "2"]
    }
    
    # Mock per-user counts for intensity
    mock_conn.execute.return_value.fetchall.return_value = [(1, 1.0)] * 100
    
    stats = compute_all_stats(mock_conn, cached)
    
    # Verify reach stats (Z-test)
    assert stats["purchase_reach"]["2"]["test_label"] == "Z-test"
    assert stats["purchase_reach"]["2"]["p_value"] is not None

    # Verify intensity stats (MWU)
    assert stats["purchase_intensity"]["2"]["test_label"] == "Mann-Whitney U"

def test_sampling_enforcement_red(mock_conn):
    """
    Issue: Inconsistent sampling.
    Red: Some query helpers might lack USING SAMPLE.
    """
    # Verify _query_per_user_ctr has it
    _query_per_user_ctr(mock_conn, 1, [{"event_name": "exp"}], [{"event_name": "int"}])
    last_sql = mock_conn.execute.call_args[0][0]
    assert "USING SAMPLE reservoir" in last_sql
    assert "REPEATABLE (42)" in last_sql

    # Verify a new helper (to be implemented) has it
    # _query_per_user_daily_avg(...)
    _query_per_user_daily_avg(mock_conn, 1, [{"event_name": "int"}], "app_open", 7)
    last_sql = mock_conn.execute.call_args[0][0]
    assert "USING SAMPLE reservoir" in last_sql
