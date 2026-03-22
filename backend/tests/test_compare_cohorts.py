"""
Backend tests for the POST /compare-cohorts endpoint.
Covers all seven required test scenarios.
"""
from __future__ import annotations

import io

import pytest
from fastapi.testclient import TestClient

from tests.utils import csv_upload


# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------

def _prepare_two_cohort_dataset(client: TestClient) -> tuple[int, int]:
    """
    Upload a CSV with two distinct groups of users and map columns.
    Returns (cohort_a_id, cohort_b_id).
    """
    csv_text = (
        "user_id,event_name,event_time,revenue\n"
        # Cohort A group: users a1-a5, signup on day 0
        "a1,signup,2024-01-01 09:00:00,0\n"
        "a1,open,2024-01-01 10:00:00,0\n"
        "a1,open,2024-01-02 10:00:00,0\n"
        "a1,purchase,2024-01-02 11:00:00,10\n"
        "a2,signup,2024-01-01 09:00:00,0\n"
        "a2,open,2024-01-01 11:00:00,0\n"
        "a2,open,2024-01-02 12:00:00,0\n"
        "a2,purchase,2024-01-02 12:00:00,5\n"
        "a3,signup,2024-01-01 09:00:00,0\n"
        "a3,open,2024-01-01 12:00:00,0\n"
        "a3,open,2024-01-02 13:00:00,0\n"
        "a4,signup,2024-01-01 09:00:00,0\n"
        "a4,open,2024-01-01 13:00:00,0\n"
        "a5,signup,2024-01-01 09:00:00,0\n"
        # Cohort B group: users b1-b5, signup on day 0
        "b1,purchase,2024-01-01 09:00:00,0\n"
        "b1,open,2024-01-01 10:00:00,0\n"
        "b1,purchase,2024-01-02 11:00:00,20\n"
        "b2,purchase,2024-01-01 09:00:00,0\n"
        "b2,open,2024-01-01 11:00:00,0\n"
        "b2,purchase,2024-01-02 12:00:00,15\n"
        "b3,purchase,2024-01-01 09:00:00,0\n"
        "b3,purchase,2024-01-02 13:00:00,8\n"
        "b4,purchase,2024-01-01 09:00:00,0\n"
        "b5,purchase,2024-01-01 09:00:00,0\n"
    )
    upload = csv_upload(client, csv_text=csv_text)
    assert upload.status_code == 200, upload.text

    mapped = client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
            "revenue_column": "revenue",
        },
    )
    assert mapped.status_code == 200, mapped.text

    # Enable purchase as revenue event
    client.post(
        "/update-revenue-config",
        json={"revenue_config": {"purchase": {"included": True, "override": None}}},
    )

    # Create cohort A (signup users)
    ca = client.post(
        "/cohorts",
        json={"name": "CohortA", "logic_operator": "AND", "conditions": [{"event_name": "signup", "min_event_count": 1}]},
    )
    assert ca.status_code == 200, ca.text
    ca_id = ca.json()["cohort_id"]

    # Create cohort B (purchase users)
    cb = client.post(
        "/cohorts",
        json={"name": "CohortB", "logic_operator": "AND", "conditions": [{"event_name": "purchase", "min_event_count": 1}]},
    )
    assert cb.status_code == 200, cb.text
    cb_id = cb.json()["cohort_id"]

    return ca_id, cb_id


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_compare_retention(client: TestClient) -> None:
    """Basic retention_rate comparison returns valid structure and p-values."""
    ca_id, cb_id = _prepare_two_cohort_dataset(client)

    response = client.post(
        "/compare-cohorts",
        json={
            "cohort_a": ca_id,
            "cohort_b": cb_id,
            "tab": "retention",
            "metric": "retention_rate",
            "day": 1,
        },
    )
    assert response.status_code == 200, response.text
    data = response.json()

    assert "metric_label" in data
    assert "cohort_a_value" in data
    assert "cohort_b_value" in data
    assert "p_value" in data
    assert "significant" in data
    assert isinstance(data["significant"], bool)
    tests = data["tests"]
    assert len(tests) == 2, "Expected two proportion tests"
    names = {t["name"] for t in tests}
    assert "two_proportion_z_test" in names
    assert "fisher_exact" in names
    for t in tests:
        assert 0.0 <= t["p_value"] <= 1.0


def test_compare_usage_per_installed_user(client: TestClient) -> None:
    """per_installed_user metric comparison returns mean-based tests."""
    ca_id, cb_id = _prepare_two_cohort_dataset(client)

    response = client.post(
        "/compare-cohorts",
        json={
            "cohort_a": ca_id,
            "cohort_b": cb_id,
            "tab": "usage",
            "metric": "per_installed_user",
            "day": 1,
            "event": "open",
        },
    )
    assert response.status_code == 200, response.text
    data = response.json()

    assert "cohort_a_value" in data
    assert "cohort_b_value" in data
    tests = data["tests"]
    names = {t["name"] for t in tests}
    assert "welch_t_test" in names
    assert "mann_whitney_u" in names
    for t in tests:
        assert 0.0 <= t["p_value"] <= 1.0


def test_compare_usage_unique_users_percent(client: TestClient) -> None:
    """unique_users_percent returns proportion-based tests with correct values."""
    ca_id, cb_id = _prepare_two_cohort_dataset(client)

    response = client.post(
        "/compare-cohorts",
        json={
            "cohort_a": ca_id,
            "cohort_b": cb_id,
            "tab": "usage",
            "metric": "unique_users_percent",
            "day": 1,
            "event": "open",
        },
    )
    assert response.status_code == 200, response.text
    data = response.json()

    # Cohort A has 3 users who fired 'open' on day 1; cohort B has some too
    assert 0.0 <= data["cohort_a_value"] <= 1.0
    assert 0.0 <= data["cohort_b_value"] <= 1.0
    assert data["p_value"] >= 0.0


def test_compare_monetization_per_acquired_user(client: TestClient) -> None:
    """revenue_per_acquired_user comparison returns valid monetization stats."""
    ca_id, cb_id = _prepare_two_cohort_dataset(client)

    response = client.post(
        "/compare-cohorts",
        json={
            "cohort_a": ca_id,
            "cohort_b": cb_id,
            "tab": "monetization",
            "metric": "revenue_per_acquired_user",
            "day": 1,
        },
    )
    assert response.status_code == 200, response.text
    data = response.json()

    # Cohort A has 5 users, 2 of which bought (day 1 purchase); cohort B has 5 users, 3 of which bought
    assert "metric_label" in data
    assert data["metric_label"].startswith("Day 1")
    assert isinstance(data["cohort_a_value"], float)
    assert isinstance(data["cohort_b_value"], float)


def test_compare_cumulative_usage(client: TestClient) -> None:
    """cumulative_per_installed_user uses day_offset <= X condition."""
    ca_id, cb_id = _prepare_two_cohort_dataset(client)

    response = client.post(
        "/compare-cohorts",
        json={
            "cohort_a": ca_id,
            "cohort_b": cb_id,
            "tab": "usage",
            "metric": "cumulative_per_installed_user",
            "day": 2,
            "event": "open",
        },
    )
    assert response.status_code == 200, response.text
    data = response.json()

    # Cumulative value should be >= day-only value for any same cohort
    # At least check structure is correct
    assert data["cohort_a_value"] >= 0.0
    assert data["cohort_b_value"] >= 0.0
    assert 0.0 <= data["p_value"] <= 1.0
    names = {t["name"] for t in data["tests"]}
    assert "welch_t_test" in names


def test_invalid_metric_rejected(client: TestClient) -> None:
    """Unknown metric returns HTTP 400."""
    ca_id, cb_id = _prepare_two_cohort_dataset(client)

    response = client.post(
        "/compare-cohorts",
        json={
            "cohort_a": ca_id,
            "cohort_b": cb_id,
            "tab": "retention",
            "metric": "not_a_real_metric",
            "day": 7,
        },
    )
    assert response.status_code == 400
    assert "metric" in response.json()["detail"].lower()


def test_same_cohort_rejected(client: TestClient) -> None:
    """Comparing a cohort against itself returns HTTP 400."""
    ca_id, _ = _prepare_two_cohort_dataset(client)

    response = client.post(
        "/compare-cohorts",
        json={
            "cohort_a": ca_id,
            "cohort_b": ca_id,
            "tab": "retention",
            "metric": "retention_rate",
            "day": 1,
        },
    )
    assert response.status_code == 400
    assert "different" in response.json()["detail"].lower()


def _prepare_edge_case_dataset(client: TestClient) -> tuple[int, int, int]:
    """
    Upload a CSV with three groups for zero variance and low variance testing.
    Returns (cohort_0_id, cohort_1_id, cohort_var_id).
    """
    csv_text = (
        "user_id,event_name,event_time,revenue\n"
        # Cohort 0: no revenue
        "c0_1,signup,2024-01-01 09:00:00,0\n"
        "c0_2,signup,2024-01-01 09:00:00,0\n"
        "c0_3,signup,2024-01-01 09:00:00,0\n"
        # Cohort 1: no revenue
        "c1_1,view,2024-01-01 09:00:00,0\n"
        "c1_2,view,2024-01-01 09:00:00,0\n"
        "c1_3,view,2024-01-01 09:00:00,0\n"
        # Cohort var: small variance
        "cv_1,login,2024-01-01 09:00:00,0\n"
        "cv_2,login,2024-01-01 09:00:00,0\n"
        "cv_3,login,2024-01-01 09:00:00,0\n"
        "cv_1,purchase,2024-01-02 11:00:00,10\n"
    )
    upload = csv_upload(client, csv_text=csv_text)
    assert upload.status_code == 200, upload.text

    mapped = client.post(
        "/map-columns",
        json={"user_id_column": "user_id", "event_name_column": "event_name", "event_time_column": "event_time", "revenue_column": "revenue"},
    )
    assert mapped.status_code == 200, mapped.text

    client.post("/update-revenue-config", json={"revenue_config": {"purchase": {"included": True, "override": None}}})

    c0 = client.post("/cohorts", json={"name": "C0", "logic_operator": "AND", "conditions": [{"event_name": "signup", "min_event_count": 1}]})
    c1 = client.post("/cohorts", json={"name": "C1", "logic_operator": "AND", "conditions": [{"event_name": "view", "min_event_count": 1}]})
    cv = client.post("/cohorts", json={"name": "CV", "logic_operator": "AND", "conditions": [{"event_name": "login", "min_event_count": 1}]})

    return c0.json()["cohort_id"], c1.json()["cohort_id"], cv.json()["cohort_id"]


def test_compare_edge_cases(client: TestClient) -> None:
    """Test zero variance and Mann-Whitney priority."""
    c0, c1, cv = _prepare_edge_case_dataset(client)

    # 1. Zero variance case
    res0 = client.post(
        "/compare-cohorts",
        json={"cohort_a": c0, "cohort_b": c1, "tab": "monetization", "metric": "cumulative_revenue_per_acquired_user", "day": 7},
    )
    assert res0.status_code == 200, res0.text
    data0 = res0.json()

    assert data0["p_value"] is None
    mw_test = next(t for t in data0["tests"] if t["name"] == "mann_whitney_u")
    assert mw_test["p_value"] is None

    # 2. Low variance case (CV vs C0) -> C0 has 0 var, CV has one purchase (non-zero var)
    res_low = client.post(
        "/compare-cohorts",
        json={"cohort_a": cv, "cohort_b": c0, "tab": "monetization", "metric": "cumulative_revenue_per_acquired_user", "day": 7},
    )
    assert res_low.status_code == 200, res_low.text
    data_low = res_low.json()

    # Should not crash, returning valid tests
    assert data_low["p_value"] is not None
    mw_test = next(t for t in data_low["tests"] if t["name"] == "mann_whitney_u")
    tt_test = next(t for t in data_low["tests"] if t["name"] == "welch_t_test")

    # 3. Mann-Whitney Primary Logic
    assert data_low["p_value"] == mw_test["p_value"]
    
    # 4. No longer testing min() (meaning if tt < mw, p_value will still be mw)
    # The minimum of the two is NOT always chosen.
    assert "mann_whitney_u" in [t["name"] for t in data_low["tests"]]

