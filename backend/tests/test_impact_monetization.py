from __future__ import annotations
import pytest
from fastapi.testclient import TestClient
from tests.utils import csv_upload

def _setup_monetization_dataset(client: TestClient):
    # Base baseline users, Base variant users
    csv_text = (
        "user_id,event_name,event_time,revenue\n"
        + "\n".join([f"base_{i},signup,2024-01-01 00:00:00,0" for i in range(1, 101)]) + "\n"
        + "\n".join([f"var_{i},signup_v,2024-01-01 00:00:00,0" for i in range(1, 101)]) + "\n"
        + "\n".join([f"base_{i},exposure,2024-01-01 01:00:00,0" for i in range(1, 61)]) + "\n"
        + "\n".join([f"base_{i},interaction,2024-01-01 02:00:00,0" for i in range(1, 41)]) + "\n"
        + "\n".join([f"var_{i},exposure,2024-01-01 01:00:00,0" for i in range(1, 61)]) + "\n"
        + "\n".join([f"var_{i},interaction,2024-01-01 02:00:00,0" for i in range(1, 41)]) + "\n"
        + "\n".join([f"base_{i},purchase,2024-01-02 00:00:00,10" for i in range(1, 21)]) + "\n"
        + "\n".join([f"var_{i},purchase,2024-01-02 00:00:00,15" for i in range(1, 41)]) + "\n"
        + "\n".join([f"base_{i},ad_impression,2024-01-02 00:00:01,1" for i in range(1, 6)]) + "\n"
    )
    csv_upload(client, csv_text=csv_text)
    client.post("/map-columns", json={"user_id_column": "user_id", "event_name_column": "event_name", "event_time_column": "event_time", "revenue_column": "revenue"})
    b_id = client.post("/cohorts", json={"name": "B", "logic_operator": "AND", "conditions": [{"event_name": "signup", "min_event_count": 1}]}).json()["cohort_id"]
    v_id = client.post("/cohorts", json={"name": "V", "logic_operator": "AND", "conditions": [{"event_name": "signup_v", "min_event_count": 1}]}).json()["cohort_id"]
    return b_id, v_id

def test_monetization_metrics_accuracy(client):
    b_id, v_id = _setup_monetization_dataset(client)
    payload = {
        "baseline_cohort_id": b_id,
        "variant_cohort_ids": [v_id],
        "monetization_events": [{"event_name": "purchase"}, {"event_name": "ad_impression"}],
        "exposure_events": [{"event_name": "exposure"}],
        "interaction_events": [{"event_name": "interaction"}],
        "retention_event": "interaction"
    }
    res = client.post("/impact/run", json=payload)
    data = res.json()
    m = next(row for row in data["metrics"] if row["metric"] == "Revenue / User")
    assert m["values"][str(b_id)]["value"] == 2.05

def test_monetization_left_join_correctness(client):
    csv_text = (
        "user_id,event_name,event_time,revenue\n"
        + "\n".join([f"lj_{i},signup,2024-01-01 00:00:00,0" for i in range(1, 11)]) + "\n"
        + "lj_1,purchase,2024-01-02 00:00:00,100\n"
    )
    csv_upload(client, csv_text=csv_text)
    client.post("/map-columns", json={"user_id_column": "user_id", "event_name_column": "event_name", "event_time_column": "event_time", "revenue_column": "revenue"})
    c_id = client.post("/cohorts", json={"name": "LJ", "logic_operator": "AND", "conditions": [{"event_name": "signup", "min_event_count": 1}]}).json()["cohort_id"]
    payload = {
        "baseline_cohort_id": c_id,
        "variant_cohort_ids": [],
        "exposure_events": [{"event_name": "signup"}],
        "interaction_events": [{"event_name": "signup"}],
        "monetization_events": [{"event_name": "purchase"}],
        "retention_event": "signup"
    }
    res = client.post("/impact/run", json=payload)
    data = res.json()
    m = next(row for row in data["metrics"] if row["metric"] == "Revenue / User")
    assert m["values"][str(c_id)]["value"] == 10.0

def test_monetization_filter_isolation(client):
    csv_text = (
        "user_id,event_name,event_time,revenue,category\n"
        + "fi_1,signup,2024-01-01 00:00:00,0,none\n"
        + "fi_1,purchase,2024-01-02 00:00:00,100,electronics\n"
        + "fi_1,purchase,2024-01-02 00:00:01,50,books\n"
    )
    csv_upload(client, csv_text=csv_text)
    client.post("/map-columns", json={"user_id_column": "user_id", "event_name_column": "event_name", "event_time_column": "event_time", "revenue_column": "revenue"})
    c_id = client.post("/cohorts", json={"name": "FI", "logic_operator": "AND", "conditions": [{"event_name": "signup", "min_event_count": 1}]}).json()["cohort_id"]
    payload = {
        "baseline_cohort_id": c_id,
        "variant_cohort_ids": [],
        "exposure_events": [{"event_name": "signup"}],
        "interaction_events": [{"event_name": "signup"}],
        "monetization_events": [{"event_name": "purchase", "filters": [{"property": "category", "value": "electronics"}]}],
        "retention_event": "signup"
    }
    res = client.post("/impact/run", json=payload)
    data = res.json()
    m = next(row for row in data["metrics"] if row["metric"] == "Revenue / User")
    assert m["values"][str(c_id)]["value"] == 100.0
    
    payload["monetization_events"] = [
        {"event_name": "purchase", "filters": [{"property": "category", "value": "electronics"}]},
        {"event_name": "purchase", "filters": [{"property": "category", "value": "books"}]}
    ]
    res = client.post("/impact/run", json=payload)
    data = res.json()
    m = next(row for row in data["metrics"] if row["metric"] == "Revenue / User")
    assert m["values"][str(c_id)]["value"] == 150.0

def test_monetization_stats_significance(client):
    b_id, v_id = _setup_monetization_dataset(client)
    payload = {
        "baseline_cohort_id": b_id,
        "variant_cohort_ids": [v_id],
        "exposure_events": [{"event_name": "exposure"}],
        "interaction_events": [{"event_name": "interaction"}],
        "monetization_events": [{"event_name": "purchase"}],
        "retention_event": "interaction"
    }
    run_res = client.post("/impact/run", json=payload)
    run_id = run_res.json()["run_id"]
    stats_res = client.post("/impact/stats", json={"run_id": run_id})
    stats = stats_res.json()["stats"]
    assert stats["revenue_per_user"][str(v_id)]["p_value"] < 0.05

def test_monetization_empty_events(client):
    b_id, v_id = _setup_monetization_dataset(client)
    payload = {
        "baseline_cohort_id": b_id,
        "variant_cohort_ids": [v_id],
        "exposure_events": [{"event_name": "exposure"}],
        "interaction_events": [{"event_name": "interaction"}],
        "monetization_events": [],
        "retention_event": "interaction"
    }
    res = client.post("/impact/run", json=payload)
    metrics = [m["metric"] for m in res.json()["metrics"]]
    assert "Revenue / User" not in metrics
