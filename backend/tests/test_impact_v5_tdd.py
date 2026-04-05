import pytest
from fastapi.testclient import TestClient
from tests.utils import csv_upload

def test_impact_request_validation(client: TestClient):
    # Setup basic dataset
    csv_text = "user_id,event_name,event_time\nb1,signup,2024-01-01 00:00:00\nb1,exposure,2024-01-02 00:00:00\nb1,interaction,2024-01-02 01:00:00"
    upload = csv_upload(client, csv_text=csv_text)
    assert upload.status_code == 200
    client.post("/map-columns", json={"user_id_column":"user_id","event_name_column":"event_name","event_time_column":"event_time"})
    b_id = client.post("/cohorts", json={"name":"B", "logic_operator": "AND", "conditions":[{"event_name":"signup","min_event_count":1}]}).json()["cohort_id"]

    # Fails if retention_event is missing
    res = client.post("/impact/run", json={
        "baseline_cohort_id": b_id,
        "variant_cohort_ids": [],
        "exposure_events": [{"event_name": "exposure"}],
        "interaction_events": [{"event_name": "interaction"}]
        # retention_event is MISSING
    })
    
    # Requirement: 400 Bad Request if missing
    assert res.status_code == 400, res.text
    assert "retention_event" in res.json()["detail"].lower()

def test_impact_request_success_with_retention_event(client: TestClient):
    csv_text = "user_id,event_name,event_time\nb1,signup,2024-01-01 00:00:00\nb1,exposure,2024-01-02 00:00:00\nb1,interaction,2024-01-02 01:00:00"
    csv_upload(client, csv_text=csv_text)
    client.post("/map-columns", json={"user_id_column":"user_id","event_name_column":"event_name","event_time_column":"event_time"})
    b_id = client.post("/cohorts", json={"name":"B", "logic_operator": "AND", "conditions":[{"event_name":"signup","min_event_count":1}]}).json()["cohort_id"]

    # Succeeds if retention_event is provided
    res = client.post("/impact/run", json={
        "baseline_cohort_id": b_id,
        "variant_cohort_ids": [],
        "exposure_events": [{"event_name": "exposure"}],
        "interaction_events": [{"event_name": "interaction"}],
        "retention_event": "signup"
    })
    assert res.status_code == 200, res.text

def test_core_metrics_accuracy(client: TestClient):
    csv_text = (
        "user_id,event_name,event_time\n"
        # 100 users in cohort
        + "\n".join([f"u{i},signup,2024-01-01 00:00:00" for i in range(1, 101)]) + "\n"
        # 60 users exposed
        + "\n".join([f"u{i},exposure,2024-01-02 00:00:00" for i in range(1, 61)]) + "\n"
        # 30 users interact (min 1 interaction)
        + "\n".join([f"u{i},interaction,2024-01-02 01:00:00" for i in range(1, 31)]) + "\n"
        # 15 of them interact again (min 2 interactions)
        + "\n".join([f"u{i},interaction,2024-01-02 02:00:00" for i in range(1, 16)]) + "\n"
        # Total interactions = 30 + 15 = 45
        # Total exposures = 60
    )
    csv_upload(client, csv_text=csv_text)
    client.post("/map-columns", json={"user_id_column":"user_id","event_name_column":"event_name","event_time_column":"event_time"})
    b_id = client.post("/cohorts", json={"name":"B", "logic_operator": "AND", "conditions":[{"event_name":"signup","min_event_count":1}]}).json()["cohort_id"]

    res = client.post("/impact/run", json={
        "baseline_cohort_id": b_id,
        "variant_cohort_ids": [],
        "exposure_events": [{"event_name": "exposure"}],
        "interaction_events": [{"event_name": "interaction"}],
        "retention_event": "signup"
    })
    assert res.status_code == 200
    metrics = res.json()["metrics"]
    
    # 1. Usage Rate: 30 / 60 = 0.5
    m = next(m for m in metrics if m["metric_key"] == "usage_rate")
    assert m["values"][str(b_id)]["value"] == 0.5

    # 2. CTR: 45 / 60 = 0.75
    m = next(m for m in metrics if m["metric_key"] == "ctr")
    assert m["values"][str(b_id)]["value"] == 0.75

    # 3. Reuse Rate: 15 / 30 = 0.5
    m = next(m for m in metrics if m["metric_key"] == "reuse_rate")
    assert m["values"][str(b_id)]["value"] == 0.5

def test_time_to_interaction_accuracy(client: TestClient):
    csv_text = (
        "user_id,event_name,event_time\n"
        # User 1: Exposure at 10s, Interaction at 30s -> 20s
        "u1,signup,2024-01-01 00:00:00\n"
        "u1,exposure,2024-01-01 00:00:10\n"
        "u1,interaction,2024-01-01 00:00:30\n"
        # User 2: Exposure at 10s, Interaction at 50s -> 40s
        "u2,signup,2024-01-01 00:00:00\n"
        "u2,exposure,2024-01-01 00:00:10\n"
        "u2,interaction,2024-01-01 00:00:50\n"
        # User 3: Exposure at 10s, Interaction at 5s -> INVALID (interaction before exposure)
        "u3,signup,2024-01-01 00:00:00\n"
        "u3,exposure,2024-01-01 00:00:10\n"
        "u3,interaction,2024-01-01 00:00:05\n"
        # User 4: Exposure ONLY -> INVALID
        "u4,signup,2024-01-01 00:00:00\n"
        "u4,exposure,2024-01-01 00:00:10\n"
        # User 5: Interaction ONLY -> INVALID (no exposure)
        "u5,signup,2024-01-01 00:00:00\n"
        "u5,interaction,2024-01-01 00:00:30\n"
        # Median of [20, 40] = 30
    )
    csv_upload(client, csv_text=csv_text)
    client.post("/map-columns", json={"user_id_column":"user_id","event_name_column":"event_name","event_time_column":"event_time"})
    b_id = client.post("/cohorts", json={"name":"B", "logic_operator": "AND", "conditions":[{"event_name":"signup","min_event_count":1}]}).json()["cohort_id"]

    res = client.post("/impact/run", json={
        "baseline_cohort_id": b_id,
        "variant_cohort_ids": [],
        "exposure_events": [{"event_name": "exposure"}],
        "interaction_events": [{"event_name": "interaction"}],
        "retention_event": "signup"
    })
    assert res.status_code == 200
    metrics = res.json()["metrics"]
    
    m = next(m for m in metrics if m["metric_key"] == "time_to_first_interaction")
    assert m["values"][str(b_id)]["value"] == 30.0

def test_daily_averages_accuracy(client: TestClient):
    csv_text = (
        "user_id,event_name,event_time,revenue\n"
        # Cohort: u1, u2
        "u1,signup,2024-01-01 00:00:00,0\n"
        "u2,signup,2024-01-01 00:00:00,0\n"
        # Day 0: u1 active (app_open), u1 interaction (1), u1 rev (10)
        "u1,app_open,2024-01-01 01:00:00,10\n"
        "u1,interaction,2024-01-01 02:00:00,0\n"
        # Day 0 values:
        # Denominator (active users u1): 1
        # Eng = 1 / 1 = 1.0
        # Rev = 10 / 1 = 10.0
        # Day 1: u1, u2 active (app_open). u1 interaction (2), u2 interaction (2). u2 rev (20)
        "u1,app_open,2024-01-02 01:00:00,0\n"
        "u1,interaction,2024-01-02 02:00:00,0\n"
        "u1,interaction,2024-01-02 03:00:00,0\n"
        "u2,app_open,2024-01-02 01:00:00,20\n"
        "u2,interaction,2024-01-02 02:00:00,0\n"
        "u2,interaction,2024-01-02 03:00:00,0\n"
        # Day 1 values:
        # Denominator (active users u1, u2): 2
        # Eng = (2 + 2) / 2 = 2.0
        # Rev = 20 / 2 = 10.0
        # Simple Averages (D0, D1):
        # Eng = (1.0 + 2.0) / 2 = 1.5
        # Rev = (10.0 + 10.0) / 2 = 10.0
    )
    csv_upload(client, csv_text=csv_text)
    client.post("/map-columns", json={"user_id_column":"user_id","event_name_column":"event_name","event_time_column":"event_time","revenue_column":"revenue"})
    b_id = client.post("/cohorts", json={"name":"B", "logic_operator": "AND", "conditions":[{"event_name":"signup","min_event_count":1}]}).json()["cohort_id"]

    res = client.post("/impact/run", json={
        "baseline_cohort_id": b_id,
        "variant_cohort_ids": [],
        "exposure_events": [{"event_name": "signup"}], 
        "interaction_events": [{"event_name": "interaction"}],
        "monetization_events": [{"event_name": "app_open"}], # revenue on app_open
        "retention_event": "app_open",
        "start_day": 0,
        "end_day": 1
    })
    assert res.status_code == 200
    metrics = res.json()["metrics"]
    
    # 1. Engagement (Retained Daily Avg): 1.5
    m = next(m for m in metrics if m["metric_key"] == "engagement_daily_avg")
    assert m["values"][str(b_id)]["value"] == 1.5
    assert m["values"][str(b_id)]["sparkline"] == [1.0, 2.0]

    # 2. Revenue / User (Retained Daily Avg): 10.0
    m = next(m for m in metrics if m["metric_key"] == "revenue_daily_avg")
    assert m["values"][str(b_id)]["value"] == 10.0
    assert m["values"][str(b_id)]["sparkline"] == [10.0, 10.0]

def test_statistical_test_mappings(client: TestClient):
    # Setup dataset with enough users for Z-test and MWU (n=40 per group)
    lines = ["user_id,event_name,event_time,revenue"]
    for i in range(1, 41):
        lines.append(f"b{i},signup,2024-01-01 00:00:00,0")
        lines.append(f"v{i},signup_v,2024-01-01 00:00:00,0")
        # Exposure
        lines.append(f"b{i},exp,2024-01-01 01:00:00,0")
        lines.append(f"v{i},exp,2024-01-01 01:00:00,0")
        # Interaction (Divergent for stats)
        lines.append(f"b{i},int,2024-01-01 02:00:00,0")
        for _ in range(5): # v has 5x more interactions
            lines.append(f"v{i},int,2024-01-01 02:00:00,0")
            
    csv_upload(client, csv_text="\n".join(lines))
    client.post("/map-columns", json={"user_id_column":"user_id","event_name_column":"event_name","event_time_column":"event_time","revenue_column":"revenue"})
    b_id = client.post("/cohorts", json={"name":"B", "logic_operator": "AND", "conditions":[{"event_name":"signup","min_event_count":1}]}).json()["cohort_id"]
    v_id = client.post("/cohorts", json={"name":"V", "logic_operator": "AND", "conditions":[{"event_name":"signup_v","min_event_count":1}]}).json()["cohort_id"]

    run_res = client.post("/impact/run", json={
        "baseline_cohort_id": b_id,
        "variant_cohort_ids": [v_id],
        "exposure_events": [{"event_name": "exp"}], 
        "interaction_events": [{"event_name": "int"}],
        "retention_event": "signup",
        "start_day": 0,
        "end_day": 1
    })
    run_id = run_res.json()["run_id"]
    
    stats_res = client.post("/impact/stats", json={"run_id": run_id})
    if stats_res.status_code != 200:
        print(f"STATS_ERROR: {stats_res.status_code} - {stats_res.text}")
    assert stats_res.status_code == 200
    stats = stats_res.json()["stats"]
    
    # Verify Mappings
    # Z-test: exposure_rate, usage_rate, reuse_rate
    assert stats["exposure_rate"][str(v_id)]["test_label"] == "Z-test"
    assert stats["usage_rate"][str(v_id)]["test_label"] == "Z-test"
    assert stats["reuse_rate"][str(v_id)]["test_label"] == "Z-test"
    
    # MWU: ctr, engagement_daily_avg, engagement (Total)
    assert stats["ctr"][str(v_id)]["test_label"] == "Mann-Whitney U"
    assert stats["engagement_daily_avg"][str(v_id)]["test_label"] == "Mann-Whitney U"
    assert stats["engagement"][str(v_id)]["test_label"] == "Mann-Whitney U"

def test_small_sample_guard(client: TestClient):
    # Setup dataset with only 10 users (n < 30)
    lines = ["user_id,event_name,event_time,revenue"]
    for i in range(1, 11):
        lines.append(f"b{i},signup,2024-01-01 00:00:00,0")
        lines.append(f"v{i},signup_v,2024-01-01 00:00:00,0")
        lines.append(f"b{i},exp,2024-01-01 01:00:00,0")
        lines.append(f"v{i},exp,2024-01-01 01:00:00,0")
        lines.append(f"b{i},int,2024-01-01 02:00:00,0")
        lines.append(f"v{i},int,2024-01-01 02:05:00,0")
            
    csv_upload(client, csv_text="\n".join(lines))
    client.post("/map-columns", json={"user_id_column":"user_id","event_name_column":"event_name","event_time_column":"event_time","revenue_column":"revenue"})
    b_id = client.post("/cohorts", json={"name":"B", "logic_operator": "AND", "conditions":[{"event_name":"signup","min_event_count":1}]}).json()["cohort_id"]
    v_id = client.post("/cohorts", json={"name":"V", "logic_operator": "AND", "conditions":[{"event_name":"signup_v","min_event_count":1}]}).json()["cohort_id"]

    run_res = client.post("/impact/run", json={
        "baseline_cohort_id": b_id,
        "variant_cohort_ids": [v_id],
        "exposure_events": [{"event_name": "exp"}], 
        "interaction_events": [{"event_name": "int"}],
        "retention_event": "signup"
    })
    run_id = run_res.json()["run_id"]
    
    stats_res = client.post("/impact/stats", json={"run_id": run_id})
    stats = stats_res.json()["stats"]
    
    # Requirement: Skip if n < 30
    assert stats["exposure_rate"][str(v_id)]["skip_reason"] == "low_sample"
    assert stats["ctr"][str(v_id)]["skip_reason"] == "low_sample"
