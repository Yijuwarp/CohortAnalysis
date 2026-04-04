from __future__ import annotations
import pytest
from fastapi.testclient import TestClient
from tests.utils import csv_upload

def _setup_basic_dataset(client: TestClient):
    csv_text = (
        "user_id,event_name,event_time,revenue\n"
        # Baseline users: b1-b100
        + "\n".join([f"b{i},signup,2024-01-01 00:00:00,0" for i in range(1, 101)]) + "\n"
        # Variant users: v1-v100
        + "\n".join([f"v{i},sig_v,2024-01-01 00:00:00,0" for i in range(1, 101)]) + "\n"
        # Baseline Exposure: 60 users
        + "\n".join([f"b{i},exposure,2024-01-02 00:00:00,0" for i in range(1, 61)]) + "\n"
        # Baseline Interaction: 30 users, multiple events for engagement test
        + "\n".join([f"b{i},interaction,2024-01-02 01:00:00,0" for i in range(1, 31)]) + "\n"
        + "\n".join([f"b{i},interaction,2024-01-02 02:00:00,0" for i in range(1, 31)]) + "\n"
        + "\n".join([f"b{i},interaction,2024-01-02 03:00:00,0" for i in range(1, 31)]) + "\n"
        + "\n".join([f"b{i},interaction,2024-01-02 04:00:00,0" for i in range(1, 31)]) + "\n" # 4 per user * 30 users = 120
        # Baseline Impact: 40 users, 2 events each
        + "\n".join([f"b{i},impact_event,2024-01-02 05:00:00,0" for i in range(1, 41)]) + "\n"
        + "\n".join([f"b{i},impact_event,2024-01-02 06:00:00,0" for i in range(1, 41)]) + "\n" # 2 per user * 40 users = 80
        # Variant Exposure: 80 users
        + "\n".join([f"v{i},exposure,2024-01-02 00:00:00,0" for i in range(1, 81)]) + "\n"
        # Variant Interaction: 60 users
        + "\n".join([f"v{i},interaction,2024-01-02 01:00:00,0" for i in range(1, 61)])
    )
    
    upload = csv_upload(client, csv_text=csv_text)
    assert upload.status_code == 200
    
    mapped = client.post("/map-columns", json={
        "user_id_column": "user_id",
        "event_name_column": "event_name",
        "event_time_column": "event_time",
        "revenue_column": "revenue"
    })
    assert mapped.status_code == 200
    
    b_res = client.post("/cohorts", json={"name": "Baseline", "logic_operator": "AND", "conditions": [{"event_name": "signup", "min_event_count": 1}]})
    assert b_res.status_code == 200
    b_id = b_res.json()["cohort_id"]
    
    v_res = client.post("/cohorts", json={"name": "Variant", "logic_operator": "AND", "conditions": [{"event_name": "sig_v", "min_event_count": 1}]})
    assert v_res.status_code == 200
    v_id = v_res.json()["cohort_id"]
    
    return b_id, v_id

def test_impact_accuracy(client):
    b_id, v_id = _setup_basic_dataset(client)
    res = client.post("/impact/run", json={
        "baseline_cohort_id": b_id,
        "variant_cohort_ids": [v_id],
        "exposure_events": ["exposure"],
        "interaction_events": ["interaction"],
        "impact_events": ["impact_event"]
    })
    if res.status_code != 200:
        print("MY_ERROR", res.text)
    assert res.status_code == 200, res.text
    data = res.json()
    
    # 1. Exposure Rate: 60/100 = 0.6
    m = next(row for row in data["metrics"] if row["metric"] == "Exposure Rate")
    assert m["values"][str(b_id)]["value"] == 0.6
    
    # 2. CTR: 30/60 = 0.5
    m = next(row for row in data["metrics"] if row["metric"] == "CTR")
    assert m["values"][str(b_id)]["value"] == 0.5
    
    # 3. Engagement: 120/100 = 1.2
    m = next(row for row in data["metrics"] if row["metric"] == "Engagement")
    assert m["values"][str(b_id)]["value"] == 1.2
    
    # 4. Reach: 40/100 = 0.4
    m = next(row for row in data["metrics"] if row["metric"] == "impact_event → Reach")
    assert m["values"][str(b_id)]["value"] == 0.4
    
    # 5. Intensity: 80/100 = 0.8
    m = next(row for row in data["metrics"] if row["metric"] == "impact_event → Intensity")
    assert m["values"][str(b_id)]["value"] == 0.8
    
    # 6. Delta: Exposure Rate (0.8 - 0.6) / 0.6 = 0.33...
    m = next(row for row in data["metrics"] if row["metric"] == "Exposure Rate")
    assert pytest.approx(m["values"][str(v_id)]["delta"]) == 0.3333333333

def test_impact_zero_baseline(client):
    csv_text = "user_id,event_name,event_time\nb1,signup,2024-01-01 00:00:00\nv1,sig_v,2024-01-01 00:00:00\nv1,exposure,2024-01-02 00:00:00"
    csv_upload(client, csv_text=csv_text)
    client.post("/map-columns", json={"user_id_column":"user_id","event_name_column":"event_name","event_time_column":"event_time"})
    b_id = client.post("/cohorts", json={"name":"B", "logic_operator": "AND", "conditions":[{"event_name":"signup","min_event_count":1}]}).json()["cohort_id"]
    v_id = client.post("/cohorts", json={"name":"V", "logic_operator": "AND", "conditions":[{"event_name":"sig_v","min_event_count":1}]}).json()["cohort_id"]
    
    res = client.post("/impact/run", json={
        "baseline_cohort_id": b_id,
        "variant_cohort_ids": [v_id],
        "exposure_events": ["exposure"],
        "interaction_events": ["interaction"]
    })
    m = next(row for row in res.json()["metrics"] if row["metric"] == "Exposure Rate")
    assert m["values"][str(b_id)]["value"] == 0
    assert m["values"][str(v_id)]["delta"] is None

def test_impact_no_exposure(client):
    csv_text = "user_id,event_name,event_time\nb1,signup,2024-01-01 00:00:00\nb1,interaction,2024-01-02 00:00:00"
    csv_upload(client, csv_text=csv_text)
    client.post("/map-columns", json={"user_id_column":"user_id","event_name_column":"event_name","event_time_column":"event_time"})
    b_id = client.post("/cohorts", json={"name":"B", "logic_operator": "AND", "conditions":[{"event_name":"signup","min_event_count":1}]}).json()["cohort_id"]
    
    res = client.post("/impact/run", json={
        "baseline_cohort_id": b_id,
        "variant_cohort_ids": [],
        "exposure_events": ["not_found"],
        "interaction_events": ["interaction"]
    })
    m = next(row for row in res.json()["metrics"] if row["metric"] == "CTR")
    assert m["values"][str(b_id)]["value"] is None
