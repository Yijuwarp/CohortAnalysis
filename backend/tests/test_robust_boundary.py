import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timedelta, timezone
from tests.utils import csv_upload

def test_robust_observation_boundary(client: TestClient):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    
    # Create dataset
    # User A: joined now
    # User B: joined 1 day ago
    # User C: joined 7 days ago
    # User D: joined in FUTURE (2037)
    
    csv_text = (
        "user_id,event_name,event_time\n"
        f"uA,signup,{(now - timedelta(seconds=1)).isoformat()}\n"
        f"uB,signup,{(now - timedelta(days=2)).isoformat()}\n"
        f"uC,signup,{(now - timedelta(days=8)).isoformat()}\n"
        "uD,signup,2037-01-01 10:00:00\n" # Future
    )
    
    upload_res = csv_upload(client, csv_text=csv_text)
    assert upload_res.status_code == 200
    map_res = client.post("/map-columns", json={
        "user_id_column": "user_id",
        "event_name_column": "event_name",
        "event_time_column": "event_time",
    })
    assert map_res.status_code == 200
    cohort_res = client.post("/cohorts", json={
        "name": "BoundTest",
        "logic_operator": "AND",
        "conditions": [{"event_name": "signup", "min_event_count": 1}]
    })
    assert cohort_res.status_code == 200

    # The observation boundary should be 'now' because p99.9 of [now-7, now-1, now, 2037] 
    # will be 2037 (since we only have 4 events), but it's clamped to 'now'.
    
    response = client.get("/retention?max_day=7")
    assert response.status_code == 200
    data = response.json()
    
    # Check observation_end_time in response
    obs_time = datetime.fromisoformat(data["observation_end_time"])
    assert obs_time <= now + timedelta(seconds=11) # Allow for slight clock skew
    
    table = data.get("retention_table", [])
    row = next((r for r in table if r["cohort_name"] == "BoundTest"), None)
    
    assert row is not None, f"BoundTest not found"
    
    # Day 0: 2 users (B,C). uA joined 1s ago and hasn't finished Day 0.
    assert row["availability"]["0"]["eligible_users"] == 2
    # Day 1: uB, uC. Total 2.
    assert row["availability"]["1"]["eligible_users"] == 2
    # Day 7: uC. Total 1.
    assert row["availability"]["7"]["eligible_users"] == 1
    
    assert row["availability"]["0"]["cohort_size"] == 3
