from __future__ import annotations
from fastapi.testclient import TestClient
from tests.utils import csv_upload

def test_cohort_creation_uses_scoped_data(client: TestClient) -> None:
    # 1. Upload data: u1 has web, u2 has app
    csv_text = (
        "user_id,event_name,event_time,source\n"
        "u1,signup,2026-01-01 00:00:00,web\n"
        "u1,open,2026-01-01 10:00:00,web\n"
        "u2,signup,2026-01-01 00:00:00,app\n"
        "u2,open,2026-01-01 10:00:00,app\n"
    )
    csv_upload(client, csv_text=csv_text)
    client.post("/map-columns", json={"user_id_column": "user_id", "event_name_column": "event_name", "event_time_column": "event_time"})
    
    # 2. Apply a global filter to scope the dataset to 'source = web'
    # This will cause 'events_scoped' to only contain u1
    filter_resp = client.post("/apply-filters", json={
        "filters": [
            {"column": "source", "operator": "=", "value": "web"}
        ]
    })
    assert filter_resp.status_code == 200
    
    # 3. Create a cohort for 'open >= 1'
    # If the fix works, it should ONLY include u1 (from events_scoped)
    # If it's broken (using events_normalized), it would include u1 and u2
    cohort_resp = client.post("/cohorts", json={
        "name": "Scoped Open",
        "logic_operator": "AND",
        "join_type": "condition_met",
        "conditions": [{"event_name": "open", "min_event_count": 1}]
    })
    
    assert cohort_resp.status_code == 200, f"Cohort creation failed: {cohort_resp.text}"
    # ONLY u1 should be in the cohort because u2 was filtered out of events_scoped
    assert cohort_resp.json()["users_joined"] == 1, f"Expected 1 user, got {cohort_resp.json()['users_joined']}"
    
    # 4. Verify update_cohort is consistent
    cohort_id = cohort_resp.json()["cohort_id"]
    update_resp = client.put(f"/cohorts/{cohort_id}", json={
        "name": "Scoped Open Updated",
        "logic_operator": "AND",
        "join_type": "condition_met",
        "conditions": [{"event_name": "open", "min_event_count": 1}]
    })
    assert update_resp.status_code == 200
    assert update_resp.json()["users_joined"] == 1
    
    # 5. Verify frequency distribution validation passes
    # If the total bucket count doesn't sum to cohort size (1), it raises RuntimeError (500)
    freq_resp = client.get("/usage-frequency", params={"event": "open"})
    assert freq_resp.status_code == 200, f"Frequency validation failed: {freq_resp.text}"
    
    payload = freq_resp.json()
    buckets = {}
    for b in payload["buckets"]:
        for c in b["cohorts"]:
            if c["cohort_id"] == cohort_id:
                buckets[b["bucket"]] = c["users"]
    
    # Total sum should be exactly 1
    assert sum(buckets.values()) == 1, f"Frequency distribution sum mismatch: {sum(buckets.values())} != 1"
