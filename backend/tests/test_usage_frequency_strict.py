from __future__ import annotations
import io
from fastapi.testclient import TestClient
from tests.utils import csv_upload

def test_usage_frequency_strict_semantics(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,signup,2026-01-02 00:00:00\n"
        "u1,open,2026-01-01 10:00:00\n"
        "u1,open,2026-01-03 10:00:00\n"
        "u2,signup,2026-01-02 00:00:00\n"
        "u2,open,2026-01-01 10:00:00\n"
        "u3,signup,2026-01-02 00:00:00\n"
        "u4,signup,2026-01-02 00:00:00\n"
        "u4,open,2026-01-02 10:00:00\n"
        "u4,open,2026-01-03 10:00:00\n"
        "u4,open,2026-01-04 10:00:00\n"
    )
    upload = csv_upload(client, csv_text=csv_text)
    assert upload.status_code == 200, f"Upload failed: {upload.text}"

    mapped = client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
        },
    )
    assert mapped.status_code == 200, f"Map columns failed: {mapped.text}"

    cohort_resp = client.post(
        "/cohorts",
        json={
            "name": "Signed Up",
            "logic_operator": "AND",
            "join_type": "condition_met",
            "conditions": [
                {"event_name": "signup", "min_event_count": 1}
            ]
        }
    )
    assert cohort_resp.status_code == 200, f"Create cohort failed: {cohort_resp.text}"
    cohort_id = cohort_resp.json()["cohort_id"]
    
    response = client.get("/usage-frequency", params={"event": "open"})
    assert response.status_code == 200, f"Usage frequency failed: {response.text}"
    payload = response.json()
    
    # User u1: join 02 00:00. Event 03 10:00 (+34h) -> Day 1. (01 10:00 is excluded)
    # User u2: join 02 00:00. Event 01 10:00 is excluded.
    # User u3: join 02 00:00. No events.
    # User u4: join 02 00:00. Event 02 10:00 (D0), 03 10:00 (D1), 04 10:00 (D2). Total 3 events.
    
    # Resulting Frequencies for 'open':
    # u1: 1
    # u2: 0
    # u3: 0
    # u4: 3
    
    found_cohort = False
    buckets = {}
    for b in payload["buckets"]:
        for c in b["cohorts"]:
            if c["cohort_id"] == cohort_id:
                buckets[b["bucket"]] = c["users"]
                found_cohort = True
    
    assert found_cohort, f"Cohort {cohort_id} not found in results: {payload}"
    assert buckets.get("0", 0) == 2, f"Expected 2 in bucket 0 (u2, u3), got {buckets.get('0')}"
    assert buckets.get("1", 0) == 1, f"Expected 1 in bucket 1 (u1), got {buckets.get('1')}"
    assert buckets.get("2-5", 0) == 1, f"Expected 1 in bucket 2-5 (u4), got {buckets.get('2-5')}"
    assert sum(buckets.values()) == 4

def test_usage_frequency_property_filter_strict(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time,source\n"
        "u1,signup,2026-01-02 00:00:00,direct\n"
        "u1,open,2026-01-03 10:00:00,web\n"
        "u2,signup,2026-01-02 00:00:00,direct\n"
        "u2,open,2026-01-03 10:00:00,app\n"
    )
    upload = csv_upload(client, csv_text=csv_text)
    assert upload.status_code == 200, f"Upload failed: {upload.text}"
    
    mapped = client.post("/map-columns", json={"user_id_column": "user_id", "event_name_column": "event_name", "event_time_column": "event_time"})
    assert mapped.status_code == 200, f"Map columns failed: {mapped.text}"
    
    client.post("/cohorts", json={
        "name": "Signed Up Prop",
        "logic_operator": "AND",
        "join_type": "condition_met",
        "conditions": [{"event_name": "signup", "min_event_count": 1}]
    })
    cohorts = client.get("/cohorts").json()["cohorts"]
    cohort_matches = [c for c in cohorts if c["cohort_name"] == "Signed Up Prop"]
    assert cohort_matches, f"Cohort 'Signed Up Prop' not found in {cohorts}"
    cohort_id = cohort_matches[0]["cohort_id"]

    response = client.get("/usage-frequency", params={"event": "open", "property": "source", "operator": "=", "value": "web"})
    assert response.status_code == 200, f"Usage frequency with prop failed: {response.text}"
    payload = response.json()
    
    buckets = {}
    for b in payload["buckets"]:
        for c in b["cohorts"]:
            if c["cohort_id"] == cohort_id:
                buckets[b["bucket"]] = c["users"]
    
    assert buckets.get("0", 0) == 1, f"Expected 1 in bucket 0, got {buckets.get('0')}"
    assert buckets.get("1", 0) == 1, f"Expected 1 in bucket 1, got {buckets.get('1')}"
