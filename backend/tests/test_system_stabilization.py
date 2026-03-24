from __future__ import annotations
import json
from fastapi.testclient import TestClient
from tests.utils import csv_upload
from app.domains.cohorts.cohort_service import normalize_values

def test_normalization_logic_edge_cases() -> None:
    # 1. Scalar string
    assert normalize_values("A") == ["A"]
    # 2. List
    assert normalize_values(["A"]) == ["A"]
    # 3. Stringified JSON List
    assert normalize_values('["A"]') == ["A"]
    # 4. Stringified JSON Scalar
    assert normalize_values('"A"') == ["A"]
    # 5. Numeric scalar
    assert normalize_values(123) == [123]
    # 6. Boolean scalar
    assert normalize_values(True) == [True]

def test_create_cohort_without_property_filter_succeeds(client: TestClient) -> None:
    csv_text = "user_id,event_name,event_time\nu1,open,2026-01-01 10:00:00\n"
    csv_upload(client, csv_text=csv_text)
    client.post("/map-columns", json={"user_id_column": "user_id", "event_name_column": "event_name", "event_time_column": "event_time"})
    
    # Payload WITHOUT property_filter (tests UnboundLocalError fix)
    payload = {
        "name": "No Filter Cohort",
        "logic_operator": "AND",
        "conditions": [{"event_name": "open", "min_event_count": 1}]
    }
    response = client.post("/cohorts", json=payload)
    assert response.status_code == 200, f"Failed without property filter: {response.text}"
    assert response.json()["users_joined"] == 1

def test_frequency_resilience_to_duplicates(client: TestClient) -> None:
    # 1. Prepare data
    csv_text = "user_id,event_name,event_time\nu1,open,2026-01-01 10:00:00\n"
    csv_upload(client, csv_text=csv_text)
    client.post("/map-columns", json={"user_id_column": "user_id", "event_name_column": "event_name", "event_time_column": "event_time"})
    
    # 2. Create cohort
    payload = {
        "name": "Resilience Test",
        "logic_operator": "AND",
        "conditions": [{"event_name": "open", "min_event_count": 1}]
    }
    resp = client.post("/cohorts", json=payload)
    cohort_id = resp.json()["cohort_id"]
    
    # 3. Verify frequency distribution
    # This should work even if duplicates were somehow present (due to DISTINCT in query)
    freq_resp = client.get("/usage-frequency", params={"event": "open"})
    assert freq_resp.status_code == 200
    
    data = freq_resp.json()
    # Check that sum of users in buckets for this cohort is 1
    total = 0
    for b in data["buckets"]:
        for c in b["cohorts"]:
            if c["cohort_id"] == cohort_id:
                total += c["users"]
    assert total == 1

def test_unique_index_enforcement(client: TestClient) -> None:
    # 1. Prepare data
    csv_text = "user_id,event_name,event_time\nu1,open,2026-01-01 10:00:00\n"
    csv_upload(client, csv_text=csv_text)
    client.post("/map-columns", json={"user_id_column": "user_id", "event_name_column": "event_name", "event_time_column": "event_time"})
    
    # 2. Create cohort
    payload = {
        "name": "Index Test",
        "logic_operator": "AND",
        "conditions": [{"event_name": "open", "min_event_count": 1}]
    }
    client.post("/cohorts", json=payload)

    # 3. Verify ensure_cohort_tables runs and returns cohort_name
    resp = client.get("/cohorts")
    assert resp.status_code == 200
    cohorts = resp.json()["cohorts"]
    assert len(cohorts) > 0
    assert "cohort_name" in cohorts[0], "UI Regression: cohort_name missing in list_cohorts"

def test_frequency_excludes_hidden_cohorts(client: TestClient) -> None:
    # 1. Prepare data
    csv_text = "user_id,event_name,event_time\nu1,open,2026-01-01 10:00:00\n"
    csv_upload(client, csv_text=csv_text)
    client.post("/map-columns", json={"user_id_column": "user_id", "event_name_column": "event_name", "event_time_column": "event_time"})
    
    # 2. Create cohort
    payload = {
        "name": "Hidden Test",
        "logic_operator": "AND",
        "conditions": [{"event_name": "open", "min_event_count": 1}]
    }
    resp = client.post("/cohorts", json=payload)
    cohort_id = resp.json()["cohort_id"]
    
    # 3. Verify it appears in frequency initially
    freq_resp = client.get("/usage-frequency", params={"event": "open"})
    cohort_ids = [c["cohort_id"] for b in freq_resp.json()["buckets"] for c in b["cohorts"]]
    assert cohort_id in cohort_ids
    
    # 4. Hide the cohort
    hide_resp = client.patch(f"/cohorts/{cohort_id}/hide")
    assert hide_resp.status_code == 200
    assert hide_resp.json()["hidden"] is True
    
    # 5. Verify it NO LONGER appears in frequency distribution
    freq_resp_hidden = client.get("/usage-frequency", params={"event": "open"})
    data = freq_resp_hidden.json()
    cohort_ids_hidden = [c["cohort_id"] for b in data["buckets"] for c in b["cohorts"]]
    print(f"DEBUG: cohort_id={cohort_id}, found_ids={cohort_ids_hidden}")
    assert cohort_id not in cohort_ids_hidden, f"Hidden cohort {cohort_id} leaked into results: {cohort_ids_hidden}"
