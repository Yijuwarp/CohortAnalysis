from fastapi.testclient import TestClient
from tests.utils import csv_upload

def setup_test_data(client: TestClient) -> None:
    csv_text = "user_id,event_name,event_time,country\nu1,open,2026-01-01 10:00:00,US\n"
    csv_upload(client, csv_text=csv_text)
    client.post("/map-columns", json={"user_id_column": "user_id", "event_name_column": "event_name", "event_time_column": "event_time"})

def test_get_cohort_detail_success(client: TestClient) -> None:
    # 1. Prepare data
    setup_test_data(client)
    
    # 2. Create cohort with property filter
    payload = {
        "name": "Detail Test Cohort",
        "logic_operator": "AND",
        "conditions": [
            {
                "event_name": "open",
                "min_event_count": 1,
                "property_filter": {
                    "column": "country",
                    "operator": "IN",
                    "values": ["US", "CA"]
                }
            }
        ]
    }
    resp = client.post("/cohorts", json=payload)
    assert resp.status_code == 200, f"Failed to create cohort: {resp.text}"
    cohort_id = resp.json()["cohort_id"]
    
    # 3. Fetch detail
    detail_resp = client.get(f"/cohorts/{cohort_id}")
    assert detail_resp.status_code == 200
    data = detail_resp.json()
    
    assert data["name"] == "Detail Test Cohort"
    assert data["cohort_name"] == "Detail Test Cohort"
    assert data["size"] == 1
    assert len(data["conditions"]) == 1
    
    cond = data["conditions"][0]
    assert cond["event_name"] == "open"
    assert cond["min_event_count"] == 1
    assert cond["property_filter"]["column"] == "country"
    assert cond["property_filter"]["operator"] == "IN"
    assert cond["property_filter"]["values"] == ["US", "CA"]

def test_get_cohort_detail_404(client: TestClient) -> None:
    resp = client.get("/cohorts/999999")
    assert resp.status_code == 404

def test_get_cohort_detail_multiple_conditions(client: TestClient) -> None:
    setup_test_data(client)
    payload = {
        "name": "Multi Condition Detail",
        "logic_operator": "OR",
        "conditions": [
            {"event_name": "open", "min_event_count": 1},
            {"event_name": "open", "min_event_count": 2}
        ]
    }
    resp = client.post("/cohorts", json=payload)
    assert resp.status_code == 200, f"Failed to create cohort: {resp.text}"
    cohort_id = resp.json()["cohort_id"]
    
    detail_resp = client.get(f"/cohorts/{cohort_id}")
    assert detail_resp.status_code == 200
    data = detail_resp.json()
    assert len(data["conditions"]) == 2
    # Check preservation of order
    assert data["conditions"][0]["min_event_count"] == 1
    assert data["conditions"][1]["min_event_count"] == 2

def test_get_cohort_detail_no_property_filter(client: TestClient) -> None:
    setup_test_data(client)
    payload = {
        "name": "No Filter Detail",
        "logic_operator": "AND",
        "conditions": [
            {"event_name": "open", "min_event_count": 1}
        ]
    }
    resp = client.post("/cohorts", json=payload)
    assert resp.status_code == 200, f"Failed to create cohort: {resp.text}"
    cohort_id = resp.json()["cohort_id"]
    
    detail_resp = client.get(f"/cohorts/{cohort_id}")
    assert detail_resp.status_code == 200
    data = detail_resp.json()
    assert data["conditions"][0]["property_filter"] is None
