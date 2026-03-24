from __future__ import annotations
from fastapi.testclient import TestClient
from tests.utils import csv_upload

def _prepare_data(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time,source\n"
        "u1,open,2026-01-01 10:00:00,web\n"
        "u2,open,2026-01-01 10:00:00,app\n"
        "u3,open,2026-01-01 10:00:00,web\n"
    )
    csv_upload(client, csv_text=csv_text)
    client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
        },
    )

def test_scalar_normalization_in_create_cohort(client: TestClient) -> None:
    _prepare_data(client)
    
    # Case 1: Scalar string "web" -> should be normalized to ["web"]
    payload = {
        "name": "Scalar Cohort",
        "logic_operator": "AND",
        "conditions": [
            {
                "event_name": "open",
                "min_event_count": 1,
                "property_filter": {
                    "column": "source",
                    "operator": "=",
                    "values": "web"
                }
            }
        ]
    }
    
    response = client.post("/cohorts", json=payload)
    assert response.status_code == 200, response.text
    # u1 and u3 match. If it stayed as "web" (scalar), it might fail depending on how it's used in SQL (usually works for '=', but maybe not consistently)
    assert response.json()["users_joined"] == 2

def test_saved_cohort_normalization_flow(client: TestClient) -> None:
    _prepare_data(client)
    
    # 1. Create a "Saved Cohort" with a scalar value
    # SavedCohortCreate also uses CohortCondition which has the same validation/models
    saved_payload = {
        "name": "My Saved Cohort",
        "logic_operator": "OR",
        "conditions": [
            {
                "event_name": "open",
                "min_event_count": 1,
                "property_filter": {
                    "column": "source",
                    "operator": "=",
                    "values": "app"
                }
            }
        ]
    }
    
    saved_resp = client.post("/saved-cohorts", json=saved_payload)
    assert saved_resp.status_code == 200
    saved_id = saved_resp.json()["id"]
    
    # 2. Materialize it into a real cohort
    # The frontend usually sends the definition to /cohorts
    definition = saved_resp.json()["definition"]
    definition["source_saved_id"] = saved_id
    
    cohort_resp = client.post("/cohorts", json=definition)
    assert cohort_resp.status_code == 200
    assert cohort_resp.json()["users_joined"] == 1 # u2

def test_membership_uniqueness_with_multiple_matches(client: TestClient) -> None:
    _prepare_data(client)
    
    # u1 matches BOTH conditions in an OR.
    # We must ensure u1 is only present ONCE in cohort_membership for this cohort.
    payload = {
        "name": "Uniqueness Cohort",
        "logic_operator": "OR",
        "conditions": [
            {"event_name": "open", "min_event_count": 1},
            {
                "event_name": "open", 
                "min_event_count": 1,
                "property_filter": {"column": "source", "operator": "=", "values": "web"}
            }
        ]
    }
    
    response = client.post("/cohorts", json=payload)
    assert response.status_code == 200
    # 3 unique users (u1, u2, u3)
    assert response.json()["users_joined"] == 3
    
    cohort_id = response.json()["cohort_id"]
    # Check DB size
    cohorts = client.get("/cohorts").json()["cohorts"]
    cohort = next(c for c in cohorts if c["cohort_id"] == cohort_id)
    assert cohort["size"] == 3
