from __future__ import annotations
import pytest
from app.db.connection import get_connection
from tests.utils import csv_upload, DeterministicTestClient, DETERMINISTIC_USER_ID

def setup_consistent_data(client: DeterministicTestClient):
    # u1: joins at 10:00. Has activity on D0 (11:00, 12:00) and D1 (11:00).
    # u2: joins at 10:00. Has activity on D0 (11:00) only.
    csv_text = (
        "user_id,event_name,event_time,revenue\n"
        "u1,registration,2026-01-01 10:00:00,0\n"
        "u1,app_open,2026-01-01 11:00:00,0\n"
        "u1,purchase,2026-01-01 12:00:00,10.0\n"
        "u1,app_open,2026-01-02 11:00:00,0\n"
        "u2,registration,2026-01-01 10:00:00,0\n"
        "u2,app_open,2026-01-01 11:00:00,0\n"
    )
    csv_upload(client, csv_text=csv_text)
    client.post("/map-columns", json={
        "user_id_column": "user_id",
        "event_name_column": "event_name",
        "event_time_column": "event_time",
        "revenue_column": "revenue",
        "column_types": {
            "user_id": "TEXT",
            "event_name": "TEXT",
            "event_time": "TIMESTAMP",
            "revenue": "NUMERIC"
        }
    })
    
    # Cohort A: users with at least 1 purchase
    res_a = client.post("/cohorts", json={
        "name": "Cohort A",
        "logic_operator": "AND",
        "conditions": [{"event_name": "purchase", "min_event_count": 1}]
    })
    assert res_a.status_code == 200
    
    # Cohort B: users with NO purchase (Negated)
    res_b = client.post("/cohorts", json={
        "name": "Cohort B Precise",
        "logic_operator": "AND",
        "conditions": [{"event_name": "purchase", "min_event_count": 1, "is_negated": True}]
    })
    assert res_b.status_code == 200
    
    cohorts_payload = client.get("/cohorts").json()
    cohorts = cohorts_payload["cohorts"]
    
    try:
        cohort_a_id = next(c["cohort_id"] for c in cohorts if c["name"] == "Cohort A")
        cohort_b_id = next(c["cohort_id"] for c in cohorts if c["name"] == "Cohort B Precise")
    except StopIteration:
        raise RuntimeError(f"Could not find cohorts. Names: {[c['name'] for c in cohorts]}")
        
    return cohort_a_id, cohort_b_id

def test_retention_consistency(client: DeterministicTestClient):
    cohort_a_id, cohort_b_id = setup_consistent_data(client)
    
    # Retention Service
    ret_resp = client.get("/retention?max_day=1&retention_event=app_open")
    assert ret_resp.status_code == 200
    ret_data = ret_resp.json().get("retention_table", [])
    
    ret_a = next(c for c in ret_data if c["cohort_id"] == cohort_a_id)
    ret_b = next(c for c in ret_data if c["cohort_id"] == cohort_b_id)
    
    # Comparison Service (POST /compare-cohorts)
    comp_resp = client.post("/compare-cohorts", json={
        "cohort_a": cohort_a_id,
        "cohort_b": cohort_b_id,
        "tab": "retention",
        "metric": "retention_rate",
        "day": 1,
        "event": "app_open"
    })
    assert comp_resp.status_code == 200
    comp_data = comp_resp.json()
    
    assert (ret_a["retention"].get("1") or 0.0) / 100.0 == comp_data["cohort_a_value"]
    assert (ret_b["retention"].get("1") or 0.0) / 100.0 == comp_data["cohort_b_value"]


def test_usage_consistency(client: DeterministicTestClient):
    cohort_a_id, cohort_b_id = setup_consistent_data(client)
    
    usage_resp = client.get("/usage?event=app_open&max_day=1")
    assert usage_resp.status_code == 200
    usage_data = usage_resp.json()
    
    vol_a = next(c for c in usage_data["usage_volume_table"] if c["cohort_id"] == cohort_a_id)
    vol_b = next(c for c in usage_data["usage_volume_table"] if c["cohort_id"] == cohort_b_id)
    
    comp_resp = client.post("/compare-cohorts", json={
        "cohort_a": cohort_a_id,
        "cohort_b": cohort_b_id,
        "tab": "usage",
        "metric": "per_installed_user",
        "day": 0,
        "event": "app_open"
    })
    assert comp_resp.status_code == 200
    comp_data = comp_resp.json()
    
    assert float(vol_a["values"]["0"]) == comp_data["cohort_a_value"]
    assert float(vol_b["values"]["0"]) == comp_data["cohort_b_value"]

def test_monetization_consistency(client: DeterministicTestClient):
    cohort_a_id, cohort_b_id = setup_consistent_data(client)
    
    mon_resp = client.get("/monetization?max_day=1")
    assert mon_resp.status_code == 200
    mon_data = mon_resp.json()
    rev_rows = mon_data["revenue_table"]
    
    rev_a_d0 = next(r["revenue"] for r in rev_rows if r["cohort_id"] == cohort_a_id and r["day_number"] == 0)
    
    comp_resp = client.post("/compare-cohorts", json={
        "cohort_a": cohort_a_id,
        "cohort_b": cohort_b_id,
        "tab": "monetization",
        "metric": "revenue_per_acquired_user",
        "day": 0
    })
    assert comp_resp.status_code == 200
    comp_data = comp_resp.json()
    
    assert float(rev_a_d0) == comp_data["cohort_a_value"]

def test_grid_completeness(client: DeterministicTestClient):
    cohort_a_id, cohort_b_id = setup_consistent_data(client)
    
    usage_resp = client.get("/usage?event=app_open&max_day=1")
    usage_data = usage_resp.json()
    vol_b = next(c for c in usage_data["usage_volume_table"] if c["cohort_id"] == cohort_b_id)
    assert int(vol_b["values"]["1"]) == 0
    
    comp_resp = client.post("/compare-cohorts", json={
        "cohort_a": cohort_a_id,
        "cohort_b": cohort_b_id,
        "tab": "usage",
        "metric": "per_installed_user",
        "day": 1,
        "event": "app_open"
    })
    assert comp_resp.status_code == 200
    assert comp_resp.json()["cohort_b_value"] == 0.0
