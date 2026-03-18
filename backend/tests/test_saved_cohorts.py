import pytest
from fastapi.testclient import TestClient
from tests.test_cohorts import _prepare_normalized_events

def test_create_saved_cohort(client, db_connection):
    _prepare_normalized_events(client)
    payload = {
        "name": "Global Active Users",
        "logic_operator": "AND",
        "join_type": "condition_met",
        "conditions": [
            {
                "event_name": "purchase",
                "min_event_count": 1
            }
        ]
    }
    resp = client.post("/saved-cohorts", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert "id" in data
    assert data["name"] == "Global Active Users"
    assert data["definition"]["conditions"][0]["event_name"] == "purchase"

def test_get_saved_cohorts(client, db_connection):
    _prepare_normalized_events(client)
    payload = {
        "name": "Global Active Users",
        "logic_operator": "AND",
        "conditions": [{"event_name": "purchase", "min_event_count": 1}]
    }
    client.post("/saved-cohorts", json=payload)
    
    resp = client.get("/saved-cohorts")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) >= 1
    assert any(c["name"] == "Global Active Users" for c in data)
    
    # get specific
    cohort_id = data[0]["id"]
    resp2 = client.get(f"/saved-cohorts/{cohort_id}")
    assert resp2.status_code == 200
    assert resp2.json()["id"] == cohort_id

def test_update_saved_cohort(client, db_connection):
    _prepare_normalized_events(client)
    payload = {
        "name": "Global Active Users",
        "logic_operator": "AND",
        "conditions": [{"event_name": "purchase", "min_event_count": 1}]
    }
    create_resp = client.post("/saved-cohorts", json=payload).json()
    cohort_id = create_resp["id"]
    
    payload["name"] = "Global Active Users V2"
    update_resp = client.put(f"/saved-cohorts/{cohort_id}", json=payload)
    assert update_resp.status_code == 200
    assert update_resp.json()["name"] == "Global Active Users V2"
    
def test_delete_saved_cohort(client, db_connection):
    _prepare_normalized_events(client)
    payload = {
        "name": "Global Active Users",
        "logic_operator": "AND",
        "conditions": [{"event_name": "purchase", "min_event_count": 1}]
    }
    create_resp = client.post("/saved-cohorts", json=payload).json()
    cohort_id = create_resp["id"]
    
    del_resp = client.delete(f"/saved-cohorts/{cohort_id}")
    assert del_resp.status_code == 200
    
    get_resp = client.get(f"/saved-cohorts/{cohort_id}")
    assert get_resp.status_code == 404

def test_saved_cohort_validity(client, db_connection):
    _prepare_normalized_events(client)
    payload = {
        "name": "Valid Cohort",
        "logic_operator": "AND",
        "conditions": [{"event_name": "purchase", "min_event_count": 1}]
    }
    create_resp = client.post("/saved-cohorts", json=payload).json()
    
    # Should be valid
    get_resp = client.get(f"/saved-cohorts/{create_resp['id']}")
    assert get_resp.json()["is_valid"] is True
    
    # Invalid cohort
    invalid_payload = {
        "name": "Invalid Cohort",
        "logic_operator": "AND",
        "conditions": [{"event_name": "nonexistent_event", "min_event_count": 1}]
    }
    inv_c_resp = client.post("/saved-cohorts", json=invalid_payload).json()
    get_inv_resp = client.get(f"/saved-cohorts/{inv_c_resp['id']}")
    assert get_inv_resp.json()["is_valid"] is False
    assert get_inv_resp.json()["errors"][0]["type"] == "missing_event"

def test_add_saved_cohort_to_dataset(client, db_connection):
    _prepare_normalized_events(client)
    # create saved cohort
    saved_payload = {
        "name": "Test Saved Add",
        "logic_operator": "AND",
        "conditions": [{"event_name": "purchase", "min_event_count": 1}]
    }
    s_resp = client.post("/saved-cohorts", json=saved_payload).json()
    s_id = s_resp["id"]
    
    # Now create real cohort from it
    c_payload = s_resp["definition"]
    c_payload["source_saved_id"] = s_id
    
    act_resp = client.post("/cohorts", json=c_payload)
    assert act_resp.status_code == 200
    assert act_resp.json()["cohort_id"] > 0

def test_edit_saved_cohort_recomputes_active(client, db_connection):
    _prepare_normalized_events(client)
    # 1. create saved cohort
    saved_payload = {
        "name": "Test Shared Edit",
        "logic_operator": "AND",
        "conditions": [{"event_name": "purchase", "min_event_count": 1}] # will have users
    }
    s_resp = client.post("/saved-cohorts", json=saved_payload).json()
    s_id = s_resp["id"]
    
    # 2. Add to dataset
    c_payload = s_resp["definition"]
    c_payload["source_saved_id"] = s_id
    act_resp = client.post("/cohorts", json=c_payload)
    assert act_resp.status_code == 200
    size_before = act_resp.json()["users_joined"]
    
    # 3. Edit saved cohort to something that has NO users
    saved_payload["conditions"][0]["event_name"] = "subscription_renewed" # assume smaller or 0
    saved_payload["conditions"][0]["min_event_count"] = 999 
    
    client.put(f"/saved-cohorts/{s_id}", json=saved_payload)
    
    # 4. Fetch the active cohort size from list (since get_cohort is not there)
    list_resp = client.get("/cohorts").json()
    cohort_obj = next(c for c in list_resp["cohorts"] if c["source_saved_id"] == s_id)
    
    assert cohort_obj["size"] == 0

def test_delete_saved_cohort_does_not_affect_active(client, db_connection):
    _prepare_normalized_events(client)
    # 1. create saved cohort
    saved_payload = {
        "name": "Test Shared Delete",
        "logic_operator": "AND",
        "conditions": [{"event_name": "purchase", "min_event_count": 1}]
    }
    s_resp = client.post("/saved-cohorts", json=saved_payload).json()
    s_id = s_resp["id"]
    
    # 2. Add to dataset
    c_payload = s_resp["definition"]
    c_payload["source_saved_id"] = s_id
    act_resp = client.post("/cohorts", json=c_payload)
    
    # 3. Delete saved cohort
    client.delete(f"/saved-cohorts/{s_id}")
    
    # 4. Check active cohort is still there
    list_resp = client.get("/cohorts").json()
    assert any(c["source_saved_id"] == s_id for c in list_resp["cohorts"])
