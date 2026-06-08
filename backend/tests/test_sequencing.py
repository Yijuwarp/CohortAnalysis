import pytest
from fastapi.testclient import TestClient
from tests.utils import csv_upload

def _upload_and_map(client: TestClient, csv_text: str) -> None:
    """Upload CSV and perform column mapping."""
    upload = csv_upload(client, csv_text=csv_text)
    assert upload.status_code == 200
    mapping = client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
        },
    )
    assert mapping.status_code == 200

def test_paths_same_timestamp_relaxed_ordering(client: TestClient):
    """
    Verifies that Events at the same timestamp can be sequenced regardless 
    of their ingestion order (row_id).
    """
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,A,2024-01-01 10:00:00\n" # row_id 1
        "u1,B,2024-01-01 10:00:00\n" # row_id 2
    )
    _upload_and_map(client, csv_text)

    # Path A -> B should succeed even though B was ingested first
    resp = client.post("/paths/run", json={"steps": ["A", "B"]})
    assert resp.status_code == 200
    payload = resp.json()
    
    # Check if u1 reached Step 2
    # result structure: results[cohort_idx].steps[step_idx].users
    all_users_result = next(r for r in payload["results"] if r["cohort_name"] == "All Users")
    assert all_users_result["steps"][1]["users"] == 1, "Should allow A -> B at same timestamp"

def test_paths_duplicate_events_same_timestamp(client: TestClient):
    """
    Verifies that multiple instances of the same event at the same timestamp 
    can be sequenced (A -> A -> A).
    """
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,A,2024-01-01 10:00:00\n"
        "u1,A,2024-01-01 10:00:00\n"
        "u1,A,2024-01-01 10:00:00\n"
    )
    _upload_and_map(client, csv_text)

    # Path A -> A -> A should succeed
    resp = client.post("/paths/run", json={"steps": ["A", "A", "A"]})
    assert resp.status_code == 200
    payload = resp.json()
    
    all_users_result = next(r for r in payload["results"] if r["cohort_name"] == "All Users")
    assert all_users_result["steps"][2]["users"] == 1, "Should allow A -> A -> A at same timestamp"

def test_paths_no_self_reuse(client: TestClient):
    """
    Verifies that a single event instance cannot be used for multiple steps 
    even with relaxed timestamp logic.
    """
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,A,2024-01-01 10:00:00\n"
    )
    _upload_and_map(client, csv_text)

    # Path A -> A should NOT succeed (only 1 A available)
    resp = client.post("/paths/run", json={"steps": ["A", "A"]})
    assert resp.status_code == 200
    payload = resp.json()
    
    all_users_result = next(r for r in payload["results"] if r["cohort_name"] == "All Users")
    assert all_users_result["steps"][1]["users"] == 0, "Should NOT allow A -> A if only 1 instance exists"

def test_paths_cross_timestamp_strict(client: TestClient):
    """
    Verifies that strict temporal ordering is still enforced for different timestamps.
    """
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,A,2024-01-01 10:00:01\n"
        "u1,B,2024-01-01 10:00:00\n"
    )
    _upload_and_map(client, csv_text)

    # Path A -> B should fail because A is after B
    resp = client.post("/paths/run", json={"steps": ["A", "B"]})
    assert resp.status_code == 200
    payload = resp.json()
    
    all_users_result = next(r for r in payload["results"] if r["cohort_name"] == "All Users")
    assert all_users_result["steps"][1]["users"] == 0, "Should NOT allow A -> B if A occurs after B"

def test_flow_forward_same_timestamp_relaxed(client: TestClient):
    """
    Verifies that Flow transitions can happen between simultaneous events.
    """
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,start,2024-01-01 10:00:00\n" # row_id 1
        "u1,next,2024-01-01 10:00:00\n"  # row_id 2
    )
    _upload_and_map(client, csv_text)
    
    # Forward Flow from "start"
    resp = client.get("/flow/l1?start_event=start&direction=forward")
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    
    # Check if "next" is in the transitions
    transition_events = [r["path"][-1] for r in rows]
    assert "next" in transition_events, "Flow should detect 'next' even if at same timestamp"

def test_flow_reverse_same_timestamp_relaxed(client: TestClient):
    """
    Verifies that Reverse Flow transitions can happen between simultaneous events.
    """
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,prev,2024-01-01 10:00:00\n" # row_id 1
        "u1,end,2024-01-01 10:00:00\n"  # row_id 2
    )
    _upload_and_map(client, csv_text)
    
    # Reverse Flow from "end"
    resp = client.get("/flow/l1?start_event=end&direction=reverse")
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    
    # Check if "prev" is in the transitions
    transition_events = [r["path"][-1] for r in rows]
    assert "prev" in transition_events, "Reverse Flow should detect 'prev' even if at same timestamp"
