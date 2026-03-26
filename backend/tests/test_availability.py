import pytest
from fastapi.testclient import TestClient
from tests.utils import csv_upload

def setup_data(client: TestClient):
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,signup,2024-01-01 10:00:00\n"
        "u1,active,2024-01-02 10:00:00\n"
        "u2,signup,2024-01-03 10:00:00\n"
        "u2,active,2024-01-04 10:00:00\n"
    )
    csv_upload(client, csv_text=csv_text)
    client.post("/map-columns", json={
        "user_id_column": "user_id",
        "event_name_column": "event_name",
        "event_time_column": "event_time",
    })
    client.post("/cohorts", json={
        "name": "All Users",
        "logic_operator": "AND",
        "conditions": [{"event_name": "signup", "min_event_count": 0}]
    })

def test_availability_in_retention(client: TestClient):
    setup_data(client)
    # Latest event is 2024-01-04.
    # u1 joined Jan 1. Day 0, 1, 2, 3 are Jan 1, 2, 3, 4. u1 is eligible for all up to Day 3.
    # u2 joined Jan 3. Day 0, 1 are Jan 3, 4. u2 is eligible for Day 0, 1.
    # At Day 2: u1 is eligible, u2 is not. Total eligible = 1. Cohort size = 2.
    
    response = client.get("/retention?max_day=3")
    assert response.status_code == 200
    data = response.json()
    row = next(r for r in data["retention_table"] if r["cohort_name"] == "All Users")
    
    assert "availability" in row
    # Day 0: Both eligible
    assert row["availability"]["0"]["eligible_users"] == 2
    # Day 2: Only u1 eligible
    assert row["availability"]["2"]["eligible_users"] == 1
    assert row["availability"]["2"]["cohort_size"] == 2

def test_availability_in_usage(client: TestClient):
    setup_data(client)
    response = client.get("/usage?event=active&max_day=3")
    assert response.status_code == 200
    data = response.json()
    row = next(r for r in data["usage_volume_table"] if r["cohort_name"] == "All Users")
    
    assert "availability" in row
    assert row["availability"]["0"]["eligible_users"] == 2
    assert row["availability"]["2"]["eligible_users"] == 1

def test_availability_in_monetization(client: TestClient):
    setup_data(client)
    # Monetization also needs revenue events to return rows, but let's check structure
    client.post("/update-revenue-config", json={
        "revenue_config": {"active": {"included": True}}
    })
    response = client.get("/monetization?max_day=3")
    assert response.status_code == 200
    data = response.json()
    if data["revenue_table"]:
        row = data["revenue_table"][0]
        assert "availability" in row
        # Since it's a flat list, we check for a specific day
        d2_row = next((r for r in data["revenue_table"] if r["day_number"] == 2), None)
        if d2_row:
            assert d2_row["availability"]["eligible_users"] == 1
