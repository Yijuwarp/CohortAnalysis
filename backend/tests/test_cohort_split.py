import pytest
import io
import json
from datetime import datetime
from fastapi.testclient import TestClient
from app.main import app
from app.db.connection import get_connection
from tests.utils import csv_upload

client = TestClient(app)

@pytest.fixture(autouse=True)
def setup_db():
    """Ensure tables are fresh and schema is up to date."""
    conn = get_connection()
    yield
    # Cleanup after each test
    conn.execute("DELETE FROM cohort_membership")
    conn.execute("DELETE FROM cohort_conditions")
    conn.execute("DELETE FROM cohorts")
    conn.execute("DELETE FROM events_normalized")

def _prepare_data(num_users=20):
    """Upload CSV and map columns to ensure schema exists."""
    rows = ["user_id,event_name,event_time,country"]
    for i in range(num_users):
        user_id = f"u{i}"
        country = "US" if i < 10 else ("India" if i < 15 else "UK")
        rows.append(f"{user_id},login,2024-01-01 10:00:00,{country}")
    
    csv_text = "\n".join(rows)
    upload = csv_upload(client, csv_text=csv_text)
    assert upload.status_code == 200
    
    mapping = client.post("/map-columns", json={
        "user_id_column": "user_id",
        "event_name_column": "event_name",
        "event_time_column": "event_time"
    })
    assert mapping.status_code == 200

def _create_parent_cohort(name="Parent"):
    """Create a cohort containing all users."""
    _prepare_data()
    resp = client.post("/cohorts", json={
        "name": name,
        "logic_operator": "AND",
        "join_type": "condition_met",
        "conditions": [{"event_name": "login", "min_event_count": 1}]
    })
    assert resp.status_code == 200
    return resp.json()["cohort_id"]

def test_random_split_creates_n_groups():
    cohort_id = _create_parent_cohort()
    n = 3
    resp = client.post(f"/cohorts/{cohort_id}/split", json={
        "type": "random",
        "random": {"num_groups": n}
    })
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["child_cohorts"]) == n
    
    conn = get_connection()
    children = conn.execute("SELECT cohort_id, name, split_type FROM cohorts WHERE split_parent_cohort_id = ?", [cohort_id]).fetchall()
    assert len(children) == n
    for child in children:
        assert child[2] == "random"

def test_property_split_creates_value_cohorts():
    cohort_id = _create_parent_cohort()
    
    # 10 US, 5 India, 5 UK
    resp = client.post(f"/cohorts/{cohort_id}/split", json={
        "type": "property",
        "property": {"column": "country", "values": ["US", "India"]}
    })
    assert resp.status_code == 200
    data = resp.json()
    
    # Should create US, India, and _other
    assert len(data["child_cohorts"]) == 3
    
    conn = get_connection()
    # Check US cohort size
    us_id = next(c["id"] for c in data["child_cohorts"] if "_US" in c["name"])
    us_size = conn.execute("SELECT COUNT(*) FROM cohort_membership WHERE cohort_id = ?", [us_id]).fetchone()[0]
    assert us_size == 10
    
    # Check India cohort size
    in_id = next(c["id"] for c in data["child_cohorts"] if "_India" in c["name"])
    in_size = conn.execute("SELECT COUNT(*) FROM cohort_membership WHERE cohort_id = ?", [in_id]).fetchone()[0]
    assert in_size == 5
    
    # Check _other (UK) cohort size
    ot_id = next(c["id"] for c in data["child_cohorts"] if "_other" in c["name"])
    ot_size = conn.execute("SELECT COUNT(*) FROM cohort_membership WHERE cohort_id = ?", [ot_id]).fetchone()[0]
    assert ot_size == 5

def test_property_split_no_other_when_all_selected():
    cohort_id = _create_parent_cohort()
    
    resp = client.post(f"/cohorts/{cohort_id}/split", json={
        "type": "property",
        "property": {"column": "country", "values": ["US", "India", "UK"]}
    })
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["child_cohorts"]) == 3
    assert not any("_other" in c["name"] for c in data["child_cohorts"])

def test_preview_split_no_persistence():
    cohort_id = _create_parent_cohort()
    
    resp = client.post(f"/cohorts/{cohort_id}/split/preview", json={
        "type": "random",
        "random": {"num_groups": 5}
    })
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["preview"]) == 5
    
    conn = get_connection()
    count = conn.execute("SELECT COUNT(*) FROM cohorts WHERE split_parent_cohort_id = ?", [cohort_id]).fetchone()[0]
    assert count == 0

def test_split_rejects_subcohort():
    cohort_id = _create_parent_cohort()
    resp = client.post(f"/cohorts/{cohort_id}/split", json={
        "type": "random",
        "random": {"num_groups": 2}
    })
    child_id = resp.json()["child_cohorts"][0]["id"]
    
    resp2 = client.post(f"/cohorts/{child_id}/split", json={
        "type": "random",
        "random": {"num_groups": 2}
    })
    assert resp2.status_code == 400
    assert "Cannot split a sub-cohort" in resp2.json()["detail"]

def test_delete_parent_cascades_to_splits():
    cohort_id = _create_parent_cohort()
    client.post(f"/cohorts/{cohort_id}/split", json={"type": "random", "random": {"num_groups": 2}})
    
    conn = get_connection()
    assert conn.execute("SELECT COUNT(*) FROM cohorts WHERE split_parent_cohort_id = ?", [cohort_id]).fetchone()[0] == 2
    
    client.delete(f"/cohorts/{cohort_id}")
    
    assert conn.execute("SELECT COUNT(*) FROM cohorts WHERE cohort_id = ?", [cohort_id]).fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM cohorts WHERE split_parent_cohort_id = ?", [cohort_id]).fetchone()[0] == 0

def test_list_cohorts_includes_split_info():
    cohort_id = _create_parent_cohort()
    client.post(f"/cohorts/{cohort_id}/split", json={
        "type": "property", 
        "property": {"column": "country", "values": ["US"]}
    })
    
    resp = client.get("/cohorts")
    cohorts = resp.json()["cohorts"]
    child = next(c for c in cohorts if c["split_parent_cohort_id"] == cohort_id)
    assert child["split_type"] == "property"
    assert child["split_property"] == "country"
    assert child["split_value"] in ["US", "__OTHER__"]
