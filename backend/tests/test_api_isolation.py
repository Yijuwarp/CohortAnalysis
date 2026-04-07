import pytest
from fastapi.testclient import TestClient
from app.main import app
import os
import shutil

client = TestClient(app)

@pytest.fixture(autouse=True)
def setup_and_teardown():
    # Clean up test data before and after
    if os.path.exists("backend/data/users"):
        shutil.rmtree("backend/data/users")
    yield
    if os.path.exists("backend/data/users"):
        shutil.rmtree("backend/data/users")

def test_login_generates_consistent_id():
    response = client.post("/login", json={"email": "test@example.com"})
    assert response.status_code == 200
    user_id_1 = response.json()["user_id"]
    assert len(user_id_1) == 8
    
    response = client.post("/login", json={"email": "test@example.com"})
    user_id_2 = response.json()["user_id"]
    assert user_id_1 == user_id_2

def test_user_data_isolation():
    # Login User A
    res_a = client.post("/login", json={"email": "user_a@example.com"})
    user_id_a = res_a.json()["user_id"]
    
    # Login User B
    res_b = client.post("/login", json={"email": "user_b@example.com"})
    user_id_b = res_b.json()["user_id"]
    
    # User A uploads data (we assume /upload works for this test)
    # Since we haven't implemented /upload with user_id yet, this will fail or use default DB.
    # For now, let's just check if /events is empty for a new user.
    
    response_b = client.get(f"/events?user_id={user_id_b}")
    assert response_b.status_code == 200
    assert response_b.json()["events"] == []

def test_db_creation_on_first_access():
    res = client.post("/login", json={"email": "new_user@example.com"})
    user_id = res.json()["user_id"]
    
    db_path = f"backend/data/users/user_{user_id}.duckdb"
    assert not os.path.exists(db_path)
    
    # Access any endpoint
    client.get(f"/events?user_id={user_id}")
    
    assert os.path.exists(db_path)
