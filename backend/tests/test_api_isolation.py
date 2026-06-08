import pytest
from fastapi.testclient import TestClient
import app.db.connection

def test_login_generates_consistent_id(client: TestClient):
    response = client.post("/login", json={"email": "test@example.com"})
    assert response.status_code == 200
    user_id_1 = response.json()["user_id"]
    assert len(user_id_1) == 8
    
    response = client.post("/login", json={"email": "test@example.com"})
    user_id_2 = response.json()["user_id"]
    assert user_id_1 == user_id_2

def test_user_data_isolation(client: TestClient):
    # Login User A
    res_a = client.post("/login", json={"email": "user_a@example.com"})
    user_id_a = res_a.json()["user_id"]
    
    # Login User B
    res_b = client.post("/login", json={"email": "user_b@example.com"})
    user_id_b = res_b.json()["user_id"]
    
    response_b = client.get(f"/events?user_id={user_id_b}")
    assert response_b.status_code == 200
    assert response_b.json()["events"] == []

def test_db_creation_on_first_access(client: TestClient):
    res = client.post("/login", json={"email": "new_user@example.com"})
    user_id = res.json()["user_id"]
    
    db_path = app.db.connection.BASE_USERS_PATH / f"user_{user_id}.duckdb"
    assert not db_path.exists()
    
    # Access any endpoint
    client.get(f"/events?user_id={user_id}")
    
    assert db_path.exists()
