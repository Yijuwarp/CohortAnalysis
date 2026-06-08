import pytest
import os
import shutil
from pathlib import Path
from tests.utils import DeterministicTestClient, DETERMINISTIC_USER_ID
from app.db.connection import get_user_db_path

def test_login_generates_consistent_id(client: DeterministicTestClient):
    response = client.post("/login", json={"email": "test@example.com"})
    assert response.status_code == 200
    user_id_1 = response.json()["user_id"]
    assert len(user_id_1) == 8
    
    response = client.post("/login", json={"email": "test@example.com"})
    user_id_2 = response.json()["user_id"]
    assert user_id_1 == user_id_2

def test_user_data_isolation(client: DeterministicTestClient):
    # Login User A
    res_a = client.post("/login", json={"email": "user_a@example.com"})
    user_id_a = res_a.json()["user_id"]
    
    # Login User B
    res_b = client.post("/login", json={"email": "user_b@example.com"})
    user_id_b = res_b.json()["user_id"]
    
    # Access any endpoint for User B
    response_b = client.get(f"/events?user_id={user_id_b}")
    assert response_b.status_code == 200
    assert response_b.json()["events"] == []

def test_db_creation_on_first_access(client: DeterministicTestClient, test_users_dir: Path):
    res = client.post("/login", json={"email": "new_user@example.com"})
    user_id = res.json()["user_id"]
    
    # construct path relative to the isolated test directory
    db_path = get_user_db_path(user_id)
    assert not db_path.exists()
    
    # Access any endpoint
    client.get(f"/events?user_id={user_id}")
    
    assert db_path.exists()
