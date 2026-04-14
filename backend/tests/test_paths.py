import pytest
from fastapi.testclient import TestClient
import duckdb
from app.domains.cohorts.cohort_service import ensure_cohort_tables

def setup_test_data(conn: duckdb.DuckDBPyConnection, start_id: int = 100):
    conn.execute("DROP TABLE IF EXISTS cohort_activity_snapshot")
    conn.execute("DROP TABLE IF EXISTS cohort_membership")
    conn.execute("DROP TABLE IF EXISTS cohorts")
    conn.execute("DROP TABLE IF EXISTS events_normalized")
    conn.execute("DROP SEQUENCE IF EXISTS cohorts_id_sequence")
    ensure_cohort_tables(conn)
    # Create normalized events table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events_normalized (
            user_id TEXT,
            event_name TEXT,
            event_time TIMESTAMP,
            row_id INTEGER
        )
    """)
    
    # insert events with explicit row_ids
    events = [
        # User 1: Perfect match
        ('u1', 'A', '2023-01-01 10:00:00', 1),
        ('u1', 'B', '2023-01-01 10:05:00', 2),
        ('u1', 'C', '2023-01-01 10:10:00', 3),
        
        # User 2: Multiple A, should pick earliest
        ('u2', 'A', '2023-01-01 10:00:00', 4),
        ('u2', 'A', '2023-01-01 10:02:00', 5),
        ('u2', 'B', '2023-01-01 10:05:00', 6),
        ('u2', 'C', '2023-01-01 10:10:00', 7),
        
        # User 3: Wrong order B -> A
        ('u3', 'B', '2023-01-01 10:00:00', 8),
        ('u3', 'A', '2023-01-01 10:05:00', 9),
        ('u3', 'C', '2023-01-01 10:10:00', 10),
        
        # User 4: Only A and B
        ('u4', 'A', '2023-01-01 10:00:00', 11),
        ('u4', 'B', '2023-01-01 10:05:00', 12),
        
        # User 5: Only A
        ('u5', 'A', '2023-01-01 10:00:00', 13),
    ]
    conn.executemany("INSERT INTO events_normalized (user_id, event_name, event_time, row_id) VALUES (?, ?, ?, ?)", events)
    
    # Create cohort
    conn.execute("INSERT INTO cohorts (cohort_id, name, is_active) VALUES (?, 'All Users', TRUE)", [start_id])
    users = ['u1', 'u2', 'u3', 'u4', 'u5']
    for u in users:
        conn.execute("INSERT INTO cohort_membership (user_id, cohort_id, join_time) VALUES (?, ?, '2023-01-01 00:00:00')", [u, start_id])
        
    # Populate snapshot (simulating ingest)
    conn.execute("""
        INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name, row_id)
        SELECT ?, user_id, event_time, event_name, row_id FROM events_normalized
    """, [start_id])


def test_run_paths_basic(client: TestClient, db_connection: duckdb.DuckDBPyConnection):
    setup_test_data(db_connection, start_id=100)
    
    response = client.post("/paths/run", json={"steps": ["A", "B", "C"]})
    assert response.status_code == 200
    data = response.json()
    
    # Verify results
    assert data["steps"] == ["A", "B", "C"]
    assert len(data["results"]) >= 1
    res = [r for r in data["results"] if r["cohort_id"] == 100][0]
    assert res["cohort_name"] == "All Users"
    assert res["cohort_size"] == 5
    
    # Step 1: A
    assert res["steps"][0]["users"] == 5
    
    # Step 2: B
    assert res["steps"][1]["users"] == 3
    
    # Step 3: C
    assert res["steps"][2]["users"] == 2

def test_run_paths_repeated_event(client: TestClient, db_connection: duckdb.DuckDBPyConnection):
    db_connection.execute("DROP TABLE IF EXISTS cohort_activity_snapshot")
    db_connection.execute("DROP TABLE IF EXISTS cohort_membership")
    db_connection.execute("DROP TABLE IF EXISTS cohorts")
    db_connection.execute("DROP SEQUENCE IF EXISTS cohorts_id_sequence")
    ensure_cohort_tables(db_connection)
    db_connection.execute("""
        CREATE TABLE IF NOT EXISTS events_normalized (user_id TEXT, event_name TEXT, event_time TIMESTAMP, row_id INTEGER)
    """)
    # A(t1), A(t2)
    db_connection.execute("INSERT INTO events_normalized VALUES ('u1', 'A', '2023-01-01 10:00:00', 1)")
    db_connection.execute("INSERT INTO events_normalized VALUES ('u1', 'A', '2023-01-01 10:05:00', 2)")
    
    db_connection.execute("INSERT INTO cohorts (cohort_id, name, is_active) VALUES (200, 'Test', TRUE)")
    db_connection.execute("INSERT INTO cohort_membership (user_id, cohort_id, join_time) VALUES ('u1', 200, '2023-01-01 00:00:00')")
    db_connection.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name, row_id) SELECT 200, user_id, event_time, event_name, row_id FROM events_normalized")

    
    # Path: A -> A
    response = client.post("/paths/run", json={"steps": ["A", "A"]})
    assert response.status_code == 200
    res = [r for r in response.json()["results"] if r["cohort_id"] == 200][0]
    assert res["steps"][0]["users"] == 1
    assert res["steps"][1]["users"] == 1 # u1 reached A twice

def test_create_dropoff_cohort(client: TestClient, db_connection: duckdb.DuckDBPyConnection):
    setup_test_data(db_connection, start_id=300)
    
    # Sequence A -> B -> C
    # Drop-off after Step 2 (B): User u4
    
    response = client.post("/paths/create-dropoff-cohort", json={
        "steps": ["A", "B", "C"],
        "step_index": 3,
        "cohort_id": 300
    })
    assert response.status_code == 200
    data = response.json()
    assert data["user_count"] == 1 # u4
    new_cohort_id = data["cohort_id"]
    
    # Verify records in db
    members = db_connection.execute("SELECT user_id FROM cohort_membership WHERE cohort_id = ?", [new_cohort_id]).fetchall()
    assert [m[0] for m in members] == ["u4"]
    # Verify origin
    origin = db_connection.execute("SELECT cohort_origin FROM cohorts WHERE cohort_id = ?", [new_cohort_id]).fetchone()[0]
    assert origin == 'paths'
def test_paths_time_consistency(client: TestClient, db_connection: duckdb.DuckDBPyConnection):
    setup_test_data(db_connection, start_id=500)
    
    # Path A -> B
    # u1: A(10:00), B(10:05) -> 300s
    # u2: A(10:02), B(10:05) -> 180s (Wait, earliest A is 10:00, so B is 300s)
    # u4: A(10:00), B(10:05) -> 300s
    
    response = client.post("/paths/run", json={"steps": ["A", "B"]})
    assert response.status_code == 200
    res = [r for r in response.json()["results"] if r["cohort_id"] == 500][0]
    
    step2 = res["steps"][1]
    assert step2["users"] == 3
    assert step2["mean_time"] is not None
    assert step2["mean_time"] > 0
    # (300 + 300 + 300) / 3 = 300
    assert abs(step2["mean_time"] - 300) < 1.0

def test_create_dropoff_cohort_rigorous(client: TestClient, db_connection: duckdb.DuckDBPyConnection):
    setup_test_data(db_connection, start_id=600)
    
    # Sequence A -> B
    # step_1 (A): u1, u2, u3, u4, u5 (5 users)
    # step_2 (B): u1, u2, u4 (3 users)
    # Drop-off after A: u3, u5
    
    response = client.post("/paths/create-dropoff-cohort", json={
        "steps": ["A", "B"],
        "step_index": 2,
        "cohort_id": 600
    })
    assert response.status_code == 200
    data = response.json()
    new_cohort_id = data["cohort_id"]
    assert data["user_count"] == 2 # u3, u5
    
    # 1. Verify membership
    members = db_connection.execute("SELECT user_id, join_time FROM cohort_membership WHERE cohort_id = ?", [new_cohort_id]).fetchall()
    member_map = {row[0]: row[1] for row in members}
    assert set(member_map.keys()) == {"u3", "u5"}
    
    # 2. Verify join_time is from original cohort (00:00:00)
    assert member_map["u3"].strftime("%H:%M:%S") == "00:00:00"
    assert member_map["u5"].strftime("%H:%M:%S") == "00:00:00"

def test_create_reached_cohort_basic(client: TestClient, db_connection: duckdb.DuckDBPyConnection):
    setup_test_data(db_connection, start_id=700)
    
    # Path A -> B -> C
    # Users reaching Step 2 (B): u1, u2, u4 (3 users)
    
    response = client.post("/paths/create-reached-cohort", json={
        "steps": ["A", "B", "C"],
        "step_index": 2,
        "cohort_id": 700
    })
    assert response.status_code == 200
    data = response.json()
    assert data["user_count"] == 3
    new_id = data["cohort_id"]
    
    # Verify name
    assert " - Reached Step 2" in data["name"]
    assert "All Users" in data["name"]
    
    # Verify membership
    members = db_connection.execute("SELECT user_id, join_time FROM cohort_membership WHERE cohort_id = ?", [new_id]).fetchall()
    assert len(members) == 3
    member_ids = {m[0] for m in members}
    assert member_ids == {"u1", "u2", "u4"}
    
    # Verify join_time is from original cohort (00:00:00)
    for m in members:
        assert m[1].strftime("%H:%M:%S") == "00:00:00"

def test_cohort_custom_naming(client: TestClient, db_connection: duckdb.DuckDBPyConnection):
    setup_test_data(db_connection, start_id=800)
    
    custom_name = "My custom reached cohort"
    response = client.post("/paths/create-reached-cohort", json={
        "steps": ["A", "B"],
        "step_index": 1,
        "cohort_id": 800,
        "cohort_name": custom_name
    })
    assert response.json()["name"] == custom_name
    
    # And for drop-off
    custom_name_2 = "My custom drop-off cohort"
    response = client.post("/paths/create-dropoff-cohort", json={
        "steps": ["A", "B"],
        "step_index": 2,
        "cohort_id": 800,
        "cohort_name": custom_name_2
    })
    assert response.json()["name"] == custom_name_2

def test_paths_persistence(client: TestClient, db_connection: duckdb.DuckDBPyConnection):
    setup_test_data(db_connection, start_id=900)
    
    # 1. Create a cohort
    response = client.post("/paths/create-reached-cohort", json={"steps": ["A"], "step_index": 1, "cohort_id": 900})
    cohort_id = response.json()["cohort_id"]
    
    # 2. Run analysis again
    client.post("/paths/run", json={"steps": ["A", "B"]})
    
    # 3. Verify cohort still exists (NO MORE AUTO-DELETE)
    exists = db_connection.execute("SELECT COUNT(*) FROM cohorts WHERE cohort_id = ?", [cohort_id]).fetchone()[0]
    assert exists == 1

def test_create_dropoff_step1(client: TestClient, db_connection: duckdb.DuckDBPyConnection):
    setup_test_data(db_connection, start_id=1000)
    
    # Source cohort 1000 has: u1, u2, u3, u4, u5
    # step_1 (A): u1, u2, u4, u5 (Wait, setup_test_data has u3 starting with B. Let's check:
    # u3 string: B(10:00), A(10:05). So u3 DOES have A later, but step 1 will catch it unless time filtered? NO, step 1 just finds A.)
    # Wait, setup_test_data:
    # u1: A, B, C
    # u2: A, A, B, C
    # u3: B, A, C
    # u4: A, B
    # u5: A
    # ALL have A! So no one drops off at step 1 for "A".
    
    # Let's insert a user u6 who only has 'Z'
    db_connection.execute("INSERT INTO events_normalized VALUES ('u6', 'Z', '2023-01-01 10:00:00', 14)")
    db_connection.execute("INSERT INTO cohort_membership (user_id, cohort_id, join_time) VALUES ('u6', 1000, '2023-01-01 00:00:00')")
    db_connection.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name, row_id) VALUES (1000, 'u6', '2023-01-01 10:00:00', 'Z', 14)")
    
    response = client.post("/paths/create-dropoff-cohort", json={
        "steps": ["A", "B"],
        "step_index": 1,
        "cohort_id": 1000
    })
    
    assert response.status_code == 200
    data = response.json()
    assert data["user_count"] == 1
    new_id = data["cohort_id"]
    
    # Verify name
    assert " - Didn't perform Step 1" in data["name"]
    
    # Verify membership
    members = db_connection.execute("SELECT user_id, join_time FROM cohort_membership WHERE cohort_id = ?", [new_id]).fetchall()
    assert len(members) == 1
    assert members[0][0] == 'u6'
    assert members[0][1].strftime("%H:%M:%S") == "00:00:00" # Retained original join_time
    
    # Sanity check sum
    step1_users = db_connection.execute("SELECT COUNT(DISTINCT user_id) FROM cohort_activity_snapshot WHERE cohort_id=1000 AND event_name='A'").fetchone()[0]
    assert step1_users + 1 == 6 # Total source cohort length is 6

def test_paths_cohort_visibility_regression(client: TestClient, db_connection: duckdb.DuckDBPyConnection):
    """
    Regression test for the issue where 'reached' cohorts showed drop-offs 
    because join_time was pinned to the achievement time.
    """
    setup_test_data(db_connection, start_id=2000)
    
    # 1. Run Baseline analysis A -> B -> C
    # u1, u2 reach C.
    
    # 2. Create 'Reached Step 3' cohort
    response = client.post("/paths/create-reached-cohort", json={
        "steps": ["A", "B", "C"],
        "step_index": 3,
        "cohort_id": 2000
    })
    assert response.status_code == 200
    new_cohort_id = response.json()["cohort_id"]
    
    # 3. Run the SAME analysis on the NEW cohort
    # Because join_time is now the original 00:00:00, it should see Step 1 and Step 2.
    response = client.post("/paths/run", json={"steps": ["A", "B", "C"]})
    assert response.status_code == 200
    data = response.json()
    
    res = [r for r in data["results"] if r["cohort_id"] == new_cohort_id][0]
    assert res["cohort_size"] == 2 # u1, u2
    
    # EVERYONE who is in this cohort MUST reach Step 3 in this analysis
    assert res["steps"][0]["users"] == 2 # Step A
    assert res["steps"][1]["users"] == 2 # Step B
    assert res["steps"][2]["users"] == 2 # Step C
    assert res["steps"][2]["conversion_pct"] == 100.0


def test_run_paths_with_time_window(client: TestClient, db_connection: duckdb.DuckDBPyConnection):
    setup_test_data(db_connection, start_id=3000)
    # u1: A(10:00), B(10:05), C(10:10) -> All gaps 5 min
    # u2: A(10:00), B(10:05), C(10:10) -> All gaps 5 min
    # u4: A(10:00), B(10:05) -> Gap 5 min
    
    # Run with 3-minute window
    response = client.post("/paths/run", json={
        "steps": ["A", "B", "C"],
        "max_step_gap_minutes": 3
    })
    assert response.status_code == 200
    res = [r for r in response.json()["results"] if r["cohort_id"] == 3000][0]
    
    # Step 1 (A) should have everyone (unconstrained)
    assert res["steps"][0]["users"] == 5
    # Step 2 (B) should have 0 users because all gaps are 5 min (> 3 min)
    assert res["steps"][1]["users"] == 0
    # Step 3 (C) should also be 0
    assert res["steps"][2]["users"] == 0

def test_run_paths_unlimited_window(client: TestClient, db_connection: duckdb.DuckDBPyConnection):
    setup_test_data(db_connection, start_id=4000)
    
    # Run with NULL (unlimited) window
    response = client.post("/paths/run", json={
        "steps": ["A", "B", "C"],
        "max_step_gap_minutes": None
    })
    assert response.status_code == 200
    res = [r for r in response.json()["results"] if r["cohort_id"] == 4000][0]
    
    # Normal conversion should happen
    assert res["steps"][0]["users"] == 5
    assert res["steps"][1]["users"] == 3
    assert res["steps"][2]["users"] == 2

def test_dropoff_with_time_window(client: TestClient, db_connection: duckdb.DuckDBPyConnection):
    setup_test_data(db_connection, start_id=5000)
    # u1, u2, u4 have A -> B with 5 min gap
    
    # Create drop-off cohort for Step 2 (B) with 3-min window
    response = client.post("/paths/create-dropoff-cohort", json={
        "steps": ["A", "B", "C"],
        "step_index": 2,
        "cohort_id": 5000,
        "max_step_gap_minutes": 3
    })
    assert response.status_code == 200
    data = response.json()
    # Everyone who reached A should drop off because gap to B is 5 min > 3 min
    assert data["user_count"] == 5 

def test_greedy_no_backtrack_with_window(client: TestClient, db_connection: duckdb.DuckDBPyConnection):
    db_connection.execute("DROP TABLE IF EXISTS cohort_activity_snapshot")
    db_connection.execute("DROP TABLE IF EXISTS cohort_membership")
    db_connection.execute("DROP TABLE IF EXISTS cohorts")
    db_connection.execute("DROP SEQUENCE IF EXISTS cohorts_id_sequence")
    ensure_cohort_tables(db_connection)
    db_connection.execute("CREATE TABLE IF NOT EXISTS events_normalized (user_id TEXT, event_name TEXT, event_time TIMESTAMP, row_id INTEGER)")
    
    # Edge case user:
    # A at 10:00
    # B at 10:02 (valid but leads to dead end for C)
    # B at 10:08 (valid if backtrack allowed, but greedy picks 10:02)
    # C at 10:09 (valid if prev_B was 10:08, but invalid since prev_B was 10:02)
    
    db_connection.execute("INSERT INTO events_normalized VALUES ('u_greedy', 'A', '2023-01-01 10:00:00', 1)")
    db_connection.execute("INSERT INTO events_normalized VALUES ('u_greedy', 'B', '2023-01-01 10:02:00', 2)")
    db_connection.execute("INSERT INTO events_normalized VALUES ('u_greedy', 'B', '2023-01-01 10:08:00', 3)")
    db_connection.execute("INSERT INTO events_normalized VALUES ('u_greedy', 'C', '2023-01-01 10:09:00', 4)")
    
    db_connection.execute("INSERT INTO cohorts (cohort_id, name, is_active) VALUES (6000, 'Greedy Test', TRUE)")
    db_connection.execute("INSERT INTO cohort_membership (user_id, cohort_id, join_time) VALUES ('u_greedy', 6000, '2023-01-01 00:00:00')")
    db_connection.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name, row_id) SELECT 6000, user_id, event_time, event_name, row_id FROM events_normalized")

    
    # Run with 5-minute window
    response = client.post("/paths/run", json={
        "steps": ["A", "B", "C"],
        "max_step_gap_minutes": 5
    })
    
    assert response.status_code == 200
    res = [r for r in response.json()["results"] if r["cohort_id"] == 6000][0]
    
    assert res["steps"][0]["users"] == 1 # A@10:00
    assert res["steps"][1]["users"] == 1 # B@10:02 (gap 2m <= 5m)
    # Step 3 should be 0 because C@10:09 is 7m after B@10:02. 
    # Backtracking to B@10:08 is NOT allowed.
    assert res["steps"][2]["users"] == 0

def test_run_paths_db_priority(client: TestClient, db_connection: duckdb.DuckDBPyConnection):
    """
    Scenario: Saved path has 60 min gap. 
    Request sends 3 min gap with path_id.
    Backend MUST use 60 min gap.
    """
    setup_test_data(db_connection, start_id=7000)
    # u1: A(10:00), B(10:05), gap=5
    
    # 1. Create a path in DB with 60m gap
    path_res = client.post("/paths", json={
        "name": "Test Priority",
        "steps": [
            {"step_order": 0, "groups": [{"event_name": "A"}]},
            {"step_order": 1, "groups": [{"event_name": "B"}]}
        ],
        "max_step_gap_minutes": 60
    })
    path_id = path_res.json()["id"]
    
    # 2. Run with path_id BUT send 3m gap in payload
    # If it uses 3m -> u1 drops off (0 users at Step 2)
    # If it uses 60m (DB) -> u1 converts (3 users at Step 2)
    response = client.post("/paths/run", json={
        "steps": ["A", "B"],
        "path_id": path_id,
        "max_step_gap_minutes": 3 
    })
    
    assert response.status_code == 200
    res = [r for r in response.json()["results"] if r["cohort_id"] == 7000][0]
    
    # Should use 60m from DB
    assert res["steps"][1]["users"] == 3

def test_run_paths_adhoc(client: TestClient, db_connection: duckdb.DuckDBPyConnection):
    """
    Scenario: No path_id. Use payload.
    """
    setup_test_data(db_connection, start_id=8000)
    
    # Run with 3m gap, no path_id
    response = client.post("/paths/run", json={
        "steps": ["A", "B"],
        "max_step_gap_minutes": 3,
        "path_id": None
    })
    
    assert response.status_code == 200
    res = [r for r in response.json()["results"] if r["cohort_id"] == 8000][0]
    
    # Should use 3m from payload -> 0 users
    assert res["steps"][1]["users"] == 0

