import pytest
import duckdb
from app.domains.analytics.flow_service import get_l1_flows, get_l2_flows
from app.domains.cohorts.cohort_service import ensure_cohort_tables

def setup_db(c):
    c.execute("CREATE TABLE cohort_activity_snapshot (cohort_id INTEGER, user_id VARCHAR, event_name VARCHAR, event_time TIMESTAMP)")
    c.execute("CREATE TABLE cohorts (cohort_id INTEGER, name VARCHAR, hidden BOOLEAN, size INTEGER)")
    c.execute("CREATE TABLE events_scoped (user_id VARCHAR, event_time TIMESTAMP, event_name VARCHAR)")
    c.execute("CREATE TABLE events_normalized (user_id VARCHAR, event_time TIMESTAMP, event_name VARCHAR)")
    # Satisfy _scoped_has_data
    c.execute("INSERT INTO events_scoped VALUES ('u1', '2024-01-01 00:00:10', 'event_0')")
    ensure_cohort_tables(c)

@pytest.fixture
def conn():
    c = duckdb.connect(":memory:")
    setup_db(c)
    return c

def test_flow_limit_respected(conn):
    # u1: event_0 -> event_1 -> event_2 ...
    # u2: event_0 -> event_1
    c = conn
    for i in range(11):
        c.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_name, event_time) VALUES (1, 'u1', ?, ?)", (f"event_{i}", f"2024-01-01 00:00:{10+i:02}"))
    c.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_name, event_time) VALUES (1, 'u2', 'event_0', '2024-01-01 00:00:10')")
    c.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_name, event_time) VALUES (1, 'u2', 'event_1', '2024-01-01 00:00:11')")
    c.execute("INSERT INTO cohorts (cohort_id, name, hidden) VALUES (1, 'All Users', FALSE)")
    
    # Event_0 is followed ONLY by event_1 for both users.
    # Total event types = 1.
    res = get_l1_flows(c, "event_0", "forward", limit=3)
    rows = res["rows"]
    
    # Rows should be: event_1 (2 users), __OTHER__ (0), No further action (0)
    assert len(rows) == 3
    events = [r["path"][-1] for r in rows]
    assert "event_1" in events
    assert "__OTHER__" in events
    assert "No further action" in events

def test_multiple_next_events_other():
    c = duckdb.connect(":memory:")
    setup_db(c)
    for i in range(1, 11):
        uid = f"u{i}"
        c.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_name, event_time) VALUES (1, ?, 'event_0', '2024-01-01 00:00:10')", (uid,))
        c.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_name, event_time) VALUES (1, ?, ?, '2024-01-01 00:00:11')", (uid, f"event_{i}"))
        
    c.execute("INSERT INTO cohorts (cohort_id, name, hidden) VALUES (1, 'All Users', FALSE)")
    
    # Limit = 2. Top-2 = event_1, event_10 (alphabetical tie-break).
    # Other = events 2..9 (8 users).
    res = get_l1_flows(c, "event_0", "forward", limit=2)
    rows = res["rows"]
    
    events = {r["path"][-1]: r["values"]["1"]["user_count"] for r in rows}
    assert events["event_1"] == 1
    assert events["event_10"] == 1
    assert events["__OTHER__"] == 8
    assert rows[0]["meta"]["total_event_types"] == 10

def test_full_expansion_zero_other():
    c = duckdb.connect(":memory:")
    setup_db(c)
    for i in range(5):
        uid = f"u{i}"
        c.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_name, event_time) VALUES (1, ?, 'event_0', '2024-01-01 00:00:10')", (uid,))
        c.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_name, event_time) VALUES (1, ?, ?, '2024-01-01 00:00:11')", (uid, f"event_{i+1}"))
    
    c.execute("INSERT INTO cohorts (cohort_id, name, hidden) VALUES (1, 'All Users', FALSE)")

    # Limit = 10 > total event types (5)
    res = get_l1_flows(c, "event_0", "forward", limit=10)
    rows = res["rows"]
    
    events = {r["path"][-1]: r["values"]["1"]["user_count"] for r in rows}
    assert "__OTHER__" in events
    assert events["__OTHER__"] == 0
    # event_1..event_5 should all be present
    assert len([e for e in events if e.startswith("event_")]) == 5

def test_other_collision_safety():
    c = duckdb.connect(":memory:")
    setup_db(c)
    # User 1 does event_0 -> Other
    c.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_name, event_time) VALUES (1, 'u1', 'event_0', '2024-01-01 00:00:10')")
    c.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_name, event_time) VALUES (1, 'u1', 'Other', '2024-01-01 00:00:11')")
    
    # User 2 does event_0 -> RealEvent
    c.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_name, event_time) VALUES (1, 'u2', 'event_0', '2024-01-01 00:00:10')")
    c.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_name, event_time) VALUES (1, 'u2', 'RealEvent', '2024-01-01 00:00:11')")
    
    c.execute("INSERT INTO cohorts (cohort_id, name, hidden) VALUES (1, 'All Users', FALSE)")

    # Limit = 1. Top-1 is 'Other' (real event)
    # Rows: 'Other', '__OTHER__', 'No further action'
    res = get_l1_flows(c, "event_0", "forward", limit=1)
    rows = res["rows"]
    
    events = {r["path"][-1]: r["values"]["1"]["user_count"] for r in rows}
    assert "Other" in events
    assert "__OTHER__" in events
    assert events["Other"] == 1
    assert events["__OTHER__"] == 1 # RealEvent is in __OTHER__
