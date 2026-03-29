import pytest
import duckdb
from app.domains.paths.paths_service import (
    ensure_path_tables, create_path, run_paths, validate_path, PathStep, PathStepFilter, _materialize_paths_cohort
)
from app.domains.cohorts.cohort_service import ensure_cohort_tables

@pytest.fixture
def conn():
    c = duckdb.connect(":memory:")
    ensure_cohort_tables(c)
    ensure_path_tables(c)
    # Seed events_scoped
    c.execute("""
        CREATE TABLE events_scoped (
            user_id INTEGER,
            event_name TEXT,
            event_time TIMESTAMP,
            prop1 TEXT,
            prop2 INTEGER
        )
    """)
    # User 1: A (t1) -> B (t2) -> A (t3) -> B (t4)
    # User 2: A (t1) -> A (t1) [Identical timestamps]
    c.execute("INSERT INTO events_scoped VALUES (1, 'A', '2023-01-01 10:00:00', 'val1', 10)")
    c.execute("INSERT INTO events_scoped VALUES (1, 'B', '2023-01-01 10:05:00', 'val2', 20)")
    c.execute("INSERT INTO events_scoped VALUES (1, 'A', '2023-01-01 10:10:00', 'val1', 30)")
    c.execute("INSERT INTO events_scoped VALUES (1, 'B', '2023-01-01 10:15:00', 'val2', 40)")
    
    c.execute("INSERT INTO events_scoped VALUES (2, 'A', '2023-01-01 10:00:00', 'val1', 10)")
    c.execute("INSERT INTO events_scoped VALUES (2, 'A', '2023-01-01 10:00:00', 'val1', 20)")

    c.execute("INSERT INTO cohorts (cohort_id, name, is_active, hidden) VALUES (1, 'Test All', TRUE, FALSE)")
    c.execute("INSERT INTO cohort_membership VALUES (1, 1, '2023-01-01 00:00:00')")
    c.execute("INSERT INTO cohort_membership VALUES (2, 1, '2023-01-01 00:00:00')")
    
    # We must also populate cohort_activity_snapshot if we want the non-filtered path to work
    c.execute("""
        INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name)
        SELECT 1, user_id, event_time, event_name FROM events_scoped
    """)
    
    return c

def test_deterministic_matching_identical_timestamps(conn):
    # Sequence A -> A
    # No filters -> uses cohort_activity_snapshot
    steps = [
        PathStep(step_order=0, event_name='A', filters=[]),
        PathStep(step_order=1, event_name='A', filters=[])
    ]
    res = run_paths(conn, steps)
    cohort_res = next(r for r in res["results"] if r.cohort_id == 1)
    
    # Both users reached step 1
    assert cohort_res.steps[0].users == 2
    # Both users reached step 2 (because User 1 has two A's at different times, 
    # and User 2 has two A's at same time but different rowids)
    assert cohort_res.steps[1].users == 2

def test_greedy_matching_no_reuse(conn):
    # Sequence A -> B
    steps = [
        PathStep(step_order=0, event_name='A', filters=[]),
        PathStep(step_order=1, event_name='B', filters=[])
    ]
    res = run_paths(conn, steps)
    cohort_res = next(r for r in res["results"] if r.cohort_id == 1)
    # User 1 reached B. User 2 never had a B.
    assert cohort_res.steps[1].users == 1

def test_validation_reasons(conn):
    # 1. Event not found
    steps = [PathStep(step_order=0, event_name='GHOST', filters=[])]
    assert "Event not found" in validate_path(conn, steps)
    
    # 2. Property not found (checked against events_scoped)
    steps = [PathStep(step_order=0, event_name='A', filters=[PathStepFilter(property_key='nonexistent', property_value='v')])]
    assert "Property not found" in validate_path(conn, steps)
    
    # 3. Invalid value (casting error)
    steps = [PathStep(step_order=0, event_name='A', filters=[PathStepFilter(property_key='prop2', property_value='not_a_number')])]
    assert "Invalid value for property prop2" in validate_path(conn, steps)
    
    # 4. Property value not found
    steps = [PathStep(step_order=0, event_name='A', filters=[PathStepFilter(property_key='prop1', property_value='nowhere')])]
    assert "Property value not found" in validate_path(conn, steps)

def test_filtered_matching_scoped(conn):
    # Sequence A (prop2=30) -> B
    # With filter -> uses events_scoped
    steps = [
        PathStep(step_order=0, event_name='A', filters=[PathStepFilter(property_key='prop2', property_value=30)]),
        PathStep(step_order=1, event_name='B', filters=[])
    ]
    res = run_paths(conn, steps)
    cohort_res = next(r for r in res["results"] if r.cohort_id == 1)
    # Only User 1 has A(30)
    assert cohort_res.steps[0].users == 1
    # User 1 has B(40) after A(30)
    assert cohort_res.steps[1].users == 1
    
    # Also verify t1 < t2
    # User 1: A(10) @ 10:00, B(20) @ 10:05, A(30) @ 10:10, B(40) @ 10:15
    # Step 1: A(30) @ 10:10. Step 2: B(40) @ 10:15. Correct.
