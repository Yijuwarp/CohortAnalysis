import pytest
import duckdb
from app.domains.paths.paths_service import (
    ensure_path_tables, create_path, run_paths, validate_path, PathStep, PathStepGroup, PathStepFilter, _materialize_paths_cohort
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
    # User 1: A (t1:00) -> B (t5:00) -> C (t10:00) -> D (t15:00)
    c.execute("INSERT INTO events_scoped VALUES (1, 'A', '2023-01-01 10:00:00', 'val1', 10)")
    c.execute("INSERT INTO events_scoped VALUES (1, 'B', '2023-01-01 10:05:00', 'val2', 20)")
    c.execute("INSERT INTO events_scoped VALUES (1, 'C', '2023-01-01 10:10:00', 'val3', 30)")
    c.execute("INSERT INTO events_scoped VALUES (1, 'D', '2023-01-01 10:15:00', 'val4', 40)")
    
    # User 2: A (t1:00) -> C (t2:00) -> B (t5:00)
    c.execute("INSERT INTO events_scoped VALUES (2, 'A', '2023-01-01 10:00:00', 'val1', 10)")
    c.execute("INSERT INTO events_scoped VALUES (2, 'C', '2023-01-01 10:02:00', 'val3', 20)")
    c.execute("INSERT INTO events_scoped VALUES (2, 'B', '2023-01-01 10:05:00', 'val2', 30)")

    # User 3: Tie-break test data (B and C at same time)
    c.execute("INSERT INTO events_scoped VALUES (3, 'A', '2023-01-01 11:00:00', 'val1', 1)")
    c.execute("INSERT INTO events_scoped VALUES (3, 'B', '2023-01-01 11:05:00', 'val1', 2)")
    c.execute("INSERT INTO events_scoped VALUES (3, 'C', '2023-01-01 11:05:00', 'val1', 3)")

    c.execute("INSERT INTO cohorts (cohort_id, name, is_active, hidden) VALUES (1, 'Test All', TRUE, FALSE)")
    c.execute("INSERT INTO cohort_membership VALUES (1, 1, '2023-01-01 00:00:00')")
    c.execute("INSERT INTO cohort_membership VALUES (2, 1, '2023-01-01 00:00:00')")
    c.execute("INSERT INTO cohort_membership VALUES (3, 1, '2023-01-01 00:00:00')")
    
    c.execute("""
        INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name)
        SELECT 1, user_id, event_time, event_name FROM events_scoped
    """)
    
    return c

def test_or_matching_logic(conn):
    # Step 1: A
    # Step 2: B OR C
    steps = [
        PathStep(step_order=0, groups=[PathStepGroup(event_name='A')]),
        PathStep(step_order=1, groups=[
            PathStepGroup(event_name='B'),
            PathStepGroup(event_name='C')
        ])
    ]
    res = run_paths(conn, steps)
    cohort_res = next(r for r in res["results"] if r.cohort_id == 1)
    
    # All 3 reached step 1 (A)
    assert cohort_res.steps[0].users == 3
    
    # All 3 reached step 2 (User 1 via B, User 2 via C, User 3 via B/C determinism)
    assert cohort_res.steps[1].users == 3
    assert cohort_res.steps[1].group_breakdown is not None
    # User 1 matched B (10:05 early)
    # User 2 matched C (10:02 early)
    # User 3 matched B or C (at 11:05)
    # Total Users = 3. 
    # Breakdown percentages should sum to 100
    total_pct = sum(cohort_res.steps[1].group_breakdown.values())
    assert total_pct == pytest.approx(100.0)

def test_greedy_determinism_same_time(conn):
    # Step: A -> (B OR C)
    # User 3 has B and C at same time.
    steps = [
        PathStep(step_order=0, groups=[PathStepGroup(event_name='A')]),
        PathStep(step_order=1, groups=[
            PathStepGroup(event_name='B'),
            PathStepGroup(event_name='C')
        ])
    ]
    res = run_paths(conn, steps)
    results_for_c1 = next(r for r in res["results"] if r.cohort_id == 1)
    # User 3 should consistently match one of them, so total reached is 3
    assert results_for_c1.steps[1].users == 3

def test_validation_or_steps(conn):
    # 1. Duplicate events in same step
    steps = [
        PathStep(step_order=0, groups=[
            PathStepGroup(event_name='A'),
            PathStepGroup(event_name='A')
        ])
    ]
    assert "Duplicate alternative event" in validate_path(conn, steps)
    
    # 2. Conflicting filters in same group
    steps = [
        PathStep(step_order=0, groups=[
            PathStepGroup(event_name='A', filters=[
                PathStepFilter(property_key='prop1', property_value='val1'), # Exists
                PathStepFilter(property_key='prop1', property_value='val2')  # Exists, but different
            ])
        ])
    ]
    assert "Conflicting filters" in validate_path(conn, steps)

def test_legacy_string_input(conn):
    # run_paths should still support List[str]
    res = run_paths(conn, ["A", "B"])
    cohort_res = next(r for r in res["results"] if r.cohort_id == 1)
    assert cohort_res.steps[0].users == 3
    # User 1 has B, User 2 has B, User 3 has B
    assert cohort_res.steps[1].users == 3
