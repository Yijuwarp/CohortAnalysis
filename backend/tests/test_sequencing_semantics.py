import pytest
import duckdb
from datetime import datetime
from app.domains.paths.paths_service import run_paths, ensure_path_tables
from app.models.paths_models import PathStep, PathStepGroup
from app.domains.cohorts.cohort_service import ensure_cohort_tables

def setup_sequencing_data(c):
    # Setup tables
    ensure_cohort_tables(c)
    ensure_path_tables(c)
    
    # Add row_id column dynamically to cohort_activity_snapshot for sequencing tie-breaker testing
    cols = [r[1] for r in c.execute("PRAGMA table_info(cohort_activity_snapshot)").fetchall()]
    if "row_id" not in cols:
        c.execute("ALTER TABLE cohort_activity_snapshot ADD COLUMN row_id INTEGER")
    
    # Cohort 1: All Users
    c.execute("INSERT INTO cohorts (cohort_id, name, is_active, hidden, join_type) VALUES (1, 'All Users', true, false, 'condition_met')")
    c.execute("INSERT INTO cohort_membership (cohort_id, user_id, join_time) VALUES (1, 'u1', '2024-01-01 00:00:00')")

def test_sequencing_respects_row_id_on_same_timestamp(db_connection):
    setup_sequencing_data(db_connection)
    
    # User u1 has three events at the SAME timestamp
    t0 = datetime(2024, 1, 1, 10, 0, 0)
    # Insert into snapshot directly. 6 columns: cohort_id, user_id, event_time, event_name, row_id, source_saved_id
    db_connection.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name, row_id, source_saved_id) VALUES (1, 'u1', ?, 'signup', 1, NULL)", [t0])
    db_connection.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name, row_id, source_saved_id) VALUES (1, 'u1', ?, 'open', 2, NULL)", [t0])
    db_connection.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name, row_id, source_saved_id) VALUES (1, 'u1', ?, 'click', 3, NULL)", [t0])

    # Path steps: signup, open, click
    steps = [
        PathStep(step_order=0, groups=[PathStepGroup(event_name='signup', filters=[])]),
        PathStep(step_order=1, groups=[PathStepGroup(event_name='open', filters=[])]),
        PathStep(step_order=2, groups=[PathStepGroup(event_name='click', filters=[])])
    ]
    
    res = run_paths(db_connection, steps)
    
    # Extract the users count from Step 3 (index 2) of the first cohort result
    step3_users = res['results'][0].steps[2].users
    assert step3_users == 1, f"Sequencing failed on same-timestamp events. Expected 1 at step 3, got {step3_users}"

def test_sequencing_prevents_greedy_overlap(db_connection):
    setup_sequencing_data(db_connection)
    
    # User has signup -> signup -> open
    t0 = datetime(2024, 1, 1, 10, 0, 0)
    t1 = datetime(2024, 1, 1, 11, 0, 0)
    db_connection.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name, row_id, source_saved_id) VALUES (1, 'u1', ?, 'signup', 1, NULL)", [t0])
    db_connection.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name, row_id, source_saved_id) VALUES (1, 'u1', ?, 'signup', 2, NULL)", [t1])
    db_connection.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name, row_id, source_saved_id) VALUES (1, 'u1', ?, 'open', 3, NULL)", [t1])
    
    steps = [
        PathStep(step_order=0, groups=[PathStepGroup(event_name='signup', filters=[])]),
        PathStep(step_order=1, groups=[PathStepGroup(event_name='open', filters=[])])
    ]
    
    res = run_paths(db_connection, steps)
    
    # Step 2 should match u1.
    step2_users = res['results'][0].steps[1].users
    assert step2_users == 1
