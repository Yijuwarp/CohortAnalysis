import pytest
import duckdb
from datetime import datetime, timedelta
from app.domains.analytics.usage_service import get_usage
from app.domains.analytics.retention_service import get_retention
from app.domains.cohorts.cohort_service import ensure_cohort_tables

def setup_alignment_data(conn: duckdb.DuckDBPyConnection):
    # Setup tables using canonical schema
    ensure_cohort_tables(conn)
    conn.execute("CREATE TABLE events_scoped (user_id VARCHAR, event_name VARCHAR, event_time TIMESTAMP, event_count INTEGER, revenue DOUBLE)")
    
    # Cohort 1: All Users
    conn.execute("INSERT INTO cohorts (cohort_id, name, is_active, hidden, join_type) VALUES (1, 'All Users', true, false, 'condition_met')")
    
    # User A: Day 0 active
    conn.execute("INSERT INTO cohort_membership VALUES ('user_a', 1, '2024-01-01 10:00:00')")
    conn.execute("INSERT INTO events_scoped VALUES ('user_a', 'click', '2024-01-01 11:00:00', 1, 0)")
    
    # User B: Day 1 active
    conn.execute("INSERT INTO cohort_membership VALUES ('user_b', 1, '2024-01-01 10:00:00')")
    conn.execute("INSERT INTO events_scoped VALUES ('user_b', 'click', '2024-01-02 11:00:00', 1, 0)")

def test_inclusive_denominator_sync(db_connection):
    setup_alignment_data(db_connection)
    
    # Retention and Usage should have the same eligible user counts per day
    ret = get_retention(db_connection, 1, 'any')
    use = get_usage(db_connection, 'click', 1, 'any')
    
    # Check D0 eligible
    ret_d0_eligible = ret['retention_table'][0]['availability']['0']['eligible_users']
    use_d0_eligible = use['usage_volume_table'][0]['availability']['0']['eligible_users']
    
    assert ret_d0_eligible == use_d0_eligible, f"Denominator mismatch D0: ret={ret_d0_eligible}, use={use_d0_eligible}"
    assert ret_d0_eligible == 2
