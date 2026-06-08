import pytest
import duckdb
from datetime import datetime, timedelta
from app.domains.analytics.retention_service import get_retention
from app.domains.cohorts.cohort_service import ensure_cohort_tables

def setup_boundary_data(conn: duckdb.DuckDBPyConnection):
    # Setup tables using canonical schema
    ensure_cohort_tables(conn)
    conn.execute("CREATE TABLE events_scoped (user_id VARCHAR, event_name VARCHAR, event_time TIMESTAMP, event_count INTEGER, revenue DOUBLE)")
    
    # Cohort 1: All Users
    conn.execute("INSERT INTO cohorts (cohort_id, name, is_active, hidden, join_type) VALUES (1, 'All Users', true, false, 'condition_met')")
    
    # User A: Joins at 00:00:00. Event at 23:59:59 (Day 0)
    conn.execute("INSERT INTO cohort_membership VALUES ('user_a', 1, '2024-01-01 00:00:00')")
    conn.execute("INSERT INTO events_scoped VALUES ('user_a', 'session', '2024-01-01 23:59:59', 1, 0)")
    
    # User B: Joins at 00:00:00. Event at 24:00:01 (Day 1)
    conn.execute("INSERT INTO cohort_membership VALUES ('user_b', 1, '2024-01-01 00:00:00')")
    conn.execute("INSERT INTO events_scoped VALUES ('user_b', 'session', '2024-01-02 00:00:01', 1, 0)")

def test_retention_boundary_24h(db_connection):
    setup_boundary_data(db_connection)
    
    # max_day=1 (D0, D1)
    res = get_retention(db_connection, 1, 'any')
    retention = res['retention_table'][0]['retention']
    
    # User A is D0, User B is D1
    # Total eligible = 2
    # D0 percent = 1/2 * 100 = 50.0
    # D1 percent = 1/2 * 100 = 50.0
    assert retention['0'] == 50.0
    assert retention['1'] == 50.0

def test_retention_boundary_push(db_connection):
    # Setup: Event exactly at 24h:00m:00s
    setup_boundary_data(db_connection)
    db_connection.execute("INSERT INTO cohort_membership VALUES ('user_c', 1, '2024-01-01 10:00:00')")
    db_connection.execute("INSERT INTO events_scoped VALUES ('user_c', 'session', '2024-01-02 10:00:00', 1, 0)")
    
    res = get_retention(db_connection, 1, 'any')
    retention = res['retention_table'][0]['retention']
    
    # The event at exactly +24h should be Day 1
    # (Bucket = floor(86400 / 86400) = 1)
    # Total eligible now = 3
    # user_a(D0), user_b(D1), user_c(D1)
    # D1 count = 2. D1 percent = 2/3 * 100 = 66.6...
    assert retention['1'] > 66.0
