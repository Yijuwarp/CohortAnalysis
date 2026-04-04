import pytest
import duckdb
from datetime import datetime, timedelta
from app.domains.analytics.usage_service import get_usage
from app.domains.analytics.retention_service import get_retention, build_active_cohort_base

def setup_test_data(conn: duckdb.DuckDBPyConnection):
    # Setup tables
    conn.execute("CREATE TABLE cohorts (cohort_id INTEGER, name VARCHAR, is_active BOOLEAN, hidden BOOLEAN, split_type VARCHAR, split_value VARCHAR, split_property VARCHAR, split_parent_cohort_id INTEGER)")
    conn.execute("CREATE TABLE cohort_membership (cohort_id INTEGER, user_id VARCHAR, join_time TIMESTAMP)")
    conn.execute("CREATE TABLE events_scoped (user_id VARCHAR, event_name VARCHAR, event_time TIMESTAMP, event_count INTEGER, revenue DOUBLE)")
    
    # Cohort 1: All Users
    conn.execute("INSERT INTO cohorts VALUES (1, 'All Users', true, false, null, null, null, null)")
    
    # User A: Joins at 10:00 AM. Has an event at 1:00 AM next day (15h later).
    join_time_a = datetime(2024, 1, 1, 10, 0, 0)
    event_time_a = datetime(2024, 1, 2, 1, 0, 0)
    conn.execute("INSERT INTO cohort_membership VALUES (1, 'user_a', ?)", [join_time_a])
    conn.execute("INSERT INTO events_scoped VALUES ('user_a', 'click', ?, 1, 0)", [event_time_a])
    
    # User B: Joins at 10:00 AM. Has NO events.
    conn.execute("INSERT INTO cohort_membership VALUES (1, 'user_b', ?)", [join_time_a])

def test_cohort_size_includes_zero_event_users(db_connection):
    setup_test_data(db_connection)
    
    # build_active_cohort_base is used by Usage and Retention tabs for cohort sizes
    cohorts, cohort_sizes = build_active_cohort_base(db_connection)
    
    # EXPECTED: Cohort size should be 2 (user_a and user_b)
    # CURRENT (FAILING): It will be 1 because user_b has no events in events_scoped
    assert cohort_sizes[1] == 2, f"Expected cohort size 2, got {cohort_sizes[1]}"

def test_usage_windowing_is_24h(db_connection):
    setup_test_data(db_connection)
    
    # get_usage(connection, event, max_day, retention_event)
    # We want to see if 'click' at 1:00 AM next day (15h later) is Day 0 (24h logic) or Day 1 (Calendar logic)
    res = get_usage(db_connection, 'click', 1, 'any')
    
    # usage_volume_table structure: [{'cohort_id': 1, ..., 'values': {'0': count_d0, '1': count_d1}}]
    usage_volume = res['usage_volume_table'][0]['values']
    
    # EXPECTED (24h): Day 0 should have 1 event, Day 1 should have 0.
    # CURRENT (FAILING): Day 1 will have 1 event, Day 0 will have 0.
    assert int(usage_volume['0']) == 1, f"Expected 1 event on Day 0 (24h window), got {usage_volume['0']}"
    assert int(usage_volume['1']) == 0, f"Expected 0 events on Day 1 (24h window), got {usage_volume['1']}"

def test_retention_windowing_is_24h(db_connection):
    setup_test_data(db_connection)
    
    # get_retention(connection, max_day, retention_event)
    res = get_retention(db_connection, 1, 'any')
    
    # retention_table structure: [{'cohort_id': 1, ..., 'retention': {'0': pct_d0, '1': pct_d1}}]
    retention = res['retention_table'][0]['retention']
    
    # In 24h logic, the event at 15h is Day 0 activity.
    # So D0 retention should be 100% (or 50% if we include user_b) 
    # and D1 retention should be 0.
    
    # Actually, Day 0 retention is always 100% of active users by definition usually, 
    # but let's check D1.
    # If it's Day 1 in calendar logic, it would show activity on D1.
    assert retention['1'] == 0 or retention['1'] is None, f"Expected 0% retention on Day 1 (24h window), got {retention['1']}%"
