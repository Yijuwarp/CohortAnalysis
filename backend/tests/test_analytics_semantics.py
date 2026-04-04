import pytest
import duckdb
from datetime import datetime
from app.domains.analytics.usage_service import get_usage
from app.domains.analytics.retention_service import get_retention

def setup_boundary_data(conn: duckdb.DuckDBPyConnection):
    # Setup tables
    conn.execute("CREATE TABLE cohorts (cohort_id INTEGER, name VARCHAR, is_active BOOLEAN, hidden BOOLEAN, split_type VARCHAR, split_value VARCHAR, split_property VARCHAR, split_parent_cohort_id INTEGER)")
    conn.execute("CREATE TABLE cohort_membership (cohort_id INTEGER, user_id VARCHAR, join_time TIMESTAMP)")
    conn.execute("CREATE TABLE events_scoped (user_id VARCHAR, event_name VARCHAR, event_time TIMESTAMP, event_count INTEGER, revenue DOUBLE)")
    
    # Cohort 1: All Users
    conn.execute("INSERT INTO cohorts VALUES (1, 'All Users', true, false, null, null, null, null)")
    
    # User Rules:
    # 1. Join at T. Event at T + 23h59m59s -> Bucket 0
    # 2. Join at T. Event at T + 24h -> Bucket 1
    # 3. Join at T. Event at T -> Bucket 0
    # 4. Join at T. Event at T - 1s -> EXCLUDED
    
    T = datetime(2024, 1, 1, 10, 0, 0)
    
    # User 1: 0s after join -> Day 0
    conn.execute("INSERT INTO cohort_membership VALUES (1, 'u1_join', ?)", [T])
    conn.execute("INSERT INTO events_scoped VALUES ('u1_join', 'click', ?, 1, 0)", [T])
    
    # User 2: 23h 59m 59s after join -> Day 0
    T2 = datetime(2024, 1, 1, 10, 0, 0)
    E2 = datetime(2024, 1, 2, 9, 59, 59)
    conn.execute("INSERT INTO cohort_membership VALUES (1, 'u2_boundary_0', ?)", [T2])
    conn.execute("INSERT INTO events_scoped VALUES ('u2_boundary_0', 'click', ?, 1, 0)", [E2])
    
    # User 3: 24h after join -> Day 1
    T3 = datetime(2024, 1, 1, 10, 0, 0)
    E3 = datetime(2024, 1, 2, 10, 0, 0)
    conn.execute("INSERT INTO cohort_membership VALUES (1, 'u3_boundary_1', ?)", [T3])
    conn.execute("INSERT INTO events_scoped VALUES ('u3_boundary_1', 'click', ?, 1, 0)", [E3])
    
    # User 4: 1s before join -> EXCLUDED
    T4 = datetime(2024, 1, 1, 10, 0, 0)
    E4 = datetime(2024, 1, 1, 9, 59, 59)
    conn.execute("INSERT INTO cohort_membership VALUES (1, 'u4_negative', ?)", [T4])
    conn.execute("INSERT INTO events_scoped VALUES ('u4_negative', 'click', ?, 1, 0)", [E4])

def test_window_boundary_exactness_usage(db_connection):
    setup_boundary_data(db_connection)
    
    # Usage window logic
    res = get_usage(db_connection, 'click', 1, 'any')
    usage_volume = res['usage_volume_table'][0]['values']
    
    # Rule 1 & 2: u1 and u2 are Day 0 (2 events)
    assert int(usage_volume.get('0', 0)) == 2, f"Expected 2 events on Day 0, got {usage_volume.get('0')}"
    # Rule 3: u3 is Day 1 (1 event)
    assert int(usage_volume.get('1', 0)) == 1, f"Expected 1 event on Day 1, got {usage_volume.get('1')}"
    # Rule 4: u4 is excluded (Total 3 events in table)
    total = sum(int(v) for v in usage_volume.values())
    assert total == 3, f"Expected 3 total events (excluding negative time), got {total}"

def test_calendar_regression_guard(db_connection):
    # Join at 23:55 PM, Event at 00:05 AM (next day)
    # Calendar -> Day 1
    # 24h -> Day 0
    T = datetime(2026, 1, 1, 23, 55, 0)
    E = datetime(2026, 1, 2, 0, 5, 0)
    
    db_connection.execute("CREATE TABLE cohorts (cohort_id INTEGER, name VARCHAR, is_active BOOLEAN, hidden BOOLEAN, split_type VARCHAR, split_value VARCHAR, split_property VARCHAR, split_parent_cohort_id INTEGER)")
    db_connection.execute("CREATE TABLE cohort_membership (cohort_id INTEGER, user_id VARCHAR, join_time TIMESTAMP)")
    db_connection.execute("CREATE TABLE events_scoped (user_id VARCHAR, event_name VARCHAR, event_time TIMESTAMP, event_count INTEGER, revenue DOUBLE)")
    
    db_connection.execute("INSERT INTO cohorts VALUES (1, 'All Users', true, false, null, null, null, null)")
    db_connection.execute("INSERT INTO cohort_membership VALUES (1, 'reg_user', ?)", [T])
    db_connection.execute("INSERT INTO events_scoped VALUES ('reg_user', 'click', ?, 1, 0)", [E])
    
    res = get_usage(db_connection, 'click', 1, 'any')
    usage_volume = res['usage_volume_table'][0]['values']
    
    # Assert Day 0 (NOT Day 1)
    assert int(usage_volume.get('0', 0)) == 1, "Regression: Event after midnight but within 24h must be Day 0"
    assert int(usage_volume.get('1', 0)) == 0, "Regression: Event after midnight but within 24h should NOT be Day 1"

def test_hourly_relative_boundary(db_connection):
    # Join at 10:00:00. 
    # Event at 10:59:59 -> Hour 0
    # Event at 11:00:00 -> Hour 1
    T = datetime(2026, 1, 1, 10, 0, 0)
    E0 = datetime(2026, 1, 1, 10, 59, 59)
    E1 = datetime(2026, 1, 1, 11, 0, 0)
    
    db_connection.execute("CREATE TABLE cohorts (cohort_id INTEGER, name VARCHAR, is_active BOOLEAN, hidden BOOLEAN, split_type VARCHAR, split_value VARCHAR, split_property VARCHAR, split_parent_cohort_id INTEGER)")
    db_connection.execute("CREATE TABLE cohort_membership (cohort_id INTEGER, user_id VARCHAR, join_time TIMESTAMP)")
    db_connection.execute("CREATE TABLE cohort_activity_snapshot (cohort_id INTEGER, user_id VARCHAR, event_name VARCHAR, event_time TIMESTAMP)")
    db_connection.execute("CREATE TABLE events_scoped (user_id VARCHAR, event_name VARCHAR, event_time TIMESTAMP, event_count INTEGER, revenue DOUBLE)")
    
    db_connection.execute("INSERT INTO cohorts VALUES (1, 'All Users', true, false, null, null, null, null)")
    db_connection.execute("INSERT INTO cohort_membership VALUES (1, 'h_user', ?)", [T])
    # Activity snapshot is used by retention. Columns: cohort_id, user_id, event_time, event_name, source_saved_id
    db_connection.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name) VALUES (1, 'h_user', ?, 'any')", [E0])
    db_connection.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name) VALUES (1, 'h_user', ?, 'any')", [E1])
    # events_scoped is needed for classic retention with specific event
    db_connection.execute("INSERT INTO events_scoped VALUES ('h_user', 'any', ?, 1, 0)", [E0])
    db_connection.execute("INSERT INTO events_scoped VALUES ('h_user', 'any', ?, 1, 0)", [E1])
    
    res = get_retention(db_connection, 1, 'any', granularity='hour')
    retention = res['retention_table'][0]['retention']
    
    # Hour 0 activity should be present
    assert retention.get('0') == 100.0
    # Hour 1 activity should be present
    assert retention.get('1') == 100.0
    # Let's verify a +2h gap
    E2 = datetime(2026, 1, 1, 12, 0, 0)
    db_connection.execute("INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name) VALUES (1, 'h_user', ?, 'any')", [E2])
    db_connection.execute("INSERT INTO events_scoped VALUES ('h_user', 'any', ?, 1, 0)", [E2])
    
    res2 = get_retention(db_connection, 2, 'any', granularity='hour')
    retention2 = res2['retention_table'][0]['retention']
    assert retention2.get('2') == 100.0
