import pytest
import duckdb
from app.domains.analytics.metric_builders.retention_vectors import build_retention_vector_sql
from app.domains.analytics.metric_builders.usage_vectors import build_usage_vector_sql
from app.domains.analytics.metric_builders.revenue_vectors import build_revenue_vector_sql

def setup_test_data(conn: duckdb.DuckDBPyConnection):
    conn.execute("CREATE OR REPLACE TABLE cohorts (cohort_id INTEGER, name TEXT, logic_operator TEXT, join_type TEXT, is_active BOOLEAN, hidden BOOLEAN)")
    conn.execute("CREATE OR REPLACE TABLE cohort_membership (cohort_id INTEGER, user_id TEXT, join_time TIMESTAMP)")
    conn.execute("CREATE OR REPLACE TABLE cohort_activity_snapshot (cohort_id INTEGER, user_id TEXT, event_time TIMESTAMP, event_name TEXT)")
    conn.execute("CREATE OR REPLACE TABLE events_scoped (user_id TEXT, event_time TIMESTAMP, event_name TEXT, modified_revenue DOUBLE, event_count INTEGER)")
    
    # Cohort 1: 2 users
    conn.execute("INSERT INTO cohorts VALUES (1, 'Test Cohort', 'OR', 'first_event', TRUE, FALSE)")
    conn.execute("INSERT INTO cohort_membership VALUES (1, 'u1', '2024-01-01 10:00:00')")
    conn.execute("INSERT INTO cohort_membership VALUES (1, 'u2', '2024-01-01 10:00:00')")
    
    # u1: active Day 0 and Day 2
    conn.execute("INSERT INTO cohort_activity_snapshot VALUES (1, 'u1', '2024-01-01 10:05:00', 'active')")
    conn.execute("INSERT INTO cohort_activity_snapshot VALUES (1, 'u1', '2024-01-03 10:05:00', 'active')")
    
    # u2: active Day 1
    conn.execute("INSERT INTO cohort_activity_snapshot VALUES (1, 'u2', '2024-01-02 10:05:00', 'active')")

def test_retention_vector_completeness(db_connection):
    conn = db_connection
    setup_test_data(conn)
    
    max_day = 2
    cohort_id = 1
    cohort_size = 2
    
    sql, params = build_retention_vector_sql(cohort_id, max_day)
    rows = conn.execute(sql, params).fetchall()
    
    # Contract 1: Length must be cohort_size * (max_day + 1)
    # 2 users * (2 + 1) days = 6 rows
    assert len(rows) == cohort_size * (max_day + 1), f"Expected {cohort_size * (max_day + 1)} rows, got {len(rows)}"
    
    # Contract 2: Uniqueness of (user_id, day_offset)
    # columns: cohort_id, user_id, day_offset, value, is_eligible
    user_day_pairs = [(r[1], r[2]) for r in rows] 
    assert len(set(user_day_pairs)) == len(rows), "Duplicate (user_id, day_offset) pairs found"
    
    # Contract 3: cohort_id must be present (Fix 7)
    # We expect columns: cohort_id, user_id, day_offset, value, is_eligible
    # Current implementation returns: user_id, day_offset, value, is_eligible
    column_names = [d[0] for d in conn.execute(sql, params).description]
    assert "cohort_id" in column_names, "Missing cohort_id in output columns"
    
    # Contract 4: Zero-filling
    # u1 should have 3 rows (D0, D1, D2)
    u1_rows = [r for r in rows if r[column_names.index("user_id")] == 'u1']
    assert len(u1_rows) == max_day + 1
    
    # D1 for u1 should be 0 (value at index 3 or 4)
    val_idx = column_names.index("value")
    u1_d1 = [r for r in u1_rows if r[column_names.index("day_offset")] == 1][0]
    assert u1_d1[val_idx] == 0

def test_retention_property_filter_safe_join(db_connection):
    conn = db_connection
    setup_test_data(conn)
    
    # Insert scoped event with property
    conn.execute("INSERT INTO events_scoped VALUES ('u1', '2024-01-01 10:05:00', 'active', 0.0, 1)")
    
    # Filter for property (this is a simplified mock of property filter logic)
    # In real logic, it would be "AND platform = 'iOS'"
    property_clause = "AND es.event_name = 'active'"
    property_params = []
    
    # We need to update build_retention_vector_sql to support property filters
    try:
        sql, params = build_retention_vector_sql(
            cohort_id=1, 
            max_day=1, 
            property_clause=property_clause, 
            property_params=property_params
        )
    except TypeError:
        pytest.fail("build_retention_vector_sql does not support property_clause yet")

    # Verify DuckDB syntax for generate_series (Fix 2)
    # If using UNNEST(GENERATE_SERIES(0, max_day)), it might fail in some DuckDB versions
    # We want to ensure it works with the suggested syntax.
    conn.execute(sql, params).fetchall()

def test_usage_vector_completeness(db_connection):
    conn = db_connection
    setup_test_data(conn)
    
    # Mock events_scoped for usage
    conn.execute("INSERT INTO events_scoped VALUES ('u1', '2024-01-01 10:05:00', 'click', 0.0, 1)")
    
    max_day = 1
    cohort_id = 1
    cohort_size = 2
    
    sql, params = build_usage_vector_sql(cohort_id, max_day, event_name='click')
    rows = conn.execute(sql, params).fetchall()
    column_names = [d[0] for d in conn.execute(sql, params).description]
    
    assert len(rows) == cohort_size * (max_day + 1)
    assert "cohort_id" in column_names

def test_revenue_vector_completeness(db_connection):
    conn = db_connection
    setup_test_data(conn)
    
    # Mock events_scoped for revenue
    conn.execute("INSERT INTO events_scoped VALUES ('u1', '2024-01-01 10:05:00', 'purchase', 10.0, 1)")
    
    max_day = 1
    cohort_id = 1
    cohort_size = 2
    
    sql, params = build_revenue_vector_sql(cohort_id, max_day)
    rows = conn.execute(sql, params).fetchall()
    column_names = [d[0] for d in conn.execute(sql, params).description]
    
    assert len(rows) == cohort_size * (max_day + 1)
    assert "cohort_id" in column_names
