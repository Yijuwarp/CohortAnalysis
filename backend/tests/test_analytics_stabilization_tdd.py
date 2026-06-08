import pytest
import duckdb
from datetime import datetime, timedelta
from app.domains.cohorts.cohort_service import ensure_cohort_tables
from app.domains.cohorts.activity_service import refresh_cohort_activity
from app.domains.scope.filter_service import initialize_scoped_dataset, apply_filters
from app.domains.cohorts.membership_builder import build_cohort_membership
from app.models.cohort_models import CreateCohortRequest, CohortCondition, CohortPropertyFilter

@pytest.fixture
def conn():
    c = duckdb.connect(":memory:")
    ensure_cohort_tables(c)
    # Setup minimal events tables
    c.execute("CREATE TABLE events (user_id TEXT, event_name TEXT, event_time TIMESTAMP, row_id BIGINT)")
    c.execute("CREATE TABLE events_raw AS SELECT * FROM events")
    # Use a view for events_normalized so inserts to 'events' are reflected
    c.execute("CREATE VIEW events_normalized AS SELECT user_id, event_name, event_time, 1.0 as event_count, 0.0 as original_revenue, 0.0 as modified_revenue FROM events")
    yield c
    c.close()

def test_snapshot_integrity_corruption(conn):
    """
    Verifies that the new Snapshot Rebuild logic correctly pairs events.
    """
    conn.execute("INSERT INTO events VALUES ('user1', 'login', '2024-01-01 10:00:00', 100)")
    conn.execute("INSERT INTO events VALUES ('user1', 'purchase', '2024-01-01 11:00:00', 200)")
    
    initialize_scoped_dataset(conn)
    
    conn.execute("INSERT INTO cohorts (cohort_id, name) VALUES (1, 'Test Cohort')")
    conn.execute("INSERT INTO cohort_membership VALUES ('user1', 1, '2024-01-01 00:00:00')")
    
    refresh_cohort_activity(conn)
    
    rows = conn.execute("SELECT event_name, event_time FROM cohort_activity_snapshot ORDER BY event_time").fetchall()
    assert len(rows) == 2
    assert rows[0] == ('login', datetime(2024, 1, 1, 10, 0))
    assert rows[1] == ('purchase', datetime(2024, 1, 1, 11, 0))

def test_transactional_rebuild_rollback(conn):
    """
    Verifies that if Snapshot rebuild fails, the entire rebuild flow rolls back.
    """
    from app.domains.scope.filter_service import apply_filters
    
    conn.execute("INSERT INTO events VALUES ('user1', 'login', '2024-01-01 10:00:00', 100)")
    initialize_scoped_dataset(conn)
    
    conn.execute("INSERT INTO cohorts (cohort_id, name) VALUES (1, 'Test Cohort')")
    conn.execute("INSERT INTO cohort_membership VALUES ('user1', 1, '2024-01-01 00:00:00')")
    refresh_cohort_activity(conn)
    
    assert conn.execute("SELECT COUNT(*) FROM cohort_activity_snapshot").fetchone()[0] == 1
    
    import app.domains.cohorts.activity_service as activity_service
    original_refresh = activity_service.refresh_cohort_activity
    
    def failing_refresh(conn, cohort_id=None):
        conn.execute("DELETE FROM cohort_membership")
        raise RuntimeError("Snapshot rebuild failed!")
    
    activity_service.refresh_cohort_activity = failing_refresh
    
    try:
        from app.models.filter_models import ApplyFiltersRequest
        apply_filters(conn, ApplyFiltersRequest(filters=[], date_range=None))
    except (RuntimeError, Exception):
        pass
    finally:
        activity_service.refresh_cohort_activity = original_refresh
    
    membership_count = conn.execute("SELECT COUNT(*) FROM cohort_membership").fetchone()[0]
    assert membership_count == 1, "Transaction did not roll back! Membership was cleared."

def test_retention_using_snapshot(conn):
    """
    Verifies that retention logic correctly uses the snapshot instead of events_scoped.
    """
    from app.domains.analytics.metric_builders.retention_vectors import build_retention_vector_sql
    
    conn.execute("INSERT INTO events VALUES ('user1', 'login', '2024-01-01 10:00:00', 100)")
    initialize_scoped_dataset(conn)
    
    conn.execute("INSERT INTO cohorts (cohort_id, name, join_type) VALUES (1, 'Test Cohort', 'condition_met')")
    conn.execute("INSERT INTO cohort_membership VALUES ('user1', 1, '2024-01-01 00:00:00')")
    
    # Clear the snapshot table
    try:
        conn.execute("DELETE FROM cohort_activity_snapshot")
    except duckdb.BinderException:
        conn.execute("DELETE FROM cohort_event_link")
    
    sql, params = build_retention_vector_sql(cohort_id=1, max_day=7, join_type='condition_met')
    
    results = conn.execute(sql, params).fetchall()
    active_count = sum(r[3] for r in results) # index 3 is 'value'
    
    assert active_count == 0, f"Retention is bypassing the snapshot! Found {active_count} active entries."
