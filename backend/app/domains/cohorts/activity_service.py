"""
Short summary: manages cohort activity status and snapshots.
"""
import duckdb
from app.domains.cohorts.membership_builder import build_cohort_membership

def create_all_users_cohort(connection: duckdb.DuckDBPyConnection) -> None:
    from app.domains.cohorts.cohort_service import ensure_cohort_tables
    ensure_cohort_tables(connection)

    existing = connection.execute("SELECT cohort_id FROM cohorts WHERE name = 'All Users'").fetchone()
    if existing:
        return

    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
    ).fetchone()[0]
    source_table = "events_scoped" if scoped_exists else "events_normalized"

    cohort_id = connection.execute(
        """
        INSERT INTO cohorts (cohort_id, name, logic_operator, join_type, is_active)
        VALUES (nextval('cohorts_id_sequence'), 'All Users', 'OR', 'first_event', TRUE)
        RETURNING cohort_id
        """
    ).fetchone()[0]

    build_cohort_membership(connection, int(cohort_id), source_table)


def refresh_cohort_activity(connection: duckdb.DuckDBPyConnection) -> None:
    from app.domains.cohorts.cohort_service import ensure_cohort_tables
    ensure_cohort_tables(connection)
    scoped_raw_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped_raw' AND table_schema = 'main'"
    ).fetchone()[0]
    source_table = "events_scoped_raw" if scoped_raw_exists else "events_raw"


    connection.execute("DELETE FROM cohort_activity_snapshot")
    connection.execute(
        f"""
        INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name, row_id)
        SELECT cm.cohort_id, e.user_id, e.event_time, e.event_name, e.row_id
        FROM cohort_membership cm
        JOIN {source_table} e
          ON cm.user_id = e.user_id
        """
    )


    activity_rows = connection.execute(
        """
        SELECT
            c.cohort_id,
            COUNT(DISTINCT cas.user_id) AS active_members
        FROM cohorts c
        LEFT JOIN cohort_activity_snapshot cas ON c.cohort_id = cas.cohort_id
        GROUP BY c.cohort_id
        """
    ).fetchall()

    if not activity_rows:
        return

    connection.executemany(
        "UPDATE cohorts SET is_active = ? WHERE cohort_id = ?",
        [(bool(active_members > 0), int(cohort_id)) for cohort_id, active_members in activity_rows],
    )
