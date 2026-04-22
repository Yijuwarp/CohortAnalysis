"""
Short summary: manages cohort activity status and snapshots.
"""
import duckdb
from typing import Optional
from app.domains.cohorts.membership_builder import build_cohort_membership

def create_all_users_cohort(connection: duckdb.DuckDBPyConnection) -> None:
    from app.domains.cohorts.cohort_service import ensure_cohort_tables
    ensure_cohort_tables(connection)

    existing = connection.execute("SELECT cohort_id FROM cohorts WHERE name = 'All Users'").fetchone()
    
    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
    ).fetchone()[0]
    source_table = "events_scoped" if scoped_exists else "events_normalized"

    cohort_id = existing[0] if existing else connection.execute(
        """
        INSERT INTO cohorts (cohort_id, name, logic_operator, join_type, is_active)
        VALUES (nextval('cohorts_id_sequence'), 'All Users', 'OR', 'first_event', TRUE)
        RETURNING cohort_id
        """
    ).fetchone()[0]

    # Always ensure membership is built
    build_cohort_membership(connection, int(cohort_id), source_table)


def refresh_cohort_activity(connection: duckdb.DuckDBPyConnection, cohort_id: Optional[int] = None) -> None:
    """
    Rebuilds the cohort_event_link index mapping and updates active status.
    This links cohorts to relevant events in events_base by filtering for post-join activity.
    """
    from app.domains.cohorts.cohort_service import ensure_cohort_tables
    from app.utils.perf import time_block
    ensure_cohort_tables(connection)
    
    end_timer = time_block("activity_refresh")
    
    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped_raw' AND table_schema = 'main'"
    ).fetchone()[0]
    
    if not scoped_exists:
        end_timer()
        return

    # 2. Rebuild index mapping
    where_clause = "WHERE cm.cohort_id = ?" if cohort_id is not None else ""
    params = [cohort_id] if cohort_id is not None else []
    
    # Optimization: Use CREATE OR REPLACE TABLE for bulk sequential writes
    end_link_timer = time_block("activity_link_rebuild")
    
    if cohort_id is not None:
        # Partial update: We must keep existing links for other cohorts
        connection.execute(f"""
            INSERT INTO cohort_event_link (cohort_id, row_id)
            SELECT cm.cohort_id, e.row_id
            FROM events_scoped_raw e
            JOIN cohort_membership cm ON e.user_id = cm.user_id
            {where_clause}
            AND e.event_time >= cm.join_time
            ON CONFLICT (cohort_id, row_id) DO NOTHING
        """, params)
    else:
        # Full refresh: Use CTAS for maximum throughput
        connection.execute("""
            CREATE OR REPLACE TABLE cohort_event_link AS
            SELECT cm.cohort_id, e.row_id
            FROM events_scoped_raw e
            JOIN cohort_membership cm ON e.user_id = cm.user_id
            WHERE e.event_time >= cm.join_time
        """)
    
    from app.domains.cohorts.cohort_service import recreate_link_indexes
    recreate_link_indexes(connection)
    end_link_timer()

    # 3. Update is_active status
    # A cohort is active if it has members who have events in the scoped dataset (linked events)
    end_active_timer = time_block("activity_status_update")
    if cohort_id is not None:
        has_activity = connection.execute(
            "SELECT 1 FROM cohort_event_link WHERE cohort_id = ? LIMIT 1",
            [cohort_id]
        ).fetchone()
        connection.execute(
            "UPDATE cohorts SET is_active = ? WHERE cohort_id = ?",
            [bool(has_activity), cohort_id]
        )
    else:
        connection.execute("""
            UPDATE cohorts
            SET is_active = (sub.has_events > 0)
            FROM (
                SELECT c.cohort_id, COUNT(cel.row_id) as has_events
                FROM cohorts c
                LEFT JOIN cohort_event_link cel ON c.cohort_id = cel.cohort_id
                GROUP BY c.cohort_id
            ) sub
            WHERE cohorts.cohort_id = sub.cohort_id
        """)
    end_active_timer()
    
    end_timer(cohort_id=cohort_id)
