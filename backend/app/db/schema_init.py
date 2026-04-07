import duckdb
from app.domains.scope.scope_metadata import ensure_scope_tables
from app.domains.cohorts.cohort_service import ensure_cohort_tables
from app.domains.paths.paths_service import ensure_path_tables
from app.domains.revenue.revenue_tables import ensure_revenue_event_selection_table

def ensure_base_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Initializes metadata and configuration tables for a new user database.
    Optimized to skip redundant checks if the core schema is already present.
    """
    # Quick guard: check if critical tables already exist
    critical_tables = [
        "dataset_metadata", 
        "cohorts", 
        "saved_cohorts", 
        "cohort_membership", 
        "cohort_conditions",
        "cohort_activity_snapshot"
    ]
    existing = conn.execute(
        f"SELECT table_name FROM information_schema.tables WHERE table_name IN ({','.join(['?']*len(critical_tables))})",
        critical_tables
    ).fetchall()
    
    if len(existing) == len(critical_tables):
        return

    # If missing any, run the full initialization suite
    ensure_scope_tables(conn)
    ensure_cohort_tables(conn)
    ensure_path_tables(conn)
    ensure_revenue_event_selection_table(conn)
    
    # Ensure dataset_metadata exists as a base anchor
    conn.execute("CREATE TABLE IF NOT EXISTS dataset_metadata (id INTEGER)")
