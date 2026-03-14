"""
Short summary: service for managing revenue event selection and overrides.
"""
import duckdb
from fastapi import HTTPException
from app.models.revenue_models import UpdateRevenueConfigRequest
from app.domains.revenue.revenue_tables import ensure_revenue_event_selection_table
from app.domains.revenue.revenue_recompute import recompute_modified_revenue_columns

def initialize_revenue_event_selection(connection: duckdb.DuckDBPyConnection) -> None:
    ensure_revenue_event_selection_table(connection)

    connection.execute(
        """
        INSERT INTO revenue_event_selection (event_name, is_included)
        SELECT DISTINCT e.event_name, TRUE
        FROM events_normalized e
        WHERE e.original_revenue != 0
          AND NOT EXISTS (
            SELECT 1
            FROM revenue_event_selection r
            WHERE r.event_name = e.event_name
          )
        """
    )


def get_revenue_config_events(connection: duckdb.DuckDBPyConnection) -> dict[str, bool | list[dict[str, object]] | list[str]]:
    ensure_revenue_event_selection_table(connection)
    
    normalized_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized'"
    ).fetchone()[0] > 0
    if not normalized_exists:
        return {"has_revenue_mapping": False, "events": [], "addable_events": []}

    rows = connection.execute(
        "SELECT event_name, is_included, override_revenue FROM revenue_event_selection ORDER BY event_name"
    ).fetchall()

    addable_rows = connection.execute(
        """
        SELECT DISTINCT event_name
        FROM events_normalized
        WHERE event_name IS NOT NULL
          AND event_name NOT IN (
            SELECT event_name FROM revenue_event_selection
          )
        ORDER BY event_name
        """
    ).fetchall()

    return {
        "has_revenue_mapping": True,
        "events": [
            {"event_name": str(name), "included": bool(inc), "override": float(ov) if ov is not None else None}
            for name, inc, ov in rows
        ],
        "addable_events": [str(row[0]) for row in addable_rows],
    }


def get_revenue_events(connection: duckdb.DuckDBPyConnection) -> dict[str, bool | list[dict[str, object]]]:
    ensure_revenue_event_selection_table(connection)
    
    rows = connection.execute(
        "SELECT event_name, is_included, override_revenue FROM revenue_event_selection ORDER BY event_name"
    ).fetchall()
    
    return {
        "has_revenue_mapping": True,
        "events": [
            {"event_name": str(name), "is_included": bool(inc), "override": float(ov) if ov is not None else None}
            for name, inc, ov in rows
        ],
    }


def update_revenue_config(connection: duckdb.DuckDBPyConnection, payload: UpdateRevenueConfigRequest) -> dict[str, object]:
    ensure_revenue_event_selection_table(connection)

    if not payload.revenue_config and not payload.events:
        raise HTTPException(status_code=400, detail="revenue_config cannot be empty")

    # Payload supports both dictionary-based and list-based updates
    updates: list[tuple[bool, float | None, str]] = []

    for event_name, config in payload.revenue_config.items():
        updates.append((config.included, config.override, event_name))

    for item in payload.events:
        updates.append((item.include, item.override, item.event_name))

    # Perform UPSERT into revenue_event_selection
    connection.executemany(
        """
        INSERT INTO revenue_event_selection (event_name, is_included, override_revenue)
        VALUES (?, ?, ?)
        ON CONFLICT (event_name) DO UPDATE SET
            is_included = EXCLUDED.is_included,
            override_revenue = EXCLUDED.override_revenue
        """,
        [(u[2], u[0], u[1]) for u in updates],
    )

    recompute_modified_revenue_columns(connection, "events_normalized")

    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
    ).fetchone()[0]
    if scoped_exists:
        recompute_modified_revenue_columns(connection, "events_scoped")

    # Match legacy return format for this endpoint
    rows = connection.execute(
        "SELECT event_name, is_included, override_revenue FROM revenue_event_selection ORDER BY event_name"
    ).fetchall()

    addable_rows = connection.execute(
        """
        SELECT DISTINCT event_name
        FROM events_normalized
        WHERE event_name IS NOT NULL
          AND event_name NOT IN (
            SELECT event_name FROM revenue_event_selection
          )
        ORDER BY event_name
        """
    ).fetchall()

    return {
        "has_revenue_mapping": True,
        "events": [
            {"event_name": str(name), "included": bool(inc), "override": float(ov) if ov is not None else None}
            for name, inc, ov in rows
        ],
        "addable_events": [str(row[0]) for row in addable_rows],
    }
