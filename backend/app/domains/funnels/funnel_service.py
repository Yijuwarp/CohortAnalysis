"""
Short summary: service for funnel creation, listing (with validity), and DuckDB-based computation.
"""
from __future__ import annotations

import duckdb
from datetime import datetime, timezone
from fastapi import HTTPException
from app.domains.cohorts.cohort_service import ensure_cohort_tables


# ---------------------------------------------------------------------------
# Table bootstrap
# ---------------------------------------------------------------------------

def ensure_funnel_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS funnels (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TIMESTAMP
        )
    """)
    conn.execute("CREATE SEQUENCE IF NOT EXISTS funnels_id_seq START 1")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS funnel_steps (
            id INTEGER PRIMARY KEY,
            funnel_id INTEGER NOT NULL,
            step_order INTEGER NOT NULL,
            event_name TEXT NOT NULL
        )
    """)
    conn.execute("CREATE SEQUENCE IF NOT EXISTS funnel_steps_id_seq START 1")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS funnel_step_filters (
            id INTEGER PRIMARY KEY,
            step_id INTEGER NOT NULL,
            property_key TEXT NOT NULL,
            property_value TEXT NOT NULL,
            operator TEXT NOT NULL DEFAULT 'equals'
        )
    """)
    conn.execute("CREATE SEQUENCE IF NOT EXISTS funnel_step_filters_id_seq START 1")


# ---------------------------------------------------------------------------
# Validity helpers  (Issue #7: validate event + property KEY only, not values)
# ---------------------------------------------------------------------------

def _get_dataset_events_and_property_keys(
    conn: duckdb.DuckDBPyConnection,
) -> tuple[set[str], set[str]]:
    """
    Returns:
      event_names: set of distinct event names in events_normalized/scoped
      property_keys: set of non-canonical column names (i.e. property columns)

    Note: we intentionally do NOT sample property values here (Issue #7).
    Sampling 500 rows causes false-negatives for rare values; validity should
    only check that the event and the property column exist in the schema.
    """
    scoped_exists = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'events_scoped' AND table_schema = 'main'"
    ).fetchone()[0]
    source = "events_scoped" if scoped_exists else "events_normalized"

    normalized_exists = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'events_normalized' AND table_schema = 'main'"
    ).fetchone()[0]
    if not normalized_exists:
        return set(), set()

    event_rows = conn.execute(f"SELECT DISTINCT event_name FROM {source}").fetchall()
    event_names = {r[0] for r in event_rows if r[0]}

    # Property keys = all non-canonical columns in events_normalized schema
    col_rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'events_normalized' AND table_schema = 'main'"
    ).fetchall()
    canonical = {"user_id", "event_name", "event_time", "event_count", "revenue"}
    property_keys = {r[0] for r in col_rows if r[0] not in canonical}

    return event_names, property_keys


def _check_funnel_validity(
    funnel_id: int,
    conn: duckdb.DuckDBPyConnection,
    event_names: set[str],
    property_keys: set[str],
) -> bool:
    """
    Lightweight validity check:
    - Every step event must exist in the dataset
    - Every filter property_key must exist as a column (not checking values)
    """
    steps = conn.execute(
        "SELECT id, event_name FROM funnel_steps WHERE funnel_id = ? ORDER BY step_order",
        [funnel_id],
    ).fetchall()

    for step_id, event_name in steps:
        if event_name not in event_names:
            return False
        filters = conn.execute(
            "SELECT property_key FROM funnel_step_filters WHERE step_id = ?",
            [step_id],
        ).fetchall()
        for (pkey,) in filters:
            if pkey not in property_keys:
                return False

    return True


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def create_funnel(
    conn: duckdb.DuckDBPyConnection,
    name: str,
    steps: list[dict],
) -> dict:
    """
    steps: [ { event_name, filters: [ { property_key, property_value } ] } ]
    Minimum 2, maximum 5 steps.
    """
    ensure_funnel_tables(conn)

    name = (name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Funnel name is required")
    if len(steps) < 2:
        raise HTTPException(status_code=400, detail="Funnels require at least 2 steps")
    if len(steps) > 5:
        raise HTTPException(status_code=400, detail="Funnels support at most 5 steps")

    for idx, step in enumerate(steps):
        if not step.get("event_name", "").strip():
            raise HTTPException(
                status_code=400, detail=f"Step {idx + 1} is missing an event name"
            )

    created_at = datetime.now(timezone.utc)
    funnel_id = conn.execute(
        "INSERT INTO funnels (id, name, created_at) VALUES (nextval('funnels_id_seq'), ?, ?) RETURNING id",
        [name, created_at],
    ).fetchone()[0]

    for order, step in enumerate(steps):
        step_id = conn.execute(
            "INSERT INTO funnel_steps (id, funnel_id, step_order, event_name) "
            "VALUES (nextval('funnel_steps_id_seq'), ?, ?, ?) RETURNING id",
            [funnel_id, order, step["event_name"].strip()],
        ).fetchone()[0]
        for f in step.get("filters", []):
            key = (f.get("property_key") or "").strip()
            val = (f.get("property_value") or "").strip()
            if key and val:
                conn.execute(
                    "INSERT INTO funnel_step_filters (id, step_id, property_key, property_value, operator) "
                    "VALUES (nextval('funnel_step_filters_id_seq'), ?, ?, ?, 'equals')",
                    [step_id, key, val],
                )

    return {"id": int(funnel_id), "name": name}


def list_funnels(conn: duckdb.DuckDBPyConnection) -> dict:
    ensure_funnel_tables(conn)

    rows = conn.execute(
        "SELECT id, name, created_at FROM funnels ORDER BY id"
    ).fetchall()
    if not rows:
        return {"funnels": []}

    # Build validity context once (cheap: event names + column schema only)
    try:
        event_names, property_keys = _get_dataset_events_and_property_keys(conn)
    except Exception:
        event_names, property_keys = set(), set()

    funnels = []
    for fid, fname, fcreated_at in rows:
        is_valid = _check_funnel_validity(int(fid), conn, event_names, property_keys)
        funnels.append({
            "id": int(fid),
            "name": str(fname),
            "created_at": str(fcreated_at) if fcreated_at else None,
            "is_valid": is_valid,
        })

    return {"funnels": funnels}


def delete_funnel(conn: duckdb.DuckDBPyConnection, funnel_id: int) -> dict:
    ensure_funnel_tables(conn)
    row = conn.execute("SELECT id FROM funnels WHERE id = ?", [funnel_id]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Funnel not found")
    # Cascade-delete manually (DuckDB doesn't enforce FK cascades)
    step_ids = [r[0] for r in conn.execute(
        "SELECT id FROM funnel_steps WHERE funnel_id = ?", [funnel_id]
    ).fetchall()]
    for sid in step_ids:
        conn.execute("DELETE FROM funnel_step_filters WHERE step_id = ?", [sid])
    conn.execute("DELETE FROM funnel_steps WHERE funnel_id = ?", [funnel_id])
    conn.execute("DELETE FROM funnels WHERE id = ?", [funnel_id])
    return {"deleted": True, "id": int(funnel_id)}


# ---------------------------------------------------------------------------
# Funnel execution (DuckDB SQL, set-based)
# Issue #3/#4: Use >= for timestamp ordering so same-timestamp events are included
# Issue #5: CAST property values to VARCHAR consistently
# ---------------------------------------------------------------------------

def _build_filter_clauses(filters: list[tuple[str, str]]) -> str:
    """Build SQL AND clauses for property filters with safe quoting and CAST."""
    clauses = ""
    for pk, pv in filters:
        safe_pk = pk.replace('"', '""')
        safe_pv = pv.replace("'", "''")
        # CAST ensures type-safe comparison regardless of column type (Issue #5)
        clauses += f' AND CAST("{safe_pk}" AS VARCHAR) = \'{safe_pv}\''
    return clauses


def _build_step_cte_with_cohort(
    step_index: int,
    event_name: str,
    filters: list[tuple[str, str]],
    source: str,
    cohort_id: int,
) -> str:
    """
    First-step CTE: earliest matching event for each user who is a member of cohort_id.
    """
    alias = f"step_{step_index}"
    filter_clauses = _build_filter_clauses(filters)
    safe_event = event_name.replace("'", "''")

    return (
        f"{alias} AS (\n"
        f"  SELECT e.user_id, MIN(e.event_time) AS ts\n"
        f"  FROM {source} e\n"
        f"  JOIN cohort_membership cm\n"
        f"    ON e.user_id = cm.user_id AND cm.cohort_id = {cohort_id}\n"
        f"  WHERE e.event_name = '{safe_event}'"
        f"{filter_clauses}\n"
        f"  GROUP BY e.user_id\n"
        f")"
    )


def _build_step_cte(
    step_index: int,
    event_name: str,
    filters: list[tuple[str, str]],
    source: str,
    prev_alias: str,
) -> str:
    """
    Subsequent-step CTE: earliest matching event occurring >= the previous step's timestamp.
    Using >= (not >) so that events at exactly the same timestamp are not excluded (Issue #3).
    """
    alias = f"step_{step_index}"
    filter_clauses = _build_filter_clauses(filters)
    safe_event = event_name.replace("'", "''")

    return (
        f"{alias} AS (\n"
        f"  SELECT e.user_id, MIN(e.event_time) AS ts\n"
        f"  FROM {source} e\n"
        f"  JOIN {prev_alias} p ON e.user_id = p.user_id\n"
        f"  WHERE e.event_name = '{safe_event}'"
        f"{filter_clauses}\n"
        f"  AND e.event_time >= p.ts\n"  # Issue #3: >= not >
        f"  GROUP BY e.user_id\n"
        f")"
    )


def run_funnel(
    conn: duckdb.DuckDBPyConnection,
    funnel_id: int,
) -> dict:
    """
    Executes the funnel against all active, non-hidden cohorts using set-based DuckDB SQL.
    Returns step-wise user counts, conversion %, and drop-off % per cohort.
    """
    ensure_funnel_tables(conn)
    ensure_cohort_tables(conn)

    # Load funnel definition
    funnel_row = conn.execute(
        "SELECT id, name FROM funnels WHERE id = ?", [funnel_id]
    ).fetchone()
    if not funnel_row:
        raise HTTPException(status_code=404, detail="Funnel not found")

    steps_rows = conn.execute(
        "SELECT id, step_order, event_name FROM funnel_steps WHERE funnel_id = ? ORDER BY step_order",
        [funnel_id],
    ).fetchall()
    if len(steps_rows) < 2:
        raise HTTPException(status_code=400, detail="Funnel has fewer than 2 steps")

    steps: list[dict] = []
    for step_id, step_order, event_name in steps_rows:
        filters_rows = conn.execute(
            "SELECT property_key, property_value FROM funnel_step_filters WHERE step_id = ?",
            [step_id],
        ).fetchall()
        steps.append({
            "order": int(step_order),
            "event_name": str(event_name),
            "filters": [(r[0], r[1]) for r in filters_rows],
        })

    # Determine source table
    scoped_exists = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'events_scoped' AND table_schema = 'main'"
    ).fetchone()[0]
    source = "events_scoped" if scoped_exists else "events_normalized"

    normalized_exists = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'events_normalized' AND table_schema = 'main'"
    ).fetchone()[0]
    if not normalized_exists:
        return {
            "funnel_id": funnel_id,
            "funnel_name": str(funnel_row[1]),
            "steps": [s["event_name"] for s in steps],
            "results": [],
        }

    cohort_rows = conn.execute(
        """
        SELECT cohort_id, name
        FROM cohorts
        WHERE is_active = TRUE AND hidden = FALSE
        ORDER BY cohort_id
        """
    ).fetchall()

    if not cohort_rows:
        return {
            "funnel_id": funnel_id,
            "funnel_name": str(funnel_row[1]),
            "steps": [s["event_name"] for s in steps],
            "results": [],
        }

    results = []

    for cohort_id, cohort_name in cohort_rows:
        cohort_id = int(cohort_id)

        # Build chained CTEs for each funnel step
        ctes = []
        prev_alias: str | None = None

        for i, step in enumerate(steps):
            if i == 0:
                # First step — restrict to cohort members
                ctes.append(_build_step_cte_with_cohort(
                    i, step["event_name"], step["filters"],
                    source, cohort_id,
                ))
            else:
                # Subsequent steps — chain from previous step (Issue #4: strict chaining)
                ctes.append(_build_step_cte(
                    i, step["event_name"], step["filters"],
                    source, f"step_{i - 1}",
                ))
            prev_alias = f"step_{i}"

        # Single query to get all step counts
        select_parts = [
            f"(SELECT COUNT(*) FROM step_{i}) AS cnt_{i}"
            for i in range(len(steps))
        ]
        sql = "WITH " + ",\n".join(ctes) + "\nSELECT " + ", ".join(select_parts)

        row = conn.execute(sql).fetchone()
        step_counts = [int(row[i]) for i in range(len(steps))]

        # Build per-step metrics
        step_metrics = []
        base_users = step_counts[0]  # always relative to step 0 (Issue #9: consistent scaling)
        for i, step in enumerate(steps):
            users = step_counts[i]
            conversion_pct = round(users / base_users * 100, 2) if base_users > 0 else 0.0
            if i > 0:
                prev_users = step_counts[i - 1]
                dropoff_pct = round((prev_users - users) / prev_users * 100, 2) if prev_users > 0 else 0.0
            else:
                dropoff_pct = 0.0

            step_metrics.append({
                "step": i,
                "event_name": step["event_name"],
                "users": users,
                "conversion_pct": conversion_pct,
                "dropoff_pct": dropoff_pct,
            })

        results.append({
            "cohort_id": cohort_id,
            "cohort_name": str(cohort_name),
            "steps": step_metrics,
        })

    return {
        "funnel_id": funnel_id,
        "funnel_name": str(funnel_row[1]),
        "steps": [s["event_name"] for s in steps],
        "results": results,
    }
