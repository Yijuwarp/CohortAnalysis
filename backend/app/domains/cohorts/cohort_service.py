"""
Short summary: cohort lifecycle management including creation and deletion.
"""
import json
import duckdb
from datetime import datetime, timezone
from fastapi import HTTPException
from app.models.cohort_models import CreateCohortRequest
from app.domains.cohorts.validation import validate_cohort_conditions
from app.domains.cohorts.membership_builder import build_cohort_membership
from app.domains.cohorts.activity_service import refresh_cohort_activity

def ensure_cohort_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cohorts (
            cohort_id INTEGER PRIMARY KEY,
            name TEXT,
            logic_operator TEXT,
            join_type TEXT DEFAULT 'condition_met',
            is_active BOOLEAN DEFAULT TRUE,
            hidden BOOLEAN DEFAULT FALSE,
            split_parent_cohort_id INTEGER,
            split_group_index INTEGER,
            split_group_total INTEGER
        )
        """
    )
    connection.execute("CREATE SEQUENCE IF NOT EXISTS cohorts_id_sequence START 1")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cohort_membership (
            user_id TEXT,
            cohort_id INTEGER,
            join_time TIMESTAMP
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cohort_activity_snapshot (
            cohort_id INTEGER,
            user_id TEXT,
            event_time TIMESTAMP,
            event_name TEXT
        )
        """
    )
    existing_condition_columns = {
        row[0]
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'cohort_conditions'
            """
        ).fetchall()
    }
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cohort_conditions (
            condition_id BIGINT PRIMARY KEY,
            cohort_id BIGINT NOT NULL,
            event_name VARCHAR NOT NULL,
            min_event_count INTEGER NOT NULL,
            property_column VARCHAR,
            property_operator VARCHAR,
            property_values TEXT
        )
        """
    )
    connection.execute("CREATE SEQUENCE IF NOT EXISTS cohort_condition_id_sequence START 1")
    if existing_condition_columns:
        if "property_values" not in existing_condition_columns:
            connection.execute("ALTER TABLE cohort_conditions ADD COLUMN property_values TEXT")
        if "property_value" in existing_condition_columns:
            connection.execute(
                """
                UPDATE cohort_conditions
                SET property_values = COALESCE(property_values, json_array(property_value))
                WHERE property_value IS NOT NULL
                """
            )
            connection.execute("ALTER TABLE cohort_conditions DROP COLUMN property_value")
        connection.execute(
            """
            UPDATE cohort_conditions
            SET property_operator = '='
            WHERE property_column IS NOT NULL
              AND property_values IS NOT NULL
              AND (property_operator IS NULL OR property_operator = '')
            """
        )

    existing_columns = {
        row[0]
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'cohorts'
            """
        ).fetchall()
    }
    if "logic_operator" not in existing_columns:
        connection.execute("ALTER TABLE cohorts ADD COLUMN logic_operator TEXT")
    if "join_type" not in existing_columns:
        connection.execute("ALTER TABLE cohorts ADD COLUMN join_type TEXT DEFAULT 'condition_met'")
    connection.execute(
        """
        UPDATE cohorts
        SET join_type = 'condition_met'
        WHERE join_type IS NULL OR join_type = ''
        """
    )
    if "is_active" not in existing_columns:
        connection.execute("ALTER TABLE cohorts ADD COLUMN is_active BOOLEAN DEFAULT TRUE")
    if "hidden" not in existing_columns:
        connection.execute("ALTER TABLE cohorts ADD COLUMN hidden BOOLEAN DEFAULT FALSE")
    if "split_parent_cohort_id" not in existing_columns:
        connection.execute("ALTER TABLE cohorts ADD COLUMN split_parent_cohort_id INTEGER")
    if "split_group_index" not in existing_columns:
        connection.execute("ALTER TABLE cohorts ADD COLUMN split_group_index INTEGER")
    if "split_group_total" not in existing_columns:
        connection.execute("ALTER TABLE cohorts ADD COLUMN split_group_total INTEGER")

    snapshot_columns = {
        row[0]
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'cohort_activity_snapshot'
            """
        ).fetchall()
    }
    if "event_name" not in snapshot_columns:
        connection.execute("ALTER TABLE cohort_activity_snapshot ADD COLUMN event_name TEXT")
        connection.execute(
            """
            UPDATE cohort_activity_snapshot cas
            SET event_name = e.event_name
            FROM events_normalized e
            WHERE cas.user_id = e.user_id
              AND cas.event_time = e.event_time
              AND cas.event_name IS NULL
            """
        )


def create_cohort(connection: duckdb.DuckDBPyConnection, payload: CreateCohortRequest) -> dict[str, int]:
    normalized_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized' AND table_schema = 'main'"
    ).fetchone()[0]
    if not normalized_exists:
        raise HTTPException(
            status_code=400,
            detail="No normalized events found. Upload a CSV and map columns first.",
        )

    ensure_cohort_tables(connection)

    if not payload.conditions:
        raise HTTPException(status_code=400, detail="At least one condition is required")

    validate_cohort_conditions(connection, "events_normalized", payload.conditions)

    cohort_id = connection.execute(
        """
        INSERT INTO cohorts (cohort_id, name, logic_operator, join_type, is_active)
        VALUES (nextval('cohorts_id_sequence'), ?, ?, ?, TRUE)
        RETURNING cohort_id
        """,
        [payload.name, (payload.condition_logic or payload.logic_operator or "AND").upper(), payload.join_type],
    ).fetchone()[0]

    for condition in payload.conditions:
        property_column = None
        property_operator = None
        property_values = None
        if condition.property_filter:
            property_column = condition.property_filter.column
            property_operator = condition.property_filter.operator.upper()
            property_values = json.dumps(condition.property_filter.values)

        connection.execute(
            """
            INSERT INTO cohort_conditions (
                condition_id,
                cohort_id,
                event_name,
                min_event_count,
                property_column,
                property_operator,
                property_values
            )
            VALUES (nextval('cohort_condition_id_sequence'), ?, ?, ?, ?, ?, ?)
            """,
            [
                cohort_id,
                condition.event_name,
                condition.min_event_count,
                property_column,
                property_operator,
                property_values,
            ],
        )

    build_cohort_membership(connection, cohort_id, "events_normalized")

    users_joined = int(
        connection.execute(
            "SELECT COUNT(*) FROM cohort_membership WHERE cohort_id = ?",
            [cohort_id],
        ).fetchone()[0]
    )
    refresh_cohort_activity(connection)
    return {"cohort_id": int(cohort_id), "users_joined": users_joined}


def list_cohorts(connection: duckdb.DuckDBPyConnection) -> dict[str, list[dict[str, object]]]:
    ensure_cohort_tables(connection)
    rows = connection.execute(
        """
        SELECT
            c.cohort_id,
            c.name,
            c.is_active,
            c.logic_operator,
            c.join_type,
            c.hidden,
            c.split_parent_cohort_id,
            c.split_group_index,
            c.split_group_total,
            cc.event_name,
            cc.min_event_count,
            cc.property_column,
            cc.property_operator,
            cc.property_values
        FROM cohorts c
        LEFT JOIN cohort_conditions cc ON c.cohort_id = cc.cohort_id
        ORDER BY c.cohort_id, cc.condition_id
        """
    ).fetchall()

    cohorts: dict[int, dict[str, object]] = {}
    for cohort_id, name, is_active, logic_operator, join_type, hidden, split_parent_cohort_id, split_group_index, split_group_total, event_name, min_event_count, property_column, property_operator, property_values in rows:
        cohort_id = int(cohort_id)
        if cohort_id not in cohorts:
            logic = str(logic_operator or "AND").upper()
            cohorts[cohort_id] = {
                "cohort_id": cohort_id,
                "cohort_name": str(name),
                "is_active": bool(is_active),
                "logic_operator": logic,
                "condition_logic": logic,
                "join_type": str(join_type or "condition_met"),
                "hidden": bool(hidden),
                "split_parent_cohort_id": int(split_parent_cohort_id) if split_parent_cohort_id is not None else None,
                "split_group_index": int(split_group_index) if split_group_index is not None else None,
                "split_group_total": int(split_group_total) if split_group_total is not None else None,
                "conditions": [],
            }

        if event_name is not None and min_event_count is not None:
            property_filter = None
            if property_column and property_operator and property_values is not None:
                property_filter = {
                    "column": str(property_column),
                    "operator": str(property_operator),
                    "values": json.loads(str(property_values)),
                }
            cohorts[cohort_id]["conditions"].append(
                {
                    "event_name": str(event_name),
                    "min_event_count": int(min_event_count),
                    "property_filter": property_filter,
                }
            )

    size_rows = connection.execute(
        """
        SELECT cohort_id, COUNT(*)
        FROM cohort_membership
        GROUP BY cohort_id
        """
    ).fetchall()
    size_by_id = {int(row[0]): int(row[1]) for row in size_rows}
    for cohort in cohorts.values():
        cohort["size"] = size_by_id.get(int(cohort["cohort_id"]), 0)

    return {
        "cohorts": sorted(cohorts.values(), key=lambda cohort: cohort["cohort_id"])
    }


def update_cohort(connection: duckdb.DuckDBPyConnection, cohort_id: int, payload: CreateCohortRequest) -> dict[str, int]:
    ensure_cohort_tables(connection)
    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
    ).fetchone()[0]
    source_table = "events_scoped" if scoped_exists else "events_normalized"

    cohort_row = connection.execute(
        "SELECT name FROM cohorts WHERE cohort_id = ?",
        [cohort_id],
    ).fetchone()
    if cohort_row is None:
        raise HTTPException(status_code=404, detail="Cohort not found")
    if cohort_row[0] == "All Users":
        raise HTTPException(status_code=400, detail="All Users cohort cannot be updated")

    if not payload.conditions:
        raise HTTPException(status_code=400, detail="At least one condition is required")

    validate_cohort_conditions(connection, source_table, payload.conditions)

    connection.execute(
        "UPDATE cohorts SET name = ?, logic_operator = ?, join_type = ? WHERE cohort_id = ?",
        [payload.name, (payload.condition_logic or payload.logic_operator or "AND").upper(), payload.join_type, cohort_id],
    )
    connection.execute("DELETE FROM cohort_conditions WHERE cohort_id = ?", [cohort_id])

    for condition in payload.conditions:
        property_column = None
        property_operator = None
        property_values = None
        if condition.property_filter:
            property_column = condition.property_filter.column
            property_operator = condition.property_filter.operator.upper()
            property_values = json.dumps(condition.property_filter.values)

        connection.execute(
            """
            INSERT INTO cohort_conditions (
                condition_id,
                cohort_id,
                event_name,
                min_event_count,
                property_column,
                property_operator,
                property_values
            )
            VALUES (nextval('cohort_condition_id_sequence'), ?, ?, ?, ?, ?, ?)
            """,
            [
                cohort_id,
                condition.event_name,
                condition.min_event_count,
                property_column,
                property_operator,
                property_values,
            ],
        )

    build_cohort_membership(connection, cohort_id, source_table)
    refresh_cohort_activity(connection)

    users_joined = int(
        connection.execute(
            "SELECT COUNT(*) FROM cohort_membership WHERE cohort_id = ?",
            [cohort_id],
        ).fetchone()[0]
    )
    return {"cohort_id": int(cohort_id), "users_joined": users_joined}


def random_split_cohort(connection: duckdb.DuckDBPyConnection, cohort_id: int) -> dict[str, int]:
    ensure_cohort_tables(connection)
    parent_row = connection.execute(
        """
        SELECT name, split_parent_cohort_id, hidden
        FROM cohorts
        WHERE cohort_id = ?
        """,
        [cohort_id],
    ).fetchone()
    if parent_row is None:
        raise HTTPException(status_code=404, detail="Cohort not found")

    parent_name = str(parent_row[0])
    if parent_row[1] is not None:
        raise HTTPException(status_code=400, detail="Cannot split sub-cohort")
    if bool(parent_row[2]):
        raise HTTPException(status_code=400, detail="Cannot split hidden cohort")

    parent_size = int(
        connection.execute(
            "SELECT COUNT(*) FROM cohort_membership WHERE cohort_id = ?",
            [cohort_id],
        ).fetchone()[0]
    )
    if parent_size < 8:
        raise HTTPException(status_code=400, detail="Minimum 8 users required")

    connection.execute("BEGIN")
    try:
        connection.execute(
            """
            DELETE FROM cohort_membership
            WHERE cohort_id IN (
                SELECT cohort_id
                FROM cohorts
                WHERE split_parent_cohort_id = ?
            )
            """,
            [cohort_id],
        )
        connection.execute(
            "DELETE FROM cohorts WHERE split_parent_cohort_id = ?",
            [cohort_id],
        )

        groups = ["A", "B", "C", "D"]
        new_ids: list[int] = []
        for idx, letter in enumerate(groups):
            row = connection.execute(
                """
                INSERT INTO cohorts (
                    cohort_id,
                    name,
                    logic_operator,
                    join_type,
                    is_active,
                    hidden,
                    split_parent_cohort_id,
                    split_group_index,
                    split_group_total
                )
                VALUES (nextval('cohorts_id_sequence'), ?, 'AND', 'condition_met', TRUE, FALSE, ?, ?, 4)
                RETURNING cohort_id
                """,
                [f"{parent_name} {letter}", cohort_id, idx],
            ).fetchone()
            new_ids.append(int(row[0]))

        seed = f"{cohort_id}-{datetime.now(timezone.utc).isoformat()}"
        connection.execute(
            """
            WITH base AS (
                SELECT user_id
                FROM cohort_membership
                WHERE cohort_id = ?
            ),
            shuffled AS (
                SELECT
                    user_id,
                    ROW_NUMBER() OVER (ORDER BY hash(CAST(user_id AS VARCHAR) || ?)) - 1 AS rn,
                    COUNT(*) OVER () AS total
                FROM base
            ),
            bucketed AS (
                SELECT
                    user_id,
                    CAST(FLOOR(rn * 4.0 / total) AS INTEGER) AS grp
                FROM shuffled
            )
            INSERT INTO cohort_membership (cohort_id, user_id, join_time)
            SELECT
                CASE b.grp
                    WHEN 0 THEN ?
                    WHEN 1 THEN ?
                    WHEN 2 THEN ?
                    WHEN 3 THEN ?
                END,
                cm.user_id,
                cm.join_time
            FROM bucketed b
            JOIN cohort_membership cm
              ON b.user_id = cm.user_id
             AND cm.cohort_id = ?
            """,
            [cohort_id, seed, new_ids[0], new_ids[1], new_ids[2], new_ids[3], cohort_id],
        )
        refresh_cohort_activity(connection)
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise

    return {"created": 4}


def delete_cohort(connection: duckdb.DuckDBPyConnection, cohort_id: int) -> dict[str, int | bool]:
    ensure_cohort_tables(connection)

    cohort_row = connection.execute(
        "SELECT name FROM cohorts WHERE cohort_id = ?",
        [cohort_id],
    ).fetchone()
    if cohort_row is None:
        raise HTTPException(status_code=404, detail="Cohort not found")
    if cohort_row[0] == "All Users":
        raise HTTPException(status_code=400, detail="All Users cohort cannot be deleted")

    connection.execute(
        """
        DELETE FROM cohort_membership
        WHERE cohort_id IN (
            SELECT cohort_id
            FROM cohorts
            WHERE split_parent_cohort_id = ?
        )
        """,
        [cohort_id],
    )
    connection.execute("DELETE FROM cohorts WHERE split_parent_cohort_id = ?", [cohort_id])

    connection.execute("DELETE FROM cohort_conditions WHERE cohort_id = ?", [cohort_id])
    connection.execute("DELETE FROM cohort_activity_snapshot WHERE cohort_id = ?", [cohort_id])
    connection.execute("DELETE FROM cohort_membership WHERE cohort_id = ?", [cohort_id])
    connection.execute("DELETE FROM cohorts WHERE cohort_id = ?", [cohort_id])
    return {"deleted": True, "cohort_id": int(cohort_id)}


def toggle_cohort_hide(connection: duckdb.DuckDBPyConnection, cohort_id: int) -> dict[str, object]:
    ensure_cohort_tables(connection)

    cohort_row = connection.execute(
        "SELECT cohort_id, hidden FROM cohorts WHERE cohort_id = ?",
        [cohort_id],
    ).fetchone()
    if cohort_row is None:
        raise HTTPException(status_code=404, detail="Cohort not found")

    connection.execute(
        """
        UPDATE cohorts
        SET hidden = NOT hidden
        WHERE cohort_id = ?
        """,
        [cohort_id],
    )

    updated_hidden = connection.execute(
        "SELECT hidden FROM cohorts WHERE cohort_id = ?",
        [cohort_id],
    ).fetchone()[0]
    return {"cohort_id": int(cohort_id), "hidden": bool(updated_hidden)}
