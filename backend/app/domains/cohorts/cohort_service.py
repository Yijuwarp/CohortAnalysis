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
            split_group_total INTEGER,
            source_saved_id UUID
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS saved_cohorts (
            id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            definition JSON NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        CREATE UNIQUE INDEX IF NOT EXISTS idx_cohort_user_unique
        ON cohort_membership(cohort_id, user_id)
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

    # Add source_saved_id to snapshot if missing
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

    if "source_saved_id" not in snapshot_columns:
        connection.execute("ALTER TABLE cohort_activity_snapshot ADD COLUMN source_saved_id UUID")


def get_events_source_table(connection: duckdb.DuckDBPyConnection) -> str:
    exists = connection.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = 'events_scoped'
          AND table_schema = 'main'
        """
    ).fetchone()[0]
    return "events_scoped" if exists else "events_normalized"


def normalize_values(values: object) -> list[object]:
    if isinstance(values, str):
        try:
            import json
            parsed = json.loads(values)
            return parsed if isinstance(parsed, list) else [parsed]
        except Exception:
            return [values]
    elif not isinstance(values, list):
        return [values]
    return values


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
    source_table = get_events_source_table(connection)

    if not payload.conditions:
        raise HTTPException(status_code=400, detail="At least one condition is required")

    validate_cohort_conditions(connection, source_table, payload.conditions)

    cohort_id = connection.execute(
        """
        INSERT INTO cohorts (cohort_id, name, logic_operator, join_type, is_active, source_saved_id)
        VALUES (nextval('cohorts_id_sequence'), ?, ?, ?, TRUE, ?)
        RETURNING cohort_id
        """,
        [payload.name, (payload.condition_logic or payload.logic_operator or "AND").upper(), payload.join_type, payload.source_saved_id],
    ).fetchone()[0]

    for condition in payload.conditions:
        property_column = None
        property_operator = None
        property_values = None
        if condition.property_filter:
            property_column = condition.property_filter.column
            property_operator = condition.property_filter.operator.upper()
            
            # Normalize to list (handles scalar, list, and stringified JSON)
            values = normalize_values(condition.property_filter.values)
            property_values = json.dumps(values)

        if property_values is not None:
            try:
                parsed = json.loads(property_values)
                if not isinstance(parsed, list):
                    raise ValueError
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid property_values format")

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
            COALESCE(sub.size, 0) as size,
            c.hidden,
            c.split_parent_cohort_id,
            c.split_group_index,
            c.split_group_total,
            c.source_saved_id
        FROM cohorts c
        LEFT JOIN (
            SELECT cohort_id, COUNT(*) as size
            FROM cohort_membership
            GROUP BY cohort_id
        ) sub ON c.cohort_id = sub.cohort_id
        ORDER BY c.cohort_id ASC
        """
    ).fetchall()

    return {
        "cohorts": [
            {
                "cohort_id": int(row[0]),
                "cohort_name": str(row[1]),
                "name": str(row[1]),
                "is_active": bool(row[2]),
                "logic_operator": str(row[3]) if row[3] else "AND",
                "join_type": str(row[4]) if row[4] else "condition_met",
                "size": int(row[5]),
                "hidden": bool(row[6]),
                "split_parent_cohort_id": int(row[7]) if row[7] is not None else None,
                "split_group_index": int(row[8]) if row[8] is not None else None,
                "split_group_total": int(row[9]) if row[9] is not None else None,
                "source_saved_id": str(row[10]) if row[10] else None,
            }
            for row in rows
        ]
    }


def update_cohort(connection: duckdb.DuckDBPyConnection, cohort_id: int, payload: CreateCohortRequest) -> dict[str, int]:
    ensure_cohort_tables(connection)
    source_table = get_events_source_table(connection)

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
            
            # Normalize to list (handles scalar, list, and stringified JSON)
            values = normalize_values(condition.property_filter.values)
            property_values = json.dumps(values)

        if property_values is not None:
            try:
                parsed = json.loads(property_values)
                if not isinstance(parsed, list):
                    raise ValueError
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid property_values format")

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


def get_cohort_detail(connection: duckdb.DuckDBPyConnection, cohort_id: int) -> dict[str, object]:
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
            c.source_saved_id,
            cc.event_name,
            cc.min_event_count,
            cc.property_column,
            cc.property_operator,
            cc.property_values,
            COALESCE(sub.size, 0) as size
        FROM cohorts c
        LEFT JOIN cohort_conditions cc
            ON c.cohort_id = cc.cohort_id
        LEFT JOIN (
            SELECT cohort_id, COUNT(*) as size
            FROM cohort_membership
            GROUP BY cohort_id
        ) sub ON c.cohort_id = sub.cohort_id
        WHERE c.cohort_id = ?
        ORDER BY cc.condition_id
        """,
        [cohort_id],
    ).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail="Cohort not found")

    # Build response
    first = rows[0]

    cohort: dict[str, object] = {
        "cohort_id": int(first[0]),
        "cohort_name": str(first[1]),
        "name": str(first[1]),
        "is_active": bool(first[2]),
        "logic_operator": str(first[3] or "AND"),
        "condition_logic": str(first[3] or "AND"),
        "join_type": str(first[4] or "condition_met"),
        "hidden": bool(first[5]),
        "split_parent_cohort_id": int(first[6]) if first[6] is not None else None,
        "split_group_index": int(first[7]) if first[7] is not None else None,
        "split_group_total": int(first[8]) if first[8] is not None else None,
        "source_saved_id": str(first[9]) if first[9] is not None else None,
        "size": int(first[15]),
        "conditions": [],
    }

    for row in rows:
        event_name = row[10]
        min_event_count = row[11]

        if event_name is None:
            continue

        property_filter = None
        if row[12] and row[13] and row[14] is not None:
            property_filter = {
                "column": str(row[12]),
                "operator": str(row[13]),
                "values": json.loads(str(row[14])),
            }

        cohort["conditions"].append(
            {
                "event_name": str(event_name),
                "min_event_count": int(min_event_count),
                "property_filter": property_filter,
            }
        )

    return cohort
