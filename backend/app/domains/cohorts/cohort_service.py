"""
Short summary: cohort lifecycle management including creation, deletion, and split operations.
"""
import json
import duckdb
from datetime import datetime, timezone
from fastapi import HTTPException
from app.models.cohort_models import CreateCohortRequest, SplitRequest, SplitResponse, SplitChildCohort, SplitPreviewItem, SplitPreviewResponse
from app.domains.cohorts.validation import validate_cohort_conditions
from app.domains.cohorts.membership_builder import build_cohort_membership
from app.domains.cohorts.activity_service import refresh_cohort_activity
from app.utils.db_utils import to_dict, to_dicts
from app.utils.sql import get_column_type_map, get_column_kind
from app.utils.timestamp import migrate_legacy_timestamp_filter, validate_timestamp_payload

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
            source_saved_id UUID,
            cohort_origin TEXT DEFAULT 'manual'
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
            join_time TIMESTAMP,
            UNIQUE(cohort_id, user_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cohort_activity_snapshot (
            cohort_id INTEGER,
            user_id TEXT,
            event_time TIMESTAMP,
            event_name TEXT,
            row_id BIGINT
        )

        """
    )
    res_conditions = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'cohort_conditions'
        """
    )
    existing_condition_columns = {row["column_name"] for row in to_dicts(res_conditions, res_conditions.fetchall())}
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cohort_conditions (
            condition_id BIGINT PRIMARY KEY,
            cohort_id BIGINT NOT NULL,
            event_name VARCHAR NOT NULL,
            min_event_count INTEGER NOT NULL,
            property_column VARCHAR,
            property_operator VARCHAR,
            property_values TEXT,
            is_negated BOOLEAN DEFAULT FALSE
        )
        """
    )
    connection.execute("CREATE SEQUENCE IF NOT EXISTS cohort_condition_id_sequence START 1")
    if existing_condition_columns:
        if "property_values" not in existing_condition_columns:
            connection.execute("ALTER TABLE cohort_conditions ADD COLUMN property_values TEXT")
        if "is_negated" not in existing_condition_columns:
            connection.execute("ALTER TABLE cohort_conditions ADD COLUMN is_negated BOOLEAN DEFAULT FALSE")

    # Migrate cohorts table: add split_type, split_property, split_value if missing
    res_cohorts = connection.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'cohorts'"
    )
    cohort_columns = {row["column_name"] for row in to_dicts(res_cohorts, res_cohorts.fetchall())}
    if "split_type" not in cohort_columns:
        connection.execute("ALTER TABLE cohorts ADD COLUMN split_type TEXT")
    if "split_property" not in cohort_columns:
        connection.execute("ALTER TABLE cohorts ADD COLUMN split_property TEXT")
    if "split_value" not in cohort_columns:
        connection.execute("ALTER TABLE cohorts ADD COLUMN split_value TEXT")
    if "is_paths_temp" in cohort_columns:
        connection.execute("ALTER TABLE cohorts DROP COLUMN is_paths_temp")
    if "cohort_origin" not in cohort_columns:
        connection.execute("ALTER TABLE cohorts ADD COLUMN cohort_origin TEXT DEFAULT 'manual'")

    # Add source_saved_id to snapshot if missing
    res_snapshot = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'cohort_activity_snapshot'
        """
    )
    snapshot_columns = {row["column_name"] for row in to_dicts(res_snapshot, res_snapshot.fetchall())}
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
            if property_operator in {"BEFORE", "AFTER", "ON", "BETWEEN"}:
                property_values = json.dumps(condition.property_filter.values)
            else:
                # Normalize to list (handles scalar, list, and stringified JSON)
                values = normalize_values(condition.property_filter.values)
                property_values = json.dumps(values)

        if property_values is not None:
            try:
                parsed = json.loads(property_values)
                if property_operator not in {"BEFORE", "AFTER", "ON", "BETWEEN"} and not isinstance(parsed, list):
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
                property_values,
                is_negated
            )
            VALUES (nextval('cohort_condition_id_sequence'), ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                cohort_id,
                condition.event_name,
                condition.min_event_count,
                property_column,
                property_operator,
                property_values,
                bool(getattr(condition, 'is_negated', False)),
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
    source_table = get_events_source_table(connection)
    source_column_types = get_column_type_map(connection, source_table)
    cursor = connection.execute(
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
            c.split_type,
            c.split_property,
            c.split_value,
            c.source_saved_id
        FROM cohorts c
        LEFT JOIN (
            SELECT cohort_id, COUNT(*) as size
            FROM cohort_membership
            GROUP BY cohort_id
        ) sub ON c.cohort_id = sub.cohort_id
        ORDER BY c.cohort_id ASC
        """
    )
    rows = to_dicts(cursor, cursor.fetchall())

    # Fetch all conditions in one query and group by cohort_id
    c_cursor = connection.execute(
        """
        SELECT cohort_id, event_name, min_event_count, property_column,
               property_operator, property_values, COALESCE(is_negated, FALSE) as is_negated
        FROM cohort_conditions
        ORDER BY cohort_id, condition_id
        """
    )
    condition_rows = to_dicts(c_cursor, c_cursor.fetchall())

    conditions_by_cohort: dict[int, list[dict]] = {}
    for crow in condition_rows:
        cid = int(crow["cohort_id"])
        property_filter = None
        if crow["property_column"] and crow["property_operator"] and crow["property_values"] is not None:
            parsed_values = json.loads(str(crow["property_values"]))
            operator = str(crow["property_operator"]).upper()
            column_name = str(crow["property_column"])
            values: object = parsed_values
            if get_column_kind(source_column_types.get(column_name, "TEXT")) == "TIMESTAMP":
                if operator in {"IN", "NOT IN"}:
                    legacy_value = parsed_values
                else:
                    legacy_value = parsed_values[0] if isinstance(parsed_values, list) and parsed_values else parsed_values
                
                migrated_operator, migrated_value = migrate_legacy_timestamp_filter(operator, legacy_value)
                operator = migrated_operator
                values = validate_timestamp_payload(migrated_operator, migrated_value)
            property_filter = {
                "column": column_name,
                "operator": operator,
                "values": values,
            }
        conditions_by_cohort.setdefault(cid, []).append({
            "event_name": str(crow["event_name"]),
            "min_event_count": int(crow["min_event_count"]),
            "property_filter": property_filter,
            "is_negated": bool(crow["is_negated"]),
        })

    return {
        "cohorts": [
            {
                "cohort_id": int(row["cohort_id"]),
                "cohort_name": str(row["name"]),
                "name": str(row["name"]),
                "is_active": bool(row["is_active"]),
                "logic_operator": str(row["logic_operator"]) if row["logic_operator"] else "AND",
                "join_type": str(row["join_type"]) if row["join_type"] else "condition_met",
                "size": int(row["size"]),
                "hidden": bool(row["hidden"]),
                "split_parent_cohort_id": int(row["split_parent_cohort_id"]) if row["split_parent_cohort_id"] is not None else None,
                "split_group_index": int(row["split_group_index"]) if row["split_group_index"] is not None else None,
                "split_group_total": int(row["split_group_total"]) if row["split_group_total"] is not None else None,
                "split_type": str(row["split_type"]) if row.get("split_type") else None,
                "split_property": str(row["split_property"]) if row.get("split_property") else None,
                "split_value": str(row["split_value"]) if row.get("split_value") else None,
                "source_saved_id": str(row["source_saved_id"]) if row["source_saved_id"] else None,
                "conditions": conditions_by_cohort.get(int(row["cohort_id"]), []),
            }
            for row in rows
        ]
    }



def update_cohort(connection: duckdb.DuckDBPyConnection, cohort_id: int, payload: CreateCohortRequest) -> dict[str, int]:
    ensure_cohort_tables(connection)
    source_table = get_events_source_table(connection)

    cursor = connection.execute(
        "SELECT name FROM cohorts WHERE cohort_id = ?",
        [cohort_id],
    )
    cohort_row = to_dict(cursor, cursor.fetchone())
    if not cohort_row:
        raise HTTPException(status_code=404, detail="Cohort not found")
    if cohort_row["name"] == "All Users":
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
            if property_operator in {"BEFORE", "AFTER", "ON", "BETWEEN"}:
                property_values = json.dumps(condition.property_filter.values)
            else:
                # Normalize to list (handles scalar, list, and stringified JSON)
                values = normalize_values(condition.property_filter.values)
                property_values = json.dumps(values)

        if property_values is not None:
            try:
                parsed = json.loads(property_values)
                if property_operator not in {"BEFORE", "AFTER", "ON", "BETWEEN"} and not isinstance(parsed, list):
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
                property_values,
                is_negated
            )
            VALUES (nextval('cohort_condition_id_sequence'), ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                cohort_id,
                condition.event_name,
                condition.min_event_count,
                property_column,
                property_operator,
                property_values,
                bool(getattr(condition, 'is_negated', False)),
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
    """Legacy backward-compat wrapper: 4 random groups."""
    from app.models.cohort_models import SplitRequest, RandomSplitOptions
    req = SplitRequest(type="random", random=RandomSplitOptions(num_groups=4))
    result = split_cohort(connection, cohort_id, req)
    return {"created": len(result.child_cohorts)}


def _get_parent_for_split(connection: duckdb.DuckDBPyConnection, cohort_id: int) -> dict:
    """Validate parent cohort is eligible for split; return its row."""
    cursor = connection.execute(
        "SELECT name, split_parent_cohort_id, hidden FROM cohorts WHERE cohort_id = ?",
        [cohort_id],
    )
    parent_row = to_dict(cursor, cursor.fetchone())
    if not parent_row:
        raise HTTPException(status_code=404, detail="Cohort not found")
    if parent_row["split_parent_cohort_id"] is not None:
        raise HTTPException(status_code=400, detail="Cannot split sub-cohort")
    if bool(parent_row["hidden"]):
        raise HTTPException(status_code=400, detail="Cannot split hidden cohort")
    return parent_row


def _clear_existing_splits(connection: duckdb.DuckDBPyConnection, cohort_id: int) -> None:
    """Remove all existing child cohorts (and their memberships) for a parent."""
    connection.execute(
        """
        DELETE FROM cohort_membership
        WHERE cohort_id IN (
            SELECT cohort_id FROM cohorts WHERE split_parent_cohort_id = ?
        )
        """,
        [cohort_id],
    )
    connection.execute("DELETE FROM cohorts WHERE split_parent_cohort_id = ?", [cohort_id])


def split_cohort(
    connection: duckdb.DuckDBPyConnection, cohort_id: int, request: SplitRequest
) -> SplitResponse:
    """Unified split: random or property-based."""
    ensure_cohort_tables(connection)
    parent_row = _get_parent_for_split(connection, cohort_id)
    parent_name = str(parent_row["name"])

    parent_size = int(
        connection.execute(
            "SELECT COUNT(*) FROM cohort_membership WHERE cohort_id = ?", [cohort_id]
        ).fetchone()[0]
    )
    if parent_size == 0:
        raise HTTPException(status_code=400, detail="Parent cohort has no members to split")

    connection.execute("BEGIN")
    try:
        _clear_existing_splits(connection, cohort_id)
        if request.type == "random":
            child_cohorts = _do_random_split(connection, cohort_id, parent_name, parent_size, request)
        else:
            child_cohorts = _do_property_split(connection, cohort_id, parent_name, request)
        refresh_cohort_activity(connection)
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise

    return SplitResponse(parent_cohort_id=cohort_id, child_cohorts=child_cohorts)


def _do_random_split(
    connection: duckdb.DuckDBPyConnection,
    cohort_id: int,
    parent_name: str,
    parent_size: int,
    request: SplitRequest,
) -> list[SplitChildCohort]:
    opts = request.random or __import__('app.models.cohort_models', fromlist=['RandomSplitOptions']).RandomSplitOptions()
    n = opts.num_groups

    if parent_size < 8:
        raise HTTPException(
            status_code=400,
            detail="Minimum 8 users required",
        )

    new_ids: list[int] = []
    for i in range(1, n + 1):
        row = connection.execute(
            """
            INSERT INTO cohorts (
                cohort_id, name, logic_operator, join_type, is_active, hidden,
                split_parent_cohort_id, split_group_index, split_group_total,
                split_type, split_property, split_value
            )
            VALUES (nextval('cohorts_id_sequence'), ?, 'AND', 'condition_met', TRUE, FALSE, ?, ?, ?, 'random', NULL, ?)
            RETURNING cohort_id
            """,
            [f"{parent_name}_Random_{i}", cohort_id, i - 1, n, str(i)],
        ).fetchone()
        new_ids.append(int(row[0]))

    seed = f"{cohort_id}-{datetime.now(timezone.utc).isoformat()}"
    # Build CASE expression for N groups dynamically
    case_branches = "\n".join(
        [f"WHEN {i} THEN {new_ids[i]}" for i in range(n)]
    )
    connection.execute(
        f"""
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
                CAST(FLOOR(rn * {n}.0 / total) AS INTEGER) AS grp
            FROM shuffled
        )
        INSERT INTO cohort_membership (cohort_id, user_id, join_time)
        SELECT
            CASE b.grp
                {case_branches}
            END,
            cm.user_id,
            cm.join_time
        FROM bucketed b
        JOIN cohort_membership cm
          ON b.user_id = cm.user_id
         AND cm.cohort_id = ?
        """,
        [cohort_id, seed, cohort_id],
    )

    return [
        SplitChildCohort(id=new_ids[i], name=f"{parent_name}_Random_{i + 1}")
        for i in range(n)
    ]


def _do_property_split(
    connection: duckdb.DuckDBPyConnection,
    cohort_id: int,
    parent_name: str,
    request: SplitRequest,
) -> list[SplitChildCohort]:
    if not request.property:
        raise HTTPException(status_code=400, detail="Property split options required")
    column = request.property.column
    values = request.property.values
    source_table = get_events_source_table(connection)

    from app.utils.sql import quote_identifier
    col_id = quote_identifier(column)

    child_cohorts: list[SplitChildCohort] = []

    for val in values:
        safe_name = f"{parent_name}_{val}"
        row = connection.execute(
            """
            INSERT INTO cohorts (
                cohort_id, name, logic_operator, join_type, is_active, hidden,
                split_parent_cohort_id, split_group_index, split_group_total,
                split_type, split_property, split_value
            )
            VALUES (nextval('cohorts_id_sequence'), ?, 'AND', 'condition_met', TRUE, FALSE, ?, NULL, NULL, 'property', ?, ?)
            RETURNING cohort_id
            """,
            [safe_name, cohort_id, column, val],
        ).fetchone()
        child_id = int(row[0])

        connection.execute(
            f"""
            INSERT INTO cohort_membership (cohort_id, user_id, join_time)
            SELECT ?, cm.user_id, cm.join_time
            FROM cohort_membership cm
            WHERE cm.cohort_id = ?
              AND EXISTS (
                  SELECT 1 FROM {source_table} e
                  WHERE e.user_id = cm.user_id
                    AND {col_id} = ?
              )
            ON CONFLICT (cohort_id, user_id) DO NOTHING
            """,
            [child_id, cohort_id, val],
        )
        child_cohorts.append(SplitChildCohort(id=child_id, name=safe_name))

    # --- _other cohort ---
    placeholders = ", ".join(["?"] * len(values))
    other_count = int(
        connection.execute(
            f"""
            SELECT COUNT(*) FROM cohort_membership cm
            WHERE cm.cohort_id = ?
              AND NOT EXISTS (
                  SELECT 1 FROM {source_table} e
                  WHERE e.user_id = cm.user_id
                    AND {col_id} IN ({placeholders})
              )
            """,
            [cohort_id, *values],
        ).fetchone()[0]
    )

    if other_count > 0:
        other_name = f"{parent_name}_other"
        other_row = connection.execute(
            """
            INSERT INTO cohorts (
                cohort_id, name, logic_operator, join_type, is_active, hidden,
                split_parent_cohort_id, split_group_index, split_group_total,
                split_type, split_property, split_value
            )
            VALUES (nextval('cohorts_id_sequence'), ?, 'AND', 'condition_met', TRUE, FALSE, ?, NULL, NULL, 'property', ?, '__OTHER__')
            RETURNING cohort_id
            """,
            [other_name, cohort_id, column],
        ).fetchone()
        other_id = int(other_row[0])

        connection.execute(
            f"""
            INSERT INTO cohort_membership (cohort_id, user_id, join_time)
            SELECT ?, cm.user_id, cm.join_time
            FROM cohort_membership cm
            WHERE cm.cohort_id = ?
              AND NOT EXISTS (
                  SELECT 1 FROM {source_table} e
                  WHERE e.user_id = cm.user_id
                    AND {col_id} IN ({placeholders})
              )
            ON CONFLICT (cohort_id, user_id) DO NOTHING
            """,
            [other_id, cohort_id, *values],
        )
        child_cohorts.append(SplitChildCohort(id=other_id, name=other_name))

    return child_cohorts


def preview_split(
    connection: duckdb.DuckDBPyConnection, cohort_id: int, request: SplitRequest
) -> SplitPreviewResponse:
    """Return expected cohort counts without persisting any changes."""
    ensure_cohort_tables(connection)
    parent_row = _get_parent_for_split(connection, cohort_id)
    parent_name = str(parent_row["name"])

    parent_size = int(
        connection.execute(
            "SELECT COUNT(*) FROM cohort_membership WHERE cohort_id = ?", [cohort_id]
        ).fetchone()[0]
    )
    if parent_size == 0:
        raise HTTPException(status_code=400, detail="Parent cohort has no members to split")

    if request.type == "random":
        from app.models.cohort_models import RandomSplitOptions
        opts = request.random or RandomSplitOptions()
        n = opts.num_groups
        if parent_size < n:
            raise HTTPException(
                status_code=400,
                detail=f"Parent cohort only has {parent_size} members; cannot split into {n} groups",
            )
        base = parent_size // n
        remainder = parent_size % n
        preview = [
            SplitPreviewItem(
                name=f"{parent_name}_Random_{i + 1}",
                count=base + (1 if i < remainder else 0),
            )
            for i in range(n)
        ]
    else:
        if not request.property:
            raise HTTPException(status_code=400, detail="Property split options required")
        column = request.property.column
        values = request.property.values
        source_table = get_events_source_table(connection)
        from app.utils.sql import quote_identifier
        col_id = quote_identifier(column)

        preview: list[SplitPreviewItem] = []
        for val in values:
            cnt = int(
                connection.execute(
                    f"""
                    SELECT COUNT(*) FROM cohort_membership cm
                    WHERE cm.cohort_id = ?
                      AND EXISTS (
                          SELECT 1 FROM {source_table} e
                          WHERE e.user_id = cm.user_id
                            AND {col_id} = ?
                      )
                    """,
                    [cohort_id, val],
                ).fetchone()[0]
            )
            preview.append(SplitPreviewItem(name=f"{parent_name}_{val}", count=cnt))

        placeholders = ", ".join(["?"] * len(values))
        other_count = int(
            connection.execute(
                f"""
                SELECT COUNT(*) FROM cohort_membership cm
                WHERE cm.cohort_id = ?
                  AND NOT EXISTS (
                      SELECT 1 FROM {source_table} e
                      WHERE e.user_id = cm.user_id
                        AND {col_id} IN ({placeholders})
                  )
                """,
                [cohort_id, *values],
            ).fetchone()[0]
        )
        if other_count > 0:
            preview.append(SplitPreviewItem(name=f"{parent_name}_other", count=other_count))

    return SplitPreviewResponse(parent_cohort_id=cohort_id, preview=preview)


def delete_cohort(connection: duckdb.DuckDBPyConnection, cohort_id: int) -> dict[str, int | bool]:
    ensure_cohort_tables(connection)

    cursor = connection.execute(
        "SELECT name FROM cohorts WHERE cohort_id = ?",
        [cohort_id],
    )
    cohort_row = to_dict(cursor, cursor.fetchone())
    if not cohort_row:
        raise HTTPException(status_code=404, detail="Cohort not found")
    if cohort_row["name"] == "All Users":
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

    cursor = connection.execute(
        "SELECT cohort_id, hidden FROM cohorts WHERE cohort_id = ?",
        [cohort_id],
    )
    cohort_row = to_dict(cursor, cursor.fetchone())
    if not cohort_row:
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
    cursor = connection.execute(
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
            c.split_type,
            c.split_property,
            c.split_value,
            c.source_saved_id,
            cc.event_name,
            cc.min_event_count,
            cc.property_column,
            cc.property_operator,
            cc.property_values,
            COALESCE(sub.size, 0) as size,
            COALESCE(cc.is_negated, FALSE) as is_negated
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
    )
    rows = cursor.fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail="Cohort not found")

    # Build response
    dicts = to_dicts(cursor, rows)
    first = dicts[0]

    cohort: dict[str, object] = {
        "cohort_id": int(first["cohort_id"]),
        "cohort_name": str(first["name"]),
        "name": str(first["name"]),
        "is_active": bool(first["is_active"]),
        "logic_operator": str(first["logic_operator"] or "AND"),
        "condition_logic": str(first["logic_operator"] or "AND"),
        "join_type": str(first["join_type"] or "condition_met"),
        "hidden": bool(first["hidden"]),
        "split_parent_cohort_id": int(first["split_parent_cohort_id"]) if first["split_parent_cohort_id"] is not None else None,
        "split_group_index": int(first["split_group_index"]) if first["split_group_index"] is not None else None,
        "split_group_total": int(first["split_group_total"]) if first["split_group_total"] is not None else None,
        "split_type": str(first["split_type"]) if first.get("split_type") else None,
        "split_property": str(first["split_property"]) if first.get("split_property") else None,
        "split_value": str(first["split_value"]) if first.get("split_value") else None,
        "source_saved_id": str(first["source_saved_id"]) if first["source_saved_id"] is not None else None,
        "size": int(first["size"]),
        "conditions": [],
    }

    for row in dicts:
        event_name = row["event_name"]
        min_event_count = row["min_event_count"]

        if event_name is None:
            continue

        property_filter = None
        if row["property_column"] and row["property_operator"] and row["property_values"] is not None:
            property_filter = {
                "column": str(row["property_column"]),
                "operator": str(row["property_operator"]),
                "values": json.loads(str(row["property_values"])),
            }

        cohort["conditions"].append(
            {
                "event_name": str(event_name),
                "min_event_count": int(min_event_count),
                "property_filter": property_filter,
                "is_negated": bool(row["is_negated"]),
            }
        )

    return cohort
