import uuid
import json
from datetime import datetime, timezone
import duckdb
from fastapi import HTTPException
from app.models.cohort_models import SavedCohortCreate, SavedCohortResponse
from app.domains.cohorts.cohort_service import ensure_cohort_tables, update_cohort
from app.utils.sql import get_column_type_map, get_column_kind, quote_identifier
from app.utils import timestamp

def get_source_table(connection: duckdb.DuckDBPyConnection) -> str:
    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
    ).fetchone()[0]
    return "events_scoped" if scoped_exists else "events_normalized"

def validate_saved_cohort(connection: duckdb.DuckDBPyConnection, definition: dict, dataset_schema: dict) -> dict:
    source_table = get_source_table(connection)
    
    table_exists = connection.execute(
        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{source_table}' AND table_schema = 'main'"
    ).fetchone()[0]
    
    if not table_exists:
        return {"is_valid": False, "errors": [{"type": "missing_dataset", "value": "no dataset loaded"}]}

    events_in_db = {
        row[0] for row in connection.execute(
            f"SELECT DISTINCT event_name FROM {source_table}"
        ).fetchall()
    }
    
    errors = []
    
    for cond in definition.get("conditions", []):
        event_name = cond.get("event_name")
        if event_name and event_name not in events_in_db:
            errors.append({"type": "missing_event", "value": event_name})
            
        prop_filter = cond.get("property_filter")
        if prop_filter:
            col = prop_filter.get("column")
            if col and col not in dataset_schema:
                errors.append({"type": "missing_property", "value": col})

    return {
        "is_valid": len(errors) == 0,
        "errors": errors
    }

def create_saved_cohort(connection: duckdb.DuckDBPyConnection, payload: SavedCohortCreate) -> dict:
    ensure_cohort_tables(connection)
    new_id = str(uuid.uuid4())
    definition_json = payload.model_dump_json()
    now_str = datetime.now(timezone.utc).isoformat()
    
    connection.execute(
        """
        INSERT INTO saved_cohorts (id, name, definition, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        [new_id, payload.name, definition_json, now_str, now_str]
    )
    
    return {
        "id": new_id,
        "name": payload.name,
        "definition": payload.model_dump(),
        "created_at": now_str,
        "updated_at": now_str,
    }

def get_saved_cohort(connection: duckdb.DuckDBPyConnection, cohort_id: str) -> dict:
    ensure_cohort_tables(connection)
    row = connection.execute(
        "SELECT id, name, definition, created_at, updated_at FROM saved_cohorts WHERE id = ?",
        [cohort_id]
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Saved cohort not found")
        
    from app.utils.sql import get_column_type_map
    
    definition_dict = json.loads(str(row[2]))
    source_table = get_source_table(connection)
    
    try:
        dataset_schema = get_column_type_map(connection, source_table)
    except Exception:
        dataset_schema = {}
        
    validity = validate_saved_cohort(connection, definition_dict, dataset_schema)
    
    return {
        "id": str(row[0]),
        "name": str(row[1]),
        "definition": definition_dict,
        "created_at": str(row[3]),
        "updated_at": str(row[4]),
        "is_valid": validity["is_valid"],
        "errors": validity["errors"]
    }

def get_all_saved_cohorts(connection: duckdb.DuckDBPyConnection) -> list[dict]:
    ensure_cohort_tables(connection)
    rows = connection.execute(
        "SELECT id, name, definition, created_at, updated_at FROM saved_cohorts ORDER BY created_at DESC"
    ).fetchall()
    
    from app.utils.sql import get_column_type_map
    source_table = get_source_table(connection)
    try:
        dataset_schema = get_column_type_map(connection, source_table)
    except Exception:
        dataset_schema = {}
        
    result = []
    for r in rows:
        def_dict = json.loads(str(r[2]))
        validity = validate_saved_cohort(connection, def_dict, dataset_schema)
        result.append({
            "id": str(r[0]),
            "name": str(r[1]),
            "definition": def_dict,
            "created_at": str(r[3]),
            "updated_at": str(r[4]),
            "is_valid": validity["is_valid"],
            "errors": validity["errors"]
        })
    return result

def update_saved_cohort(connection: duckdb.DuckDBPyConnection, cohort_id: str, payload: SavedCohortCreate) -> dict:
    ensure_cohort_tables(connection)
    
    row = connection.execute("SELECT id FROM saved_cohorts WHERE id = ?", [cohort_id]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Saved cohort not found")
        
    definition_json = payload.model_dump_json()
    now_str = datetime.now(timezone.utc).isoformat()
    
    connection.execute(
        """
        UPDATE saved_cohorts
        SET name = ?, definition = ?, updated_at = ?
        WHERE id = ?
        """,
        [payload.name, definition_json, now_str, cohort_id]
    )
    
    # Recompute all cohorts that derive from this
    affected_cohorts = connection.execute(
        "SELECT cohort_id FROM cohorts WHERE source_saved_id = ?",
        [cohort_id]
    ).fetchall()
    
    from app.models.cohort_models import CreateCohortRequest
    for (act_id,) in affected_cohorts:
        def_dict = payload.model_dump()
        c_req = CreateCohortRequest(**def_dict)
        c_req.source_saved_id = cohort_id
        update_cohort(connection, int(act_id), c_req)
        
    return get_saved_cohort(connection, cohort_id)

def delete_saved_cohort(connection: duckdb.DuckDBPyConnection, cohort_id: str) -> dict:
    ensure_cohort_tables(connection)
    row = connection.execute("SELECT id FROM saved_cohorts WHERE id = ?", [cohort_id]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Saved cohort not found")
        
    connection.execute("DELETE FROM saved_cohorts WHERE id = ?", [cohort_id])
    return {"deleted": True, "id": cohort_id}

def estimate_cohort(connection: duckdb.DuckDBPyConnection, definition: SavedCohortCreate) -> dict:
    from app.domains.cohorts.validation import validate_cohort_conditions
    
    source_table = get_source_table(connection)
    table_exists = connection.execute(
        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{source_table}' AND table_schema = 'main'"
    ).fetchone()[0]
    
    if not table_exists:
        return {"estimated_users": 0, "percentage_of_total": 0.0}

    validate_cohort_conditions(connection, source_table, definition.conditions)
    column_types = get_column_type_map(connection, source_table)
    
    conditions = definition.conditions
    logic_operator = (definition.condition_logic or definition.logic_operator or "AND").upper()
    join_type = definition.join_type or "condition_met"
    
    if not conditions:
        query = f"SELECT DISTINCT user_id FROM {source_table}"
        params = []
    else:
        has_negated = any(bool(getattr(cond, 'is_negated', False)) for cond in conditions)

        cte_parts: list[str] = []
        query_params: list[object] = []

        if has_negated:
            cte_parts.append(
                f"all_users AS (SELECT DISTINCT user_id FROM {source_table})"
            )

        for index, cond in enumerate(conditions):
            event_name = cond.event_name
            min_event_count = cond.min_event_count
            is_negated = bool(getattr(cond, 'is_negated', False))
            event_conditions = ["event_name = ?"]
            event_params: list[object] = [event_name]

            if cond.property_filter:
                property_column = cond.property_filter.column
                property_operator = cond.property_filter.operator
                property_values = cond.property_filter.values
                
                normalized_operator = str(property_operator).upper()
                column_kind = get_column_kind(column_types.get(property_column, "TEXT"))
                if column_kind == "TIMESTAMP":
                    migrated_operator, migrated_value = timestamp.migrate_legacy_timestamp_filter(normalized_operator, property_values)
                    clause, ts_params = timestamp.build_sql_clause(
                        quote_identifier(str(property_column)),
                        migrated_operator,
                        migrated_value,
                        parameterized=True,
                    )
                    event_conditions.append(clause)
                    event_params.extend(ts_params)
                elif normalized_operator in {"IN", "NOT IN"}:
                    parsed_values = property_values if isinstance(property_values, list) else [property_values]
                    placeholders = ", ".join(["?"] * len(parsed_values))
                    event_conditions.append(f"{quote_identifier(str(property_column))} {normalized_operator} ({placeholders})")
                    event_params.extend(parsed_values)
                else:
                    event_conditions.append(f"{quote_identifier(str(property_column))} {normalized_operator} ?")
                    event_params.append(property_values)

            where_clause = " AND ".join(event_conditions)

            # Base CTE: users who DID perform the event
            cte_parts.append(
                f"""
                c{index}_base AS (
                    SELECT user_id, MIN(event_time) AS event_time
                    FROM (
                        SELECT
                            user_id,
                            event_time,
                            SUM(event_count) OVER (
                                PARTITION BY user_id
                                ORDER BY event_time, event_name
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            ) AS cumulative_event_count
                        FROM {source_table}
                        WHERE {where_clause}
                    ) t
                    WHERE cumulative_event_count >= ?
                    GROUP BY user_id
                )
                """
            )
            query_params.extend([*event_params, min_event_count])

            if is_negated:
                cte_parts.append(
                    f"""
                    c{index} AS (
                        SELECT au.user_id, MIN(e.event_time) AS event_time
                        FROM all_users au
                        LEFT JOIN {source_table} e ON au.user_id = e.user_id
                        WHERE au.user_id NOT IN (SELECT user_id FROM c{index}_base)
                        GROUP BY au.user_id
                    )
                    """
                )
            else:
                cte_parts.append(f"c{index} AS (SELECT user_id, event_time FROM c{index}_base)")

        if logic_operator == "AND":
            if len(conditions) == 1:
                cte_parts.append("combined_conditions AS (SELECT user_id, event_time FROM c0)")
            else:
                least_time_expression = ", ".join([f"c{index}.event_time" for index in range(len(conditions))])
                join_clauses = "\n".join(
                    [f"INNER JOIN c{index} ON c0.user_id = c{index}.user_id" for index in range(1, len(conditions))]
                )
                cte_parts.append(
                    f"""
                    combined_conditions AS (
                        SELECT c0.user_id, LEAST({least_time_expression}) AS event_time
                        FROM c0
                        {join_clauses}
                    )
                    """
                )
        else:
            union_query = "\nUNION ALL\n".join(
                [f"SELECT user_id, event_time FROM c{index}" for index in range(len(conditions))]
            )
            cte_parts.append(f"combined_conditions AS ({union_query})")

        query = f"""
        WITH {', '.join(cte_parts)}
        SELECT DISTINCT user_id
        FROM combined_conditions
        """
        params = query_params
        
    count_query = f"SELECT COUNT(*) FROM ({query})"
    estimated_users = connection.execute(count_query, params).fetchone()[0]
    
    total_users = connection.execute(f"SELECT COUNT(DISTINCT user_id) FROM {source_table}").fetchone()[0]
    percentage = (estimated_users / total_users * 100.0) if total_users > 0 else 0.0
    
    return {
        "estimated_users": int(estimated_users),
        "percentage_of_total": float(percentage)
    }
