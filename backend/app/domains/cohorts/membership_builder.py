"""
Short summary: rebuilds cohort memberships from conditions.
"""
import json
import duckdb
from fastapi import HTTPException
from app.utils.perf import time_block
from app.utils.sql import quote_identifier, get_column_type_map, get_column_kind
from app.utils import timestamp

def build_cohort_membership(
    connection: duckdb.DuckDBPyConnection,
    cohort_id: int,
    source_table: str,
    target_table: str = "cohort_membership",
) -> None:
    if source_table not in {"events_normalized", "events_scoped"}:
        raise ValueError("Unsupported source table")

    from app.utils.db_utils import to_dict
    res = connection.execute("SELECT logic_operator, join_type, source_saved_id FROM cohorts WHERE cohort_id = ?", [cohort_id])
    cohort_row = to_dict(res, res.fetchone())
    if not cohort_row:
        raise HTTPException(status_code=404, detail="Cohort not found")

    logic_operator = str(cohort_row.get("logic_operator") or "OR").upper()
    join_type = str(cohort_row.get("join_type") or "condition_met")
    source_saved_id = cohort_row.get("source_saved_id")

    if target_table == "cohort_membership":
        connection.execute("DELETE FROM cohort_membership WHERE cohort_id = ?", [cohort_id])
        connection.execute("DELETE FROM cohort_activity_snapshot WHERE cohort_id = ?", [cohort_id])

    cursor = connection.execute(
        """
        SELECT event_name, min_event_count, property_column, property_operator, property_values,
               COALESCE(is_negated, FALSE) as is_negated
        FROM cohort_conditions
        WHERE cohort_id = ?
        ORDER BY condition_id
        """,
        [cohort_id],
    )
    from app.utils.db_utils import to_dicts
    conditions = to_dicts(cursor, cursor.fetchall())

    column_types = get_column_type_map(connection, source_table)

    if not conditions:
        connection.execute(
            f"""
            INSERT INTO {target_table} (user_id, cohort_id, join_time)
            SELECT user_id, ?, MIN(event_time)
            FROM {source_table}
            GROUP BY user_id
            ON CONFLICT (cohort_id, user_id) DO NOTHING
            """,
            [cohort_id],
        )
    else:
        has_negated = any(bool(row["is_negated"]) for row in conditions)

        cte_parts: list[str] = []
        query_params: list[object] = []

        # Compute all_users once if any condition is negated
        if has_negated:
            cte_parts.append(
                f"all_users AS (SELECT DISTINCT user_id FROM {source_table})"
            )

        for index, crow in enumerate(conditions):
            event_name = crow["event_name"]
            min_event_count = crow["min_event_count"]
            property_column = crow["property_column"]
            property_operator = crow["property_operator"]
            property_values = crow["property_values"]
            is_negated = crow["is_negated"]

            event_conditions = ["event_name = ?"]
            event_params: list[object] = [event_name]

            if property_column and property_operator and property_values is not None:
                from app.domains.cohorts.cohort_service import normalize_values
                parsed_values = normalize_values(property_values)
                normalized_operator = str(property_operator).upper()
                column_kind = get_column_kind(column_types.get(str(property_column), "TEXT"))
                if column_kind == "TIMESTAMP":
                    if normalized_operator in {"IN", "NOT IN"}:
                        timestamp_value = parsed_values
                    else:
                        timestamp_value = parsed_values[0] if isinstance(parsed_values, list) and parsed_values else parsed_values
                    
                    normalized_operator, timestamp_value = timestamp.migrate_legacy_timestamp_filter(normalized_operator, timestamp_value)
                    clause, params = timestamp.build_sql_clause(
                        quote_identifier(str(property_column)),
                        normalized_operator,
                        timestamp_value,
                        parameterized=True,
                    )
                    event_conditions.append(clause)
                    event_params.extend(params)
                elif normalized_operator in {"IN", "NOT IN"}:
                    placeholders = ", ".join(["?"] * len(parsed_values))
                    event_conditions.append(f"{quote_identifier(str(property_column))} {normalized_operator} ({placeholders})")
                    event_params.extend(parsed_values)
                else:
                    scalar_value = parsed_values[0] if isinstance(parsed_values, list) else parsed_values
                    event_conditions.append(f"{quote_identifier(str(property_column))} {normalized_operator} ?")
                    event_params.append(scalar_value)

            where_clause = " AND ".join(event_conditions)

            # Base condition CTE: users who DID perform the event
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

            if bool(is_negated):
                # Negated: all_users EXCEPT users who DID perform the event
                # For join_time, use the user's earliest event in the source table
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
            # Use UNION ALL and then GROUP BY user_id + MIN(event_time) downstream
            union_query = "\nUNION ALL\n".join(
                [f"SELECT user_id, event_time FROM c{index}" for index in range(len(conditions))]
            )
            cte_parts.append(f"combined_conditions AS ({union_query})")

        connection.execute(
            f"""
            INSERT INTO {target_table} (user_id, cohort_id, join_time)
            WITH {', '.join(cte_parts)}
            SELECT user_id, ?, MIN(event_time)
            FROM combined_conditions
            GROUP BY user_id
            ON CONFLICT (cohort_id, user_id) DO NOTHING
            """,
            [*query_params, cohort_id],
        )

    if join_type == "first_event":
        connection.execute(
            f"""
            UPDATE {target_table} cm
            SET join_time = sub.min_event_time
            FROM (
                SELECT user_id, MIN(event_time) AS min_event_time
                FROM {source_table}
                GROUP BY user_id
            ) sub
            WHERE cm.user_id = sub.user_id
              AND cm.cohort_id = ?
            """,
            [cohort_id],
        )

    # Fill the activity snapshot with row-level data for sequencing (Paths/Flows)
    snapshot_table = target_table.replace('membership', 'activity_snapshot')
    snapshot_source = source_table + "_raw" if source_table == "events_scoped" else ("events_raw" if source_table == "events_normalized" else source_table)
    
    # Check if the raw source actually exists
    raw_exists = connection.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{snapshot_source}'").fetchone()[0]
    if not raw_exists:
        snapshot_source = source_table

    connection.execute(
        f"""
        INSERT INTO {snapshot_table} (cohort_id, user_id, event_time, event_name, row_id, source_saved_id)
        SELECT ?, e.user_id, e.event_time, e.event_name, e.row_id, ?
        FROM {snapshot_source} e
        JOIN {target_table} cm
            ON cm.user_id = e.user_id
           AND cm.cohort_id = ?
        """,
        [cohort_id, source_saved_id, cohort_id],
    )



def rebuild_all_cohort_memberships(connection: duckdb.DuckDBPyConnection) -> None:
    from app.domains.cohorts.cohort_service import ensure_cohort_tables
    from app.utils.db_utils import to_dict, to_dicts
    ensure_cohort_tables(connection)

    res_scoped = connection.execute(
        "SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
    )
    scoped_exists = to_dict(res_scoped, res_scoped.fetchone()).get("table_count", 0)
    if not scoped_exists:
        return

    res = connection.execute("SELECT cohort_id FROM cohorts ORDER BY cohort_id")
    cohort_ids = [int(row["cohort_id"]) for row in to_dicts(res, res.fetchall())]
    
    connection.execute("DROP TABLE IF EXISTS cohort_membership_staging")
    connection.execute("""
        CREATE TEMP TABLE cohort_membership_staging (
            user_id TEXT,
            cohort_id INTEGER,
            join_time TIMESTAMP,
            UNIQUE(cohort_id, user_id)
        )
    """)
    
    connection.execute("DROP TABLE IF EXISTS cohort_activity_snapshot_staging")
    connection.execute("""
        CREATE TEMP TABLE cohort_activity_snapshot_staging (
            cohort_id INTEGER,
            user_id TEXT,
            event_time TIMESTAMP,
            event_name TEXT,
            row_id BIGINT,
            source_saved_id UUID
        )
    """)


    try:
        end_timer = time_block("cohort_rebuild")
        for cohort_id in cohort_ids:
            build_cohort_membership(connection, cohort_id, "events_scoped", target_table="cohort_membership_staging")

            res_size = connection.execute(
                "SELECT COUNT(*) as cohort_size FROM cohort_membership_staging WHERE cohort_id = ?",
                [cohort_id],
            )
            cohort_size = to_dict(res_size, res_size.fetchone()).get("cohort_size", 0)
            connection.execute(
                "UPDATE cohorts SET is_active = ? WHERE cohort_id = ?",
                [bool(cohort_size > 0), cohort_id],
            )
        end_timer(cohort_count=len(cohort_ids))
        
        connection.execute("BEGIN")
        connection.execute("DELETE FROM cohort_membership")
        connection.execute("""
            INSERT INTO cohort_membership (user_id, cohort_id, join_time)
            SELECT user_id, cohort_id, join_time FROM cohort_membership_staging
        """)
        connection.execute("DELETE FROM cohort_activity_snapshot")
        connection.execute("""
            INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name, row_id, source_saved_id)
            SELECT cohort_id, user_id, event_time, event_name, row_id, source_saved_id FROM cohort_activity_snapshot_staging
        """)

        connection.execute("COMMIT")
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        connection.execute("DROP TABLE IF EXISTS cohort_membership_staging")
        connection.execute("DROP TABLE IF EXISTS cohort_activity_snapshot_staging")
