"""
Short summary: rebuilds cohort memberships from conditions.
"""
import json
import duckdb
from fastapi import HTTPException
from app.utils.perf import time_block
from app.utils.sql import quote_identifier, get_column_type_map, get_column_kind
from app.utils import timestamp

def get_cohort_membership_sql_parts(
    connection: duckdb.DuckDBPyConnection,
    cohort_id: int,
    source_table: str,
    column_types: dict[str, str],
    cte_prefix: str = "c"
) -> tuple[list[str], list[object], str]:
    """
    Returns (cte_parts, query_params, combined_conditions_cte_name)
    """
    from app.utils.db_utils import to_dict, to_dicts
    res = connection.execute("SELECT logic_operator, join_type FROM cohorts WHERE cohort_id = ?", [cohort_id])
    cohort_row = to_dict(res, res.fetchone())
    if not cohort_row:
        raise HTTPException(status_code=404, detail=f"Cohort {cohort_id} not found")

    logic_operator = str(cohort_row.get("logic_operator") or "OR").upper()
    
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
    conditions = to_dicts(cursor, cursor.fetchall())

    cte_parts: list[str] = []
    query_params: list[object] = []

    if not conditions:
        # Default All Users logic
        cte_name = f"{cte_prefix}_all"
        cte_parts.append(
            f"""
            {cte_name} AS (
                SELECT user_id, MIN(event_time) AS event_time
                FROM {source_table}
                GROUP BY user_id
            )
            """
        )
        return cte_parts, query_params, cte_name

    has_negated = any(bool(row["is_negated"]) for row in conditions)
    if has_negated:
        cte_parts.append(
            f"{cte_prefix}_all_users AS (SELECT DISTINCT user_id FROM {source_table})"
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
        base_cte = f"{cte_prefix}_cond{index}_base"
        
        if int(min_event_count) <= 1:
            cte_parts.append(
                f"""
                {base_cte} AS (
                    SELECT user_id, MIN(event_time) AS event_time
                    FROM {source_table}
                    WHERE {where_clause}
                    GROUP BY user_id
                )
                """
            )
            query_params.extend(event_params)
        else:
            cte_parts.append(
                f"""
                {base_cte} AS (
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
            query_params.extend(event_params)
            query_params.append(min_event_count)

        final_cond_cte = f"{cte_prefix}_cond{index}"
        if bool(is_negated):
            cte_parts.append(
                f"""
                {final_cond_cte} AS (
                    SELECT au.user_id, MIN(e.event_time) AS event_time
                    FROM {cte_prefix}_all_users au
                    LEFT JOIN {base_cte} cb ON au.user_id = cb.user_id
                    LEFT JOIN {source_table} e ON au.user_id = e.user_id
                    WHERE cb.user_id IS NULL
                    GROUP BY au.user_id
                )
                """
            )
        else:
            cte_parts.append(f"{final_cond_cte} AS (SELECT user_id, event_time FROM {base_cte})")

    combined_cte = f"{cte_prefix}_combined"
    if logic_operator == "AND":
        if len(conditions) == 1:
            cte_parts.append(f"{combined_cte} AS (SELECT user_id, event_time FROM {cte_prefix}_cond0)")
        else:
            least_time_expression = ", ".join([f"{cte_prefix}_cond{index}.event_time" for index in range(len(conditions))])
            join_clauses = "\n".join(
                [f"INNER JOIN {cte_prefix}_cond{index} ON {cte_prefix}_cond0.user_id = {cte_prefix}_cond{index}.user_id" for index in range(1, len(conditions))]
            )
            cte_parts.append(
                f"""
                {combined_cte} AS (
                    SELECT {cte_prefix}_cond0.user_id, LEAST({least_time_expression}) AS event_time
                    FROM {cte_prefix}_cond0
                    {join_clauses}
                )
                """
            )
    else:
        union_query = "\nUNION ALL\n".join(
            [f"SELECT user_id, event_time FROM {cte_prefix}_cond{index}" for index in range(len(conditions))]
        )
        cte_parts.append(f"{combined_cte} AS (SELECT user_id, event_time FROM ({union_query}) t)")

    return cte_parts, query_params, combined_cte


def build_cohort_membership(
    connection: duckdb.DuckDBPyConnection,
    cohort_id: int,
    source_table: str,
    target_table: str = "cohort_membership",
) -> None:
    if source_table not in {"events_normalized", "events_scoped"}:
        raise ValueError("Unsupported source table")

    from app.utils.db_utils import to_dict
    res = connection.execute("SELECT join_type FROM cohorts WHERE cohort_id = ?", [cohort_id])
    cohort_row = to_dict(res, res.fetchone())
    if not cohort_row:
        raise HTTPException(status_code=404, detail="Cohort not found")

    join_type = str(cohort_row.get("join_type") or "condition_met")
    column_types = get_column_type_map(connection, source_table)

    if target_table == "cohort_membership":
        connection.execute("DELETE FROM cohort_membership WHERE cohort_id = ?", [cohort_id])
        connection.execute("DELETE FROM cohort_event_link WHERE cohort_id = ?", [cohort_id])

    cte_parts, query_params, combined_cte = get_cohort_membership_sql_parts(
        connection, cohort_id, source_table, column_types
    )

    connection.execute(
        f"""
        INSERT INTO {target_table} (user_id, cohort_id, join_time)
        WITH {', '.join(cte_parts)}
        SELECT user_id, ?, MIN(event_time)
        FROM {combined_cte}
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

    # Note: Link build is now strictly deferred to activity_service refresh



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
    if not cohort_ids:
        return

    connection.execute("DROP TABLE IF EXISTS cohort_membership_staging")
    connection.execute("""
        CREATE TEMP TABLE cohort_membership_staging (
            user_id TEXT,
            cohort_id INTEGER,
            join_time TIMESTAMP,
            UNIQUE(cohort_id, user_id)
        )
    """)
    
    column_types = get_column_type_map(connection, "events_scoped")
    batch_size = 10  # Process 10 cohorts at a time to keep SQL size manageable

    try:
        end_timer = time_block("cohort_rebuild")
        
        # Optimization: Pre-calculate the absolute minimum arrival time for every user in one pass.
        # This will be used directly for ALL cohorts with join_type = 'first_event'.
        connection.execute("DROP TABLE IF EXISTS user_arrival_times")
        connection.execute("""
            CREATE TEMP TABLE user_arrival_times AS
            SELECT user_id, MIN(event_time) as arrival_time
            FROM events_scoped
            GROUP BY user_id
        """)
        connection.execute("CREATE INDEX idx_arrival_user ON user_arrival_times (user_id)")

        for i in range(0, len(cohort_ids), batch_size):
            batch_cohort_ids = cohort_ids[i:i + batch_size]
            end_batch_timer = time_block(f"cohort_batch_{i//batch_size}")
            
            # Fetch join_type for each cohort in the batch
            placeholders = ", ".join(["?"] * len(batch_cohort_ids))
            jt_cursor = connection.execute(f"SELECT cohort_id, join_type FROM cohorts WHERE cohort_id IN ({placeholders})", batch_cohort_ids)
            join_types = {row[0]: str(row[1] or "condition_met") for row in jt_cursor.fetchall()}
            
            # Fetch condition count for each cohort to detect "All Users" fast-path
            cond_cursor = connection.execute(f"SELECT cohort_id, COUNT(*) as c FROM cohort_conditions WHERE cohort_id IN ({placeholders}) GROUP BY cohort_id", batch_cohort_ids)
            cond_counts = {row[0]: row[1] for row in cond_cursor.fetchall()}

            all_cte_parts: list[str] = []
            select_clauses: list[str] = []
            batch_cte_params: list[object] = []
            batch_cid_params: list[object] = []
            
            for index, cid in enumerate(batch_cohort_ids):
                cte_prefix = f"b{i}_h{index}"
                join_type = join_types.get(cid, "condition_met")
                has_conditions = cond_counts.get(cid, 0) > 0

                if not has_conditions:
                    # Fast-path: "All Users" - select from arrival times directly
                    select_clauses.append(f"SELECT user_id, ? AS cohort_id, arrival_time as join_time FROM user_arrival_times")
                    batch_cid_params.append(cid)
                    continue

                ctes, params, final_cte = get_cohort_membership_sql_parts(
                    connection, cid, "events_scoped", column_types, cte_prefix=cte_prefix
                )
                all_cte_parts.extend(ctes)
                batch_cte_params.extend(params)
                
                # Optimized Select: Direct join_time based on join_type
                if join_type == "first_event":
                    # Optimization: Join the qualifying user set with our pre-calculated arrival times
                    select_clauses.append(f"""
                        SELECT f.user_id, ? AS cohort_id, a.arrival_time as join_time
                        FROM {final_cte} f
                        JOIN user_arrival_times a ON f.user_id = a.user_id
                        GROUP BY f.user_id, a.arrival_time
                    """)
                else:
                    # Standard logic: Join on first qualifying event
                    select_clauses.append(f"SELECT user_id, ? AS cohort_id, MIN(event_time) as join_time FROM {final_cte} GROUP BY user_id")
                
                batch_cid_params.append(cid)

            if not select_clauses:
                end_batch_timer()
                continue

            combined_insert_sql = f"""
                INSERT INTO cohort_membership_staging (user_id, cohort_id, join_time)
                {f"WITH {', '.join(all_cte_parts)}" if all_cte_parts else ""}
                {' UNION ALL '.join(select_clauses)}
                ON CONFLICT (cohort_id, user_id) DO NOTHING
            """
            
            connection.execute(combined_insert_sql, batch_cte_params + batch_cid_params)
            end_batch_timer(batch_size=len(batch_cohort_ids))

        # 3. Update active status
        end_status_timer = time_block("membership_status_update")
        connection.execute("""
            UPDATE cohorts
            SET is_active = (sub.size > 0)
            FROM (
                SELECT c.cohort_id, COUNT(cms.user_id) as size
                FROM cohorts c
                LEFT JOIN cohort_membership_staging cms ON c.cohort_id = cms.cohort_id
                GROUP BY c.cohort_id
            ) sub
            WHERE cohorts.cohort_id = sub.cohort_id
        """)
        end_status_timer()

        end_timer(cohort_count=len(cohort_ids))

        # 4. Swap staging into the real cohort_membership table
        end_swap_timer = time_block("membership_swap")
        connection.execute("""
            CREATE OR REPLACE TABLE cohort_membership AS 
            SELECT user_id, cohort_id, join_time FROM cohort_membership_staging
        """)
        
        from app.domains.cohorts.cohort_service import recreate_membership_indexes
        recreate_membership_indexes(connection)
        end_swap_timer()

    except Exception:
        raise
    finally:
        connection.execute("DROP TABLE IF EXISTS cohort_membership_staging")
        connection.execute("DROP TABLE IF EXISTS user_arrival_times")
