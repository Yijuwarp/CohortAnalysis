"""
Short summary: rebuilds cohort memberships from conditions.
"""
import json
import duckdb
from fastapi import HTTPException
from app.utils.perf import time_block
from app.utils.sql import quote_identifier

def build_cohort_membership(
    connection: duckdb.DuckDBPyConnection,
    cohort_id: int,
    source_table: str,
) -> None:
    if source_table not in {"events_normalized", "events_scoped"}:
        raise ValueError("Unsupported source table")

    cohort_row = connection.execute(
        "SELECT logic_operator, join_type FROM cohorts WHERE cohort_id = ?",
        [cohort_id],
    ).fetchone()
    if cohort_row is None:
        raise HTTPException(status_code=404, detail="Cohort not found")

    logic_operator = str(cohort_row[0] or "OR").upper()
    join_type = str(cohort_row[1] or "condition_met")

    connection.execute("DELETE FROM cohort_membership WHERE cohort_id = ?", [cohort_id])
    connection.execute("DELETE FROM cohort_activity_snapshot WHERE cohort_id = ?", [cohort_id])
    conditions = connection.execute(
        """
        SELECT event_name, min_event_count, property_column, property_operator, property_values
        FROM cohort_conditions
        WHERE cohort_id = ?
        ORDER BY condition_id
        """,
        [cohort_id],
    ).fetchall()

    if not conditions:
        connection.execute(
            f"""
            INSERT INTO cohort_membership (user_id, cohort_id, join_time)
            SELECT user_id, ?, MIN(event_time)
            FROM {source_table}
            GROUP BY user_id
            """,
            [cohort_id],
        )
    else:
        cte_parts: list[str] = []
        query_params: list[object] = []
        for index, (event_name, min_event_count, property_column, property_operator, property_values) in enumerate(conditions):
            event_conditions = ["event_name = ?"]
            event_params: list[object] = [event_name]

            if property_column and property_operator and property_values is not None:
                from app.domains.cohorts.cohort_service import normalize_values
                parsed_values = normalize_values(property_values)
                normalized_operator = str(property_operator).upper()
                if normalized_operator in {"IN", "NOT IN"}:
                    placeholders = ", ".join(["?"] * len(parsed_values))
                    event_conditions.append(f"{quote_identifier(str(property_column))} {normalized_operator} ({placeholders})")
                    event_params.extend(parsed_values)
                else:
                    scalar_value = parsed_values[0] if isinstance(parsed_values, list) else parsed_values
                    event_conditions.append(f"{quote_identifier(str(property_column))} {normalized_operator} ?")
                    event_params.append(scalar_value)

            where_clause = " AND ".join(event_conditions)
            cte_parts.append(
                f"""
                c{index} AS (
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

        connection.execute(
            f"""
            INSERT INTO cohort_membership (user_id, cohort_id, join_time)
            WITH {', '.join(cte_parts)}
            SELECT user_id, ?, MIN(event_time)
            FROM combined_conditions
            GROUP BY user_id
            """,
            [*query_params, cohort_id],
        )

    if join_type == "first_event":
        connection.execute(
            f"""
            UPDATE cohort_membership cm
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

    connection.execute(
        f"""
        INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name)
        SELECT ?, e.user_id, e.event_time, e.event_name
        FROM {source_table} e
        JOIN cohort_membership cm
            ON cm.user_id = e.user_id
           AND cm.cohort_id = ?
        """,
        [cohort_id, cohort_id],
    )


def rebuild_all_cohort_memberships(connection: duckdb.DuckDBPyConnection) -> None:
    from app.domains.cohorts.cohort_service import ensure_cohort_tables

    ensure_cohort_tables(connection)

    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
    ).fetchone()[0]
    if not scoped_exists:
        return

    cohort_ids = [int(row[0]) for row in connection.execute("SELECT cohort_id FROM cohorts ORDER BY cohort_id").fetchall()]
    end_timer = time_block("cohort_rebuild")
    for cohort_id in cohort_ids:
        connection.execute("DELETE FROM cohort_membership WHERE cohort_id = ?", [cohort_id])
        connection.execute("DELETE FROM cohort_activity_snapshot WHERE cohort_id = ?", [cohort_id])
        build_cohort_membership(connection, cohort_id, "events_scoped")

        cohort_size = int(
            connection.execute(
                "SELECT COUNT(*) FROM cohort_membership WHERE cohort_id = ?",
                [cohort_id],
            ).fetchone()[0]
        )
        connection.execute(
            "UPDATE cohorts SET is_active = ? WHERE cohort_id = ?",
            [bool(cohort_size > 0), cohort_id],
        )
    end_timer(cohort_count=len(cohort_ids))
