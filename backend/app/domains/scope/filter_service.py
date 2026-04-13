"""
Short summary: applies filters to rebuild events_scoped.
"""
import duckdb
from datetime import date
from fastapi import HTTPException
from app.utils.perf import time_block
from app.utils.sql import sql_quote_value, quote_identifier, get_column_kind
from app.models.filter_models import ApplyFiltersRequest
from app.domains.scope.scope_metadata import upsert_dataset_scope
from app.utils.db_utils import to_dicts
from app.utils import timestamp

def build_where_clause(payload: ApplyFiltersRequest, column_types: dict[str, str]) -> str:
    clauses: list[str] = []

    if payload.date_range:
        start = sql_quote_value(payload.date_range.start)
        end = sql_quote_value(payload.date_range.end)
        clauses.append(f"event_time >= {start}::TIMESTAMP AND event_time < ({end}::DATE + INTERVAL 1 DAY)")

    supported = {"=", "!=", "<", ">", "<=", ">=", "IN", "NOT IN"}
    for filter_row in payload.filters:
        operator = filter_row.operator.upper()
        raw_type = column_types.get(filter_row.column, "TEXT")
        column_kind = get_column_kind(raw_type)
        column = quote_identifier(filter_row.column)
        if column_kind == "TIMESTAMP":
            clause, _ = timestamp.build_sql_clause(column, operator, filter_row.value, parameterized=False)
            clauses.append(clause)
            continue

        if operator not in supported:
            raise HTTPException(status_code=400, detail=f"Unsupported operator: {filter_row.operator}")

        if operator in {"IN", "NOT IN"}:
            if not isinstance(filter_row.value, list) or not filter_row.value:
                raise HTTPException(status_code=400, detail=f"Operator {operator} requires a non-empty array value")
            val_str = sql_quote_value(filter_row.value)
            clauses.append(f"{column} {operator} {val_str}")
        else:
            if isinstance(filter_row.value, list):
                raise HTTPException(status_code=400, detail=f"Operator {operator} requires a scalar value")
            val_str = sql_quote_value(filter_row.value)
            clauses.append(f"{column} {operator} {val_str}")

    if not clauses:
        return ""
    return f"WHERE {' AND '.join(clauses)}"


def initialize_scoped_dataset(connection: duckdb.DuckDBPyConnection) -> None:
    normalized_exists = connection.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = 'events_normalized'
        AND table_schema = 'main'
        """
    ).fetchone()[0]

    if not normalized_exists:
        return

    # Restoring system invariants: events_scoped matches events_normalized (aggregated)
    connection.execute("""
        CREATE OR REPLACE VIEW events_scoped AS
        SELECT * FROM events_normalized
    """)

    # events_scoped_raw is for sequencing (row-level)
    connection.execute("""
        CREATE OR REPLACE VIEW events_scoped_raw AS
        SELECT * FROM events_raw
    """)

    upsert_dataset_scope(connection, {"date_range": None, "filters": []})

    # Cohort activity still needs refresh
    from app.domains.cohorts.activity_service import refresh_cohort_activity
    refresh_cohort_activity(connection)


def apply_filters(connection: duckdb.DuckDBPyConnection, payload: ApplyFiltersRequest) -> dict[str, object]:
    from app.domains.cohorts.membership_builder import rebuild_all_cohort_memberships
    from app.domains.cohorts.activity_service import refresh_cohort_activity

    normalized_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized' AND table_schema = 'main'"
    ).fetchone()[0]

    if not normalized_exists:
        raise HTTPException(
            status_code=400,
            detail="No normalized events found. Upload and map columns first."
        )

    cursor = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'events_raw'
        """
    )
    known_columns = {row["column_name"] for row in to_dicts(cursor, cursor.fetchall())}

    t_cursor = connection.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'events_raw'
        """
    )
    column_types = {
        row["column_name"]: str(row["data_type"]).upper()
        for row in to_dicts(t_cursor, t_cursor.fetchall())
    }

    # ---------------- DATE VALIDATION ----------------

    if payload.date_range:
        try:
            start_date = date.fromisoformat(payload.date_range.start)
            end_date = date.fromisoformat(payload.date_range.end)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail="Invalid date range: start must be before or equal to end",
            ) from exc

        if start_date > end_date:
            raise HTTPException(
                status_code=400,
                detail="Invalid date range: start must be before or equal to end",
            )

    # ---------------- FILTER VALIDATION ----------------

    numeric_types = {
        "TINYINT","SMALLINT","INTEGER","BIGINT","HUGEINT",
        "UTINYINT","USMALLINT","UINTEGER","UBIGINT",
        "FLOAT","REAL","DOUBLE","DECIMAL"
    }

    text_allowed = {"=", "!=", "IN", "NOT IN"}
    numeric_allowed = {"=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN"}
    timestamp_allowed = timestamp.TIMESTAMP_OPERATORS

    for filter_row in payload.filters:
        if filter_row.column not in known_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown filter column: {filter_row.column}"
            )

        operator = filter_row.operator.upper()
        raw_type = column_types.get(filter_row.column, "TEXT")

        if "TIMESTAMP" in raw_type or raw_type == "DATE":
            migrated_operator, migrated_value = timestamp.migrate_legacy_timestamp_filter(operator, filter_row.value)
            filter_row.operator = migrated_operator
            filter_row.value = migrated_value
            operator = migrated_operator
            allowed_ops = timestamp_allowed
        elif raw_type in numeric_types or raw_type.startswith("DECIMAL"):
            allowed_ops = numeric_allowed
        else:
            allowed_ops = text_allowed

        if operator not in allowed_ops:
            column_kind = get_column_kind(raw_type)
            raise HTTPException(
                status_code=400,
                detail=f"Operator '{operator}' not allowed for column type {column_kind}",
            )

    # ---------------- BUILD FILTER SQL ----------------

    where_clause = build_where_clause(payload, column_types)

    end_timer = time_block("scope_rebuild")

    # Recreate filtered aggregated VIEW
    connection.execute(
        f"""
        CREATE OR REPLACE VIEW events_scoped AS
        SELECT *
        FROM events_normalized
        {where_clause}
        """
    )

    # Recreate filtered row-level VIEW
    connection.execute(
        f"""
        CREATE OR REPLACE VIEW events_scoped_raw AS
        SELECT *
        FROM events_raw
        {where_clause}
        """
    )

    counts = upsert_dataset_scope(
        connection,
        {
            "date_range": payload.date_range.model_dump() if payload.date_range else None,
            "filters": [f.model_dump() for f in payload.filters],
        },
    )

    rebuild_all_cohort_memberships(connection)
    refresh_cohort_activity(connection)

    end_timer(filtered_rows=counts["filtered_rows"])

    return {
        "status": "ok",
        **counts,
        "percentage": (
            counts["filtered_rows"] / counts["total_rows"] * 100.0
            if counts["total_rows"]
            else 0.0
        ),
    }
