import json
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator

app = FastAPI(title="Behavioral Cohort Analysis API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_PATH = Path(__file__).resolve().parent.parent / "cohort_analysis.duckdb"


class ColumnMappingRequest(BaseModel):
    user_id_column: str
    event_name_column: str
    event_time_column: str


class ScopeFilter(BaseModel):
    column: str
    operator: str
    value: str | float | int | list[str] | list[float] | list[int]


class CohortCondition(BaseModel):
    event_name: str
    min_event_count: int = Field(ge=1)
    property_filter: ScopeFilter | None = None

    @field_validator("property_filter")
    @classmethod
    def validate_property_filter(cls, value: ScopeFilter | None) -> ScopeFilter | None:
        if value is None:
            return value
        if value.value is None or (isinstance(value.value, str) and value.value == ""):
            raise ValueError("property_filter.value is required")
        if isinstance(value.value, list):
            raise ValueError("property_filter.value must be a scalar")
        return value


class CreateCohortRequest(BaseModel):
    name: str = Field(min_length=1)
    logic_operator: str
    conditions: list[CohortCondition] = Field(max_length=5)

    @field_validator("logic_operator")
    @classmethod
    def validate_logic_operator(cls, value: str) -> str:
        normalized = value.upper()
        if normalized not in {"AND", "OR"}:
            raise ValueError("logic_operator must be either AND or OR")
        return normalized


class DateRange(BaseModel):
    start: str
    end: str


class ApplyFiltersRequest(BaseModel):
    date_range: DateRange | None = None
    filters: list[ScopeFilter] = Field(default_factory=list)


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DATABASE_PATH))


def quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


NUMERIC_TYPES = {
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "FLOAT",
    "REAL",
    "DOUBLE",
    "DECIMAL",
}
TEXT_ALLOWED_OPERATORS = {"=", "!="}
NUMERIC_ALLOWED_OPERATORS = {"=", "!=", ">", "<", ">=", "<="}
TIMESTAMP_ALLOWED_OPERATORS = {"=", "!=", ">", "<", ">=", "<="}


def get_column_type_map(connection: duckdb.DuckDBPyConnection, table_name: str) -> dict[str, str]:
    return {
        row[0]: str(row[1]).upper()
        for row in connection.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = ?
            """,
            [table_name],
        ).fetchall()
    }


def get_column_kind(data_type: str) -> str:
    if "TIMESTAMP" in data_type or data_type == "DATE":
        return "TIMESTAMP"
    if data_type in NUMERIC_TYPES or data_type.startswith("DECIMAL"):
        return "NUMERIC"
    return "TEXT"


def get_allowed_operators(column_kind: str) -> set[str]:
    if column_kind == "TIMESTAMP":
        return TIMESTAMP_ALLOWED_OPERATORS
    if column_kind == "NUMERIC":
        return NUMERIC_ALLOWED_OPERATORS
    return TEXT_ALLOWED_OPERATORS


def validate_cohort_conditions(
    connection: duckdb.DuckDBPyConnection,
    source_table: str,
    conditions: list[CohortCondition],
) -> None:
    column_types = get_column_type_map(connection, source_table)
    if not column_types:
        raise HTTPException(status_code=400, detail="No normalized events found. Upload a CSV and map columns first.")

    for condition in conditions:
        property_filter = condition.property_filter
        if property_filter is None:
            continue

        if property_filter.column not in column_types:
            raise HTTPException(status_code=400, detail=f"Unknown filter column: {property_filter.column}")

        operator = property_filter.operator.upper()
        column_kind = get_column_kind(column_types[property_filter.column])
        allowed_ops = get_allowed_operators(column_kind)
        if operator not in allowed_ops:
            raise HTTPException(
                status_code=400,
                detail=f"Operator '{operator}' not allowed for column type {column_kind}",
            )


def ensure_cohort_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cohorts (
            cohort_id INTEGER PRIMARY KEY,
            name TEXT,
            logic_operator TEXT,
            is_active BOOLEAN DEFAULT TRUE
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
    expected_condition_columns = {
        "condition_id",
        "cohort_id",
        "event_name",
        "min_event_count",
        "property_column",
        "property_operator",
        "property_value",
    }
    if existing_condition_columns and existing_condition_columns != expected_condition_columns:
        connection.execute("DROP TABLE cohort_conditions")
        connection.execute("DROP SEQUENCE IF EXISTS cohort_condition_id_sequence")

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cohort_conditions (
            condition_id BIGINT PRIMARY KEY,
            cohort_id BIGINT NOT NULL,
            event_name VARCHAR NOT NULL,
            min_event_count INTEGER NOT NULL,
            property_column VARCHAR,
            property_operator VARCHAR,
            property_value VARCHAR
        )
        """
    )
    connection.execute("CREATE SEQUENCE IF NOT EXISTS cohort_condition_id_sequence START 1")

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
    if "is_active" not in existing_columns:
        connection.execute("ALTER TABLE cohorts ADD COLUMN is_active BOOLEAN DEFAULT TRUE")

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


def ensure_scope_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dataset_scope (
            id INTEGER PRIMARY KEY,
            filters_json TEXT,
            total_rows INTEGER,
            filtered_rows INTEGER,
            updated_at TIMESTAMP
        )
        """
    )


def create_scoped_indexes(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("CREATE INDEX IF NOT EXISTS idx_events_scoped_user_id ON events_scoped(user_id)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_events_scoped_event_name ON events_scoped(event_name)")


def initialize_scoped_dataset(connection: duckdb.DuckDBPyConnection) -> None:
    normalized_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized'"
    ).fetchone()[0]
    if not normalized_exists:
        return

    connection.execute("CREATE OR REPLACE TABLE events_scoped AS SELECT * FROM events_normalized")
    create_scoped_indexes(connection)
    upsert_dataset_scope(connection, {"date_range": None, "filters": []})
    refresh_cohort_activity(connection)


def upsert_dataset_scope(connection: duckdb.DuckDBPyConnection, payload: dict[str, object]) -> dict[str, int]:
    ensure_scope_tables(connection)
    total_rows = int(connection.execute("SELECT COUNT(*) FROM events_normalized").fetchone()[0])
    filtered_rows = int(connection.execute("SELECT COUNT(*) FROM events_scoped").fetchone()[0])

    connection.execute(
        """
        INSERT INTO dataset_scope (id, filters_json, total_rows, filtered_rows, updated_at)
        VALUES (1, ?, ?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET
            filters_json = excluded.filters_json,
            total_rows = excluded.total_rows,
            filtered_rows = excluded.filtered_rows,
            updated_at = excluded.updated_at
        """,
        [
            json.dumps(payload),
            total_rows,
            filtered_rows,
            datetime.now(timezone.utc),
        ],
    )
    return {"total_rows": total_rows, "filtered_rows": filtered_rows}


def refresh_cohort_activity(connection: duckdb.DuckDBPyConnection) -> None:
    ensure_cohort_tables(connection)
    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped'"
    ).fetchone()[0]
    if not scoped_exists:
        return

    activity_rows = connection.execute(
        """
        SELECT
            c.cohort_id,
            COUNT(DISTINCT es.user_id) AS active_members
        FROM cohorts c
        LEFT JOIN cohort_membership cm ON c.cohort_id = cm.cohort_id
        LEFT JOIN events_scoped es ON cm.user_id = es.user_id
        GROUP BY c.cohort_id
        """
    ).fetchall()

    if not activity_rows:
        return

    connection.executemany(
        "UPDATE cohorts SET is_active = ? WHERE cohort_id = ?",
        [(bool(active_members > 0), int(cohort_id)) for cohort_id, active_members in activity_rows],
    )


def build_cohort_membership(
    connection: duckdb.DuckDBPyConnection,
    cohort_id: int,
    source_table: str,
) -> None:
    if source_table not in {"events_normalized", "events_scoped"}:
        raise ValueError("Unsupported source table")

    cohort_row = connection.execute(
        "SELECT logic_operator FROM cohorts WHERE cohort_id = ?",
        [cohort_id],
    ).fetchone()
    if cohort_row is None:
        raise HTTPException(status_code=404, detail="Cohort not found")

    logic_operator = str(cohort_row[0] or "OR").upper()
    conditions = connection.execute(
        """
        SELECT event_name, min_event_count, property_column, property_operator, property_value
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
        for index, (event_name, min_event_count, property_column, property_operator, property_value) in enumerate(conditions):
            event_conditions = ["event_name = ?"]
            event_params: list[object] = [event_name]

            if property_column and property_operator and property_value is not None:
                event_conditions.append(f"{quote_identifier(str(property_column))} {str(property_operator).upper()} ?")
                event_params.append(property_value)

            where_clause = " AND ".join(event_conditions)
            cte_parts.append(
                f"""
                c{index} AS (
                    SELECT user_id, event_time
                    FROM (
                        SELECT
                            user_id,
                            event_time,
                            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time) AS rn
                        FROM {source_table}
                        WHERE {where_clause}
                    ) t
                    WHERE rn = ?
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
    ensure_cohort_tables(connection)

    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped'"
    ).fetchone()[0]
    if not scoped_exists:
        return

    cohort_ids = [int(row[0]) for row in connection.execute("SELECT cohort_id FROM cohorts ORDER BY cohort_id").fetchall()]
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


def create_all_users_cohort(connection: duckdb.DuckDBPyConnection) -> None:
    ensure_cohort_tables(connection)

    existing = connection.execute("SELECT cohort_id FROM cohorts WHERE name = 'All Users'").fetchone()
    if existing:
        return

    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped'"
    ).fetchone()[0]
    source_table = "events_scoped" if scoped_exists else "events_normalized"

    cohort_id = connection.execute(
        """
        INSERT INTO cohorts (cohort_id, name, logic_operator, is_active)
        VALUES (nextval('cohorts_id_sequence'), 'All Users', 'OR', TRUE)
        RETURNING cohort_id
        """
    ).fetchone()[0]

    build_cohort_membership(connection, int(cohort_id), source_table)


def build_where_clause(payload: ApplyFiltersRequest) -> tuple[str, list[object]]:
    clauses: list[str] = []
    params: list[object] = []

    if payload.date_range:
        clauses.append("event_time >= ?::TIMESTAMP AND event_time < (?::DATE + INTERVAL 1 DAY)")
        params.extend([payload.date_range.start, payload.date_range.end])

    supported = {"=", "!=", "<", ">", "<=", ">=", "IN", "NOT IN"}
    for filter_row in payload.filters:
        operator = filter_row.operator.upper()
        if operator not in supported:
            raise HTTPException(status_code=400, detail=f"Unsupported operator: {filter_row.operator}")

        column = quote_identifier(filter_row.column)
        if operator in {"IN", "NOT IN"}:
            if not isinstance(filter_row.value, list) or not filter_row.value:
                raise HTTPException(status_code=400, detail=f"Operator {operator} requires a non-empty array value")
            placeholders = ", ".join(["?"] * len(filter_row.value))
            clauses.append(f"{column} {operator} ({placeholders})")
            params.extend(filter_row.value)
        else:
            if isinstance(filter_row.value, list):
                raise HTTPException(status_code=400, detail=f"Operator {operator} requires a scalar value")
            clauses.append(f"{column} {operator} ?")
            params.append(filter_row.value)

    if not clauses:
        return "", []
    return f"WHERE {' AND '.join(clauses)}", params


@app.get("/")
def read_root() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)) -> dict[str, int | list[str]]:
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    try:
        dataframe = pd.read_csv(file.file, keep_default_na=False, na_values=[""])
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid CSV file") from exc
    finally:
        await file.close()

    if len(dataframe.columns) < 3:
        raise HTTPException(status_code=400, detail="CSV must contain at least 3 columns")

    connection = get_connection()
    try:
        connection.register("uploaded_events", dataframe)
        connection.execute("CREATE OR REPLACE TABLE events AS SELECT * FROM uploaded_events")
    finally:
        connection.close()

    return {
        "rows_imported": int(len(dataframe)),
        "columns": [str(column) for column in dataframe.columns.tolist()],
    }


@app.post("/map-columns")
def map_columns(mapping: ColumnMappingRequest) -> dict[str, str | int]:
    connection = get_connection()
    try:
        table_exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events'"
        ).fetchone()[0]
        if not table_exists:
            raise HTTPException(status_code=400, detail="No uploaded CSV found. Upload a CSV first.")

        existing_columns = [
            row[0]
            for row in connection.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'events'
                ORDER BY ordinal_position
                """
            ).fetchall()
        ]

        requested_columns = {
            mapping.user_id_column,
            mapping.event_name_column,
            mapping.event_time_column,
        }
        missing_columns = sorted(requested_columns - set(existing_columns))
        if missing_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Mapped columns not found in uploaded CSV: {', '.join(missing_columns)}",
            )

        column_defs: list[str] = []
        select_defs: list[str] = []
        for column in existing_columns:
            col_ref = quote_identifier(column)
            col_alias = quote_identifier(column)
            if column == mapping.event_time_column:
                column_defs.append(f"{col_alias} TIMESTAMP")
                select_defs.append(f"CAST({col_ref} AS TIMESTAMP) AS {col_alias}")
            elif column in {mapping.user_id_column, mapping.event_name_column}:
                column_defs.append(f"{col_alias} TEXT")
                select_defs.append(f"CAST({col_ref} AS TEXT) AS {col_alias}")
            else:
                numeric_probe = connection.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM events
                    WHERE {col_ref} IS NOT NULL
                      AND CAST({col_ref} AS VARCHAR) <> ''
                      AND TRY_CAST({col_ref} AS DOUBLE) IS NULL
                    """
                ).fetchone()[0]
                if numeric_probe == 0:
                    column_defs.append(f"{col_alias} DOUBLE")
                    select_defs.append(f"TRY_CAST({col_ref} AS DOUBLE) AS {col_alias}")
                else:
                    column_defs.append(f"{col_alias} TEXT")
                    select_defs.append(f"CAST({col_ref} AS TEXT) AS {col_alias}")

        connection.execute("DROP TABLE IF EXISTS events_normalized")
        connection.execute(f"CREATE TABLE events_normalized ({', '.join(column_defs)})")
        connection.execute(
            f"INSERT INTO events_normalized SELECT {', '.join(select_defs)} FROM events"
        )

        # Canonical aliases used by analytics/cohort logic.
        connection.execute(
            f"ALTER TABLE events_normalized RENAME COLUMN {quote_identifier(mapping.user_id_column)} TO user_id"
        )
        connection.execute(
            f"ALTER TABLE events_normalized RENAME COLUMN {quote_identifier(mapping.event_name_column)} TO event_name"
        )
        connection.execute(
            f"ALTER TABLE events_normalized RENAME COLUMN {quote_identifier(mapping.event_time_column)} TO event_time"
        )

        ensure_cohort_tables(connection)
        ensure_scope_tables(connection)

        connection.execute("DELETE FROM cohort_membership")
        connection.execute("DELETE FROM cohort_activity_snapshot")
        connection.execute("DELETE FROM cohort_conditions")
        connection.execute("DELETE FROM cohorts")
        initialize_scoped_dataset(connection)
        create_all_users_cohort(connection)
        refresh_cohort_activity(connection)

        row_count = int(connection.execute("SELECT COUNT(*) FROM events_normalized").fetchone()[0])
    except duckdb.ConversionException as exc:
        raise HTTPException(status_code=400, detail="Failed to convert event_time column to TIMESTAMP") from exc
    finally:
        connection.close()

    return {"status": "normalized", "row_count": row_count}


@app.post("/apply-filters")
def apply_filters(payload: ApplyFiltersRequest) -> dict[str, object]:
    connection = get_connection()
    try:
        normalized_exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized'"
        ).fetchone()[0]
        if not normalized_exists:
            raise HTTPException(status_code=400, detail="No normalized events found. Upload and map columns first.")

        known_columns = {
            row[0]
            for row in connection.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'events_normalized'
                """
            ).fetchall()
        }
        column_types = {
            row[0]: str(row[1]).upper()
            for row in connection.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'events_normalized'
                """
            ).fetchall()
        }

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

        numeric_types = {
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
            "FLOAT",
            "REAL",
            "DOUBLE",
            "DECIMAL",
        }
        text_allowed = {"=", "!=", "IN", "NOT IN"}
        numeric_allowed = {"=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN"}
        timestamp_allowed = {"=", "!=", ">", "<", ">=", "<="}

        for filter_row in payload.filters:
            if filter_row.column not in known_columns:
                raise HTTPException(status_code=400, detail=f"Unknown filter column: {filter_row.column}")

            operator = filter_row.operator.upper()
            raw_type = column_types.get(filter_row.column, "TEXT")
            if "TIMESTAMP" in raw_type or raw_type == "DATE":
                column_kind = "TIMESTAMP"
                allowed_ops = timestamp_allowed
            elif raw_type in numeric_types or raw_type.startswith("DECIMAL"):
                column_kind = "NUMERIC"
                allowed_ops = numeric_allowed
            else:
                column_kind = "TEXT"
                allowed_ops = text_allowed

            if operator not in allowed_ops:
                raise HTTPException(
                    status_code=400,
                    detail=f"Operator '{operator}' not allowed for column type {column_kind}",
                )

        where_clause, params = build_where_clause(payload)
        connection.execute("DROP TABLE IF EXISTS events_scoped")
        connection.execute(
            f"CREATE TABLE events_scoped AS SELECT * FROM events_normalized {where_clause}",
            params,
        )
        create_scoped_indexes(connection)

        counts = upsert_dataset_scope(
            connection,
            {
                "date_range": payload.date_range.model_dump() if payload.date_range else None,
                "filters": [filter_row.model_dump() for filter_row in payload.filters],
            },
        )
        rebuild_all_cohort_memberships(connection)
        refresh_cohort_activity(connection)

        return {
            "status": "ok",
            **counts,
            "percentage": (counts["filtered_rows"] / counts["total_rows"] * 100.0)
            if counts["total_rows"]
            else 0.0,
        }
    finally:
        connection.close()


@app.get("/scope")
def get_scope() -> dict[str, object]:
    connection = get_connection()
    try:
        ensure_scope_tables(connection)
        row = connection.execute(
            "SELECT filters_json, total_rows, filtered_rows, updated_at FROM dataset_scope WHERE id = 1"
        ).fetchone()
        if row is None:
            return {
                "filters_json": {"date_range": None, "filters": []},
                "total_rows": 0,
                "filtered_rows": 0,
                "updated_at": None,
            }

        return {
            "filters_json": json.loads(row[0]) if row[0] else {"date_range": None, "filters": []},
            "total_rows": int(row[1] or 0),
            "filtered_rows": int(row[2] or 0),
            "updated_at": row[3].isoformat() if row[3] else None,
        }
    finally:
        connection.close()


@app.get("/columns")
def get_columns() -> dict[str, list[dict[str, str | None]]]:
    connection = get_connection()
    try:
        exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized'"
        ).fetchone()[0]
        if not exists:
            return {"columns": []}

        rows = connection.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'events_normalized'
            ORDER BY ordinal_position
            """
        ).fetchall()
        role_map = {
            "user_id": "user id",
            "event_name": "event name",
            "event_time": "event time",
        }
        payload = [
            {
                "name": str(name),
                "role": role_map.get(str(name)),
                "data_type": str(data_type).upper(),
            }
            for name, data_type in rows
        ]
        return {"columns": payload}
    finally:
        connection.close()


@app.get("/column-values")
def get_column_values(column: str = Query(..., min_length=1)) -> dict[str, list[str] | int]:
    connection = get_connection()
    try:
        normalized_exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized'"
        ).fetchone()[0]
        if not normalized_exists:
            return {"values": [], "total_distinct": 0}

        known_columns = {
            row[0]
            for row in connection.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'events_normalized'
                """
            ).fetchall()
        }
        if column not in known_columns:
            raise HTTPException(status_code=400, detail=f"Unknown column: {column}")

        column_ref = quote_identifier(column)
        rows = connection.execute(
            f"""
            SELECT DISTINCT {column_ref}
            FROM events_normalized
            WHERE {column_ref} IS NOT NULL
            ORDER BY 1
            LIMIT 100
            """
        ).fetchall()
        total_distinct = int(
            connection.execute(
                f"SELECT COUNT(DISTINCT {column_ref}) FROM events_normalized WHERE {column_ref} IS NOT NULL"
            ).fetchone()[0]
        )
        return {
            "values": [str(value) for (value,) in rows],
            "total_distinct": total_distinct,
        }
    finally:
        connection.close()


@app.get("/date-range")
def get_date_range() -> dict[str, str | None]:
    connection = get_connection()
    try:
        normalized_exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized'"
        ).fetchone()[0]
        if not normalized_exists:
            return {"min_date": None, "max_date": None}

        min_event_time, max_event_time = connection.execute(
            "SELECT MIN(event_time), MAX(event_time) FROM events_normalized"
        ).fetchone()

        return {
            "min_date": min_event_time.date().isoformat() if min_event_time else None,
            "max_date": max_event_time.date().isoformat() if max_event_time else None,
        }
    finally:
        connection.close()


@app.post("/cohorts")
def create_cohort(payload: CreateCohortRequest) -> dict[str, int]:
    connection = get_connection()
    try:
        normalized_exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized'"
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
            INSERT INTO cohorts (cohort_id, name, logic_operator, is_active)
            VALUES (nextval('cohorts_id_sequence'), ?, ?, TRUE)
            RETURNING cohort_id
            """,
            [payload.name, payload.logic_operator],
        ).fetchone()[0]

        for condition in payload.conditions:
            property_column = None
            property_operator = None
            property_value = None
            if condition.property_filter:
                property_column = condition.property_filter.column
                property_operator = condition.property_filter.operator.upper()
                property_value = str(condition.property_filter.value)

            connection.execute(
                """
                INSERT INTO cohort_conditions (
                    condition_id,
                    cohort_id,
                    event_name,
                    min_event_count,
                    property_column,
                    property_operator,
                    property_value
                )
                VALUES (nextval('cohort_condition_id_sequence'), ?, ?, ?, ?, ?, ?)
                """,
                [
                    cohort_id,
                    condition.event_name,
                    condition.min_event_count,
                    property_column,
                    property_operator,
                    property_value,
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
    finally:
        connection.close()

    return {"cohort_id": int(cohort_id), "users_joined": users_joined}


@app.get("/cohorts")
def list_cohorts() -> dict[str, list[dict[str, object]]]:
    connection = get_connection()
    try:
        ensure_cohort_tables(connection)
        rows = connection.execute(
            """
            SELECT
                c.cohort_id,
                c.name,
                c.is_active,
                c.logic_operator,
                cc.event_name,
                cc.min_event_count,
                cc.property_column,
                cc.property_operator,
                cc.property_value
            FROM cohorts c
            LEFT JOIN cohort_conditions cc ON c.cohort_id = cc.cohort_id
            ORDER BY c.cohort_id, cc.condition_id
            """
        ).fetchall()

        cohorts: dict[int, dict[str, object]] = {}
        for cohort_id, name, is_active, logic_operator, event_name, min_event_count, property_column, property_operator, property_value in rows:
            cohort_id = int(cohort_id)
            if cohort_id not in cohorts:
                cohorts[cohort_id] = {
                    "cohort_id": cohort_id,
                    "cohort_name": str(name),
                    "is_active": bool(is_active),
                    "logic_operator": str(logic_operator or "AND"),
                    "conditions": [],
                }

            if event_name is not None and min_event_count is not None:
                property_filter = None
                if property_column and property_operator and property_value is not None:
                    property_filter = {
                        "column": str(property_column),
                        "operator": str(property_operator),
                        "value": str(property_value),
                    }
                cohorts[cohort_id]["conditions"].append(
                    {
                        "event_name": str(event_name),
                        "min_event_count": int(min_event_count),
                        "property_filter": property_filter,
                    }
                )

        return {
            "cohorts": sorted(cohorts.values(), key=lambda cohort: cohort["cohort_id"])
        }
    finally:
        connection.close()


@app.put("/cohorts/{cohort_id}")
def update_cohort(cohort_id: int, payload: CreateCohortRequest) -> dict[str, int]:
    connection = get_connection()
    try:
        ensure_cohort_tables(connection)
        scoped_exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped'"
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
            "UPDATE cohorts SET name = ?, logic_operator = ? WHERE cohort_id = ?",
            [payload.name, payload.logic_operator, cohort_id],
        )
        connection.execute("DELETE FROM cohort_conditions WHERE cohort_id = ?", [cohort_id])

        for condition in payload.conditions:
            property_column = None
            property_operator = None
            property_value = None
            if condition.property_filter:
                property_column = condition.property_filter.column
                property_operator = condition.property_filter.operator.upper()
                property_value = str(condition.property_filter.value)

            connection.execute(
                """
                INSERT INTO cohort_conditions (
                    condition_id,
                    cohort_id,
                    event_name,
                    min_event_count,
                    property_column,
                    property_operator,
                    property_value
                )
                VALUES (nextval('cohort_condition_id_sequence'), ?, ?, ?, ?, ?, ?)
                """,
                [
                    cohort_id,
                    condition.event_name,
                    condition.min_event_count,
                    property_column,
                    property_operator,
                    property_value,
                ],
            )

        connection.execute("DELETE FROM cohort_membership WHERE cohort_id = ?", [cohort_id])
        connection.execute("DELETE FROM cohort_activity_snapshot WHERE cohort_id = ?", [cohort_id])

        build_cohort_membership(connection, cohort_id, source_table)
        refresh_cohort_activity(connection)

        users_joined = int(
            connection.execute(
                "SELECT COUNT(*) FROM cohort_membership WHERE cohort_id = ?",
                [cohort_id],
            ).fetchone()[0]
        )
    finally:
        connection.close()

    return {"cohort_id": int(cohort_id), "users_joined": users_joined}


@app.delete("/cohorts/{cohort_id}")
def delete_cohort(cohort_id: int) -> dict[str, int | bool]:
    connection = get_connection()
    try:
        ensure_cohort_tables(connection)

        cohort_row = connection.execute(
            "SELECT name FROM cohorts WHERE cohort_id = ?",
            [cohort_id],
        ).fetchone()
        if cohort_row is None:
            raise HTTPException(status_code=404, detail="Cohort not found")
        if cohort_row[0] == "All Users":
            raise HTTPException(status_code=400, detail="All Users cohort cannot be deleted")

        connection.execute("DELETE FROM cohort_conditions WHERE cohort_id = ?", [cohort_id])
        connection.execute("DELETE FROM cohort_activity_snapshot WHERE cohort_id = ?", [cohort_id])
        connection.execute("DELETE FROM cohort_membership WHERE cohort_id = ?", [cohort_id])
        connection.execute("DELETE FROM cohorts WHERE cohort_id = ?", [cohort_id])
    finally:
        connection.close()

    return {"deleted": True, "cohort_id": int(cohort_id)}


def build_active_cohort_base(connection: duckdb.DuckDBPyConnection) -> tuple[list[tuple[int, str]], dict[int, int]]:
    cohorts = connection.execute(
        """
        SELECT cohort_id, name
        FROM cohorts
        WHERE is_active = TRUE
        ORDER BY cohort_id
        """
    ).fetchall()
    cohort_sizes = {
        int(row[0]): int(row[1])
        for row in connection.execute(
            """
            SELECT c.cohort_id, COUNT(DISTINCT cm.user_id) AS cohort_size
            FROM cohorts c
            LEFT JOIN cohort_membership cm ON c.cohort_id = cm.cohort_id
            LEFT JOIN events_scoped es ON cm.user_id = es.user_id
            WHERE c.is_active = TRUE AND es.user_id IS NOT NULL
            GROUP BY c.cohort_id
            """
        ).fetchall()
    }
    return cohorts, cohort_sizes


def fetch_retention_active_rows(
    connection: duckdb.DuckDBPyConnection,
    max_day: int,
    retention_event: str | None,
) -> list[tuple[int, int, int]]:
    if retention_event and retention_event != "any":
        return connection.execute(
            """
            WITH activity_deltas AS (
                SELECT
                    cm.cohort_id,
                    cm.user_id,
                    DATE_DIFF('day', cm.join_time::DATE, cas.event_time::DATE) AS day_number
                FROM cohort_membership cm
                JOIN cohort_activity_snapshot cas
                  ON cm.cohort_id = cas.cohort_id
                 AND cm.user_id = cas.user_id
                JOIN events_scoped es
                  ON es.user_id = cas.user_id
                 AND es.event_time = cas.event_time
                 AND es.event_name = cas.event_name
                WHERE es.event_name = ?
                  AND DATE_DIFF('day', cm.join_time::DATE, cas.event_time::DATE) BETWEEN 0 AND ?
            )
            SELECT cohort_id, day_number, COUNT(DISTINCT user_id) AS active_users
            FROM activity_deltas
            GROUP BY cohort_id, day_number
            """,
            [retention_event, max_day],
        ).fetchall()

    return connection.execute(
        """
        WITH activity_deltas AS (
            SELECT
                cm.cohort_id,
                cm.user_id,
                DATE_DIFF('day', cm.join_time::DATE, cas.event_time::DATE) AS day_number
            FROM cohort_membership cm
            JOIN cohort_activity_snapshot cas
              ON cm.cohort_id = cas.cohort_id
             AND cm.user_id = cas.user_id
            JOIN events_scoped es
              ON es.user_id = cas.user_id
             AND es.event_time = cas.event_time
             AND es.event_name = cas.event_name
            WHERE DATE_DIFF('day', cm.join_time::DATE, cas.event_time::DATE) BETWEEN 0 AND ?
        )
        SELECT cohort_id, day_number, COUNT(DISTINCT user_id) AS active_users
        FROM activity_deltas
        GROUP BY cohort_id, day_number
        """,
        [max_day],
    ).fetchall()


@app.get("/retention")
def get_retention(
    max_day: int = Query(7, ge=0),
    retention_event: str | None = Query(None),
) -> dict[str, int | str | list[dict[str, object]]]:
    connection = get_connection()
    try:
        ensure_cohort_tables(connection)
        scoped_exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped'"
        ).fetchone()[0]
        if not scoped_exists:
            return {"max_day": int(max_day), "retention_event": retention_event or "any", "retention_table": []}

        cohorts, cohort_sizes = build_active_cohort_base(connection)
        if not cohorts:
            return {"max_day": int(max_day), "retention_event": retention_event or "any", "retention_table": []}

        active_rows = fetch_retention_active_rows(connection, max_day, retention_event)

        active_by_day = {(int(c), int(d)): int(a) for c, d, a in active_rows}

        retention_table = []
        for cohort_id, cohort_name in cohorts:
            cohort_id = int(cohort_id)
            cohort_size = cohort_sizes.get(cohort_id, 0)
            retention = {}
            for day_number in range(max_day + 1):
                active_users = active_by_day.get((cohort_id, day_number), 0)
                percent = (active_users / cohort_size * 100.0) if cohort_size > 0 else 0.0
                retention[str(day_number)] = float(percent)

            retention_table.append(
                {
                    "cohort_id": cohort_id,
                    "cohort_name": str(cohort_name),
                    "size": int(cohort_size),
                    "retention": retention,
                }
            )

        return {"max_day": int(max_day), "retention_event": retention_event or "any", "retention_table": retention_table}
    finally:
        connection.close()


@app.get("/events")
def list_events() -> dict[str, list[str]]:
    connection = get_connection()
    try:
        scoped_exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped'"
        ).fetchone()[0]
        if not scoped_exists:
            return {"events": []}

        rows = connection.execute("SELECT DISTINCT event_name FROM events_scoped ORDER BY event_name").fetchall()
        return {"events": [str(row[0]) for row in rows]}
    finally:
        connection.close()


@app.get("/usage")
def get_usage(
    event: str = Query(...),
    max_day: int = Query(7, ge=0),
    retention_event: str | None = Query(None),
) -> dict[str, object]:
    connection = get_connection()
    try:
        scoped_exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped'"
        ).fetchone()[0]
        ensure_cohort_tables(connection)

        empty_response = {
            "max_day": int(max_day),
            "event": event,
            "retention_event": retention_event or "any",
            "usage_volume_table": [],
            "usage_users_table": [],
            "retained_users_table": [],
        }
        if not scoped_exists:
            return empty_response

        cohorts, cohort_sizes = build_active_cohort_base(connection)
        if not cohorts:
            return empty_response

        event_exists = connection.execute("SELECT 1 FROM events_scoped WHERE event_name = ? LIMIT 1", [event]).fetchone()
        if event_exists is None:
            return empty_response

        usage_rows = connection.execute(
            """
            WITH usage_deltas AS (
                SELECT
                    cm.cohort_id,
                    cm.user_id,
                    DATE_DIFF('day', cm.join_time::DATE, es.event_time::DATE) AS day_number
                FROM cohort_membership cm
                JOIN events_scoped es ON es.user_id = cm.user_id
                WHERE es.event_name = ?
                  AND DATE_DIFF('day', cm.join_time::DATE, es.event_time::DATE) BETWEEN 0 AND ?
            )
            SELECT cohort_id, day_number, COUNT(*) AS total_events, COUNT(DISTINCT user_id) AS distinct_users
            FROM usage_deltas
            GROUP BY cohort_id, day_number
            """,
            [event, max_day],
        ).fetchall()

        usage_by_day = {
            (int(cohort_id), int(day_number)): {"total_events": int(total_events), "distinct_users": int(distinct_users)}
            for cohort_id, day_number, total_events, distinct_users in usage_rows
        }

        retention_rows = fetch_retention_active_rows(connection, max_day, retention_event)
        retained_by_day = {(int(c), int(d)): int(a) for c, d, a in retention_rows}

        usage_volume_table = []
        usage_users_table = []
        retained_users_table = []
        for cohort_id, cohort_name in cohorts:
            cohort_id = int(cohort_id)
            cohort_size = cohort_sizes.get(cohort_id, 0)
            volume_values = {}
            user_values = {}
            retained_values = {}
            for day_number in range(max_day + 1):
                bucket = usage_by_day.get((cohort_id, day_number), {})
                volume_values[str(day_number)] = int(bucket.get("total_events", 0))
                user_values[str(day_number)] = int(bucket.get("distinct_users", 0))
                retained_values[str(day_number)] = int(retained_by_day.get((cohort_id, day_number), 0))

            common_metadata = {"cohort_id": cohort_id, "cohort_name": str(cohort_name), "size": int(cohort_size)}
            usage_volume_table.append({**common_metadata, "values": volume_values})
            usage_users_table.append({**common_metadata, "values": user_values})
            retained_users_table.append({**common_metadata, "values": retained_values})

        return {
            "max_day": int(max_day),
            "event": event,
            "retention_event": retention_event or "any",
            "usage_volume_table": usage_volume_table,
            "usage_users_table": usage_users_table,
            "retained_users_table": retained_users_table,
        }
    finally:
        connection.close()
