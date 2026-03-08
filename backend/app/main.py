import json
import logging
import math
import re
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator

from app.utils.perf import time_block

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

# Logger is configured globally, perf.py uses it internally.

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
    event_count_column: str | None = None
    revenue_column: str | None = None
    column_types: dict[str, str] = Field(default_factory=dict)


class RevenueEventSelectionItem(BaseModel):
    event_name: str
    is_included: bool
    override: float | None = None


class RevenueEventSelectionRequest(BaseModel):
    events: list[RevenueEventSelectionItem] = Field(default_factory=list)


class RevenueConfigItem(BaseModel):
    included: bool
    override: float | None = None


class UpdateRevenueConfigRequest(BaseModel):
    revenue_config: dict[str, RevenueConfigItem] = Field(default_factory=dict)


class ScopeFilter(BaseModel):
    column: str
    operator: str
    value: str | float | int | list[str] | list[float] | list[int]


class CohortPropertyFilter(BaseModel):
    column: str
    operator: str
    values: str | float | int | bool | list[str] | list[float] | list[int] | list[bool]


class CohortCondition(BaseModel):
    event_name: str
    min_event_count: int = Field(ge=1)
    property_filter: CohortPropertyFilter | None = None

    @field_validator("property_filter")
    @classmethod
    def validate_property_filter(cls, value: CohortPropertyFilter | None) -> CohortPropertyFilter | None:
        if value is None:
            return value
        if value.values is None or (isinstance(value.values, str) and value.values == ""):
            raise ValueError("property_filter.values is required")
        return value


class CreateCohortRequest(BaseModel):
    name: str = Field(min_length=1)
    logic_operator: str
    join_type: str = "condition_met"
    conditions: list[CohortCondition] = Field(max_length=5)

    @field_validator("logic_operator")
    @classmethod
    def validate_logic_operator(cls, value: str) -> str:
        normalized = value.upper()
        if normalized not in {"AND", "OR"}:
            raise ValueError("logic_operator must be either AND or OR")
        return normalized

    @field_validator("join_type")
    @classmethod
    def validate_join_type(cls, value: str) -> str:
        normalized = value.lower()
        if normalized not in {"condition_met", "first_event"}:
            raise ValueError("join_type must be 'condition_met' or 'first_event'")
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
TEXT_ALLOWED_OPERATORS = {"=", "!=", "IN", "NOT IN"}
NUMERIC_ALLOWED_OPERATORS = {"=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN"}
TIMESTAMP_ALLOWED_OPERATORS = {"=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN"}
BOOLEAN_ALLOWED_OPERATORS = {"=", "!="}
Z_SCORES = {
    0.90: 1.645,
    0.95: 1.96,
    0.99: 2.576,
}


def wilson_ci(x: int, n: int, confidence: float = 0.95) -> tuple[float | None, float | None]:
    if n == 0:
        return None, None

    z = Z_SCORES.get(confidence, 1.96)
    p = x / n
    z2 = z * z

    denominator = 1 + z2 / n
    center = (p + z2 / (2 * n)) / denominator
    margin = (z * math.sqrt((p * (1 - p) / n) + (z2 / (4 * n * n)))) / denominator

    lower = max(0.0, center - margin)
    upper = min(1.0, center + margin)

    return lower, upper


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
    if data_type in {"BOOLEAN", "BOOL"}:
        return "BOOLEAN"
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
    if column_kind == "BOOLEAN":
        return BOOLEAN_ALLOWED_OPERATORS
    return TEXT_ALLOWED_OPERATORS


def normalize_timestamp_filter_value(value: str) -> str:
    normalized = value.strip().replace("T", " ")
    if not normalized:
        return ""

    try:
        parsed = datetime.fromisoformat(normalized.replace(" ", "T"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid timestamp format") from exc

    return parsed.strftime("%Y-%m-%d %H:%M:%S")


TIMESTAMP_INPUT_FORMATS: tuple[str, ...] = (
    "%Y-%m-%d",
    "%Y-%m-%d %H",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
)


def normalize_event_timestamp_value(value: object, *, allow_empty: bool) -> datetime | None:
    if value is None:
        if allow_empty:
            return None
        raise HTTPException(status_code=400, detail="Timestamp value cannot be null")

    raw = str(value).strip().replace("T", " ")
    if raw == "":
        if allow_empty:
            return None
        raise HTTPException(status_code=400, detail="Timestamp value cannot be null")

    for fmt in TIMESTAMP_INPUT_FORMATS:
        try:
            parsed = datetime.strptime(raw, fmt)
            return datetime.strptime(parsed.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    raise HTTPException(status_code=400, detail=f"Invalid timestamp format: {raw}")


def parse_bool_value(value: object) -> bool:
    normalized = str(value).strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError("Expected true/false")


def parse_int_value(value: object) -> int:
    normalized = str(value).strip()
    if not re.fullmatch(r"[+-]?\d+", normalized):
        raise ValueError("Expected integer")
    return int(normalized)


def detect_column_type(values: pd.Series) -> str:
    cleaned = values.dropna().astype(str).str.strip()
    cleaned = cleaned[cleaned != ""]

    if cleaned.empty:
        return "TEXT"

    try:
        cleaned.astype(float)
        return "NUMERIC"
    except ValueError:
        pass

    non_null = cleaned.tolist()

    try:
        for value in non_null:
            parse_int_value(value)
        return "NUMERIC"
    except ValueError:
        pass

    try:
        for value in non_null:
            parse_bool_value(value)
        return "BOOLEAN"
    except ValueError:
        pass

    try:
        for value in non_null:
            normalize_event_timestamp_value(value, allow_empty=False)
        return "TIMESTAMP"
    except HTTPException:
        return "TEXT"


def suggest_user_id(columns: list[str]) -> str | None:
    for col in columns:
        name = col.lower()

        if name == "user_id":
            return col
        if name.endswith("_user_id"):
            return col
        if "user_id" in name:
            return col
        if "userid" in name:
            return col
        if name.endswith("_uid") or name == "uid":
            return col
        if "customer_id" in name:
            return col

    for col in columns:
        if "user" in col.lower() and "type" not in col.lower() and "segment" not in col.lower():
            return col

    return None


def suggest_event_name(columns: list[str]) -> str | None:
    strong_keywords = ["event_name", "event", "action", "activity"]

    for keyword in strong_keywords:
        for col in columns:
            if keyword in col.lower():
                return col

    return None


def suggest_event_time(columns: list[str]) -> str | None:
    strong_keywords = ["event_time", "timestamp", "created_at", "time", "date"]

    for keyword in strong_keywords:
        for col in columns:
            if keyword in col.lower():
                return col

    return None


def suggest_event_count(columns: list[str]) -> str | None:
    strong_keywords = ["event_count", "count", "frequency", "occurrence"]

    for keyword in strong_keywords:
        for col in columns:
            if keyword in col.lower():
                return col

    return None


def suggest_column_mapping(columns: list[str]) -> dict[str, str | None]:
    return {
        "user_id": suggest_user_id(columns),
        "event_name": suggest_event_name(columns),
        "event_time": suggest_event_time(columns),
        "event_count": suggest_event_count(columns),
    }


def reset_application_state(connection: duckdb.DuckDBPyConnection) -> None:
    tables_to_drop = [
        "events_normalized",
        "events_scoped",
        "cohort_membership",
        "cohort_activity_snapshot",
        "cohort_conditions",
        "cohorts",
        "dataset_scope",
    ]

    for table in tables_to_drop:
        connection.execute(f'DROP TABLE IF EXISTS "{table}"')

    connection.execute("DROP SEQUENCE IF EXISTS cohort_id_seq")
    connection.execute("DROP SEQUENCE IF EXISTS condition_id_seq")
    connection.execute("DROP SEQUENCE IF EXISTS cohorts_id_sequence")
    connection.execute("DROP SEQUENCE IF EXISTS cohort_condition_id_sequence")


def validate_cohort_property_filter_value(property_filter: CohortPropertyFilter, column_kind: str) -> None:
    operator = property_filter.operator.upper()
    values = property_filter.values
    if operator in {"IN", "NOT IN"}:
        if not isinstance(values, list) or not values:
            raise HTTPException(status_code=400, detail=f"Operator {operator} requires a non-empty array value")
    else:
        if isinstance(values, list):
            raise HTTPException(status_code=400, detail=f"Operator {operator} requires a scalar value")

    scalar_values = values if isinstance(values, list) else [values]
    if column_kind == "NUMERIC":
        normalized_values: list[int | float] = []
        for value in scalar_values:
            if isinstance(value, bool):
                raise HTTPException(status_code=400, detail="Numeric operators require numeric values")
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="Numeric operators require numeric values") from None

            if parsed.is_integer():
                normalized_values.append(int(parsed))
            else:
                normalized_values.append(parsed)

        property_filter.values = normalized_values if isinstance(values, list) else normalized_values[0]
    elif column_kind == "TIMESTAMP":
        normalized_values: list[str] = []
        for value in scalar_values:
            if not isinstance(value, str):
                raise HTTPException(status_code=400, detail="Timestamp filters require string values")
            normalized = normalize_timestamp_filter_value(value)
            if not normalized:
                raise HTTPException(status_code=400, detail="Timestamp filters require non-empty string values")
            normalized_values.append(normalized)

        property_filter.values = normalized_values if isinstance(values, list) else normalized_values[0]
    elif column_kind == "BOOLEAN":
        if operator in {"IN", "NOT IN"}:
            raise HTTPException(status_code=400, detail=f"Operator '{operator}' not allowed for column type BOOLEAN")
        for value in scalar_values:
            if not isinstance(value, bool):
                raise HTTPException(status_code=400, detail="Boolean filters only accept true/false values")


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
        validate_cohort_property_filter_value(property_filter, column_kind)


def ensure_cohort_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cohorts (
            cohort_id INTEGER PRIMARY KEY,
            name TEXT,
            logic_operator TEXT,
            join_type TEXT DEFAULT 'condition_met',
            is_active BOOLEAN DEFAULT TRUE,
            hidden BOOLEAN DEFAULT FALSE
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
        "property_values",
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





def ensure_dataset_metadata_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dataset_metadata (
            id INTEGER PRIMARY KEY,
            has_revenue_mapping BOOLEAN NOT NULL DEFAULT FALSE
        )
        """
    )


def set_has_revenue_mapping(connection: duckdb.DuckDBPyConnection, has_revenue_mapping: bool) -> None:
    ensure_dataset_metadata_table(connection)
    connection.execute(
        """
        INSERT INTO dataset_metadata (id, has_revenue_mapping)
        VALUES (1, ?)
        ON CONFLICT (id) DO UPDATE SET has_revenue_mapping = excluded.has_revenue_mapping
        """,
        [bool(has_revenue_mapping)],
    )


def get_has_revenue_mapping(connection: duckdb.DuckDBPyConnection) -> bool:
    ensure_dataset_metadata_table(connection)
    row = connection.execute("SELECT has_revenue_mapping FROM dataset_metadata WHERE id = 1").fetchone()
    return bool(row[0]) if row else False

def ensure_revenue_event_selection_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS revenue_event_selection (
            event_name TEXT PRIMARY KEY,
            is_included BOOLEAN NOT NULL,
            override_value DOUBLE
        )
        """
    )

    existing_columns = {
        row[0]
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'revenue_event_selection'
            """
        ).fetchall()
    }
    if "override_value" not in existing_columns:
        connection.execute("ALTER TABLE revenue_event_selection ADD COLUMN override_value DOUBLE")


def ensure_normalized_events_revenue_columns(connection: duckdb.DuckDBPyConnection, table_name: str = "events_normalized") -> None:
    table_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()[0]
    if not table_exists:
        return

    existing_columns = {
        row[0]
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = ?
            """,
            [table_name],
        ).fetchall()
    }

    if "original_event_count" not in existing_columns and "event_count" in existing_columns:
        connection.execute(f"ALTER TABLE {table_name} RENAME COLUMN event_count TO original_event_count")
        existing_columns.discard("event_count")
        existing_columns.add("original_event_count")
    if "original_revenue" not in existing_columns and "revenue_amount" in existing_columns:
        connection.execute(f"ALTER TABLE {table_name} RENAME COLUMN revenue_amount TO original_revenue")
        existing_columns.discard("revenue_amount")
        existing_columns.add("original_revenue")

    if "original_event_count" not in existing_columns:
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN original_event_count INTEGER")
    if "original_revenue" not in existing_columns:
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN original_revenue DOUBLE")
    if "modified_event_count" not in existing_columns:
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN modified_event_count INTEGER")
    if "modified_revenue" not in existing_columns:
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN modified_revenue DOUBLE")

    connection.execute(
        f"""
        UPDATE {table_name}
        SET modified_event_count = COALESCE(modified_event_count, original_event_count),
            modified_revenue = COALESCE(modified_revenue, original_revenue)
        """
    )




def recompute_modified_revenue_columns(connection: duckdb.DuckDBPyConnection, table_name: str) -> None:
    table_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()[0]
    if not table_exists:
        return

    ensure_normalized_events_revenue_columns(connection, table_name)
    connection.execute(
        f"""
        UPDATE {table_name} en
        SET
            modified_event_count = CASE
                WHEN rc.event_name IS NULL OR rc.is_included = FALSE THEN 0
                ELSE en.original_event_count
            END,
            modified_revenue = CASE
                WHEN rc.event_name IS NULL OR rc.is_included = FALSE THEN 0
                WHEN rc.override_value IS NOT NULL THEN en.original_event_count * rc.override_value
                ELSE en.original_revenue
            END
        FROM revenue_event_selection rc
        WHERE en.event_name = rc.event_name
        """
    )
    connection.execute(
        f"""
        UPDATE {table_name}
        SET
            modified_event_count = 0,
            modified_revenue = 0
        WHERE event_name NOT IN (
            SELECT event_name FROM revenue_event_selection
        )
        """
    )

def initialize_revenue_event_selection(connection: duckdb.DuckDBPyConnection) -> None:
    ensure_revenue_event_selection_table(connection)
    connection.execute("DELETE FROM revenue_event_selection")
    connection.execute(
        """
        INSERT INTO revenue_event_selection (event_name, is_included, override_value)
        SELECT
            event_name,
            TRUE,
            NULL
        FROM (
            SELECT
                event_name,
                SUM(original_revenue) AS total_revenue
            FROM events_normalized
            WHERE event_name IS NOT NULL
            GROUP BY event_name
        ) revenue_by_event
        WHERE total_revenue != 0
        ORDER BY event_name
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
    ensure_normalized_events_revenue_columns(connection, "events_scoped")
    recompute_modified_revenue_columns(connection, "events_scoped")
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
                parsed_values = json.loads(str(property_values))
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
                            SUM(original_event_count) OVER (
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
    ensure_cohort_tables(connection)

    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped'"
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
        INSERT INTO cohorts (cohort_id, name, logic_operator, join_type, is_active)
        VALUES (nextval('cohorts_id_sequence'), 'All Users', 'OR', 'first_event', TRUE)
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
async def upload_csv(file: UploadFile = File(...)) -> dict[str, object]:
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    end_timer = time_block("csv_upload")
    try:
        try:
            dataframe = pd.read_csv(file.file, keep_default_na=False, na_values=[""])
        except Exception as exc:
            end_timer(error=str(exc))
            raise HTTPException(status_code=400, detail="Invalid CSV file") from exc
        finally:
            await file.close()

        if len(dataframe.columns) < 3:
            end_timer(error="insufficient_columns")
            raise HTTPException(status_code=400, detail="CSV must contain at least 3 columns")

        connection = get_connection()
        try:
            connection.register("uploaded_events", dataframe)
            connection.execute("DROP TABLE IF EXISTS events")
            connection.execute("CREATE TABLE events AS SELECT * FROM uploaded_events")
            reset_application_state(connection)
        finally:
            connection.close()

        column_names = [str(column) for column in dataframe.columns.tolist()]
        detected_types = {
            str(column): detect_column_type(dataframe[column])
            for column in dataframe.columns
        }
        mapping_suggestions = suggest_column_mapping(column_names)

        end_timer(
            row_count=len(dataframe),
            column_count=len(dataframe.columns),
            file_size=file.size
        )

        return {
            "rows_imported": int(len(dataframe)),
            "columns": column_names,
            "detected_types": detected_types,
            "mapping_suggestions": mapping_suggestions,
        }
    except HTTPException:
        raise
    except Exception as exc:
        end_timer(error=str(exc))
        raise


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
        if mapping.event_count_column:
            requested_columns.add(mapping.event_count_column)
        if mapping.revenue_column:
            requested_columns.add(mapping.revenue_column)
        missing_columns = sorted(requested_columns - set(existing_columns))
        if missing_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Mapped columns not found in uploaded CSV: {', '.join(missing_columns)}",
            )
        end_timer = time_block("csv_normalization")
        events_df = connection.execute("SELECT * FROM events").df()
        selected_types = {
            column: str(mapping.column_types.get(column, detect_column_type(events_df[column]))).upper()
            for column in existing_columns
        }
        allowed_types = {"TEXT", "NUMERIC", "TIMESTAMP", "BOOLEAN"}
        for column, selected_type in selected_types.items():
            if selected_type not in allowed_types:
                raise HTTPException(status_code=400, detail=f"Invalid type override for column '{column}': {selected_type}")

        for field_name, column_name, expected_type in [
            ("user_id", mapping.user_id_column, "TEXT"),
            ("event_name", mapping.event_name_column, "TEXT"),
            ("event_time", mapping.event_time_column, "TIMESTAMP"),
        ]:
            actual = selected_types[column_name]
            if actual != expected_type:
                end_timer(error="type_mismatch")
                raise HTTPException(
                    status_code=400,
                    detail=f"Mapped field '{field_name}' requires {expected_type} type, got {actual}",
                )
        if mapping.event_count_column:
            actual = selected_types[mapping.event_count_column]
            if actual != "NUMERIC":
                end_timer(error="type_mismatch_event_count")
                raise HTTPException(
                    status_code=400,
                    detail=f"Mapped field 'event_count' requires NUMERIC type, got {actual}",
                )
        if mapping.revenue_column:
            actual = selected_types[mapping.revenue_column]
            if actual != "NUMERIC":
                end_timer(error="type_mismatch_revenue")
                raise HTTPException(
                    status_code=400,
                    detail=f"Mapped field 'revenue_column' requires NUMERIC type, got {actual}",
                )

        parsed_rows: list[dict[str, object]] = []
        for _, row in events_df.iterrows():
            parsed_row: dict[str, object] = {}
            for column in existing_columns:
                value = row[column]
                if pd.isna(value):
                    value = None
                selected_type = selected_types[column]
                if selected_type == "TEXT":
                    parsed_row[column] = None if value is None or str(value).strip() == "" else str(value)
                elif selected_type == "NUMERIC":
                    if value is None or str(value).strip() == "":
                        parsed_row[column] = None
                    else:
                        try:
                            if column == mapping.event_count_column:
                                parsed_row[column] = parse_int_value(value)
                            else:
                                parsed_row[column] = float(value)
                        except ValueError as exc:
                            error_prefix = "Invalid integer value" if column == mapping.event_count_column else "Invalid numeric value"
                            raise HTTPException(status_code=400, detail=f"{error_prefix} in column '{column}': {value}") from exc
                elif selected_type == "BOOLEAN":
                    if value is None or str(value).strip() == "":
                        parsed_row[column] = None
                    else:
                        try:
                            parsed_row[column] = parse_bool_value(value)
                        except ValueError as exc:
                            raise HTTPException(status_code=400, detail=f"Invalid boolean value in column '{column}': {value}") from exc
                else:
                    parsed_row[column] = normalize_event_timestamp_value(
                        value,
                        allow_empty=column != mapping.event_time_column,
                    )

            event_count = 1
            if mapping.event_count_column:
                candidate_count = parsed_row[mapping.event_count_column]
                if candidate_count is None:
                    raise HTTPException(status_code=400, detail="event_count must not be null")
                if not isinstance(candidate_count, int):
                    raise HTTPException(status_code=400, detail="event_count must be an integer")
                if candidate_count < 1:
                    raise HTTPException(status_code=400, detail="event_count must be >= 1")
                event_count = candidate_count

            parsed_row["user_id"] = parsed_row.pop(mapping.user_id_column)
            parsed_row["event_name"] = parsed_row.pop(mapping.event_name_column)
            parsed_row["event_time"] = parsed_row.pop(mapping.event_time_column)
            revenue_amount = 0.0
            if mapping.revenue_column:
                revenue_candidate = parsed_row[mapping.revenue_column]
                revenue_amount = 0.0 if revenue_candidate is None else float(revenue_candidate)
                parsed_row.pop(mapping.revenue_column)
            if mapping.event_count_column:
                parsed_row.pop(mapping.event_count_column)
            parsed_row["original_event_count"] = event_count
            parsed_row["original_revenue"] = revenue_amount
            parsed_row["modified_event_count"] = event_count
            parsed_row["modified_revenue"] = revenue_amount
            parsed_rows.append(parsed_row)

        normalized_df = pd.DataFrame(parsed_rows)
        group_columns = [column for column in normalized_df.columns if column not in {"original_event_count", "original_revenue", "modified_event_count", "modified_revenue"}]
        normalized_df = normalized_df.groupby(group_columns, dropna=False, as_index=False).agg(
            original_event_count=("original_event_count", "sum"),
            original_revenue=("original_revenue", "sum"),
            modified_event_count=("modified_event_count", "sum"),
            modified_revenue=("modified_revenue", "sum"),
        )
        end_timer(row_count=len(normalized_df))

        connection.execute("DROP TABLE IF EXISTS events_normalized")
        connection.register("temp_import", normalized_df)
        connection.execute("CREATE TABLE events_normalized AS SELECT * FROM temp_import")
        ensure_normalized_events_revenue_columns(connection)
        connection.execute("ALTER TABLE events_normalized ALTER COLUMN original_event_count SET NOT NULL")
        connection.execute(
            "ALTER TABLE events_normalized ALTER COLUMN original_revenue SET DEFAULT 0"
        )
        connection.execute("ALTER TABLE events_normalized ALTER COLUMN original_revenue SET NOT NULL")
        connection.execute("ALTER TABLE events_normalized ALTER COLUMN modified_event_count SET DEFAULT 0")
        connection.execute("ALTER TABLE events_normalized ALTER COLUMN modified_event_count SET NOT NULL")
        connection.execute("ALTER TABLE events_normalized ALTER COLUMN modified_revenue SET DEFAULT 0")
        connection.execute("ALTER TABLE events_normalized ALTER COLUMN modified_revenue SET NOT NULL")

        ensure_cohort_tables(connection)
        ensure_scope_tables(connection)
        ensure_revenue_event_selection_table(connection)
        ensure_dataset_metadata_table(connection)

        connection.execute("DELETE FROM cohort_membership")
        connection.execute("DELETE FROM cohort_activity_snapshot")
        connection.execute("DELETE FROM cohort_conditions")
        connection.execute("DELETE FROM cohorts")
        initialize_scoped_dataset(connection)
        if mapping.revenue_column:
            initialize_revenue_event_selection(connection)
            recompute_modified_revenue_columns(connection, "events_normalized")
            recompute_modified_revenue_columns(connection, "events_scoped")
            has_revenue = connection.execute(
                "SELECT COUNT(*) FROM events_normalized WHERE original_revenue != 0"
            ).fetchone()[0] > 0
            set_has_revenue_mapping(connection, has_revenue)
        else:
            connection.execute("DELETE FROM revenue_event_selection")
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
        end_timer = time_block("scope_rebuild")
        connection.execute("DROP TABLE IF EXISTS events_scoped")
        connection.execute(
            f"CREATE TABLE events_scoped AS SELECT * FROM events_normalized {where_clause}",
            params,
        )
        ensure_normalized_events_revenue_columns(connection, "events_scoped")
        recompute_modified_revenue_columns(connection, "events_scoped")
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
        end_timer(filtered_rows=counts["filtered_rows"])

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
            "event_count": "event count",
        }
        payload = [
            {
                "name": str(name),
                "role": role_map.get(str(name)),
                "data_type": "TIMESTAMP" if "TIMESTAMP" in str(data_type).upper() else str(data_type).upper(),
            }
            for name, data_type in rows
        ]
        return {"columns": payload}
    finally:
        connection.close()


@app.get("/column-values")
def get_column_values(
    column: str = Query(..., min_length=1),
    event_name: str | None = Query(default=None, min_length=1),
) -> dict[str, list[str] | int]:
    connection = get_connection()
    try:
        normalized_exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized'"
        ).fetchone()[0]
        if not normalized_exists:
            return {"values": [], "total_distinct": 0}

        table_name = "events_scoped" if event_name is not None else "events_normalized"
        known_columns = {
            row[0]
            for row in connection.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = ?
                """,
                [table_name],
            ).fetchall()
        }
        if column not in known_columns:
            raise HTTPException(status_code=400, detail=f"Unknown column: {column}")

        column_ref = quote_identifier(column)

        if event_name is not None:
            event_exists = connection.execute(
                "SELECT COUNT(*) FROM events_scoped WHERE event_name = ?",
                [event_name],
            ).fetchone()[0]
            if not event_exists:
                raise HTTPException(status_code=400, detail=f"Unknown event_name: {event_name}")

            rows = connection.execute(
                f"""
                SELECT DISTINCT {column_ref}
                FROM events_scoped
                WHERE {column_ref} IS NOT NULL AND event_name = ?
                ORDER BY 1
                LIMIT 100
                """,
                [event_name],
            ).fetchall()
            total_distinct = int(
                connection.execute(
                    f"""
                    SELECT COUNT(DISTINCT {column_ref})
                    FROM events_scoped
                    WHERE {column_ref} IS NOT NULL AND event_name = ?
                    """,
                    [event_name],
                ).fetchone()[0]
            )
        else:
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
            INSERT INTO cohorts (cohort_id, name, logic_operator, join_type, is_active)
            VALUES (nextval('cohorts_id_sequence'), ?, ?, ?, TRUE)
            RETURNING cohort_id
            """,
            [payload.name, payload.logic_operator, payload.join_type],
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
                c.join_type,
                c.hidden,
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
        for cohort_id, name, is_active, logic_operator, join_type, hidden, event_name, min_event_count, property_column, property_operator, property_values in rows:
            cohort_id = int(cohort_id)
            if cohort_id not in cohorts:
                cohorts[cohort_id] = {
                    "cohort_id": cohort_id,
                    "cohort_name": str(name),
                    "is_active": bool(is_active),
                    "logic_operator": str(logic_operator or "AND"),
                    "join_type": str(join_type or "condition_met"),
                    "hidden": bool(hidden),
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
            "UPDATE cohorts SET name = ?, logic_operator = ?, join_type = ? WHERE cohort_id = ?",
            [payload.name, payload.logic_operator, payload.join_type, cohort_id],
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


@app.patch("/cohorts/{cohort_id}/hide")
def toggle_cohort_hide(cohort_id: int) -> dict[str, object]:
    connection = get_connection()
    try:
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
    finally:
        connection.close()

    return {"cohort_id": int(cohort_id), "hidden": bool(updated_hidden)}


def build_active_cohort_base(connection: duckdb.DuckDBPyConnection) -> tuple[list[tuple[int, str]], dict[int, int]]:
    cohorts = connection.execute(
        """
        SELECT cohort_id, name
        FROM cohorts
        WHERE is_active = TRUE AND hidden = FALSE
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
            WHERE c.is_active = TRUE AND c.hidden = FALSE AND es.user_id IS NOT NULL
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
                JOIN cohorts c ON c.cohort_id = cm.cohort_id
                JOIN cohort_activity_snapshot cas
                  ON cm.cohort_id = cas.cohort_id
                 AND cm.user_id = cas.user_id
                JOIN events_scoped es
                  ON es.user_id = cas.user_id
                 AND es.event_time = cas.event_time
                 AND es.event_name = cas.event_name
                WHERE c.hidden = FALSE
                  AND es.event_name = ?
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
            JOIN cohorts c ON c.cohort_id = cm.cohort_id
            JOIN cohort_activity_snapshot cas
              ON cm.cohort_id = cas.cohort_id
             AND cm.user_id = cas.user_id
            JOIN events_scoped es
              ON es.user_id = cas.user_id
             AND es.event_time = cas.event_time
             AND es.event_name = cas.event_name
            WHERE c.hidden = FALSE
              AND DATE_DIFF('day', cm.join_time::DATE, cas.event_time::DATE) BETWEEN 0 AND ?
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
    include_ci: bool = Query(False),
    confidence: float = Query(0.95),
) -> dict[str, int | str | list[dict[str, object]]]:
    confidence = round(confidence, 2)
    if confidence not in Z_SCORES:
        raise HTTPException(status_code=400, detail="confidence must be one of: 0.90, 0.95, 0.99")

    connection = get_connection()
    try:
        ensure_cohort_tables(connection)
        scoped_exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped'"
        ).fetchone()[0]
        if not scoped_exists:
            return {"max_day": int(max_day), "retention_event": retention_event or "any", "retention_table": []}

        end_timer = time_block("retention_query")
        cohorts, cohort_sizes = build_active_cohort_base(connection)
        if not cohorts:
            end_timer(max_day=max_day, retention_event=retention_event, cohort_count=0)
            return {"max_day": int(max_day), "retention_event": retention_event or "any", "retention_table": []}

        active_rows = fetch_retention_active_rows(connection, max_day, retention_event)

        active_by_day = {(int(c), int(d)): int(a) for c, d, a in active_rows}

        retention_table = []
        for cohort_id, cohort_name in cohorts:
            cohort_id = int(cohort_id)
            cohort_size = cohort_sizes.get(cohort_id, 0)
            retention = {}
            retention_ci = {}
            for day_number in range(max_day + 1):
                active_users = active_by_day.get((cohort_id, day_number), 0)
                if cohort_size == 0:
                    percent = None
                else:
                    percent = active_users / cohort_size * 100.0
                retention[str(day_number)] = float(percent) if percent is not None else None
                if include_ci:
                    lower, upper = wilson_ci(active_users, cohort_size, confidence)
                    retention_ci[str(day_number)] = {
                        "lower": (float(lower) * 100.0) if lower is not None else None,
                        "upper": (float(upper) * 100.0) if upper is not None else None,
                    }

            row = {
                "cohort_id": cohort_id,
                "cohort_name": str(cohort_name),
                "size": int(cohort_size),
                "retention": retention,
            }
            if include_ci:
                row["retention_ci"] = retention_ci
            retention_table.append(row)

        end_timer(
            max_day=max_day,
            retention_event=retention_event,
            cohort_count=len(cohorts)
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




@app.get("/revenue-config-events")
def get_revenue_config_events() -> dict[str, object]:
    connection = get_connection()
    try:
        ensure_revenue_event_selection_table(connection)
        normalized_exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized'"
        ).fetchone()[0]
        if not normalized_exists:
            return {"has_revenue_mapping": False, "events": [], "addable_events": []}

        rows = connection.execute(
            """
            SELECT
                event_name,
                is_included,
                rc.override_value
            FROM revenue_event_selection rc
            ORDER BY event_name
            """
        ).fetchall()
        addable_rows = connection.execute(
            """
            SELECT DISTINCT event_name
            FROM events_normalized
            WHERE event_name IS NOT NULL
              AND event_name NOT IN (
                SELECT event_name FROM revenue_event_selection
              )
            ORDER BY event_name
            """
        ).fetchall()
        return {
            "has_revenue_mapping": True,
            "events": [
                {"event_name": str(event_name), "included": bool(included), "override": override}
                for event_name, included, override in rows
            ],
            "addable_events": [str(row[0]) for row in addable_rows],
        }
    finally:
        connection.close()


@app.get("/revenue-events")
def get_revenue_events() -> dict[str, object]:
    connection = get_connection()
    try:
        ensure_revenue_event_selection_table(connection)
        has_revenue_mapping = get_has_revenue_mapping(connection)
        rows = connection.execute(
            "SELECT event_name, is_included, override_value FROM revenue_event_selection ORDER BY event_name"
        ).fetchall()
        return {
            "has_revenue_mapping": has_revenue_mapping,
            "events": [
                {"event_name": str(event_name), "is_included": bool(is_included), "override": override_value}
                for event_name, is_included, override_value in rows
            ],
        }
    finally:
        connection.close()


@app.post("/update-revenue-config")
def update_revenue_config(payload: UpdateRevenueConfigRequest) -> dict[str, object]:
    connection = get_connection()
    try:
        ensure_revenue_event_selection_table(connection)

        normalized_exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized'"
        ).fetchone()[0]
        if not normalized_exists:
            raise HTTPException(status_code=400, detail="No normalized events found. Upload and map columns first.")
        if not payload.revenue_config:
            raise HTTPException(status_code=400, detail="revenue_config cannot be empty")

        for event_name, config in payload.revenue_config.items():
            connection.execute(
                """
                INSERT INTO revenue_event_selection (event_name, is_included, override_value)
                VALUES (?, ?, ?)
                ON CONFLICT (event_name)
                DO UPDATE SET
                    is_included = excluded.is_included,
                    override_value = excluded.override_value
                """,
                [event_name, bool(config.included), config.override],
            )

        has_revenue = connection.execute(
            "SELECT COUNT(*) FROM events_normalized WHERE original_revenue != 0"
        ).fetchone()[0] > 0
        set_has_revenue_mapping(connection, has_revenue)

        recompute_modified_revenue_columns(connection, "events_normalized")

        scoped_exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped'"
        ).fetchone()[0]
        if scoped_exists:
            recompute_modified_revenue_columns(connection, "events_scoped")
            create_scoped_indexes(connection)

        rows = connection.execute(
        """
        SELECT
            event_name,
            is_included,
            override_value
        FROM revenue_event_selection
        ORDER BY event_name
        """
        ).fetchall()
        
        addable_rows = connection.execute(
        """
        SELECT DISTINCT event_name
        FROM events_normalized
        WHERE event_name IS NOT NULL
          AND event_name NOT IN (
              SELECT event_name FROM revenue_event_selection
          )
        ORDER BY event_name
        """
        ).fetchall()
        return {
            "has_revenue_mapping": has_revenue,
            "events": [
                {"event_name": str(event_name), "included": bool(included), "override": override}
                for event_name, included, override in rows
            ],
            "addable_events": [str(row[0]) for row in addable_rows],
        }
    finally:
        connection.close()


@app.get("/monetization")
def get_monetization(max_day: int = Query(7, ge=0)) -> dict[str, object]:
    connection = get_connection()
    try:
        ensure_cohort_tables(connection)
        ensure_revenue_event_selection_table(connection)
        scoped_exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped'"
        ).fetchone()[0]

        empty_response = {
            "max_day": int(max_day),
            "revenue_table": [],
            "cohort_sizes": [],
            "retained_users_table": [],
        }
        if not scoped_exists:
            return empty_response

        end_timer = time_block("monetization_query")
        cohorts, cohort_sizes = build_active_cohort_base(connection)
        if not cohorts:
            end_timer(metric="cumulative_revenue_per_acquired_user", max_day=max_day, cohort_count=0)
            return empty_response

        revenue_rows = connection.execute(
            """
            WITH revenue_events AS (
                SELECT event_name
                FROM revenue_event_selection
                WHERE is_included = TRUE
            ),
            revenue_by_day AS (
                SELECT
                    cm.cohort_id,
                    DATE_DIFF('day', cm.join_time::DATE, es.event_time::DATE) AS day_number,
                    SUM(es.modified_revenue) AS revenue,
                    SUM(es.modified_event_count) AS event_count
                FROM cohort_membership cm
                JOIN cohorts c ON c.cohort_id = cm.cohort_id
                JOIN events_scoped es
                  ON cm.user_id = es.user_id
                WHERE c.hidden = FALSE
                  AND es.event_name IN (SELECT event_name FROM revenue_events)
                GROUP BY cm.cohort_id, day_number
            )
            SELECT cohort_id, day_number, revenue, event_count
            FROM revenue_by_day
            WHERE day_number BETWEEN 0 AND ?
            ORDER BY cohort_id, day_number
            """,
            [max_day],
        ).fetchall()

        retained_rows = fetch_retention_active_rows(connection, max_day, None)

        cohort_name_by_id = {int(cohort_id): str(cohort_name) for cohort_id, cohort_name in cohorts}
        revenue_table = [
            {
                "cohort_id": int(cohort_id),
                "cohort_name": cohort_name_by_id[int(cohort_id)],
                "day_number": int(day_number),
                "revenue": float(revenue),
                "event_count": int(event_count),
            }
            for cohort_id, day_number, revenue, event_count in revenue_rows
        ]
        cohort_size_table = [
            {"cohort_id": int(cohort_id), "cohort_name": str(cohort_name), "size": int(cohort_sizes.get(int(cohort_id), 0))}
            for cohort_id, cohort_name in cohorts
        ]
        retained_users_table = [
            {"cohort_id": int(cohort_id), "day_number": int(day_number), "retained_users": int(active_users)}
            for cohort_id, day_number, active_users in retained_rows
        ]

        end_timer(
            metric="cumulative_revenue_per_acquired_user",
            max_day=max_day,
            cohort_count=len(cohorts)
        )

        return {
            "max_day": int(max_day),
            "revenue_table": revenue_table,
            "cohort_sizes": cohort_size_table,
            "retained_users_table": retained_users_table,
        }
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

        end_timer = time_block("usage_query")
        cohorts, cohort_sizes = build_active_cohort_base(connection)
        if not cohorts:
            end_timer(event=event, max_day=max_day, retention_event=retention_event, cohort_count=0)
            return empty_response

        event_exists = connection.execute("SELECT 1 FROM events_scoped WHERE event_name = ? LIMIT 1", [event]).fetchone()
        if event_exists is None:
            end_timer(event=event, max_day=max_day, retention_event=retention_event, error="event_not_found")
            return empty_response

        usage_rows = connection.execute(
            """
            WITH usage_deltas AS (
                SELECT
                    cm.cohort_id,
                    cm.user_id,
                    DATE_DIFF('day', cm.join_time::DATE, es.event_time::DATE) AS day_number
                FROM cohort_membership cm
                JOIN cohorts c ON c.cohort_id = cm.cohort_id
                JOIN events_scoped es ON es.user_id = cm.user_id
                WHERE c.hidden = FALSE
                  AND es.event_name = ?
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

        end_timer(
            event=event,
            max_day=max_day,
            retention_event=retention_event,
            cohort_count=len(cohorts)
        )

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
