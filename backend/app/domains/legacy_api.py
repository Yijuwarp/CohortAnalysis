"""
Short summary: contains legacy backend business logic used by routers and services.
"""
import json
import logging
import math
import os
import re
import tempfile
from datetime import date, datetime, timezone

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


class RevenueEventSelectionPayloadItem(BaseModel):
    """Item format used in the events-list variant of UpdateRevenueConfigRequest."""
    event_name: str
    include: bool
    override: float | None = None


class UpdateRevenueConfigRequest(BaseModel):
    revenue_config: dict[str, RevenueConfigItem] = Field(default_factory=dict)
    events: list[RevenueEventSelectionPayloadItem] = Field(default_factory=list)


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
    condition_logic: str | None = None
    join_type: str = "condition_met"
    conditions: list[CohortCondition] = Field(max_length=5)

    @field_validator("logic_operator")
    @classmethod
    def validate_logic_operator(cls, value: str) -> str:
        normalized = value.upper()
        if normalized not in {"AND", "OR"}:
            raise ValueError("logic_operator must be either AND or OR")
        return normalized

    @field_validator("condition_logic")
    @classmethod
    def validate_condition_logic(cls, value: str | None) -> str | None:
        if value is None:
            return value
        normalized = value.upper()
        if normalized not in {"AND", "OR"}:
            raise ValueError("condition_logic must be either AND or OR")
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
    from app import main as app_main

    return app_main.get_connection()


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
    primary_keywords = ["timestamp", "event_time", "time"]
    secondary_keywords = ["date", "day", "hour"]

    for keyword in primary_keywords:
        for col in columns:
            if keyword in col.lower():
                return col

    for keyword in secondary_keywords:
        for col in columns:
            if keyword in col.lower():
                return col

    return None


def suggest_revenue(columns: list[str]) -> str | None:
    revenue_keywords = ["revenue", "price", "amount", "value"]

    for keyword in revenue_keywords:
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
        "revenue": suggest_revenue(columns) or None,
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


def ensure_scope_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dataset_scope (
            id INTEGER PRIMARY KEY,
            filters_json TEXT,
            total_rows INTEGER,
            filtered_rows INTEGER,
            total_events BIGINT,
            updated_at TIMESTAMP
        )
        """
    )

    existing_columns = {
        row[0]
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'dataset_scope' AND table_schema = 'main'
            """
        ).fetchall()
    }
    if "total_events" not in existing_columns:
        connection.execute("ALTER TABLE dataset_scope ADD COLUMN total_events BIGINT")

    connection.execute("UPDATE dataset_scope SET total_events = 0 WHERE total_events IS NULL")





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

    # Ensure any pre-existing revenue columns with DECIMAL precision are widened to DOUBLE
    column_types = get_column_type_map(connection, table_name)
    for col in ("original_revenue", "modified_revenue"):
        col_type = column_types.get(col, "").upper()
        if col_type and col_type != "DOUBLE" and not col_type.startswith("FLOAT"):
            connection.execute(f"ALTER TABLE {table_name} ALTER COLUMN {col} TYPE DOUBLE")

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
                WHEN rc.override_value IS NOT NULL THEN CAST(en.original_event_count AS DOUBLE) * CAST(rc.override_value AS DOUBLE)
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
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized' AND table_schema = 'main'"
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
    total_events = int(connection.execute("SELECT COALESCE(SUM(original_event_count), 0) FROM events_scoped").fetchone()[0] or 0)

    connection.execute(
        """
        INSERT INTO dataset_scope (id, filters_json, total_rows, filtered_rows, total_events, updated_at)
        VALUES (1, ?, ?, ?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET
            filters_json = excluded.filters_json,
            total_rows = excluded.total_rows,
            filtered_rows = excluded.filtered_rows,
            total_events = excluded.total_events,
            updated_at = excluded.updated_at
        """,
        [
            json.dumps(payload),
            total_rows,
            filtered_rows,
            total_events,
            datetime.now(timezone.utc),
        ],
    )
    return {"total_rows": total_rows, "filtered_rows": filtered_rows, "total_events": total_events}


def refresh_cohort_activity(connection: duckdb.DuckDBPyConnection) -> None:
    ensure_cohort_tables(connection)
    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
    ).fetchone()[0]
    source_table = "events_scoped" if scoped_exists else "events_normalized"

    connection.execute("DELETE FROM cohort_activity_snapshot")
    connection.execute(
        f"""
        INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name)
        SELECT cm.cohort_id, e.user_id, e.event_time, e.event_name
        FROM cohort_membership cm
        JOIN {source_table} e
          ON cm.user_id = e.user_id
        """
    )

    activity_rows = connection.execute(
        """
        SELECT
            c.cohort_id,
            COUNT(DISTINCT cas.user_id) AS active_members
        FROM cohorts c
        LEFT JOIN cohort_activity_snapshot cas ON c.cohort_id = cas.cohort_id
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


def create_all_users_cohort(connection: duckdb.DuckDBPyConnection) -> None:
    ensure_cohort_tables(connection)

    existing = connection.execute("SELECT cohort_id FROM cohorts WHERE name = 'All Users'").fetchone()
    if existing:
        return

    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
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
    tmp_path = None
    try:
        file_size = 0

        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            while chunk := await file.read(1024 * 1024):
                file_size += len(chunk)
                tmp.write(chunk)
            tmp_path = tmp.name

        connection = get_connection()
        try:
            input_rows = connection.execute(
                "SELECT COUNT(*) FROM read_csv(?, auto_detect=true, ignore_errors=false)",
                [tmp_path],
            ).fetchone()[0]

            connection.execute("DROP TABLE IF EXISTS events")
            reset_application_state(connection)
            try:
                connection.execute(
                    """
                    CREATE TABLE events AS
                    SELECT *
                    FROM read_csv(
                        ?,
                        auto_detect=true,
                        sample_size=100000,
                        all_varchar=true,
                        quote='"',
                        escape='"',
                        ignore_errors=true,
                        maximum_line_size=20000000,  -- allow very large text/JSON fields in CSV rows
                        parallel=true
                    )
                    """,
                    [tmp_path],
                )
            except Exception as exc:
                end_timer(error=str(exc))
                raise HTTPException(status_code=400, detail=f"Invalid CSV file: {str(exc)}") from exc

            row_count = int(connection.execute("SELECT COUNT(*) FROM events").fetchone()[0])
            skipped_rows = max(input_rows - row_count, 0)
            column_info = connection.execute("PRAGMA table_info('events')").fetchall()
            column_names = [row[1] for row in column_info]

            if len(column_names) < 3:
                end_timer(error="insufficient_columns")
                raise HTTPException(status_code=400, detail="CSV must contain at least 3 columns")

            # Get a sample for type detection to avoid materializing the entire dataset in Pandas
            sample_df = connection.execute("SELECT * FROM events LIMIT 10000").df()
            detected_types = {
                str(column): detect_column_type(sample_df[column])
                for column in column_names
            }
            mapping_suggestions = suggest_column_mapping(column_names)

            end_timer(
                row_count=row_count,
                column_count=len(column_names),
                file_size=file_size
            )

            return {
                "rows_imported": row_count,
                "skipped_rows": skipped_rows,
                "columns": column_names,
                "detected_types": detected_types,
                "mapping_suggestions": mapping_suggestions,
            }
        finally:
            connection.close()
    except HTTPException:
        raise
    except Exception as exc:
        end_timer(error=str(exc))
        raise
    finally:
        await file.close()
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)



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

        # Get a sample for type detection if column_types are not fully provided
        sample_df = connection.execute("SELECT * FROM events LIMIT 10000").df()
        selected_types = {
            column: str(mapping.column_types.get(column, detect_column_type(sample_df[column]))).upper()
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

        # Validation 1: Reject empty event_time values
        time_col_q = quote_identifier(str(mapping.event_time_column))

        empty_time_count = connection.execute(f"""
            SELECT COUNT(*)
            FROM events
            WHERE {time_col_q} IS NULL
            OR TRIM(CAST({time_col_q} AS VARCHAR)) = ''
        """).fetchone()[0]
        if empty_time_count > 0:
            end_timer(error="empty_event_time")
            raise HTTPException(status_code=400, detail="event_time must not be empty")

        # Validation 2: Reject non-integer event_count values
        if mapping.event_count_column:
            count_col_q = quote_identifier(mapping.event_count_column)
            invalid_count_check = connection.execute(f"""
                SELECT COUNT(*)
                FROM events
                WHERE {count_col_q} IS NULL
                OR TRY_CAST({count_col_q} AS INTEGER) IS NULL
                OR CAST({count_col_q} AS DOUBLE) != CAST({count_col_q} AS INTEGER)
                OR CAST({count_col_q} AS INTEGER) < 1
            """).fetchone()[0]
            if invalid_count_check > 0:
                end_timer(error="invalid_event_count")
                raise HTTPException(status_code=400, detail="event_count must be integer >= 1")

        # Build SQL for normalization and aggregation
        # We need to distinguish between core mapped columns, value columns (count/revenue), and metadata columns
        core_map = {
            mapping.user_id_column: "user_id",
            mapping.event_name_column: "event_name",
            mapping.event_time_column: "event_time",
        }
        value_cols = {mapping.event_count_column, mapping.revenue_column} - {None}
        metadata_cols = [c for c in existing_columns if c not in core_map and c not in value_cols]

        # Get actual DuckDB types to determine if we can fast-path TIMESTAMP parsing
        actual_types = {
            row[1]: str(row[2]).upper()
            for row in connection.execute("PRAGMA table_info('events')").fetchall()
        }

        def get_cast_expr(col_name: str, target_name: str | None = None) -> str:
            ctype = selected_types[col_name]
            q_col = quote_identifier(col_name)
            alias = f" AS {quote_identifier(target_name)}" if target_name else ""
            
            if ctype == "TEXT":
                return f"NULLIF(TRIM(CAST({q_col} AS VARCHAR)), ''){alias}"
            elif ctype == "NUMERIC":
                return f"CAST({q_col} AS DOUBLE){alias}"
            elif ctype == "BOOLEAN":
                return f"CAST({q_col} AS BOOLEAN){alias}"
            elif ctype == "TIMESTAMP":
                # If DuckDB already inferred TIMESTAMP, use column directly
                if actual_types.get(col_name) == "TIMESTAMP":
                    return f"{q_col}{alias}"

                # Otherwise rely on DuckDB parser
                v_col = f"CAST({q_col} AS VARCHAR)"

                # Remove surrounding CSV quotes and normalize ISO timestamps
                cleaned = f"REPLACE(TRIM(BOTH '\"' FROM {v_col}), 'T', ' ')"

                # DuckDB parses most formats natively; the only exception is
                # coarse hour-precision strings like "2024-01-01 09" (length 13)
                # which need ":00:00" appended before casting.
                return (
                    f"COALESCE("
                    f"TRY_CAST({cleaned} AS TIMESTAMP), "
                    f"TRY_CAST({cleaned} || ':00:00' AS TIMESTAMP)"
                    f"){alias}"
                )










            return f"{q_col}{alias}"


        # Expressions for grouping (core + metadata)
        grouped_expressions = []
        for col, target in core_map.items():
            grouped_expressions.append(get_cast_expr(col, target))
        for col in metadata_cols:
            grouped_expressions.append(get_cast_expr(col, col))
        
        group_by_indices = ", ".join([str(i + 1) for i in range(len(grouped_expressions))])

        # Expressions for values
        if mapping.event_count_column:
            count_col_str = str(mapping.event_count_column)
            count_expr = f"SUM(CAST({quote_identifier(count_col_str)} AS INTEGER))"
        else:
            count_expr = "COUNT(*)"
        
        if mapping.revenue_column:
            rev_col_str = str(mapping.revenue_column)
            rev_expr = f"SUM(COALESCE(CAST({quote_identifier(rev_col_str)} AS DOUBLE), 0.0))"
        else:
            rev_expr = "0.0"


        connection.execute("DROP TABLE IF EXISTS events_normalized")
        sql = f"""
            CREATE TABLE events_normalized AS
            SELECT
                {", ".join(grouped_expressions)},
                {count_expr} AS original_event_count,
                {rev_expr} AS original_revenue,
                {count_expr} AS modified_event_count,
                {rev_expr} AS modified_revenue
            FROM events
            GROUP BY {group_by_indices}
        """
        connection.execute(sql)

        row_count = int(connection.execute("SELECT COUNT(*) FROM events_normalized").fetchone()[0])
        end_timer(row_count=row_count)




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

        stats = connection.execute(
            """
            SELECT
                SUM(modified_event_count),
                COUNT(DISTINCT user_id)
            FROM events_normalized
            """
        ).fetchone()
        total_events = stats[0] or 0
        total_users = stats[1] or 0
    except duckdb.ConversionException as exc:
        raise HTTPException(status_code=400, detail="Failed to convert event_time column to TIMESTAMP") from exc
    finally:
        connection.close()

    return {
        "status": "ok",
        "total_events": int(total_events),
        "total_users": int(total_users),
    }


@app.post("/apply-filters")
def apply_filters(payload: ApplyFiltersRequest) -> dict[str, object]:
    connection = get_connection()
    try:
        normalized_exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized' AND table_schema = 'main'"
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
            "SELECT filters_json, total_rows, filtered_rows, total_events, updated_at FROM dataset_scope WHERE id = 1"
        ).fetchone()
        if row is None:
            return {
                "filters_json": {"date_range": None, "filters": []},
                "total_rows": 0,
                "filtered_rows": 0,
                "total_events": 0,
                "updated_at": None,
            }

        return {
            "filters_json": json.loads(row[0]) if row[0] else {"date_range": None, "filters": []},
            "total_rows": int(row[1] or 0),
            "filtered_rows": int(row[2] or 0),
            "total_events": int(row[3] or 0),
            "updated_at": row[4].isoformat() if row[4] else None,
        }
    finally:
        connection.close()


CANONICAL_COLUMNS = {"user_id", "event_name", "event_time"}
METRIC_COLUMNS = {"original_event_count", "modified_event_count", "original_revenue", "modified_revenue"}


def classify_column(column_name: str) -> str:
    if column_name in CANONICAL_COLUMNS:
        return "canonical"
    if column_name in METRIC_COLUMNS:
        return "metric"
    return "property"


@app.get("/columns")
def get_columns() -> dict[str, list[dict[str, str | None]]]:
    connection = get_connection()
    try:
        exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized' AND table_schema = 'main'"
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
                "category": classify_column(str(name)),
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
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized' AND table_schema = 'main'"
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
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized' AND table_schema = 'main'"
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
    finally:
        connection.close()


@app.put("/cohorts/{cohort_id}")
def update_cohort(cohort_id: int, payload: CreateCohortRequest) -> dict[str, int]:
    connection = get_connection()
    try:
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
    finally:
        connection.close()

    return {"cohort_id": int(cohort_id), "users_joined": users_joined}


@app.post("/cohorts/{cohort_id}/random_split")
def random_split_cohort(cohort_id: int) -> dict[str, int]:
    connection = get_connection()
    try:
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
    finally:
        connection.close()


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
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
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
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
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
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized' AND table_schema = 'main'"
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
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized' AND table_schema = 'main'"
        ).fetchone()[0]
        if not normalized_exists:
            raise HTTPException(status_code=400, detail="No normalized events found. Upload and map columns first.")
        if not payload.revenue_config and not payload.events:
            raise HTTPException(status_code=400, detail="revenue_config cannot be empty")

        if payload.events:
            for item in payload.events:
                connection.execute(
                    """
                    INSERT INTO revenue_event_selection (event_name, is_included, override_value)
                    VALUES (?, ?, ?)
                    ON CONFLICT (event_name)
                    DO UPDATE SET
                        is_included = excluded.is_included,
                        override_value = excluded.override_value
                    """,
                    [item.event_name, bool(item.include), item.override],
                )
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
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
        ).fetchone()[0]
        if scoped_exists:
            recompute_modified_revenue_columns(connection, "events_scoped")
            create_scoped_indexes(connection)

            ensure_scope_tables(connection)
            scope_row = connection.execute(
                "SELECT filters_json FROM dataset_scope WHERE id = 1"
            ).fetchone()
            filters_payload = json.loads(scope_row[0]) if scope_row and scope_row[0] else {"date_range": None, "filters": []}
            upsert_dataset_scope(connection, filters_payload)

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
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
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
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
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
