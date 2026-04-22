"""
Short summary: contains SQL utility helpers.
"""
import duckdb

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
TIMESTAMP_ALLOWED_OPERATORS = {
    "=",
    "!=",
    ">",
    "<",
    ">=",
    "<=",
    "IN",
    "NOT IN",
    "BEFORE",
    "AFTER",
    "ON",
    "BETWEEN",
}
BOOLEAN_ALLOWED_OPERATORS = {"=", "!="}

CANONICAL_COLUMNS = {"user_id", "event_name", "event_time"}
METRIC_COLUMNS = {"event_count", "original_revenue", "modified_revenue"}


def quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def sql_quote_value(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        quoted_list = [sql_quote_value(v) for v in value]
        return f"({', '.join(quoted_list)})"

    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


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


def classify_column(column_name: str) -> str:
    if column_name in CANONICAL_COLUMNS:
        return "canonical"
    if column_name in METRIC_COLUMNS:
        return "metric"
    return "property"


def reset_application_state(connection: duckdb.DuckDBPyConnection) -> None:
    tables_to_drop = [
        "events_normalized",
        "cohort_membership",
        "cohort_activity_snapshot",
        "cohort_conditions",
        "cohorts",
        "dataset_scope",
    ]

    for table in tables_to_drop:
        try:
            connection.execute(f'DROP VIEW IF EXISTS "{table}"')
        except:
            pass
        try:
            connection.execute(f'DROP TABLE IF EXISTS "{table}"')
        except:
            pass

    # Also drop the core unified tables
    try:
        connection.execute('DROP TABLE IF EXISTS events_base')
    except:
        pass
    try:
        connection.execute('DROP TABLE IF EXISTS cohort_event_link')
    except:
        pass

    try:
        connection.execute('DROP VIEW IF EXISTS "events_scoped"')
    except:
        pass

    connection.execute("DROP SEQUENCE IF EXISTS cohort_id_seq")
    connection.execute("DROP SEQUENCE IF EXISTS condition_id_seq")
    connection.execute("DROP SEQUENCE IF EXISTS cohorts_id_sequence")
    connection.execute("DROP SEQUENCE IF EXISTS cohort_condition_id_sequence")


def build_eligibility_expr(join_time_col: str, day_offset_col: str, granularity: str, observation_end_time_str: str) -> str:
    """
    Returns SQL expression to determine if a user is eligible for a bucket.
    """
    if granularity == "hour":
        return f"({join_time_col} + INTERVAL ({day_offset_col}) HOUR) <= '{observation_end_time_str}'::TIMESTAMP"
    else:
        return f"({join_time_col} + INTERVAL ({day_offset_col}) DAY) <= '{observation_end_time_str}'::TIMESTAMP"


def build_bucket_expr(join_time_col: str, event_time_col: str, granularity: str) -> str:
    """
    Returns SQL expression to calculate the bucket offset (days or hours).
    Assumes event_time >= join_time has already been filtered.
    """
    if granularity == "hour":
        return f"GREATEST(0, FLOOR(date_diff('second', {join_time_col}, {event_time_col}) / 3600))"
    else:
        return f"GREATEST(0, FLOOR(date_diff('second', {join_time_col}, {event_time_col}) / 86400))"
