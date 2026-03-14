"""
Short summary: suggests and validates CSV column mappings.
"""
import json
import duckdb
from fastapi import HTTPException
from app.utils.perf import time_block
from app.utils.sql import quote_identifier
from app.domains.ingestion.normalization_service import ensure_normalized_events_revenue_columns
from app.domains.ingestion.type_detection import detect_column_type
from app.models.ingestion_models import ColumnMappingRequest

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


def map_columns(connection: duckdb.DuckDBPyConnection, mapping: ColumnMappingRequest) -> dict[str, str | int]:
    from app.domains.ingestion.normalization_service import ensure_normalized_events_revenue_columns
    from app.domains.scope.scope_metadata import ensure_scope_tables
    from app.domains.scope.filter_service import initialize_scoped_dataset
    from app.domains.cohorts.cohort_service import ensure_cohort_tables
    from app.domains.cohorts.activity_service import create_all_users_cohort, refresh_cohort_activity
    from app.domains.revenue.revenue_tables import ensure_revenue_event_selection_table
    from app.domains.revenue.revenue_config_service import initialize_revenue_event_selection
    from app.domains.revenue.revenue_recompute import recompute_modified_revenue_columns

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

        # ---------- TYPE DETECTION ----------
        detect_timer = time_block("type_detection")

        sample_df = connection.execute("SELECT * FROM events LIMIT 10000").df()
        selected_types = {
            column: str(mapping.column_types.get(column, detect_column_type(sample_df[column]))).upper()
            for column in existing_columns
        }
        # ---------- TYPE VALIDATION ----------
        allowed_types = {"TEXT", "NUMERIC", "TIMESTAMP", "BOOLEAN"}

        for column, selected_type in selected_types.items():
            if selected_type not in allowed_types:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid type override for column '{column}': {selected_type}"
                )

        # Core column type requirements
        core_requirements = [
            ("user_id", mapping.user_id_column, "TEXT"),
            ("event_name", mapping.event_name_column, "TEXT"),
            ("event_time", mapping.event_time_column, "TIMESTAMP"),
        ]

        for field_name, column_name, expected_type in core_requirements:
            actual = selected_types[column_name]
            if actual != expected_type:
                raise HTTPException(
                    status_code=400,
                    detail=f"Mapped field '{field_name}' requires {expected_type} type, got {actual}"
                )

        # Optional columns
        if mapping.event_count_column:
            if selected_types[mapping.event_count_column] != "NUMERIC":
                raise HTTPException(
                    status_code=400,
                    detail="Mapped field 'event_count' must be NUMERIC"
                )

        if mapping.revenue_column:
            if selected_types[mapping.revenue_column] != "NUMERIC":
                raise HTTPException(
                    status_code=400,
                    detail="Mapped field 'revenue_column' must be NUMERIC"
                )
        detect_timer()

        # ---------- VALIDATION ----------
        validation_timer = time_block("validation")

        time_col_q = quote_identifier(str(mapping.event_time_column))
        empty_time_count = connection.execute(f"""
            SELECT COUNT(*)
            FROM events
            WHERE {time_col_q} IS NULL
            OR TRIM(CAST({time_col_q} AS VARCHAR)) = ''
        """).fetchone()[0]

        if empty_time_count > 0:
            validation_timer(error="empty_event_time")
            raise HTTPException(status_code=400, detail="event_time must not be empty")

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
                validation_timer(error="invalid_event_count")
                raise HTTPException(status_code=400, detail="event_count must be integer >= 1")

        validation_timer()

        # ---------- NORMALIZATION ----------
        normalize_timer = time_block("events_normalization")

        core_map = {
            mapping.user_id_column: "user_id",
            mapping.event_name_column: "event_name",
            mapping.event_time_column: "event_time",
        }

        value_cols = {mapping.event_count_column, mapping.revenue_column} - {None}
        metadata_cols = [c for c in existing_columns if c not in core_map and c not in value_cols]

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
                if actual_types.get(col_name) == "TIMESTAMP":
                    return f"{q_col}{alias}"

                v_col = f"TRIM(BOTH '\"' FROM TRIM(CAST({q_col} AS VARCHAR)))"
                cleaned = (
                    f"REPLACE("
                    f"TRIM(REGEXP_REPLACE({v_col}, '\\s*(UTC|Z|[+-]\\d{{2}}:?\\d{{2}})$', '', 'i')), "
                    f"'T',' ')"
                )

                return f"COALESCE(TRY_CAST({cleaned} AS TIMESTAMP), TRY_CAST({cleaned} || ':00:00' AS TIMESTAMP)){alias}"

            return f"{q_col}{alias}"

        grouped_expressions = []
        for col, target in core_map.items():
            grouped_expressions.append(get_cast_expr(col, target))
        for col in metadata_cols:
            grouped_expressions.append(get_cast_expr(col, col))

        group_by_indices = ", ".join([str(i + 1) for i in range(len(grouped_expressions))])

        if mapping.event_count_column:
            count_expr = f"SUM(CAST({quote_identifier(mapping.event_count_column)} AS INTEGER))"
        else:
            count_expr = "COUNT(*)"

        if mapping.revenue_column:
            rev_expr = f"SUM(COALESCE(CAST({quote_identifier(mapping.revenue_column)} AS DOUBLE), 0.0))"
        else:
            rev_expr = "0.0"

        connection.execute("DROP TABLE IF EXISTS events_normalized")

        sql = f"""
            CREATE TABLE events_normalized AS
            SELECT
                {", ".join(grouped_expressions)},
                CAST({count_expr} AS DOUBLE) AS event_count,
                {rev_expr} AS original_revenue,
                {rev_expr} AS modified_revenue
            FROM events
            GROUP BY {group_by_indices}
        """

        connection.execute(sql)
        bad_time = connection.execute("""
            SELECT 1
            FROM events_normalized
            WHERE event_time IS NULL
            LIMIT 1
        """).fetchone()

        if bad_time:
            raise HTTPException(
                status_code=400,
                detail="Some event_time values could not be parsed as TIMESTAMP"
            )
        row_count = int(connection.execute("SELECT COUNT(*) FROM events_normalized").fetchone()[0])
        normalize_timer(row_count=row_count)

        # ---------- TABLE SETUP ----------
        setup_timer = time_block("table_setup")

        ensure_normalized_events_revenue_columns(connection)
        ensure_cohort_tables(connection)
        ensure_scope_tables(connection)
        ensure_revenue_event_selection_table(connection)
        ensure_dataset_metadata_table(connection)

        setup_timer()

        # ---------- DATASET INITIALIZATION ----------
        init_timer = time_block("dataset_initialization")

        connection.execute("DELETE FROM cohort_membership")
        connection.execute("DELETE FROM cohort_activity_snapshot")
        connection.execute("DELETE FROM cohort_conditions")
        connection.execute("DELETE FROM cohorts")

        initialize_scoped_dataset(connection)

        init_timer()

        # ---------- REVENUE PIPELINE ----------
        if mapping.revenue_column:
            revenue_timer = time_block("revenue_initialization")

            initialize_revenue_event_selection(connection)
            recompute_modified_revenue_columns(connection, "events_normalized")

            has_revenue = connection.execute(
                "SELECT EXISTS (SELECT 1 FROM events_normalized WHERE original_revenue != 0)"
            ).fetchone()[0]

            set_has_revenue_mapping(connection, has_revenue)

            revenue_timer()
        else:
            connection.execute("DELETE FROM revenue_event_selection")

        # ---------- COHORT PRECOMPUTATION ----------
        cohort_timer = time_block("cohort_initialization")

        create_all_users_cohort(connection)
        refresh_cohort_activity(connection)

        cohort_timer()

        # ---------- FINAL STATS ----------
        stats_timer = time_block("final_stats")

        stats = connection.execute(
            """
            SELECT
                SUM(event_count),
                COUNT(DISTINCT user_id)
            FROM events_normalized
            """
        ).fetchone()

        total_events = stats[0] or 0
        total_users = stats[1] or 0

        stats_timer()

    except duckdb.ConversionException as exc:
        raise HTTPException(status_code=400, detail="Failed to convert event_time column to TIMESTAMP") from exc

    return {
        "status": "ok",
        "total_events": int(total_events),
        "total_users": int(total_users),
    }
