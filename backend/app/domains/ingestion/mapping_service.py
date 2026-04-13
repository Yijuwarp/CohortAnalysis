"""
Short summary: suggests and validates CSV column mappings.
"""
import json
import duckdb
from datetime import datetime
from fastapi import HTTPException
from app.utils.perf import time_block
from app.utils.sql import quote_identifier
from app.domains.ingestion.normalization_service import ensure_normalized_events_revenue_columns
from app.domains.ingestion.type_detection import detect_column_type
from app.models.ingestion_models import ColumnMappingRequest
from app.utils.db_utils import to_dict, to_dicts

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

        cursor = connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'events'
            ORDER BY ordinal_position
            """
        )
        existing_columns = [
            row["column_name"]
            for row in to_dicts(cursor, cursor.fetchall())
            if row["column_name"] != "row_id"
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

        p_cursor = connection.execute("PRAGMA table_info('events')")
        actual_types = {
            row["name"]: str(row["type"]).upper()
            for row in to_dicts(p_cursor, p_cursor.fetchall())
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

        # ---------- CALCULATE BOUNDARY ----------
        time_q = quote_identifier(mapping.event_time_column)
        actual_time_type = actual_types.get(mapping.event_time_column)
        
        if actual_time_type == "TIMESTAMP":
            raw_time_expr = time_q
        else:
            v_col_raw = f"TRIM(BOTH '\"' FROM TRIM(CAST({time_q} AS VARCHAR)))"
            cleaned_expr = f"REPLACE(TRIM(REGEXP_REPLACE({v_col_raw}, '\\s*(UTC|Z|[+-]\\d{{2}}:?\\d{{2}})$', '', 'i')), 'T', ' ')"
            raw_time_expr = f"COALESCE(TRY_CAST({cleaned_expr} AS TIMESTAMP), TRY_CAST({cleaned_expr} || ':00:00' AS TIMESTAMP))"

        p99_99_row = connection.execute(f"SELECT quantile_cont({raw_time_expr}, 0.9999) FROM events").fetchone()
        p99_99_threshold = p99_99_row[0] if p99_99_row else None
        
        now_local = datetime.now()
        if p99_99_threshold:
            # Handle possible conversion issues
            if not isinstance(p99_99_threshold, datetime):
                try:
                    p99_99_threshold = datetime.fromisoformat(str(p99_99_threshold))
                except:
                    p99_99_threshold = now_local
            import_upper_bound = min(p99_99_threshold, now_local)
        else:
            import_upper_bound = now_local
            
        import_upper_bound_str = import_upper_bound.strftime('%Y-%m-%d %H:%M:%S')

        connection.execute("DROP TABLE IF EXISTS events_raw")
        connection.execute("DROP TABLE IF EXISTS events_normalized")

        # 1. Create events_raw (row-level, no aggregation)
        # Preserve original_revenue and event_count as scalar values if mapped, or 1.0/0.0 defaults
        raw_count_expr = f"CAST({quote_identifier(mapping.event_count_column)} AS DOUBLE)" if mapping.event_count_column else "1.0"
        raw_rev_expr = f"COALESCE(CAST({quote_identifier(mapping.revenue_column)} AS DOUBLE), 0.0)" if mapping.revenue_column else "0.0"

        raw_sql = f"""
            CREATE TABLE events_raw AS
            SELECT
                {", ".join(grouped_expressions)},
                {raw_count_expr} AS event_count,
                {raw_rev_expr} AS original_revenue,
                {raw_rev_expr} AS modified_revenue,
                row_id
            FROM events
            WHERE {raw_time_expr} <= '{import_upper_bound_str}'::TIMESTAMP
        """
        connection.execute(raw_sql)

        # 2. Create events_normalized (aggregated version for metrics/retention)
        sql = f"""
            CREATE TABLE events_normalized AS
            SELECT
                {", ".join(grouped_expressions)},
                CAST({count_expr} AS DOUBLE) AS event_count,
                {rev_expr} AS original_revenue,
                {rev_expr} AS modified_revenue

            FROM events
            WHERE {raw_time_expr} <= '{import_upper_bound_str}'::TIMESTAMP
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

            # Reset revenue config for new dataset
            connection.execute("DELETE FROM revenue_event_selection")

            initialize_revenue_event_selection(connection)
            recompute_modified_revenue_columns(connection, "events_raw")
            recompute_modified_revenue_columns(connection, "events_normalized")

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

        s_cursor = connection.execute(
            """
            SELECT
                SUM(event_count) as total_events,
                COUNT(DISTINCT user_id) as total_users
            FROM events_normalized
            """
        )
        stats = to_dict(s_cursor, s_cursor.fetchone())

        total_events = stats.get("total_events") or 0
        total_users = stats.get("total_users") or 0

        stats_timer()

    except duckdb.ConversionException as exc:
        raise HTTPException(status_code=400, detail="Failed to convert event_time column to TIMESTAMP") from exc

    return {
        "status": "ok",
        "total_events": int(total_events),
        "total_users": int(total_users),
    }
