"""
Short summary: suggests and validates CSV column mappings.
"""
import json
import duckdb
from datetime import datetime, timedelta
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
    from app.domains.scope.filter_service import initialize_scoped_dataset_for_mapping
    from app.domains.cohorts.cohort_service import ensure_cohort_tables
    from app.domains.cohorts.activity_service import create_all_users_cohort, refresh_cohort_activity
    from app.domains.revenue.revenue_tables import ensure_revenue_event_selection_table
    from app.domains.revenue.revenue_config_service import initialize_revenue_event_selection
    from app.domains.revenue.revenue_recompute import recompute_modified_revenue_columns

    connection.execute("BEGIN TRANSACTION")
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

        connection.execute("DROP TABLE IF EXISTS events_base")
        connection.execute("DROP VIEW IF EXISTS events_normalized")
        
        # Explicit State Reset: Remove ghost data and potential blocking views
        connection.execute("DROP INDEX IF EXISTS pk_membership")
        connection.execute("DROP INDEX IF EXISTS idx_membership_user")
        connection.execute("DROP INDEX IF EXISTS idx_membership_cohort")
        connection.execute("DROP TABLE IF EXISTS cohort_membership")
        
        connection.execute("DROP INDEX IF EXISTS pk_link")
        connection.execute("DROP INDEX IF EXISTS idx_link_row")
        connection.execute("DROP INDEX IF EXISTS idx_link_cohort")
        connection.execute("DROP TABLE IF EXISTS cohort_event_link")
        
        connection.execute("DROP VIEW IF EXISTS cohort_activity_snapshot")
        connection.execute("DROP TABLE IF EXISTS cohort_activity_snapshot")
        connection.execute("DROP VIEW IF EXISTS events_scoped")
        connection.execute("DROP VIEW IF EXISTS events_scoped_raw")

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

        # ---------- CALCULATE BOUNDARY ----------
        time_q = quote_identifier(mapping.event_time_column)
        actual_time_type = actual_types.get(mapping.event_time_column)
        
        if actual_time_type == "TIMESTAMP":
            raw_time_expr = time_q
        else:
            v_col_raw = f"TRIM(BOTH '\"' FROM TRIM(CAST({time_q} AS VARCHAR)))"
            cleaned_expr = f"REPLACE(TRIM(REGEXP_REPLACE({v_col_raw}, '\\s*(UTC|Z|[+-]\\d{{2}}:?\\d{{2}})$', '', 'i')), 'T', ' ')"
            raw_time_expr = f"COALESCE(TRY_CAST({cleaned_expr} AS TIMESTAMP), TRY_CAST({cleaned_expr} || ':00:00' AS TIMESTAMP))"

        max_time_row = connection.execute(f"SELECT MAX({raw_time_expr}) FROM events").fetchone()
        max_time = max_time_row[0] if max_time_row else None
        
        now_local = datetime.now()
        future_cap = now_local + timedelta(days=1)
        if max_time:
            if not isinstance(max_time, datetime):
                try:
                    max_time = datetime.fromisoformat(str(max_time))
                except:
                    max_time = now_local
            import_upper_bound = min(max_time, future_cap)
        else:
            import_upper_bound = future_cap
        import_upper_bound_str = import_upper_bound.strftime('%Y-%m-%d %H:%M:%S')

        # ---------- MAPPING INITIALIZATION ----------
        init_mapping_timer = time_block("mapping_initialization")
        
        # 1. Create events_base (Primary physical table with all cleaned data)
        raw_count_expr = f"CAST({quote_identifier(mapping.event_count_column)} AS DOUBLE)" if mapping.event_count_column else "1.0"
        raw_rev_expr = f"COALESCE(CAST({quote_identifier(mapping.revenue_column)} AS DOUBLE), 0.0)" if mapping.revenue_column else "0.0"

        base_sql = f"""
            CREATE TABLE events_base AS
            SELECT
                {", ".join(grouped_expressions)},
                {raw_count_expr} AS event_count,
                {raw_rev_expr} AS original_revenue,
                {raw_rev_expr} AS modified_revenue,
                row_id
            FROM events
            WHERE {raw_time_expr} <= '{import_upper_bound_str}'::TIMESTAMP
        """
        connection.execute(base_sql)

        init_mapping_timer(rows=int(connection.execute("SELECT COUNT(*) FROM events_base").fetchone()[0]))

        # ---------- TABLE SETUP ----------
        # Crucial: Setup columns BEFORE attaching views or indexes to avoid Dependency Error
        setup_timer = time_block("table_setup")
        ensure_normalized_events_revenue_columns(connection, table_name="events_base")
        ensure_cohort_tables(connection)
        ensure_scope_tables(connection)
        ensure_revenue_event_selection_table(connection)

        # Defer indexing until after table setup is complete
        connection.execute("CREATE INDEX IF NOT EXISTS idx_events_base_user ON events_base (user_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_events_base_row ON events_base (row_id)")
        
        setup_timer()

        # ---------- DATA NORMALIZATION ----------
        normalize_timer = time_block("events_normalization")

        # 2. Create events_normalized View (Aggregated version for standard analytics)
        all_cols_to_group = ["user_id", "event_name", "event_time"] + [quote_identifier(c) for c in metadata_cols]
        group_by_sql = ", ".join(all_cols_to_group)
        
        # Ensure any legacy physical table is dropped to avoid conflicts with view creation
        try:
            connection.execute("DROP VIEW IF EXISTS events_normalized")
        except:
            pass
        try:
            connection.execute("DROP TABLE IF EXISTS events_normalized")
        except:
            pass

        meta_select = "".join([quote_identifier(c) + "," for c in metadata_cols])

        view_sql = f"""
            CREATE OR REPLACE VIEW events_normalized AS
            SELECT
                user_id,
                event_name,
                event_time,
                {meta_select}
                CAST(SUM(event_count) AS DOUBLE) AS event_count,
                CAST(SUM(original_revenue) AS DOUBLE) AS original_revenue,
                CAST(SUM(modified_revenue) AS DOUBLE) AS modified_revenue,
                MIN(row_id) AS row_id
            FROM events_base
            WHERE user_id IS NOT NULL 
              AND event_name IS NOT NULL 
              AND event_time IS NOT NULL
            GROUP BY {group_by_sql}
        """
        connection.execute(view_sql)
        
        # Fail Fast: Sanity checks
        row_count = int(connection.execute("SELECT COUNT(*) FROM events_base").fetchone()[0])
        if row_count == 0:
            raise HTTPException(status_code=400, detail="Normalization produced 0 rows. Check your column mapping.")

        normalize_timer(row_count=row_count)

        # ---------- DATASET INITIALIZATION ----------
        init_timer = time_block("dataset_initialization")

        # Global delete across all cohort-related tables
        connection.execute("DELETE FROM cohort_conditions")
        connection.execute("DELETE FROM cohorts")

        # Specialized initializer: Skips activity refresh redundant scan
        initialize_scoped_dataset_for_mapping(connection)

        init_timer()

        # ---------- REVENUE PIPELINE ----------
        if mapping.revenue_column:
            revenue_timer = time_block("revenue_initialization")
            connection.execute("DELETE FROM revenue_event_selection")
            initialize_revenue_event_selection(connection)
            recompute_modified_revenue_columns(connection, "events_base")
            
            # Refresh views as table replacement via CTAS may require catalog sync
            from app.domains.scope.filter_service import refresh_scoped_views
            refresh_scoped_views(connection)
            
            revenue_timer()
        else:
            connection.execute("DELETE FROM revenue_event_selection")

        # ---------- COHORT PRECOMPUTATION ----------
        cohort_timer = time_block("cohort_initialization")

        create_all_users_cohort(connection)
        
        # Invariant Guard: Ensure membership exists before snapshot
        membership_count = int(connection.execute("SELECT COUNT(*) FROM cohort_membership").fetchone()[0])
        if membership_count == 0:
             raise HTTPException(status_code=500, detail="Initial cohort membership build failed")

        # Final Single Activity Refresh
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
        
        if total_users == 0:
             raise HTTPException(status_code=400, detail="No unique users identified. Verify user_id mapping.")

        stats_timer()
        
        connection.commit()

    except Exception as e:
        connection.rollback()
        if isinstance(e, duckdb.ConversionException):
            raise HTTPException(status_code=400, detail="Failed to convert event_time column to TIMESTAMP") from e
        raise e

    return {
        "status": "ok",
        "total_events": int(total_events),
        "total_users": int(total_users),
    }
