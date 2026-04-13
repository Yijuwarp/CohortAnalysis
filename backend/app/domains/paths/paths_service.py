"""
Short summary: Service for Paths (Sequence Analysis) computation using DuckDB.
"""
from __future__ import annotations
import duckdb
import json
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Union
from fastapi import HTTPException
from app.models.paths_models import (
    PathsResponse, PathsCohortResult, PathsStepResult, 
    PathStep, PathStepGroup, PathStepFilter, PathDetail
)
from app.domains.cohorts.cohort_service import ensure_cohort_tables, get_events_source_table

# ---------------------------------------------------------------------------
# Table bootstrap
# ---------------------------------------------------------------------------

def ensure_path_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS paths (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            max_step_gap_minutes INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Migration: Add column if it doesn't exist
    cols = conn.execute("PRAGMA table_info('paths')").fetchall()
    if not any(col[1] == 'max_step_gap_minutes' for col in cols):
        conn.execute("ALTER TABLE paths ADD COLUMN max_step_gap_minutes INTEGER")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS paths_id_seq START 1")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS path_steps (
            id INTEGER PRIMARY KEY,
            path_id INTEGER NOT NULL,
            step_order INTEGER NOT NULL,
            group_id INTEGER DEFAULT 0,
            event_name TEXT NOT NULL
        )
    """)
    # Migration: Add group_id if it doesn't exist
    step_cols = conn.execute("PRAGMA table_info('path_steps')").fetchall()
    if not any(col[1] == 'group_id' for col in step_cols):
        conn.execute("ALTER TABLE path_steps ADD COLUMN group_id INTEGER DEFAULT 0")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS path_steps_id_seq START 1")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS path_step_filters (
            id INTEGER PRIMARY KEY,
            step_id INTEGER NOT NULL,
            property_key TEXT NOT NULL,
            property_value TEXT NOT NULL,
            property_type TEXT NOT NULL, -- 'str', 'int', 'float'
            operator TEXT NOT NULL DEFAULT 'equals'
        )
    """)
    conn.execute("CREATE SEQUENCE IF NOT EXISTS path_step_filters_id_seq START 1")

# ---------------------------------------------------------------------------
# Validation & Optimization Helpers
# ---------------------------------------------------------------------------

def path_uses_filters(steps: List[PathStep]) -> bool:
    return any(group.filters for step in steps for group in step.groups)

def _get_column_types(conn: duckdb.DuckDBPyConnection, table_name: str) -> Dict[str, str]:
    rows = conn.execute(f"DESCRIBE {table_name}").fetchall()
    return {row[0]: row[1] for row in rows}

def validate_path(conn: duckdb.DuckDBPyConnection, steps: List[PathStep]) -> Optional[str]:
    """
    Validates path steps in order:
    1. Event exists
    2. Property exists in events_scoped schema
    3. Property value exists (with casting check)
    Returns first failure reason or None.
    """
    source = get_events_source_table(conn)
    
    # Pre-fetch existing events to avoid repeat queries
    existing_events = {r[0] for r in conn.execute(f"SELECT DISTINCT event_name FROM {source}").fetchall()}
    col_types = _get_column_types(conn, source)

    # Step order validation (Duplicate/Gap check)
    orders = [s.step_order for s in steps]
    if len(orders) != len(set(orders)):
        return "Duplicate step orders found"
    if sorted(orders) != list(range(len(steps))):
        return f"Step orders must be continuous from 0 to {len(steps)-1}"

    for step in steps:
        if not step.groups:
            return f"Step {step.step_order + 1} has no events"
            
        seen_groups = set()
        for group in step.groups:
            if not group.event_name:
                return f"Step {step.step_order + 1}: Event name is required"
            
            if group.event_name not in existing_events:
                return f"Event not found: {group.event_name}"
            
            # Deduplicate groups logic (event + filters)
            # Simplified: just event name + sorted filters
            f_key = (group.event_name, tuple(sorted([(f.property_key, str(f.property_value)) for f in group.filters])))
            if f_key in seen_groups:
                # We'll just ignore or return error? PRD says dedupe or reject. 
                # Let's reject for now to be explicit.
                return f"Duplicate alternative event found in Step {step.step_order + 1}: {group.event_name}"
            seen_groups.add(f_key)

            # Conflicting filters check (basic)
            seen_filters = {} # key -> value
            for f in group.filters:
                if f.property_key in seen_filters and seen_filters[f.property_key] != f.property_value:
                    return f"Conflicting filters in Step {step.step_order + 1} for '{group.event_name}': {f.property_key} has multiple values"
                seen_filters[f.property_key] = f.property_value

                if f.property_key not in col_types:
                    return f"Property not found: {f.property_key}"
                
                # Casting check
                col_type = col_types[f.property_key].upper()
                val = f.property_value
                try:
                    if "INT" in col_type:
                        int(val)
                    elif "FLOAT" in col_type or "DOUBLE" in col_type or "DECIMAL" in col_type:
                        float(val)
                except (ValueError, TypeError):
                    return f"Invalid value for property {f.property_key}"

                # Value existence check
                safe_key = f.property_key.replace('"', '""')
                exists = conn.execute(
                    f'SELECT 1 FROM {source} WHERE "{safe_key}" = ? LIMIT 1',
                    [val]
                ).fetchone()
                if not exists:
                    return f"Property value not found: {f.property_key}={val}"
    
    return None

# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def create_path(conn: duckdb.DuckDBPyConnection, name: str, steps: List[PathStep], max_step_gap_minutes: Optional[int] = None) -> PathDetail:
    ensure_path_tables(conn)
    path_id = conn.execute(
        "INSERT INTO paths (id, name, max_step_gap_minutes, created_at) VALUES (nextval('paths_id_seq'), ?, ?, ?) RETURNING id",
        [name, max_step_gap_minutes, datetime.now(timezone.utc)]
    ).fetchone()[0]

    for step in steps:
        for g_idx, group in enumerate(step.groups):
            step_id = conn.execute(
                "INSERT INTO path_steps (id, path_id, step_order, group_id, event_name) VALUES (nextval('path_steps_id_seq'), ?, ?, ?, ?) RETURNING id",
                [path_id, step.step_order, g_idx, group.event_name]
            ).fetchone()[0]
            
            for f in group.filters:
                ptype = 'str'
                if isinstance(f.property_value, int): ptype = 'int'
                elif isinstance(f.property_value, float): ptype = 'float'
                
                conn.execute(
                    "INSERT INTO path_step_filters (id, step_id, property_key, property_value, property_type) VALUES (nextval('path_step_filters_id_seq'), ?, ?, ?, ?)",
                    [step_id, f.property_key, str(f.property_value), ptype]
                )

    reason = validate_path(conn, steps)
    return PathDetail(
        id=path_id,
        name=name,
        steps=steps,
        is_valid=(reason is None),
        invalid_reason=reason,
        max_step_gap_minutes=max_step_gap_minutes,
        created_at=datetime.now(timezone.utc).isoformat()
    )

def update_path(conn: duckdb.DuckDBPyConnection, path_id: int, name: str, steps: List[PathStep], max_step_gap_minutes: Optional[int] = None) -> PathDetail:
    ensure_path_tables(conn)
    conn.execute("UPDATE paths SET name = ?, max_step_gap_minutes = ? WHERE id = ?", [name, max_step_gap_minutes, path_id])
    
    # Delete sequences
    old_step_ids = [r[0] for r in conn.execute("SELECT id FROM path_steps WHERE path_id = ?", [path_id]).fetchall()]
    for sid in old_step_ids:
        conn.execute("DELETE FROM path_step_filters WHERE step_id = ?", [sid])
    conn.execute("DELETE FROM path_steps WHERE path_id = ?", [path_id])

    # Insert new
    for step in steps:
        for g_idx, group in enumerate(step.groups):
            step_id = conn.execute(
                "INSERT INTO path_steps (id, path_id, step_order, group_id, event_name) VALUES (nextval('path_steps_id_seq'), ?, ?, ?, ?) RETURNING id",
                [path_id, step.step_order, g_idx, group.event_name]
            ).fetchone()[0]
            
            for f in group.filters:
                ptype = 'str'
                if isinstance(f.property_value, int): ptype = 'int'
                elif isinstance(f.property_value, float): ptype = 'float'
                
                conn.execute(
                    "INSERT INTO path_step_filters (id, step_id, property_key, property_value, property_type) VALUES (nextval('path_step_filters_id_seq'), ?, ?, ?, ?)",
                    [step_id, f.property_key, str(f.property_value), ptype]
                )

    reason = validate_path(conn, steps)
    return PathDetail(
        id=path_id,
        name=name,
        steps=steps,
        is_valid=(reason is None),
        invalid_reason=reason,
        max_step_gap_minutes=max_step_gap_minutes,
        created_at=datetime.now(timezone.utc).isoformat()
    )

def get_path_by_id(conn: duckdb.DuckDBPyConnection, path_id: int) -> PathDetail:
    """Helper to fetch a single path by ID with all its steps and filters. Handles one-way migration."""
    rows = conn.execute("SELECT id, name, max_step_gap_minutes, created_at FROM paths WHERE id = ?", [path_id]).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"Path with ID {path_id} not found")
    
    pid, name, max_step_gap_minutes, created_at = rows[0]
    # Fetch all steps for this path
    step_rows = conn.execute("SELECT id, step_order, group_id, event_name FROM path_steps WHERE path_id = ? ORDER BY step_order, group_id", [pid]).fetchall()
    
    from app.models.paths_models import PathStepGroup
    
    steps_dict: Dict[int, PathStep] = {}
    for sid, s_order, g_id, s_event in step_rows:
        filter_rows = conn.execute("SELECT property_key, property_value, property_type FROM path_step_filters WHERE step_id = ? ORDER BY id", [sid]).fetchall()
        filters = []
        for f_key, f_val, f_type in filter_rows:
            typed_val = f_val
            if f_type == 'int': typed_val = int(f_val)
            elif f_type == 'float': typed_val = float(f_val)
            filters.append(PathStepFilter(property_key=f_key, property_value=typed_val))
        
        group = PathStepGroup(event_name=s_event, filters=filters)
        if s_order not in steps_dict:
            steps_dict[s_order] = PathStep(step_order=s_order, groups=[])
        steps_dict[s_order].groups.append(group)
    
    # Sort steps by order
    steps = [steps_dict[order] for order in sorted(steps_dict.keys())]
    
    reason = validate_path(conn, steps)
    return PathDetail(
        id=pid,
        name=name,
        steps=steps,
        max_step_gap_minutes=max_step_gap_minutes,
        is_valid=(reason is None),
        invalid_reason=reason,
        created_at=created_at.isoformat() if created_at else ""
    )

def list_paths(conn: duckdb.DuckDBPyConnection) -> List[PathDetail]:
    ensure_path_tables(conn)
    rows = conn.execute("SELECT id FROM paths ORDER BY id").fetchall()
    return [get_path_by_id(conn, r[0]) for r in rows]

def delete_path(conn: duckdb.DuckDBPyConnection, path_id: int) -> bool:
    ensure_path_tables(conn)
    old_step_ids = [r[0] for r in conn.execute("SELECT id FROM path_steps WHERE path_id = ?", [path_id]).fetchall()]
    for sid in old_step_ids:
        conn.execute("DELETE FROM path_step_filters WHERE step_id = ?", [sid])
    conn.execute("DELETE FROM path_steps WHERE path_id = ?", [path_id])
    conn.execute("DELETE FROM paths WHERE id = ?", [path_id])
    return True

# ---------------------------------------------------------------------------
# Matching Logic
# ---------------------------------------------------------------------------

def _build_filter_clause(filters: List[PathStepFilter], col_types: Dict[str, str]) -> str:
    if not filters:
        return ""
    clauses = []
    for f in filters:
        safe_key = f.property_key.replace('"', '""')
        col_type = col_types.get(f.property_key, "VARCHAR").upper()
        
        # Cast value to column type in SQL
        if isinstance(f.property_value, str):
            val_sql = f"'{f.property_value.replace(chr(39), chr(39)+chr(39))}'"
        else:
            val_sql = str(f.property_value)
            
        clauses.append(f'CAST("{safe_key}" AS {col_type}) = CAST({val_sql} AS {col_type})')
    
    return " AND " + " AND ".join(clauses)

def _build_paths_base_query(steps: List[PathStep], conn: duckdb.DuckDBPyConnection, cohort_id: Optional[int] = None, limit_steps: Optional[int] = None, max_step_gap_minutes: Optional[int] = None) -> str:
    """
    Refactored SQL builder for Paths sequence matching with OR support.
    Enforces deterministic greedy matching using internal rn as tie-breaker.
    Uses the Union Candidates pattern for clean SQL and traceability.
    """
    use_filters = path_uses_filters(steps)
    source_table = get_events_source_table(conn) if use_filters else "cohort_activity_snapshot"
    col_types = _get_column_types(conn, source_table)
    
    # Pre-calculate which columns we need to project
    filter_keys = set()
    for s in steps:
        for group in s.groups:
            for f in group.filters:
                filter_keys.add(f.property_key)
    
    core_cols = {"user_id", "event_name", "event_time"}
    cols_to_select = core_cols.union(filter_keys)
    available_select = [c for c in col_types if c in cols_to_select]
    
    # We always need cohort_id. If not in source, we join with membership.
    has_cohort_id = "cohort_id" in col_types
    
    sql = "WITH "
    
    # ---------------- BASE CTE (Materialized with Tie-Break) ----------------
    proj_list = []
    if not has_cohort_id:
        proj_list.append("cm.cohort_id")
        source_ref = f"{source_table} e JOIN cohort_membership cm ON e.user_id = cm.user_id AND e.event_time >= cm.join_time"
        if cohort_id is not None:
            source_ref += f" AND cm.cohort_id = {cohort_id}"
        event_ref = "e"
    else:
        proj_list.append("cohort_id")
        source_ref = source_table
        if cohort_id is not None:
            source_ref += f" WHERE cohort_id = {cohort_id}"
        event_ref = ""
        
    for c in available_select:
        safe_c = f'"{c.replace(chr(34), chr(34)+chr(34))}"'
        proj_list.append(f"{event_ref}.{safe_c}" if event_ref else safe_c)

    # Use a stable tie-breaker: capture the ingestion-time order (row_id)
    prefix = "e." if not has_cohort_id else ""
    tie_breaker = f"{prefix}row_id"

    sql += f"""
    base AS (
      SELECT
        {", ".join(proj_list)},
        ROW_NUMBER() OVER (
          PARTITION BY {"cm.cohort_id" if not has_cohort_id else "cohort_id"}, {prefix}user_id
          ORDER BY {prefix}event_time, {tie_breaker}
        ) AS rn
      FROM {source_ref}
    ),
    """
    
    steps_to_process = limit_steps if limit_steps else len(steps)
    ctes = []
    
    for i in range(steps_to_process):
        step = steps[i]
        s_idx = i + 1
        
        # Step-scoped event filter optimization
        events_in_step = [g.event_name for g in step.groups]
        event_in_sql = ", ".join([f"'{e.replace(chr(39), chr(39)+chr(39))}'" for e in events_in_step])
        
        # Candidate generation
        group_queries = []
        for g_idx, group in enumerate(step.groups):
            safe_event = group.event_name.replace("'", "''")
            filter_clause = _build_filter_clause(group.filters, col_types)
            
            if s_idx == 1:
                query = f"""
                SELECT cohort_id, user_id, event_time AS t1, rn AS rn1, {g_idx} AS group_id
                FROM base
                WHERE event_name = '{safe_event}' {filter_clause}
                """
            else:
                prev = i
                # Gap constraint
                gap_clause = ""
                if max_step_gap_minutes is not None:
                    gap_clause = f"AND b.event_time <= s.t{prev} + INTERVAL '{max_step_gap_minutes} MINUTES'"
                
                query = f"""
                SELECT
                  s.cohort_id, s.user_id,
                  {", ".join([f"s.t{j}" for j in range(1, s_idx)])},
                  b.event_time AS t{s_idx},
                  b.rn AS rn{s_idx},
                  {g_idx} AS group_id,
                  EXTRACT(EPOCH FROM (b.event_time - s.t{prev})) AS time_sec
                FROM step_{prev} s
                JOIN base b ON b.user_id = s.user_id AND b.cohort_id = s.cohort_id
                WHERE b.event_name = '{safe_event}' {filter_clause}
                  AND (b.event_time > s.t{prev} OR (b.event_time = s.t{prev} AND b.rn > s.rn{prev}))
                  {gap_clause}
                """
            group_queries.append(query)
            
        union_sql = "\nUNION ALL\n".join(group_queries)
        
        candidates_cte = f"step_{s_idx}_candidates AS (\n{union_sql}\n)"
        step_cte = f"""
        step_{s_idx} AS (
          SELECT *
          FROM step_{s_idx}_candidates
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY cohort_id, user_id 
            ORDER BY t{s_idx}, rn{s_idx}
          ) = 1
        )
        """
        ctes.append(candidates_cte)
        ctes.append(step_cte)
        
    final_sql = sql + ",\n".join(ctes) + "\n"
    return final_sql

# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

def _format_step_name(step: PathStep) -> str:
    """Formats a multi-event step name: 'Event A (prop=val) OR Event B'."""
    parts = []
    for group in step.groups:
        s = group.event_name
        if group.filters:
            f_parts = [f"{f.property_key}={f.property_value}" for f in group.filters]
            s += f" ({' & '.join(f_parts)})"
        parts.append(s)
    return " OR ".join(parts)

def run_paths(conn: duckdb.DuckDBPyConnection, input_steps: Union[List[str], List[PathStep]], max_step_gap_minutes: Optional[int] = None, path_id: Optional[int] = None) -> Dict[str, Any]:
    """
    Executes Paths analysis with deterministic greedy matching and branch-level breakdown.
    """
    path_name = "Unsaved Path"
    if path_id is not None:
        path = get_path_by_id(conn, path_id)
        path_name = path.name
        steps = path.steps
        max_step_gap_minutes = path.max_step_gap_minutes
    else:
        # Normalize input_steps if they are raw strings
        steps: List[PathStep] = []
        if all(isinstance(s, str) for s in input_steps):
            for idx, sname in enumerate(input_steps):
                steps.append(PathStep(step_order=idx, groups=[PathStepGroup(event_name=sname, filters=[])]))
        else:
            steps = input_steps

    if not steps:
        raise HTTPException(status_code=400, detail="No steps provided")

    ensure_cohort_tables(conn)

    # 1. Get active, non-hidden cohorts
    active_cohorts = conn.execute("""
        SELECT cohort_id, name 
        FROM cohorts 
        WHERE is_active = TRUE AND hidden = FALSE
        ORDER BY cohort_id
    """).fetchall()

    if not active_cohorts:
        return {
            "path_name": path_name,
            "steps": [_format_step_name(s) for s in steps],
            "max_step_gap_minutes": max_step_gap_minutes,
            "results": [],
            "global_insights": []
        }

    # Identify top 3 cohorts by size for cross-cohort comparison
    cohort_sizes_raw = conn.execute("""
        SELECT cm.cohort_id, COUNT(DISTINCT cm.user_id) as size
        FROM cohort_membership cm
        JOIN cohorts c ON cm.cohort_id = c.cohort_id
        WHERE c.is_active = TRUE AND c.hidden = FALSE
        GROUP BY cm.cohort_id
        ORDER BY size DESC
        LIMIT 3
    """).fetchall()
    top_cohort_ids = {row[0] for row in cohort_sizes_raw}

    # Build Sequence Matching SQL
    base_sql = _build_paths_base_query(steps, conn, max_step_gap_minutes=max_step_gap_minutes)
    
    # ---------------- AGGREGATION ----------------
    # We aggregate total users AND group_id distribution per step
    agg_parts = []
    for i in range(len(steps)):
        s_idx = i + 1
        agg_parts.append(f"""
        SELECT 
            {s_idx} AS step_idx, cohort_id, group_id, COUNT(DISTINCT user_id) AS users,
            {f"AVG(time_sec)" if s_idx > 1 else "NULL"} AS mean_time,
            {f"CASE WHEN COUNT(DISTINCT user_id) >= 50 THEN approx_quantile(time_sec, 0.2) ELSE NULL END" if s_idx > 1 else "NULL"} AS p20,
            {f"CASE WHEN COUNT(DISTINCT user_id) >= 50 THEN approx_quantile(time_sec, 0.8) ELSE NULL END" if s_idx > 1 else "NULL"} AS p80
        FROM step_{s_idx}
        GROUP BY 1, 2, 3""")
            
    full_sql = base_sql + " UNION ALL ".join(agg_parts)
    raw_results = conn.execute(full_sql).fetchall()
    
    # Map results: cohort_id -> step_idx -> breakdown(group_id -> metrics)
    metrics_map = {} # c_id -> s_idx -> { total_users, groups: { g_id: users }, ...times }
    for row in raw_results:
        s_idx, c_id, g_id, users, mean, p20, p80 = row
        s_data = metrics_map.setdefault(c_id, {}).setdefault(s_idx, {
            "total_users": 0, "groups": {}, "mean": None, "p20": None, "p80": None
        })
        s_data["total_users"] += users
        s_data["groups"][g_id] = users
        if mean is not None: s_data["mean"] = mean
        if p20 is not None: s_data["p20"] = p20
        if p80 is not None: s_data["p80"] = p80
    
    # Get cohort sizes
    cohort_sizes = {row[0]: row[1] for row in conn.execute("""
        SELECT cohort_id, COUNT(DISTINCT user_id) 
        FROM cohort_membership 
        GROUP BY cohort_id
    """).fetchall()}

    results = []
    for c_id, c_name in active_cohorts:
        c_size = cohort_sizes.get(c_id, 0)
        if c_size == 0: continue
            
        c_metrics = metrics_map.get(c_id, {})
        if 1 not in c_metrics:
            results.append(PathsCohortResult(
                cohort_id=c_id,
                cohort_name=c_name,
                cohort_size=c_size,
                steps=[PathsStepResult(step=i+1, event=_format_step_name(steps[i]), users=0, conversion_pct=0.0) for i in range(len(steps))],
                insights=["No users reached Step 1."]
            ))
            continue

        cohort_steps = []
        for i, step_def in enumerate(steps):
            step_idx = i + 1
            m = c_metrics.get(step_idx, {"total_users": 0, "groups": {}, "mean": None, "p20": None, "p80": None})
            
            user_count = m["total_users"]
            conversion_pct = round(user_count / c_size * 100, 1) if c_size > 0 else 0.0
            
            drop_off_pct = None
            if step_idx > 1:
                prev_users = c_metrics.get(step_idx - 1, {"total_users": 0})["total_users"]
                drop_off_pct = round((prev_users - user_count) / prev_users * 100, 1) if prev_users > 0 else 0.0

            # Calculate group breakdown
            breakdown = {}
            if len(step_def.groups) > 1 and user_count > 0:
                for g_idx, group in enumerate(step_def.groups):
                    g_users = m["groups"].get(g_idx, 0)
                    g_name = group.event_name
                    if group.filters:
                        g_name += f" ({group.filters[0].property_key}={group.filters[0].property_value}...)"
                    breakdown[g_name] = round(g_users / user_count * 100, 1)

            cohort_steps.append(PathsStepResult(
                step=step_idx,
                event=_format_step_name(step_def),
                users=user_count,
                conversion_pct=conversion_pct,
                drop_off_pct=drop_off_pct,
                mean_time=round(m["mean"], 1) if m["mean"] is not None else None,
                p20=round(m["p20"], 1) if m["p20"] is not None else None,
                p80=round(m["p80"], 1) if m["p80"] is not None else None,
                group_breakdown=breakdown if breakdown else None
            ))

            # Insights
            insights = []
            if drop_off_pct and drop_off_pct > 20:
                insights.append(f"Significant drop-off ({drop_off_pct}%) at step {step_idx} ({cohort_steps[-1].event})")
            if m["p80"] and m["p80"] > 120:
                insights.append(f"Slow progression to step {step_idx}: 80% takes >120s")
            if m["p80"] and m["p20"] and m["p20"] > 0:
                variability = m["p80"] / m["p20"]
                if variability > 5:
                    insights.append(f"High variability in time to step {step_idx} (p80/p20 > 5x)")
            
            if step_idx == 1 and c_size > 0:
                dropoff_count = c_size - user_count
                dropoff_rate = dropoff_count / c_size
                if dropoff_rate > 0.2:
                    insights.append(f"{round(dropoff_rate * 100, 1)}% of users did not start this flow ({cohort_steps[-1].event})")

        results.append(PathsCohortResult(
            cohort_id=c_id,
            cohort_name=c_name,
            cohort_size=c_size,
            steps=cohort_steps,
            insights=insights
        ))

    # Cross-cohort Insights (Top 3 only)
    global_insights = []
    comparison_results = [r for r in results if r.cohort_id in top_cohort_ids]
    if len(comparison_results) >= 2:
        for i in range(len(comparison_results)):
            for j in range(i + 1, len(comparison_results)):
                r1, r2 = comparison_results[i], comparison_results[j]
                for k in range(len(steps)):
                    c1, c2 = r1.steps[k].conversion_pct, r2.steps[k].conversion_pct
                    if abs(c1 - c2) > 10:
                        higher = r1.cohort_name if c1 > c2 else r2.cohort_name
                        lower = r2.cohort_name if c1 > c2 else r1.cohort_name
                        global_insights.append(
                            f"{higher} has significantly higher conversion (+{abs(c1-c2):.1f}%) "
                            f"at step {k+1} ({steps[k].groups[0].event_name if hasattr(steps[k], 'groups') else steps[k].event_name}) than {lower}"
                        )

    return {
        "path_name": path_name,
        "steps": [_format_step_name(s) for s in steps],
        "max_step_gap_minutes": max_step_gap_minutes,
        "results": results,
        "global_insights": global_insights
    }

# ---------------------------------------------------------------------------
# Cohort Creation logic using optimized queries
# ---------------------------------------------------------------------------

def create_paths_dropoff_cohort(
    conn: duckdb.DuckDBPyConnection, 
    cohort_id: int, 
    step_index: int, 
    steps_raw: Union[List[str], List[PathStep]],
    max_step_gap_minutes: Optional[int] = None,
    path_id: Optional[int] = None,
    cohort_name: Optional[str] = None
) -> Dict[str, Any]:
    if path_id is not None:
        path = get_path_by_id(conn, path_id)
        steps = path.steps
        max_step_gap_minutes = path.max_step_gap_minutes
    else:
        steps: List[PathStep] = []
        if all(isinstance(s, str) for s in steps_raw):
            for idx, sname in enumerate(steps_raw):
                steps.append(PathStep(step_order=idx, groups=[PathStepGroup(event_name=sname, filters=[])]))
        else:
            steps = steps_raw

    if step_index < 1 or step_index > len(steps):
        raise HTTPException(status_code=400, detail=f"Invalid step index {step_index} for path with {len(steps)} steps")
    
    if step_index == 1:
        find_users_sql = _build_paths_base_query(steps, conn, cohort_id, 1, max_step_gap_minutes=max_step_gap_minutes) + f"""
            SELECT DISTINCT m.user_id, m.join_time
            FROM cohort_membership m
            LEFT JOIN step_1 s ON m.user_id = s.user_id AND m.cohort_id = s.cohort_id
            WHERE m.cohort_id = {cohort_id}
              AND s.user_id IS NULL
        """
    else:
        find_users_sql = _build_paths_base_query(steps, conn, cohort_id, step_index, max_step_gap_minutes=max_step_gap_minutes) + f"""
            SELECT DISTINCT s_prev.user_id, m.join_time
            FROM step_{step_index-1} s_prev
            JOIN cohort_membership m ON s_prev.user_id = m.user_id AND s_prev.cohort_id = m.cohort_id
            ANTI JOIN step_{step_index} s_curr ON s_prev.user_id = s_curr.user_id AND s_prev.cohort_id = s_curr.cohort_id
            WHERE s_prev.cohort_id = {cohort_id}
        """
    
    rows = conn.execute(find_users_sql).fetchall()
    drop_off_users = [(row[0], row[1]) for row in rows]
    
    if not drop_off_users:
        return {"cohort_id": None, "message": "No users found in drop-off."}

    if cohort_name:
        new_name = cohort_name
    else:
        c_name_row = conn.execute("SELECT name FROM cohorts WHERE cohort_id = ?", [cohort_id]).fetchone()
        parent_name = c_name_row[0] if c_name_row else "Unknown"
        event_names = [_format_step_name(s) for s in steps]
        if step_index == 1:
            new_name = f"{parent_name} - Didn't perform Step 1 ({event_names[0]})"
        else:
            new_name = f"{parent_name} - Drop off after Step {step_index-1} ({event_names[step_index-2]})"

    return _materialize_paths_cohort(conn, new_name, drop_off_users)


def create_paths_reached_cohort(
    conn: duckdb.DuckDBPyConnection, 
    cohort_id: int, 
    step_index: int, 
    steps_raw: Union[List[str], List[PathStep]],
    max_step_gap_minutes: Optional[int] = None,
    path_id: Optional[int] = None,
    cohort_name: Optional[str] = None
) -> Dict[str, Any]:
    if path_id is not None:
        path = get_path_by_id(conn, path_id)
        steps = path.steps
        max_step_gap_minutes = path.max_step_gap_minutes
    else:
        steps: List[PathStep] = []
        if all(isinstance(s, str) for s in steps_raw):
            for idx, sname in enumerate(steps_raw):
                steps.append(PathStep(step_order=idx, groups=[PathStepGroup(event_name=sname, filters=[])]))
        else:
            steps = steps_raw

    if step_index < 1 or step_index > len(steps):
        raise HTTPException(status_code=400, detail=f"Invalid step index {step_index} for path with {len(steps)} steps")

    find_users_sql = _build_paths_base_query(steps, conn, cohort_id, step_index, max_step_gap_minutes=max_step_gap_minutes) + f"""
        SELECT DISTINCT s.user_id, m.join_time
        FROM step_{step_index} s
        JOIN cohort_membership m ON s.user_id = m.user_id AND s.cohort_id = m.cohort_id
        WHERE s.cohort_id = {cohort_id}
    """
    
    rows = conn.execute(find_users_sql).fetchall()
    reached_users = [(row[0], row[1]) for row in rows]
    
    if not reached_users:
        return {"cohort_id": None, "message": "No users found for this step."}

    if cohort_name:
        new_name = cohort_name
    else:
        c_name_row = conn.execute("SELECT name FROM cohorts WHERE cohort_id = ?", [cohort_id]).fetchone()
        parent_name = c_name_row[0] if c_name_row else "Unknown"
        event_names = [_format_step_name(s) for s in steps]
        new_name = f"{parent_name} - Reached Step {step_index} ({event_names[step_index-1]})"

    return _materialize_paths_cohort(conn, new_name, reached_users)

def _materialize_paths_cohort(conn: duckdb.DuckDBPyConnection, name: str, users: List[tuple]) -> Dict[str, Any]:
    ensure_cohort_tables(conn)
    
    new_c_id = conn.execute(
        "INSERT INTO cohorts (cohort_id, name, is_active, hidden, cohort_origin) "
        "VALUES (nextval('cohorts_id_sequence'), ?, TRUE, FALSE, 'paths') RETURNING cohort_id",
        [name]
    ).fetchone()[0]

    membership_data = [(u[0], new_c_id, u[1]) for u in users]
    conn.executemany(
        "INSERT INTO cohort_membership (user_id, cohort_id, join_time) VALUES (?, ?, ?)",
        membership_data
    )

    source_table = get_events_source_table(conn)

    conn.execute(f"""
        INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name)
        SELECT m.cohort_id, e.user_id, e.event_time, e.event_name
        FROM {source_table} e
        JOIN cohort_membership m ON e.user_id = m.user_id
        WHERE m.cohort_id = {new_c_id}
    """)

    return {"cohort_id": int(new_c_id), "name": name, "user_count": len(users)}
