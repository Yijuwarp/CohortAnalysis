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
    PathStep, PathStepFilter, PathDetail
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("CREATE SEQUENCE IF NOT EXISTS paths_id_seq START 1")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS path_steps (
            id INTEGER PRIMARY KEY,
            path_id INTEGER NOT NULL,
            step_order INTEGER NOT NULL,
            event_name TEXT NOT NULL
        )
    """)
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
    return any(step.filters for step in steps)

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
        if step.event_name not in existing_events:
            return f"Event not found: {step.event_name}"
        
        for f in step.filters:
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
            # For existence check, we cast value to SQL string if needed, 
            # or use placeholders if we were using a real DB driver. 
            # DuckDB execute takes params.
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

def create_path(conn: duckdb.DuckDBPyConnection, name: str, steps: List[PathStep]) -> PathDetail:
    ensure_path_tables(conn)
    path_id = conn.execute(
        "INSERT INTO paths (id, name, created_at) VALUES (nextval('paths_id_seq'), ?, ?) RETURNING id",
        [name, datetime.now(timezone.utc)]
    ).fetchone()[0]

    for step in steps:
        step_id = conn.execute(
            "INSERT INTO path_steps (id, path_id, step_order, event_name) VALUES (nextval('path_steps_id_seq'), ?, ?, ?) RETURNING id",
            [path_id, step.step_order, step.event_name]
        ).fetchone()[0]
        
        for f in step.filters:
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
        created_at=datetime.now(timezone.utc).isoformat()
    )

def update_path(conn: duckdb.DuckDBPyConnection, path_id: int, name: str, steps: List[PathStep]) -> PathDetail:
    ensure_path_tables(conn)
    conn.execute("UPDATE paths SET name = ? WHERE id = ?", [name, path_id])
    
    # Delete sequences
    old_step_ids = [r[0] for r in conn.execute("SELECT id FROM path_steps WHERE path_id = ?", [path_id]).fetchall()]
    for sid in old_step_ids:
        conn.execute("DELETE FROM path_step_filters WHERE step_id = ?", [sid])
    conn.execute("DELETE FROM path_steps WHERE path_id = ?", [path_id])

    # Insert new
    for step in steps:
        step_id = conn.execute(
            "INSERT INTO path_steps (id, path_id, step_order, event_name) VALUES (nextval('path_steps_id_seq'), ?, ?, ?) RETURNING id",
            [path_id, step.step_order, step.event_name]
        ).fetchone()[0]
        
        for f in step.filters:
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
        created_at=datetime.now(timezone.utc).isoformat()
    )

def list_paths(conn: duckdb.DuckDBPyConnection) -> List[PathDetail]:
    ensure_path_tables(conn)
    rows = conn.execute("SELECT id, name, created_at FROM paths ORDER BY id").fetchall()
    results = []
    for pid, name, created_at in rows:
        step_rows = conn.execute("SELECT id, step_order, event_name FROM path_steps WHERE path_id = ? ORDER BY step_order", [pid]).fetchall()
        steps = []
        for sid, s_order, s_event in step_rows:
            filter_rows = conn.execute("SELECT property_key, property_value, property_type FROM path_step_filters WHERE step_id = ? ORDER BY id", [sid]).fetchall()
            filters = []
            for f_key, f_val, f_type in filter_rows:
                typed_val = f_val
                if f_type == 'int': typed_val = int(f_val)
                elif f_type == 'float': typed_val = float(f_val)
                filters.append(PathStepFilter(property_key=f_key, property_value=typed_val))
            steps.append(PathStep(step_order=s_order, event_name=s_event, filters=filters))
        
        reason = validate_path(conn, steps)
        results.append(PathDetail(
            id=pid,
            name=name,
            steps=steps,
            is_valid=(reason is None),
            invalid_reason=reason,
            created_at=created_at.isoformat() if created_at else ""
        ))
    return results

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

def _build_paths_base_query(steps: List[PathStep], conn: duckdb.DuckDBPyConnection, cohort_id: Optional[int] = None, limit_steps: Optional[int] = None) -> str:
    """
    Refactored SQL builder for Paths sequence matching.
    Enforces deterministic greedy matching using internal rn as tie-breaker.
    If filters exist, joins events_scoped with cohort_membership.
    Otherwise uses cohort_activity_snapshot.
    """
    use_filters = path_uses_filters(steps)
    source_table = get_events_source_table(conn) if use_filters else "cohort_activity_snapshot"
    col_types = _get_column_types(conn, source_table)
    
    sql = "WITH "
    
    # Identify unique keys used in filters for selective projection
    filter_keys = set()
    for s in steps:
        for f in s.filters:
            if f.property_key:
                filter_keys.add(f.property_key)
    
    # Core columns always needed for matching + deterministic sorting
    # We must ensure cohort_id, user_id, event_name, event_time are always projected.
    core_cols = {"user_id", "event_name", "event_time"}
    cols_to_select = core_cols.union(filter_keys)
    
    # Identify which columns from the source table to project
    available_select = [c for c in col_types if c in cols_to_select]
    
    if use_filters:
        # 1. Scoped Path (events_scoped): Joined with cohort_membership
        cohort_join_filter = f"AND cm.cohort_id = {cohort_id}" if cohort_id is not None else ""

        # Build projection list. cm.cohort_id is mandatory.
        proj_list = ["cm.cohort_id"]
        for c in available_select:
            escaped_c = c.replace(chr(34), chr(34)+chr(34))
            proj_list.append(f'e."{escaped_c}"')

        sql += f"""
        base AS (
          SELECT
            {", ".join(proj_list)},
            ROW_NUMBER() OVER (
              PARTITION BY cm.cohort_id, e.user_id
              ORDER BY e.event_time, e.event_name
            ) AS rn
          FROM {source_table} e
          JOIN cohort_membership cm 
            ON e.user_id = cm.user_id
            AND e.event_time >= cm.join_time
            {cohort_join_filter}
        ),
        """
    else:
        # 2. Snapshot Path (cohort_activity_snapshot)
        cohort_filter = f"WHERE cohort_id = {cohort_id}" if cohort_id is not None else ""
        
        # Determine if cohort_id is native to the snapshot
        snapshot_has_cohort_id = "cohort_id" in col_types
        
        if snapshot_has_cohort_id:
            proj_list = ["cohort_id"]
            for c in available_select:
                proj_list.append(f'"{c.replace(chr(34), chr(34)+chr(34))}"')
                
            sql += f"""
            base AS (
              SELECT
                {", ".join(proj_list)},
                ROW_NUMBER() OVER (
                  PARTITION BY cohort_id, user_id
                  ORDER BY event_time, event_name
                ) AS rn
              FROM cohort_activity_snapshot
              {cohort_filter}
            ),
            """
        else:
            # Defensive fallback if snapshot is missing cohort_id: Join with membership
            proj_list = ["cm.cohort_id"]
            for c in available_select:
                proj_list.append(f's."{c.replace(chr(34), chr(34)+chr(34))}"')
                
            sql += f"""
            base AS (
              SELECT
                {", ".join(proj_list)},
                ROW_NUMBER() OVER (
                  PARTITION BY cm.cohort_id, s.user_id
                  ORDER BY s.event_time, s.event_name
                ) AS rn
              FROM cohort_activity_snapshot s
              JOIN cohort_membership cm ON s.user_id = cm.user_id
              {cohort_filter.replace("cohort_id", "cm.cohort_id")}
            ),
            """
    
    steps_to_process = limit_steps if limit_steps else len(steps)
    ctes = []
    
    for i in range(steps_to_process):
        step = steps[i]
        s_idx = i + 1
        safe_event = step.event_name.replace("'", "''")
        filter_clause = _build_filter_clause(step.filters, col_types)
        
        if s_idx == 1:
            # Step 1: matches the first valid occurrence per user (after filters)
            cte = f"""
            step_1 AS (
              SELECT cohort_id, user_id, event_time AS t1, rn AS rn1
              FROM base
              WHERE event_name = '{safe_event}'
              {filter_clause}
              QUALIFY ROW_NUMBER() OVER (
                PARTITION BY cohort_id, user_id 
                ORDER BY event_time, rn
              ) = 1
            )
            """
        else:
            prev = i
            # Step N: matches the first valid occurrence where (t > prev_t) OR (t = prev_t AND rn > prev_rn)
            # Greedy earliest match enforced by QUALIFY ROW_NUMBER ... = 1
            cte = f"""
            step_{s_idx} AS (
              SELECT
                s.cohort_id, s.user_id,
                {", ".join([f"s.t{j}" for j in range(1, s_idx)])},
                b.event_time AS t{s_idx},
                b.rn AS rn{s_idx},
                EXTRACT(EPOCH FROM (b.event_time - s.t{prev})) AS time_sec
              FROM step_{prev} s
              JOIN base b ON b.user_id = s.user_id AND b.cohort_id = s.cohort_id
              WHERE b.event_name = '{safe_event}'
                {filter_clause}
                AND (b.event_time > s.t{prev} OR (b.event_time = s.t{prev} AND b.rn > s.rn{prev}))
              QUALIFY ROW_NUMBER() OVER (
                PARTITION BY s.cohort_id, s.user_id 
                ORDER BY b.event_time, b.rn
              ) = 1
            )
            """
        ctes.append(cte)
        
    final_sql = sql + ",\n".join(ctes) + "\n"
    
    # Defensive guard to ensure cohort_id propagation is intact across all execution paths
    if "cohort_id" not in final_sql:
        raise Exception("cohort_id missing in base query generation")
        
    return final_sql

# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

def run_paths(conn: duckdb.DuckDBPyConnection, input_steps: Union[List[str], List[PathStep]]) -> Dict[str, Any]:
    """
    Executes Paths analysis with deterministic greedy matching and cross-cohort insights.
    """
    # Normalize input_steps if they are raw strings
    steps: List[PathStep] = []
    if all(isinstance(s, str) for s in input_steps):
        for idx, sname in enumerate(input_steps):
            steps.append(PathStep(step_order=idx, event_name=sname, filters=[]))
    else:
        steps = input_steps

    if len(steps) < 2:
        raise HTTPException(status_code=400, detail="Paths require at least 2 steps")
    if len(steps) > 10:
        raise HTTPException(status_code=400, detail="Paths support at most 10 steps")

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
            "steps": [s.event_name for s in steps],
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
    # Note: Using steps list which now has filters
    base_sql = _build_paths_base_query(steps, conn)
    
    agg_parts = []
    for i in range(len(steps)):
        s_idx = i + 1
        if s_idx == 1:
            agg_parts.append(f"""
            SELECT {s_idx} AS step_idx, cohort_id, COUNT(user_id) AS users, NULL AS mean_time, NULL AS p20, NULL AS p80
            FROM step_1 GROUP BY cohort_id""")
        else:
            agg_parts.append(f"""
            SELECT 
                {s_idx} AS step_idx, cohort_id, COUNT(user_id) AS users,
                AVG(time_sec) AS mean_time,
                CASE WHEN COUNT(user_id) >= 50 THEN approx_quantile(time_sec, 0.2) ELSE NULL END AS p20,
                CASE WHEN COUNT(user_id) >= 50 THEN approx_quantile(time_sec, 0.8) ELSE NULL END AS p80
            FROM step_{s_idx} WHERE time_sec IS NOT NULL AND time_sec >= 0
            GROUP BY cohort_id""")
            
    full_sql = base_sql + " UNION ALL ".join(agg_parts)
    raw_results = conn.execute(full_sql).fetchall()
    
    # Map results: cohort_id -> step_idx -> metrics
    metrics_map = {}
    for row in raw_results:
        s_idx, c_id, users, mean, p20, p80 = row
        metrics_map.setdefault(c_id, {})[s_idx] = {
            "users": users,
            "mean": mean,
            "p20": p20,
            "p80": p80
        }

    # Get cohort sizes
    cohort_sizes = {row[0]: row[1] for row in conn.execute("""
        SELECT cohort_id, COUNT(DISTINCT user_id) 
        FROM cohort_membership 
        GROUP BY cohort_id
    """).fetchall()}

    results = []
    for c_id, c_name in active_cohorts:
        c_size = cohort_sizes.get(c_id, 0)
        if c_size == 0:
            continue
            
        c_metrics = metrics_map.get(c_id, {})
        if 1 not in c_metrics:
            # Handle no users reaching Step 1
            results.append(PathsCohortResult(
                cohort_id=c_id,
                cohort_name=c_name,
                cohort_size=c_size,
                steps=[PathsStepResult(
                    step=i+1,
                    event=steps[i].event_name,
                    users=0,
                    conversion_pct=0.0
                ) for i in range(len(steps))],
                insights=["No users in this cohort reached the first step."]
            ))
            continue

        cohort_steps = []
        for i, step_def in enumerate(steps):
            step_idx = i + 1
            m = c_metrics.get(step_idx, {"users": 0, "mean": None, "p20": None, "p80": None})
            
            user_count = m["users"]
            conversion_pct = round(user_count / c_size * 100, 1) if c_size > 0 else 0.0
            
            drop_off_pct = None
            if step_idx > 1:
                prev_users = c_metrics.get(step_idx - 1, {"users": 0})["users"]
                if prev_users > 0:
                    drop_off_pct = round((prev_users - user_count) / prev_users * 100, 1)
                else:
                    drop_off_pct = 0.0

            cohort_steps.append(PathsStepResult(
                step=step_idx,
                event=step_def.event_name,
                users=user_count,
                conversion_pct=conversion_pct,
                drop_off_pct=drop_off_pct,
                mean_time=round(m["mean"], 1) if m["mean"] is not None else None,
                p20=round(m["p20"], 1) if m["p20"] is not None else None,
                p80=round(m["p80"], 1) if m["p80"] is not None else None
            ))

        # Insights
        insights = []
        for s in cohort_steps:
            if s.drop_off_pct and s.drop_off_pct > 20:
                insights.append(f"Significant drop-off ({s.drop_off_pct}%) at step {s.step} ({s.event})")
            if s.p80 and s.p80 > 120:
                insights.append(f"Slow progression to step {s.step}: 80% takes >120s")
            if s.p80 and s.p20 and s.p20 > 0:
                variability = s.p80 / s.p20
                if variability > 5:
                    insights.append(f"High variability in time to step {s.step} (p80/p20 > 5x)")
            
            # Step 1 Drop-off Insight
            if s.step == 1 and c_size > 0:
                dropoff_count = c_size - s.users
                dropoff_rate = dropoff_count / c_size
                if dropoff_rate > 0.2:
                    insights.append(f"{round(dropoff_rate * 100, 1)}% of users did not start this flow ({s.event})")

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
                            f"at step {k+1} ({steps[k].event_name}) than {lower}"
                        )

    return {
        "steps": [s.event_name for s in steps],
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
    cohort_name: Optional[str] = None
) -> Dict[str, Any]:
    # Normalize steps
    steps: List[PathStep] = []
    if all(isinstance(s, str) for s in steps_raw):
        for idx, sname in enumerate(steps_raw):
            steps.append(PathStep(step_order=idx, event_name=sname, filters=[]))
    else:
        steps = steps_raw

    if step_index < 1:
        raise HTTPException(status_code=400, detail="Invalid step index")
    
    # Find users and join_times
    if step_index == 1:
        find_users_sql = _build_paths_base_query(steps, conn, cohort_id, 1) + f"""
            SELECT DISTINCT m.user_id, m.join_time
            FROM cohort_membership m
            LEFT JOIN step_1 s ON m.user_id = s.user_id AND m.cohort_id = s.cohort_id
            WHERE m.cohort_id = {cohort_id}
              AND s.user_id IS NULL
        """
    else:
        find_users_sql = _build_paths_base_query(steps, conn, cohort_id, step_index) + f"""
            SELECT DISTINCT s_prev.user_id, s_prev.t{step_index-1} as join_time 
            FROM step_{step_index-1} s_prev
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
        event_names = [s.event_name for s in steps]
        seq = " -> ".join(event_names[:step_index])
        if step_index == 1:
            new_name = f"Did not start ({event_names[0]}) ({parent_name})"
        else:
            new_name = f"Drop-off at Step {step_index} ({event_names[step_index-1]}) ({parent_name}): {seq}"

    return _materialize_paths_cohort(conn, new_name, drop_off_users)


def create_paths_reached_cohort(
    conn: duckdb.DuckDBPyConnection, 
    cohort_id: int, 
    step_index: int, 
    steps_raw: Union[List[str], List[PathStep]],
    cohort_name: Optional[str] = None
) -> Dict[str, Any]:
    # Normalize steps
    steps: List[PathStep] = []
    if all(isinstance(s, str) for s in steps_raw):
        for idx, sname in enumerate(steps_raw):
            steps.append(PathStep(step_order=idx, event_name=sname, filters=[]))
    else:
        steps = steps_raw

    if step_index < 1:
        raise HTTPException(status_code=400, detail="Invalid step index")

    find_users_sql = _build_paths_base_query(steps, conn, cohort_id, step_index) + f"""
        SELECT DISTINCT user_id, t{step_index} AS join_time
        FROM step_{step_index}
        WHERE cohort_id = {cohort_id}
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
        event_names = [s.event_name for s in steps]
        seq = " -> ".join(event_names[:step_index])
        new_name = f"Reached Step {step_index} ({event_names[step_index-1]}) ({parent_name}): {seq}"

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
