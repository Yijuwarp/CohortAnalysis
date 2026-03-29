"""
Short summary: Service for Paths (Sequence Analysis) computation using DuckDB.
"""
from __future__ import annotations
import duckdb
import json
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from fastapi import HTTPException
from app.models.paths_models import PathsResponse, PathsCohortResult, PathsStepResult
from app.domains.cohorts.cohort_service import ensure_cohort_tables

def run_paths(conn: duckdb.DuckDBPyConnection, steps: List[str]) -> Dict[str, Any]:
    """
    Executes Paths analysis with strict greedy matching and cross-cohort insights.
    """
    if len(steps) < 2:
        raise HTTPException(status_code=400, detail="Paths require at least 2 steps")
    if len(steps) > 10:
        raise HTTPException(status_code=400, detail="Paths support at most 10 steps")

    ensure_cohort_tables(conn)

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
            "steps": steps,
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

    # 2. Build Sequence Matching SQL
    full_sql = _build_paths_metrics_query(steps)
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
                    event=steps[i],
                    users=0,
                    conversion_pct=0.0
                ) for i in range(len(steps))],
                insights=["No users in this cohort reached the first step."]
            ))
            continue

        cohort_steps = []
        for i, step_name in enumerate(steps):
            step_idx = i + 1
            m = c_metrics.get(step_idx, {"users": 0, "mean": None, "p20": None, "p80": None})
            
            user_count = m["users"]
            conversion_pct = round(user_count / c_size * 100, 1)
            
            drop_off_pct = None
            if step_idx > 1:
                prev_users = c_metrics.get(step_idx - 1, {"users": 0})["users"]
                if prev_users > 0:
                    drop_off_pct = round((prev_users - user_count) / prev_users * 100, 1)
                else:
                    drop_off_pct = 0.0

            cohort_steps.append(PathsStepResult(
                step=step_idx,
                event=step_name,
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
                            f"at step {k+1} ({steps[k]}) than {lower}"
                        )

    return {
        "steps": steps,
        "results": results,
        "global_insights": global_insights
    }

def create_paths_dropoff_cohort(
    conn: duckdb.DuckDBPyConnection, 
    cohort_id: int, 
    step_index: int, 
    steps: List[str],
    cohort_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Creates a drop-off cohort: users who reached step_{step_index-1} but NOT step_{step_index}.
    If step_index is 1, it finds users who are in the source cohort but not in step_1.
    """
    if step_index < 1:
        raise HTTPException(status_code=400, detail="Invalid step index")
    
    # 1. Find users and join_times
    if step_index == 1:
        # Special case: Drop-off at Step 1 (Never Started)
        # Use source cohort membership as the "success" state for people who haven't started.
        find_users_sql = _build_paths_base_query(steps, cohort_id, 1) + f"""
            SELECT DISTINCT m.user_id, m.join_time
            FROM cohort_membership m
            LEFT JOIN step_1 s ON m.user_id = s.user_id AND m.cohort_id = s.cohort_id
            WHERE m.cohort_id = {cohort_id}
              AND s.user_id IS NULL
        """
    else:
        # Standard case: Drop-off at Step N
        find_users_sql = _build_paths_base_query(steps, cohort_id, step_index) + f"""
            SELECT DISTINCT s_prev.user_id, s_prev.t{step_index-1} as join_time 
            FROM step_{step_index-1} s_prev
            ANTI JOIN step_{step_index} s_curr ON s_prev.user_id = s_curr.user_id AND s_prev.cohort_id = s_curr.cohort_id
            WHERE s_prev.cohort_id = {cohort_id}
        """
    
    rows = conn.execute(find_users_sql).fetchall()
    drop_off_users = [(row[0], row[1]) for row in rows]
    
    if not drop_off_users:
        return {"cohort_id": None, "message": "No users found in drop-off."}

    # 2. Determine Name
    if cohort_name:
        new_name = cohort_name
    else:
        c_name_row = conn.execute("SELECT name FROM cohorts WHERE cohort_id = ?", [cohort_id]).fetchone()
        parent_name = c_name_row[0] if c_name_row else "Unknown"
        seq = " -> ".join(steps[:step_index])
        if step_index == 1:
            new_name = f"Did not start ({steps[0]}) ({parent_name})"
        else:
            new_name = f"Drop-off at Step {step_index} ({steps[step_index-1]}) ({parent_name}): {seq}"

    # 3. Insert and Populate
    return _materialize_paths_cohort(conn, new_name, drop_off_users)


def create_paths_reached_cohort(
    conn: duckdb.DuckDBPyConnection, 
    cohort_id: int, 
    step_index: int, 
    steps: List[str],
    cohort_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Creates a reached-step cohort: users who reached step_{step_index}.
    """
    if step_index < 1:
        raise HTTPException(status_code=400, detail="Invalid step index")

    # 1. Find users and join_times (t_{step_index})
    find_users_sql = _build_paths_base_query(steps, cohort_id, step_index) + f"""
        SELECT DISTINCT user_id, t{step_index} AS join_time
        FROM step_{step_index}
        WHERE cohort_id = {cohort_id}
    """
    
    rows = conn.execute(find_users_sql).fetchall()
    reached_users = [(row[0], row[1]) for row in rows]
    
    if not reached_users:
        return {"cohort_id": None, "message": "No users found for this step."}

    # 2. Determine Name
    if cohort_name:
        new_name = cohort_name
    else:
        c_name_row = conn.execute("SELECT name FROM cohorts WHERE cohort_id = ?", [cohort_id]).fetchone()
        parent_name = c_name_row[0] if c_name_row else "Unknown"
        seq = " -> ".join(steps[:step_index])
        new_name = f"Reached Step {step_index} ({steps[step_index-1]}) ({parent_name}): {seq}"

    # 3. Insert and Populate
    return _materialize_paths_cohort(conn, new_name, reached_users)


def _build_paths_base_query(steps: List[str], cohort_id: Optional[int] = None, limit_steps: Optional[int] = None) -> str:
    """
    Central SQL builder for Paths sequence matching.
    Enforces strict greedy matching, partitioning by (cohort_id, user_id).
    """
    cohort_filter = f"WHERE cohort_id = {cohort_id}" if cohort_id is not None else ""
    
    sql = f"""
    WITH base AS (
      SELECT
        cohort_id,
        user_id,
        event_name,
        event_time,
        ROW_NUMBER() OVER (
          PARTITION BY cohort_id, user_id
          ORDER BY event_time, event_name
        ) AS rn
      FROM cohort_activity_snapshot
      {cohort_filter}
    )
    """
    
    steps_to_process = limit_steps if limit_steps else len(steps)
    ctes = []
    
    for i in range(steps_to_process):
        s_idx = i + 1
        safe_step = steps[i].replace("'", "''")
        
        if s_idx == 1:
            cte = f"""
            step_1 AS (
              SELECT cohort_id, user_id, event_time AS t1, rn AS rn1
              FROM base
              WHERE event_name = '{safe_step}'
              QUALIFY ROW_NUMBER() OVER (
                PARTITION BY cohort_id, user_id 
                ORDER BY event_time, rn
              ) = 1
            )
            """
        else:
            prev = i
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
              WHERE b.event_name = '{safe_step}'
                AND (b.event_time > s.t{prev} OR (b.event_time = s.t{prev} AND b.rn > s.rn{prev}))
              QUALIFY ROW_NUMBER() OVER (
                PARTITION BY s.cohort_id, s.user_id 
                ORDER BY b.event_time, b.rn
              ) = 1
            )
            """
        ctes.append(cte)
        
    return sql + ", " + ",\n".join(ctes) + "\n"


def _build_paths_metrics_query(steps: List[str]) -> str:
    """
    Builds the full metrics aggregation query for run_paths.
    """
    base_sql = _build_paths_base_query(steps)
    
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
            
    return base_sql + "\n" + " UNION ALL ".join(agg_parts)


def _materialize_paths_cohort(conn: duckdb.DuckDBPyConnection, name: str, users: List[tuple]) -> Dict[str, Any]:
    """
    Internal helper to insert cohort records and populate snapshot with explicit columns.
    """
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

    scoped_exists = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
    ).fetchone()[0]
    source_table = "events_scoped" if scoped_exists else "events_normalized"

    # Targeted insert with explicit column selection
    conn.execute(f"""
        INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time, event_name)
        SELECT m.cohort_id, e.user_id, e.event_time, e.event_name
        FROM {source_table} e
        JOIN cohort_membership m ON e.user_id = m.user_id
        WHERE m.cohort_id = {new_c_id}
    """)

    return {"cohort_id": int(new_c_id), "name": name, "user_count": len(users)}
