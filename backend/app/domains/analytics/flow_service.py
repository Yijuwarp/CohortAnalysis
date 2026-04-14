from __future__ import annotations
from typing import Optional, cast, Any

import duckdb
from fastapi import HTTPException

from app.domains.cohorts.cohort_service import ensure_cohort_tables
from app.utils.sql import (
    get_allowed_operators,
    get_column_kind,
    get_column_type_map,
    quote_identifier,
)

# Default top N for flows (now dynamic via limit param)
_DEFAULT_TOP_N = 3

TABLE_MAX_DEPTH = 20
_DIRECTIONS = ("forward", "reverse")
_ALLOWED_PROPERTY_OPERATORS = {"=", "!=", "IN", "NOT IN", ">", "<", ">=", "<="}


def _fetch_active_cohorts(connection: duckdb.DuckDBPyConnection) -> list[tuple[int, str]]:
    rows = connection.execute(
        "SELECT cohort_id, name FROM cohorts WHERE hidden = FALSE ORDER BY cohort_id"
    ).fetchall()
    return [(int(r[0]), str(r[1])) for r in rows]


def _scoped_has_data(connection: duckdb.DuckDBPyConnection) -> bool:
    """Returns True if events_scoped exists and has at least one row."""
    tables = {
        row[0]
        for row in connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    if "events_scoped" not in tables or "events_normalized" not in tables:
        return False

    try:
        count = connection.execute("SELECT COUNT(*) FROM events_scoped").fetchone()[0]
        return bool(count and count > 0)
    except Exception:
        return False


def _get_column_type_map_resilient(connection: duckdb.DuckDBPyConnection, table_name: str) -> dict[str, str]:
    # Use PRAGMA table_info which is reliable for both tables and views in DuckDB
    rows = connection.execute(f"PRAGMA table_info({quote_identifier(table_name)})").fetchall()
    return {row[1]: str(row[2]).upper() for row in rows}


def _validate_property_column(
    connection: duckdb.DuckDBPyConnection,
    property_column: str | None,
    property_operator: str | None,
) -> str | None:
    """Validates property_column and returns its canonical name as found in metadata."""
    if not property_column:
        return None

    metadata = _get_column_type_map_resilient(connection, "events_scoped")
    metadata_lower = {k.lower(): k for k in metadata}
    
    canonical_name = metadata_lower.get(property_column.lower())
    if not canonical_name:
        col_list = ", ".join(sorted(metadata.keys()))
        raise HTTPException(
            status_code=400,
            detail=f"Unknown property column: {property_column}. Available: {col_list}"
        )

    data_type = metadata[canonical_name]
    kind = get_column_kind(data_type)
    allowed = get_allowed_operators(kind)

    operator = (property_operator or "=").upper()
    if operator not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"Operator {operator} not supported for {kind.lower()} column {canonical_name}",
        )
    
    return canonical_name


def _ensure_performance_indexes(connection: duckdb.DuckDBPyConnection):
    # Flows now use snapshot and scoped CTEs, no longer relying on normalized table directly.
    pass


def _validate_depth(depth: int) -> int:
    return max(1, min(depth, TABLE_MAX_DEPTH))


def _is_empty_filter(values: list[str] | None) -> bool:
    if not values:
        return True
    return not any(v is not None and str(v).strip() != "" for v in values)


def _build_property_filter_clause(
    property_column: str | None,
    property_operator: str | None,
    property_values: list[str] | None,
) -> tuple[str, list[object]]:
    if not property_column or _is_empty_filter(property_values):
        return "", []

    operator = (property_operator or "=").upper()
    if operator not in _ALLOWED_PROPERTY_OPERATORS:
        raise HTTPException(status_code=400, detail="Unsupported property_operator")

    values = property_values or []
    if not values:
        return "", []

    if operator in {"IN", "NOT IN"}:
        placeholders = ", ".join(["?"] * len(values))
        return f" AND {quote_identifier(property_column)} {operator} ({placeholders})", list(values)

    return f" AND {quote_identifier(property_column)} {operator} ?", [values[0]]


def _build_level_sql(
    direction: str,
    parent_path: list[str],
    property_column: str | None,
    property_clause: str,
    property_params: list[object],
    cohort_id: int | None = None,
    limit: int = _DEFAULT_TOP_N,
) -> tuple[str, list[object]]:
    order_dir = "ASC" if direction == "forward" else "DESC"
    time_op = ">" if direction == "forward" else "<"

    parent_params: list[object] = []
    parent_params.append(parent_path[0])  # root_step parent_event
    parent_params.append(parent_path[0])  # root_step WHERE event_name = ?
    parent_params.extend(property_params)  # root_step {property_clause}

    cohort_clause = " AND e.cohort_id = ?" if cohort_id is not None else ""
    if cohort_id is not None:
        parent_params.append(cohort_id)

    property_exists_clause = ""
    if property_column:
        property_exists_clause = f"""
        AND EXISTS (
            SELECT 1 FROM events_scoped es
            WHERE es.user_id = e.user_id
              AND es.event_time = e.event_time
              AND es.event_name = e.event_name
              {property_clause}
        )
        """

    ctes: list[str] = [
        f"""
        root_step AS (
            SELECT e.cohort_id, e.user_id, e.event_time AS parent_time, e.row_id AS parent_row_id, ? AS parent_event
            FROM (
                SELECT
                    e.cohort_id,
                    e.user_id,
                    e.event_time,
                    e.row_id,
                    ROW_NUMBER() OVER (PARTITION BY e.cohort_id, e.user_id ORDER BY e.event_time ASC, e.row_id ASC) AS rn
                FROM cohort_activity_snapshot e
                WHERE e.event_name = ?{property_exists_clause}{cohort_clause}
            ) e
            WHERE rn = 1
        )
        """
    ]

    prev_cte = "root_step"
    for idx, target_event in enumerate(parent_path[1:], start=1):
        cte_name = f"step_{idx}"
        ctes.append(
            f"""
            {cte_name} AS (
                SELECT s.cohort_id, s.user_id, n.event_time AS parent_time, n.row_id AS parent_row_id, n.event_name AS parent_event
                FROM {prev_cte} s
                JOIN LATERAL (
                    SELECT e.event_name, e.event_time, e.row_id
                    FROM cohort_activity_snapshot e
                    WHERE e.user_id = s.user_id
                      AND e.cohort_id = s.cohort_id
                      AND e.event_time >= s.parent_time
                      AND e.row_id != s.parent_row_id
                    ORDER BY e.event_time {order_dir}, e.row_id {order_dir}
                    LIMIT 1
                ) n ON TRUE
                WHERE n.event_name = ?
            )
            """
        )
        parent_params.append(target_event)
        prev_cte = cte_name

    ctes.append(
        f"""
        transition_candidates AS (
            SELECT
                s.cohort_id,
                s.user_id,
                n.event_name AS next_event,
                ABS(EXTRACT(EPOCH FROM (n.event_time - s.parent_time)))::DOUBLE AS time_diff_sec
            FROM {prev_cte} s
            JOIN LATERAL (
                SELECT e.event_name, e.event_time, e.row_id
                FROM cohort_activity_snapshot e
                WHERE e.user_id = s.user_id
                  AND e.cohort_id = s.cohort_id
                  AND (
                    ('{direction}' = 'forward' AND e.event_time >= s.parent_time AND e.row_id != s.parent_row_id)
                    OR
                    ('{direction}' = 'reverse' AND e.event_time <= s.parent_time AND e.row_id != s.parent_row_id)
                  )
                ORDER BY e.event_time {order_dir}, e.row_id {order_dir}
                LIMIT 1
            ) n ON TRUE
        ),
        denominators AS (
            SELECT cohort_id, COUNT(*) AS total_users
            FROM {prev_cte}
            GROUP BY cohort_id
        ),
        agg AS (
            SELECT
                cohort_id,
                next_event,
                -- We use COUNT(DISTINCT user_id) for the display metric to ensure consistency with ranking
                COUNT(DISTINCT user_id) AS transition_users,
                MEDIAN(time_diff_sec) AS median_time_sec,
                APPROX_QUANTILE(time_diff_sec, 0.2) AS p20_time_sec,
                APPROX_QUANTILE(time_diff_sec, 0.8) AS p80_time_sec
            FROM transition_candidates
            GROUP BY cohort_id, next_event
        ),
        ranking_basis AS (
            -- Cannonical ranking at the node level regardless of user cohort membership
            SELECT 
                next_event, 
                COUNT(DISTINCT user_id) AS total_distinct_users
            FROM transition_candidates
            GROUP BY next_event
        ),
        ranked_events AS (
          SELECT 
            next_event, 
            ROW_NUMBER() OVER (ORDER BY total_distinct_users DESC, next_event ASC) as rnk
          FROM ranking_basis
        ),
        node_stats AS (
          SELECT COUNT(*) as total_event_types FROM ranking_basis
        ),
        top_event_names AS (
          SELECT next_event FROM ranked_events WHERE rnk <= ?
        ),
        final_payload AS (
          -- Top N
          SELECT 
              a.cohort_id, 
              a.next_event, 
              a.transition_users, 
              d.total_users as anchor_users,
              a.median_time_sec,
              a.p20_time_sec,
              a.p80_time_sec,
              0 as is_other,
              ns.total_event_types
          FROM agg a
          JOIN top_event_names ten ON a.next_event = ten.next_event
          JOIN denominators d ON a.cohort_id = d.cohort_id
          CROSS JOIN node_stats ns

          UNION ALL

          -- Synthetic Other bucket
          SELECT 
              a.cohort_id, 
              '__OTHER__' as next_event, 
              -- Summing distinct user_count is safe here because each user has exactly ONE next_event per node level
              (SUM(a.transition_users))::BIGINT as transition_users,
              d.total_users as anchor_users,
              NULL as median_time_sec,
              NULL as p20_time_sec,
              NULL as p80_time_sec,
              1 as is_other,
              ns.total_event_types
          FROM agg a
          LEFT JOIN top_event_names ten ON a.next_event = ten.next_event
          JOIN denominators d ON a.cohort_id = d.cohort_id
          CROSS JOIN node_stats ns
          WHERE ten.next_event IS NULL
          GROUP BY a.cohort_id, d.total_users, ns.total_event_types
        )
        """
    )

    params: list[object] = parent_params + [limit]

    sql = f"""
        WITH {', '.join(ctes)}
        SELECT 
            cohort_id,
            next_event,
            transition_users,
            anchor_users,
            median_time_sec,
            p20_time_sec,
            p80_time_sec,
            total_event_types
        FROM final_payload
        ORDER BY cohort_id ASC, is_other ASC, transition_users DESC, next_event ASC
    """
    return sql, params


# Prune and aggregate is now handled in SQL


def _run_level_query(
    connection: duckdb.DuckDBPyConnection,
    start_event: str,
    parent_path: list[str],
    direction: str,
    depth: int,
    property_column: str | None,
    property_operator: str | None,
    property_values: list[str] | None,
    cohort_id: int | None = None,
    limit: int = _DEFAULT_TOP_N,
) -> tuple[list[tuple], int, int]:
    parent_depth = len(parent_path)
    if parent_depth < 1 or parent_depth >= depth:
        return [], parent_depth, 0
    if "No further action" in parent_path or "Other" in parent_path:
        return [], parent_depth, 0
    if parent_path[0].strip() != start_event.strip():
        raise HTTPException(status_code=400, detail=f"parent_path must start with start_event ({start_event}). Got: {parent_path[0]}")
    if property_column and (not property_values or _is_empty_filter(property_values)):
        raise HTTPException(status_code=400, detail="property_values are required when property_column is specified")

    if _is_empty_filter(property_values):
        property_column = None

    property_clause, property_params = _build_property_filter_clause(property_column, property_operator, property_values)
    sql, params = _build_level_sql(direction, parent_path, property_column, property_clause, property_params, cohort_id=cohort_id, limit=limit)
    rows = connection.execute(sql, params).fetchall()
    
    total_event_types = rows[0][7] if rows else 0
    return rows, parent_depth, total_event_types


def _rows_payload(
    raw_rows: list[tuple],
    cohorts: list[tuple[int, str]],
    path_prefix: list[str],
    include_expandable: bool,
    top_k_enabled: bool,
    total_event_types: int = 0,
) -> list[dict]:
    per_cohort: dict[int, dict[str, object]] = {}
    next_event_names: set[str] = set()

    for cohort_id, next_event, transition_users, anchor_users, median_time, p20, p80, _card in raw_rows:
        cid = int(cohort_id)
        ev_name = str(next_event)
        next_event_names.add(ev_name)
        if cid not in per_cohort:
            per_cohort[cid] = {"anchor": int(anchor_users), "events": cast(dict[str, object], {})}
        per_cohort[cid]["events"][ev_name] = (
            int(transition_users),
            median_time,
            p20,
            p80,
        )

    if not per_cohort:
        return []

    # Final result row names: exclude __OTHER__ for main list, handle Other separately at footer
    top_events_in_sql = [name for name in sorted(next_event_names) if name != "__OTHER__"]
    
    first_cohort_id = cohorts[0][0] if cohorts else None

    # SQL already sorted them correctly, but _rows_payload needs to preserve that order across cohorts
    # To maintain consistency, we follow the order from the first cohort column primarily 
    # but since SQL already ranked them, we can just extract unique names from results in order.
    # However raw_rows is sorted by cohort_id ASC, then Rank ASC.
    ordered_top_names = []
    seen = set()
    for row in raw_rows:
        name = str(row[1])
        if name != "__OTHER__" and name not in seen:
            ordered_top_names.append(name)
            seen.add(name)

    named_rows = []
    for event_name in ordered_top_names:
        values = {}
        for cid, _ in cohorts:
            cohort_data = per_cohort.get(cid, {"anchor": 0, "events": {}})
            anchor_users = int(cohort_data.get("anchor", 0))
            event_tuple = cohort_data.get("events", {}).get(event_name)
            if event_tuple:
                count, median, p20, p80 = event_tuple
                values[str(cid)] = {
                    "user_count": count,
                    "parent_users": anchor_users,
                    "has_event": True,
                    "median_time_sec": float(median) if median is not None else None,
                    "p20_time_sec": float(p20) if p20 is not None else None,
                    "p80_time_sec": float(p80) if p80 is not None else None,
                }
            else:
                values[str(cid)] = {
                    "user_count": 0,
                    "parent_users": anchor_users,
                    "has_event": False,
                    "median_time_sec": None,
                    "p20_time_sec": None,
                    "p80_time_sec": None,
                }
        named_rows.append({
            "path": [*path_prefix, event_name], 
            "values": values, 
            "expandable": True,
            "meta": {"total_event_types": total_event_types}
        })

    other_values = {}
    has_any_other = False
    for cid, _ in cohorts:
        cohort_data = per_cohort.get(cid, {"anchor": 0, "events": {}})
        anchor_users = int(cohort_data.get("anchor", 0))
        event_tuple = cohort_data.get("events", {}).get("__OTHER__")
        if event_tuple:
            count, *_ = event_tuple
            has_any_other = has_any_other or (count > 0)
            other_values[str(cid)] = {
                "user_count": count,
                "parent_users": anchor_users,
                "has_event": True,
                "median_time_sec": None,
                "p20_time_sec": None,
                "p80_time_sec": None,
            }
        else:
            other_values[str(cid)] = {
                "user_count": 0,
                "parent_users": anchor_users,
                "has_event": False,
                "median_time_sec": None,
                "p20_time_sec": None,
                "p80_time_sec": None,
            }

    output = [{"path": r["path"], "values": r["values"], "children": [], **({"expandable": r["expandable"]} if include_expandable else {}), "meta": r["meta"]} for r in named_rows]

    if top_k_enabled:
        output.append({
            "path": [*path_prefix, "__OTHER__"], 
            "values": other_values, 
            "children": [], 
            **({"expandable": False} if include_expandable else {}),
            "meta": {"total_event_types": total_event_types}
        })

    visible_cohort_ids = {int(cid) for cid, _ in cohorts}
    no_further_values = {}
    for cohort_id, data in per_cohort.items():
        if cohort_id not in visible_cohort_ids:
            continue
        anchor_users = int(data["anchor"])
        continued_users = sum(count for count, *_timing in data["events"].values())
        no_further_users = max(0, anchor_users - continued_users)
        no_further_values[str(cohort_id)] = {
            "user_count": no_further_users,
            "parent_users": anchor_users,
            "has_event": True,
            "median_time_sec": None,
            "p20_time_sec": None,
            "p80_time_sec": None,
        }
    if no_further_values:
        for cid, _ in cohorts:
            no_further_values.setdefault(str(cid), {
                "user_count": 0,
                "parent_users": int((per_cohort.get(cid, {"anchor": 0}).get("anchor") or 0)),
                "has_event": False,
                "median_time_sec": None,
                "p20_time_sec": None,
                "p80_time_sec": None,
            })
        output.append({"path": [*path_prefix, "No further action"], "values": no_further_values, "children": [], **({"expandable": False} if include_expandable else {})})

    return output


def get_l1_flows(
    connection: duckdb.DuckDBPyConnection,
    start_event: str,
    direction: str,
    depth: int = 2,
    property_column: str | None = None,
    property_operator: str | None = None,
    property_values: list[str] | None = None,
    include_top_k: bool = True,
    limit: int = _DEFAULT_TOP_N,
) -> dict:
    if direction not in _DIRECTIONS:
        raise HTTPException(status_code=400, detail=f"direction must be one of: {', '.join(_DIRECTIONS)}")
    depth = _validate_depth(depth)

    ensure_cohort_tables(connection)
    canonical_col = _validate_property_column(connection, property_column, property_operator)
    _ensure_performance_indexes(connection)

    if not _scoped_has_data(connection):
        return {"rows": []}

    cohorts = _fetch_active_cohorts(connection)
    if not cohorts:
        return {"rows": []}

    raw_rows, _, total_event_types = _run_level_query(connection, start_event, [start_event], direction, depth, canonical_col, property_operator, property_values, limit=limit)
    return {"rows": _rows_payload(raw_rows, cohorts, [start_event], include_expandable=True, top_k_enabled=include_top_k, total_event_types=total_event_types)}


def get_l2_flows(
    connection: duckdb.DuckDBPyConnection,
    start_event: str,
    parent_path: list[str],
    direction: str,
    depth: int = 2,
    property_column: str | None = None,
    property_operator: str | None = None,
    property_values: list[str] | None = None,
    include_top_k: bool = True,
    limit: int = _DEFAULT_TOP_N,
) -> dict:
    if direction not in _DIRECTIONS:
        raise HTTPException(status_code=400, detail=f"direction must be one of: {', '.join(_DIRECTIONS)}")
    depth = _validate_depth(depth)

    ensure_cohort_tables(connection)
    canonical_col = _validate_property_column(connection, property_column, property_operator)
    _ensure_performance_indexes(connection)

    if not _scoped_has_data(connection):
        return {"parent_path": parent_path, "rows": []}

    cohorts = _fetch_active_cohorts(connection)
    if not cohorts:
        return {"parent_path": parent_path, "rows": []}

    raw_rows, parent_depth, total_event_types = _run_level_query(connection, start_event, parent_path, direction, depth, canonical_col, property_operator, property_values, limit=limit)
    if parent_depth >= depth:
        return {"parent_path": parent_path, "rows": []}

    return {
        "parent_path": parent_path,
        "rows": _rows_payload(raw_rows, cohorts, parent_path, include_expandable=False, top_k_enabled=include_top_k, total_event_types=total_event_types),
    }
