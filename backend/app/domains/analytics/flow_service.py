from __future__ import annotations

import duckdb
from fastapi import HTTPException

from app.domains.cohorts.cohort_service import ensure_cohort_tables
from app.utils.sql import quote_identifier

_TOP_N = 3
_MAX_DEPTH = 7
_DIRECTIONS = ("forward", "reverse")
_ALLOWED_PROPERTY_OPERATORS = {"=", "!=", "IN", "NOT IN", ">", "<", ">=", "<="}


def _fetch_active_cohorts(connection: duckdb.DuckDBPyConnection) -> list[tuple[int, str]]:
    rows = connection.execute(
        "SELECT cohort_id, name FROM cohorts WHERE hidden = FALSE ORDER BY cohort_id"
    ).fetchall()
    return [(int(r[0]), str(r[1])) for r in rows]


def _snapshot_exists(connection: duckdb.DuckDBPyConnection) -> bool:
    count = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'cohort_activity_snapshot' AND table_schema = 'main'"
    ).fetchone()[0]
    return bool(count)


def _validate_depth(depth: int) -> int:
    if depth < 2 or depth > _MAX_DEPTH:
        raise HTTPException(status_code=400, detail=f"depth must be between 2 and {_MAX_DEPTH}")
    return depth


def _build_property_filter_clause(
    property_column: str | None,
    property_operator: str | None,
    property_values: list[str] | None,
) -> tuple[str, list[object]]:
    if not property_column:
        return "", []

    operator = (property_operator or "=").upper()
    if operator not in _ALLOWED_PROPERTY_OPERATORS:
        raise HTTPException(status_code=400, detail="Unsupported property_operator")

    values = property_values or []
    if operator in {"IN", "NOT IN"}:
        if not values:
            raise HTTPException(status_code=400, detail=f"Operator {operator} requires property_values")
        placeholders = ", ".join(["?"] * len(values))
        return f" AND {quote_identifier(property_column)} {operator} ({placeholders})", list(values)

    if not values:
        raise HTTPException(status_code=400, detail="property_values is required")
    return f" AND {quote_identifier(property_column)} {operator} ?", [values[0]]


def _build_level_sql(
    direction: str,
    parent_path: list[str],
    property_clause: str,
) -> tuple[str, list[object]]:
    order_dir = "ASC" if direction == "forward" else "DESC"
    time_op = ">" if direction == "forward" else "<"

    params: list[object] = [parent_path[0]]
    if property_clause:
        pass

    ctes: list[str] = [
        f"""
        root_step AS (
            SELECT cohort_id, user_id, event_time AS parent_time, ? AS parent_event
            FROM (
                SELECT
                    cohort_id,
                    user_id,
                    event_time,
                    ROW_NUMBER() OVER (PARTITION BY cohort_id, user_id ORDER BY event_time) AS rn
                FROM cohort_activity_snapshot
                WHERE event_name = ?{property_clause}
            ) r
            WHERE rn = 1
        )
        """
    ]
    params = [parent_path[0], parent_path[0]]

    prev_cte = "root_step"
    prev_event = parent_path[0]

    for idx, target_event in enumerate(parent_path[1:], start=1):
        cte_name = f"step_{idx}"
        ctes.append(
            f"""
            {cte_name} AS (
                SELECT s.cohort_id, s.user_id, n.event_time AS parent_time, n.event_name AS parent_event
                FROM {prev_cte} s
                JOIN LATERAL (
                    SELECT e.event_name, e.event_time
                    FROM cohort_activity_snapshot e
                    WHERE e.cohort_id = s.cohort_id
                      AND e.user_id = s.user_id
                      AND e.event_time {time_op} s.parent_time
                      AND e.event_name <> s.parent_event
                    ORDER BY e.event_time {order_dir}
                    LIMIT 1
                ) n ON TRUE
                WHERE n.event_name = ?
            )
            """
        )
        params.append(target_event)
        prev_cte = cte_name
        prev_event = target_event

    ctes.append(
        f"""
        transition_candidates AS (
            SELECT
                s.cohort_id,
                s.user_id,
                n.event_name AS next_event,
                ABS(DATE_DIFF('second', s.parent_time, n.event_time))::DOUBLE AS time_diff_sec
            FROM {prev_cte} s
            JOIN LATERAL (
                SELECT e.event_name, e.event_time
                FROM cohort_activity_snapshot e
                WHERE e.cohort_id = s.cohort_id
                  AND e.user_id = s.user_id
                  AND e.event_time {time_op} s.parent_time
                  AND e.event_name <> s.parent_event
                ORDER BY e.event_time {order_dir}
                LIMIT 1
            ) n ON TRUE
        ),
        denominators AS (
            SELECT cohort_id, COUNT(*) AS total_users
            FROM {prev_cte}
            GROUP BY cohort_id
        ),
        continuing AS (
            SELECT cohort_id, COUNT(*) AS continuing_users
            FROM transition_candidates
            GROUP BY cohort_id
        ),
        agg AS (
            SELECT
                cohort_id,
                next_event,
                COUNT(*) AS transition_users,
                MEDIAN(time_diff_sec) AS median_time_sec,
                APPROX_QUANTILE(time_diff_sec, 0.9) AS p90_time_sec
            FROM transition_candidates
            GROUP BY cohort_id, next_event
        )
        """
    )

    sql = f"""
        WITH {', '.join(ctes)}
        SELECT
            a.cohort_id,
            a.next_event,
            a.transition_users,
            d.total_users,
            COALESCE(c.continuing_users, 0) AS continuing_users,
            a.median_time_sec,
            a.p90_time_sec
        FROM agg a
        JOIN denominators d ON d.cohort_id = a.cohort_id
        LEFT JOIN continuing c ON c.cohort_id = a.cohort_id
        ORDER BY a.cohort_id, a.transition_users DESC
    """
    return sql, params


def _prune_and_aggregate(events_with_counts: list[tuple[str, int]], anchor_users: int) -> tuple[list[dict], int]:
    sorted_events = sorted(events_with_counts, key=lambda x: x[1], reverse=True)
    top = sorted_events[:_TOP_N]
    rest = sorted_events[_TOP_N:]

    top_rows = []
    for event_name, count in top:
        pct = (count / anchor_users) if anchor_users > 0 else 0.0
        top_rows.append({"event_name": event_name, "count": count, "pct": round(pct, 6)})

    other_count = sum(c for _, c in rest)
    return top_rows, other_count


def _run_level_query(
    connection: duckdb.DuckDBPyConnection,
    start_event: str,
    parent_path: list[str],
    direction: str,
    depth: int,
    property_column: str | None,
    property_operator: str | None,
    property_values: list[str] | None,
) -> tuple[list[tuple], int]:
    parent_depth = len(parent_path)
    if parent_depth < 1 or parent_depth >= depth:
        return [], parent_depth
    if parent_path[0] != start_event:
        raise HTTPException(status_code=400, detail="parent_path must start with start_event")

    property_clause, property_params = _build_property_filter_clause(property_column, property_operator, property_values)
    sql, params = _build_level_sql(direction, parent_path, property_clause)
    full_params = [params[0], *property_params, *params[1:]]
    return connection.execute(sql, full_params).fetchall(), parent_depth


def _rows_payload(
    raw_rows: list[tuple],
    cohorts: list[tuple[int, str]],
    path_prefix: list[str],
    include_expandable: bool,
    top_k_enabled: bool,
) -> list[dict]:
    per_cohort: dict[int, dict[str, object]] = {}
    for cohort_id, next_event, transition_users, total_users, continuing_users, median_time_sec, p90_time_sec in raw_rows:
        cid = int(cohort_id)
        if cid not in per_cohort:
            per_cohort[cid] = {"anchor": int(total_users), "continuing": int(continuing_users), "events": []}
        per_cohort[cid]["events"].append((str(next_event), int(transition_users), median_time_sec, p90_time_sec))

    if not per_cohort:
        return []

    first_cohort_id = cohorts[0][0] if cohorts else None
    event_cohort_data: dict[str, dict[int, dict]] = {}
    cohort_other: dict[int, dict] = {}

    for cohort_id, data in per_cohort.items():
        anchor_users = int(data["anchor"])
        continuing_users = int(data["continuing"])
        continue_pct = round((continuing_users / anchor_users) if anchor_users > 0 else 0.0, 6)
        dropoff_pct = round(1.0 - continue_pct, 6)

        if top_k_enabled:
            top_rows, other_count = _prune_and_aggregate([(n, c) for n, c, _, _ in data["events"]], anchor_users)
        else:
            top_rows = [
                {"event_name": name, "count": count, "pct": round((count / anchor_users) if anchor_users > 0 else 0.0, 6)}
                for name, count, _, _ in sorted(data["events"], key=lambda item: item[1], reverse=True)
            ]
            other_count = 0
        timing_map = {n: (m, p) for n, _, m, p in data["events"]}

        for row in top_rows:
            event_name = row["event_name"]
            median, p90 = timing_map.get(event_name, (None, None))
            event_cohort_data.setdefault(event_name, {})[cohort_id] = {
                "user_count": row["count"],
                "pct_of_parent": row["pct"],
                "count": row["count"],
                "pct": row["pct"],
                "continue_pct": continue_pct,
                "dropoff_pct": dropoff_pct,
                "median_time_sec": float(median) if median is not None else None,
                "p90_time_sec": float(p90) if p90 is not None else None,
            }

        if other_count > 0:
            other_pct = round(other_count / anchor_users, 6) if anchor_users > 0 else 0.0
            cohort_other[cohort_id] = {
                "user_count": other_count,
                "pct_of_parent": other_pct,
                "count": other_count,
                "pct": other_pct,
                "continue_pct": continue_pct,
                "dropoff_pct": dropoff_pct,
                "median_time_sec": None,
                "p90_time_sec": None,
            }

    named_rows = []
    for event_name, cohort_vals in event_cohort_data.items():
        values = {}
        for cid, _ in cohorts:
            values[str(cid)] = cohort_vals.get(cid, {
                "user_count": 0,
                "pct_of_parent": 0.0,
                "count": 0,
                "pct": 0.0,
                "continue_pct": 0.0,
                "dropoff_pct": 1.0,
                "median_time_sec": None,
                "p90_time_sec": None,
            })
        sort_pct = cohort_vals.get(first_cohort_id, {}).get("pct", 0.0) if first_cohort_id else 0.0
        sort_count = cohort_vals.get(first_cohort_id, {}).get("count", 0) if first_cohort_id else 0
        named_rows.append({"path": [*path_prefix, event_name], "values": values, "expandable": True, "_sp": sort_pct, "_sc": sort_count})

    named_rows.sort(key=lambda row: (-row["_sp"], -row["_sc"]))
    output = [{"path": r["path"], "values": r["values"], **({"expandable": r["expandable"]} if include_expandable else {})} for r in named_rows]

    if top_k_enabled and cohort_other:
        other_values = {}
        for cid, _ in cohorts:
            other_values[str(cid)] = cohort_other.get(cid, {
                "user_count": 0,
                "pct_of_parent": 0.0,
                "count": 0,
                "pct": 0.0,
                "continue_pct": 0.0,
                "dropoff_pct": 1.0,
                "median_time_sec": None,
                "p90_time_sec": None,
            })
        output.append({"path": [*path_prefix, "Other"], "values": other_values, **({"expandable": False} if include_expandable else {})})

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
) -> dict:
    if direction not in _DIRECTIONS:
        raise HTTPException(status_code=400, detail=f"direction must be one of: {', '.join(_DIRECTIONS)}")
    _validate_depth(depth)

    ensure_cohort_tables(connection)
    if not _snapshot_exists(connection):
        return {"rows": []}

    cohorts = _fetch_active_cohorts(connection)
    if not cohorts:
        return {"rows": []}

    raw_rows, _ = _run_level_query(connection, start_event, [start_event], direction, depth, property_column, property_operator, property_values)
    return {"rows": _rows_payload(raw_rows, cohorts, [start_event], include_expandable=True, top_k_enabled=include_top_k)}


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
) -> dict:
    if direction not in _DIRECTIONS:
        raise HTTPException(status_code=400, detail=f"direction must be one of: {', '.join(_DIRECTIONS)}")
    _validate_depth(depth)

    ensure_cohort_tables(connection)
    if not _snapshot_exists(connection):
        return {"parent_path": parent_path, "rows": []}

    cohorts = _fetch_active_cohorts(connection)
    if not cohorts:
        return {"parent_path": parent_path, "rows": []}

    raw_rows, parent_depth = _run_level_query(connection, start_event, parent_path, direction, depth, property_column, property_operator, property_values)
    if parent_depth >= depth:
        return {"parent_path": parent_path, "rows": []}

    return {
        "parent_path": parent_path,
        "rows": _rows_payload(raw_rows, cohorts, parent_path, include_expandable=False, top_k_enabled=include_top_k),
    }
