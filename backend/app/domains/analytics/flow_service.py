"""
Short summary: service for computing event-anchored user flows (L1 and L2) across cohorts.

Supports:
- Forward and reverse directions
- First-occurrence-per-user anchoring
- User-based percentages (denominator = users who performed start_event)
- Top-3 pruning with an "Other" bucket
- Multi-cohort output
- Expandable rows (rows that have valid L2 data)
"""
from __future__ import annotations

import duckdb
from fastapi import HTTPException
from app.domains.cohorts.cohort_service import ensure_cohort_tables

_TOP_N = 3
_DIRECTIONS = ("forward", "reverse")


def _fetch_active_cohorts(
    connection: duckdb.DuckDBPyConnection,
) -> list[tuple[int, str]]:
    """Return list of (cohort_id, name) for all non-hidden, active cohorts."""
    rows = connection.execute(
        """
        SELECT cohort_id, name
        FROM cohorts
        WHERE hidden = FALSE
        ORDER BY cohort_id
        """
    ).fetchall()
    return [(int(r[0]), str(r[1])) for r in rows]


def _snapshot_exists(connection: duckdb.DuckDBPyConnection) -> bool:
    """Return True if the cohort_activity_snapshot table exists and has rows."""
    count = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'cohort_activity_snapshot' AND table_schema = 'main'"
    ).fetchone()[0]
    return bool(count)


# ---------------------------------------------------------------------------
# L1 SQL helpers
# ---------------------------------------------------------------------------

def _build_l1_sql(direction: str) -> str:
    """
    Build the parameterised SQL for L1 flows.

    Parameters (positional, in order):
        1. start_event: str  – the anchor event name
        2. start_event: str  – repeated for denominator CTE

    Returns columns: cohort_id, next_event, transition_users
    Also returns denominator: cohort_id, anchor_users
    """
    if direction == "forward":
        time_filter = "e.event_time > fe.event_time"
        order_dir = "ASC"
    else:
        time_filter = "e.event_time < fe.event_time"
        order_dir = "DESC"

    return f"""
        WITH first_events AS (
            -- Step 1: first occurrence of start_event per user per cohort
            SELECT cohort_id, user_id, event_time
            FROM (
                SELECT
                    cohort_id,
                    user_id,
                    event_time,
                    ROW_NUMBER() OVER (
                        PARTITION BY cohort_id, user_id
                        ORDER BY event_time
                    ) AS rn
                FROM cohort_activity_snapshot
                WHERE event_name = ?
            ) sub
            WHERE rn = 1
        ),
        transitions AS (
            -- Step 2: join to find the first/last adjacent event per user
            SELECT
                fe.cohort_id,
                fe.user_id,
                e.event_name AS next_event,
                ROW_NUMBER() OVER (
                    PARTITION BY fe.cohort_id, fe.user_id
                    ORDER BY e.event_time {order_dir}
                ) AS rn
            FROM first_events fe
            JOIN cohort_activity_snapshot e
              ON e.cohort_id = fe.cohort_id
             AND e.user_id   = fe.user_id
             AND {time_filter}
             AND e.event_name <> ?  -- exclude self-loops
        ),
        first_transitions AS (
            -- Step 3: only the first transition per user
            SELECT cohort_id, user_id, next_event
            FROM transitions
            WHERE rn = 1
        ),
        agg AS (
            -- Step 4: aggregate per cohort + event
            SELECT cohort_id, next_event, COUNT(*) AS transition_users
            FROM first_transitions
            GROUP BY cohort_id, next_event
        ),
        denominators AS (
            -- Denominator: users who performed start_event (per cohort)
            SELECT cohort_id, COUNT(*) AS anchor_users
            FROM first_events
            GROUP BY cohort_id
        )
        SELECT
            a.cohort_id,
            a.next_event,
            a.transition_users,
            d.anchor_users
        FROM agg a
        JOIN denominators d ON a.cohort_id = d.cohort_id
        ORDER BY a.cohort_id, a.transition_users DESC
    """


def _build_l2_sql(direction: str) -> str:
    """
    Build the parameterised SQL for L2 flows.

    Parameters (positional, in order):
        1. start_event: str
        2. start_event: str  (self-loop guard in transitions)
        3. parent_event: str (filter to users whose L1 = parent_event)
        4. parent_event: str (self-loop guard in second transition)

    Returns columns: cohort_id, next_event, transition_users, anchor_users
    where anchor_users = users at the L1 parent_event step (per cohort).
    """
    if direction == "forward":
        time_filter_l1 = "e1.event_time > fe.event_time"
        order_dir_l1 = "ASC"
        time_filter_l2 = "e2.event_time > l1.l1_time"
        order_dir_l2 = "ASC"
    else:
        time_filter_l1 = "e1.event_time < fe.event_time"
        order_dir_l1 = "DESC"
        time_filter_l2 = "e2.event_time < l1.l1_time"
        order_dir_l2 = "DESC"


    return f"""
        WITH first_events AS (
            SELECT cohort_id, user_id, event_time
            FROM (
                SELECT
                    cohort_id,
                    user_id,
                    event_time,
                    ROW_NUMBER() OVER (
                        PARTITION BY cohort_id, user_id
                        ORDER BY event_time
                    ) AS rn
                FROM cohort_activity_snapshot
                WHERE event_name = ?
            ) sub
            WHERE rn = 1
        ),
        l1_transitions AS (
            -- First transition per user (L1 step)
            SELECT
                fe.cohort_id,
                fe.user_id,
                e1.event_name AS l1_event,
                e1.event_time AS l1_time,
                ROW_NUMBER() OVER (
                    PARTITION BY fe.cohort_id, fe.user_id
                    ORDER BY e1.event_time {order_dir_l1}
                ) AS rn
            FROM first_events fe
            JOIN cohort_activity_snapshot e1
              ON e1.cohort_id = fe.cohort_id
             AND e1.user_id   = fe.user_id
             AND {time_filter_l1}
             AND e1.event_name <> ?
        ),
        l1_first AS (
            SELECT cohort_id, user_id, l1_event, l1_time
            FROM l1_transitions
            WHERE rn = 1
              AND l1_event = ?  -- filter to users whose L1 = parent_event
        ),
        l2_transitions AS (
            -- Second transition per user (L2 step)
            SELECT
                l1.cohort_id,
                l1.user_id,
                e2.event_name AS next_event,
                ROW_NUMBER() OVER (
                    PARTITION BY l1.cohort_id, l1.user_id
                    ORDER BY e2.event_time {order_dir_l2}
                ) AS rn
            FROM l1_first l1
            JOIN cohort_activity_snapshot e2
              ON e2.cohort_id = l1.cohort_id
             AND e2.user_id   = l1.user_id
             AND {time_filter_l2}
             AND e2.event_name <> ?
        ),
        l2_first AS (
            SELECT cohort_id, user_id, next_event
            FROM l2_transitions
            WHERE rn = 1
        ),
        agg AS (
            SELECT cohort_id, next_event, COUNT(*) AS transition_users
            FROM l2_first
            GROUP BY cohort_id, next_event
        ),
        denominators AS (
            -- Denominator = users at the L1 parent step
            SELECT cohort_id, COUNT(*) AS anchor_users
            FROM l1_first
            GROUP BY cohort_id
        )
        SELECT
            a.cohort_id,
            a.next_event,
            a.transition_users,
            d.anchor_users
        FROM agg a
        JOIN denominators d ON a.cohort_id = d.cohort_id
        ORDER BY a.cohort_id, a.transition_users DESC
    """


# ---------------------------------------------------------------------------
# Pruning + "Other" logic
# ---------------------------------------------------------------------------

def _prune_and_aggregate(
    events_with_counts: list[tuple[str, int]],
    anchor_users: int,
) -> tuple[list[dict], int, int]:
    """
    Apply top-3 pruning and compute "Other" bucket.

    Returns:
        (top_rows, other_count, anchor_users)
        where top_rows is list of {event_name, count, pct}
    """
    # Sort descending by count (already sorted from SQL, but be defensive)
    sorted_events = sorted(events_with_counts, key=lambda x: x[1], reverse=True)
    top = sorted_events[:_TOP_N]
    rest = sorted_events[_TOP_N:]

    top_rows = []
    for event_name, count in top:
        pct = (count / anchor_users) if anchor_users > 0 else 0.0
        top_rows.append({"event_name": event_name, "count": count, "pct": round(pct, 6)})

    other_count = sum(c for _, c in rest)
    return top_rows, other_count, anchor_users


# ---------------------------------------------------------------------------
# Public service functions
# ---------------------------------------------------------------------------

def get_l1_flows(
    connection: duckdb.DuckDBPyConnection,
    start_event: str,
    direction: str,
) -> dict:
    """
    Compute L1 flows for a given start_event and direction.

    Returns:
        {
            "rows": [
                {
                    "path": [start_event, next_event],
                    "values": {
                        "<cohort_id>": {"count": int, "pct": float}
                    },
                    "expandable": bool
                }
            ]
        }

    Sorting: by pct of first visible cohort descending.
    Expandable: True for all named top-3 events (never for "Other").
    """
    if direction not in _DIRECTIONS:
        raise HTTPException(status_code=400, detail=f"direction must be one of: {', '.join(_DIRECTIONS)}")

    ensure_cohort_tables(connection)
    if not _snapshot_exists(connection):
        return {"rows": []}

    cohorts = _fetch_active_cohorts(connection)
    if not cohorts:
        return {"rows": []}

    sql = _build_l1_sql(direction)
    # Parameters: start_event (first_events WHERE), start_event (self-loop guard)
    raw_rows = connection.execute(sql, [start_event, start_event]).fetchall()

    # Organise raw rows: {cohort_id: [(event_name, transition_users, anchor_users), ...]}
    per_cohort: dict[int, tuple[int, list[tuple[str, int]]]] = {}
    for cohort_id, next_event, transition_users, anchor_users in raw_rows:
        cohort_id = int(cohort_id)
        anchor_users = int(anchor_users)
        transition_users = int(transition_users)
        if cohort_id not in per_cohort:
            per_cohort[cohort_id] = (anchor_users, [])
        per_cohort[cohort_id][1].append((str(next_event), transition_users))

    if not per_cohort:
        return {"rows": []}

    # Build cohort_id list in cohorts order
    active_cohort_ids = [cid for cid, _ in cohorts]

    # Collect all unique event names across cohorts (top-3 per cohort)
    # We gather every top-3 event seen in any cohort for the union path list
    event_cohort_data: dict[str, dict[int, dict]] = {}  # event_name -> {cohort_id: {count, pct}}

    cohort_other: dict[int, dict] = {}  # cohort_id -> {count, pct}

    for cohort_id, (anchor_users, events) in per_cohort.items():
        top_rows, other_count, _ = _prune_and_aggregate(events, anchor_users)
        for row in top_rows:
            en = row["event_name"]
            if en not in event_cohort_data:
                event_cohort_data[en] = {}
            event_cohort_data[en][cohort_id] = {"count": row["count"], "pct": row["pct"]}

        if other_count > 0:
            other_pct = round(other_count / anchor_users, 6) if anchor_users > 0 else 0.0
            cohort_other[cohort_id] = {"count": other_count, "pct": other_pct}

    # Determine sorting cohort: first visible active cohort
    first_cohort_id = active_cohort_ids[0] if active_cohort_ids else None

    # Build output rows for named events
    named_rows = []
    for event_name, cohort_vals in event_cohort_data.items():
        values: dict[str, dict] = {}
        for cid, _ in cohorts:
            if cid in cohort_vals:
                values[str(cid)] = {"count": cohort_vals[cid]["count"], "pct": cohort_vals[cid]["pct"]}
            else:
                values[str(cid)] = {"count": 0, "pct": 0.0}

        sort_pct = cohort_vals.get(first_cohort_id, {}).get("pct", 0.0) if first_cohort_id else 0.0
        sort_count = cohort_vals.get(first_cohort_id, {}).get("count", 0) if first_cohort_id else 0

        named_rows.append({
            "path": [start_event, event_name],
            "values": values,
            "expandable": True,
            "_sort_pct": sort_pct,
            "_sort_count": sort_count,
        })

    # Sort by first cohort pct desc, then count desc
    named_rows.sort(key=lambda r: (-r["_sort_pct"], -r["_sort_count"]))

    # Strip internal sort keys
    output_rows = []
    for row in named_rows:
        output_rows.append({
            "path": row["path"],
            "values": row["values"],
            "expandable": row["expandable"],
        })

    # Build "Other" row if any cohort has other_count > 0
    if cohort_other:
        other_values: dict[str, dict] = {}
        for cid, _ in cohorts:
            if cid in cohort_other:
                other_values[str(cid)] = cohort_other[cid]
            else:
                other_values[str(cid)] = {"count": 0, "pct": 0.0}
        output_rows.append({
            "path": [start_event, "Other"],
            "values": other_values,
            "expandable": False,
        })

    return {"rows": output_rows}


def get_l2_flows(
    connection: duckdb.DuckDBPyConnection,
    start_event: str,
    parent_event: str,
    direction: str,
) -> dict:
    """
    Compute L2 flows for users who hit parent_event as their L1 step.

    Returns:
        {
            "parent_path": [start_event, parent_event],
            "rows": [
                {
                    "path": [start_event, parent_event, next_event],
                    "values": {
                        "<cohort_id>": {"count": int, "pct": float}
                    }
                }
            ]
        }
    """
    if direction not in _DIRECTIONS:
        raise HTTPException(status_code=400, detail=f"direction must be one of: {', '.join(_DIRECTIONS)}")

    ensure_cohort_tables(connection)
    if not _snapshot_exists(connection):
        return {"parent_path": [start_event, parent_event], "rows": []}

    cohorts = _fetch_active_cohorts(connection)
    if not cohorts:
        return {"parent_path": [start_event, parent_event], "rows": []}

    sql = _build_l2_sql(direction)
    # Parameters:
    #   1. start_event (first_events WHERE)
    #   2. start_event (self-loop guard for L1)
    #   3. parent_event (l1_event = parent_event filter)
    #   4. parent_event (self-loop guard for L2)
    raw_rows = connection.execute(
        sql, [start_event, start_event, parent_event, parent_event]
    ).fetchall()

    per_cohort: dict[int, tuple[int, list[tuple[str, int]]]] = {}
    for cohort_id, next_event, transition_users, anchor_users in raw_rows:
        cohort_id = int(cohort_id)
        anchor_users = int(anchor_users)
        transition_users = int(transition_users)
        if cohort_id not in per_cohort:
            per_cohort[cohort_id] = (anchor_users, [])
        per_cohort[cohort_id][1].append((str(next_event), transition_users))

    if not per_cohort:
        return {"parent_path": [start_event, parent_event], "rows": []}

    active_cohort_ids = [cid for cid, _ in cohorts]
    first_cohort_id = active_cohort_ids[0] if active_cohort_ids else None

    event_cohort_data: dict[str, dict[int, dict]] = {}
    cohort_other: dict[int, dict] = {}

    for cohort_id, (anchor_users, events) in per_cohort.items():
        top_rows, other_count, _ = _prune_and_aggregate(events, anchor_users)
        for row in top_rows:
            en = row["event_name"]
            if en not in event_cohort_data:
                event_cohort_data[en] = {}
            event_cohort_data[en][cohort_id] = {"count": row["count"], "pct": row["pct"]}

        if other_count > 0:
            other_pct = round(other_count / anchor_users, 6) if anchor_users > 0 else 0.0
            cohort_other[cohort_id] = {"count": other_count, "pct": other_pct}

    named_rows = []
    for event_name, cohort_vals in event_cohort_data.items():
        values: dict[str, dict] = {}
        for cid, _ in cohorts:
            if cid in cohort_vals:
                values[str(cid)] = {"count": cohort_vals[cid]["count"], "pct": cohort_vals[cid]["pct"]}
            else:
                values[str(cid)] = {"count": 0, "pct": 0.0}

        sort_pct = cohort_vals.get(first_cohort_id, {}).get("pct", 0.0) if first_cohort_id else 0.0
        sort_count = cohort_vals.get(first_cohort_id, {}).get("count", 0) if first_cohort_id else 0

        named_rows.append({
            "path": [start_event, parent_event, event_name],
            "values": values,
            "_sort_pct": sort_pct,
            "_sort_count": sort_count,
        })

    named_rows.sort(key=lambda r: (-r["_sort_pct"], -r["_sort_count"]))

    output_rows = []
    for row in named_rows:
        output_rows.append({
            "path": row["path"],
            "values": row["values"],
        })

    if cohort_other:
        other_values: dict[str, dict] = {}
        for cid, _ in cohorts:
            if cid in cohort_other:
                other_values[str(cid)] = cohort_other[cid]
            else:
                other_values[str(cid)] = {"count": 0, "pct": 0.0}
        output_rows.append({
            "path": [start_event, parent_event, "Other"],
            "values": other_values,
        })

    return {"parent_path": [start_event, parent_event], "rows": output_rows}
