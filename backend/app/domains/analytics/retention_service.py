"""
Short summary: service for computing user retention across cohorts.
"""
import duckdb
from typing import Any
from fastapi import HTTPException
from app.utils.perf import time_block
from app.utils.math_utils import Z_SCORES, wilson_ci
from app.domains.cohorts.cohort_service import ensure_cohort_tables
from app.queries.retention_queries import fetch_retention_active_rows, fetch_eligibility_rows
from app.utils.time_boundary import get_observation_end_time
from app.utils.db_utils import to_dict, to_dicts

def build_active_cohort_base(connection: duckdb.DuckDBPyConnection) -> tuple[list[tuple[int, str]], dict[int, int]]:
    cursor = connection.execute(
        """
        SELECT cohort_id, name
        FROM cohorts
        WHERE is_active = TRUE AND hidden = FALSE
        ORDER BY cohort_id
        """
    )
    cohorts_rows = cursor.fetchall()
    cohorts = [(row["cohort_id"], row["name"]) for row in to_dicts(cursor, cohorts_rows)]
    s_cursor = connection.execute(
        """
        SELECT c.cohort_id, COUNT(DISTINCT cm.user_id) AS cohort_size
        FROM cohorts c
        LEFT JOIN cohort_membership cm ON c.cohort_id = cm.cohort_id
        LEFT JOIN events_scoped es ON cm.user_id = es.user_id
        WHERE c.is_active = TRUE AND c.hidden = FALSE AND es.user_id IS NOT NULL
        GROUP BY c.cohort_id
        """
    )
    cohort_sizes = {
        int(row["cohort_id"]): int(row["cohort_size"])
        for row in to_dicts(s_cursor, s_cursor.fetchall())
    }
    return cohorts, cohort_sizes


def get_retention(
    connection: duckdb.DuckDBPyConnection,
    max_day: int,
    retention_event: str | None = None,
    include_ci: bool = False,
    confidence: float = 0.95,
    granularity: str = "day",
    retention_type: str = "classic",
) -> dict[str, Any]:
    if granularity not in {"day", "hour"}:
        raise HTTPException(status_code=400, detail="granularity must be day or hour")
    if retention_type not in {"classic", "ever_after"}:
        raise HTTPException(status_code=400, detail="retention_type must be classic or ever_after")

    confidence = round(float(confidence), 2)
    if confidence not in Z_SCORES:
        raise HTTPException(status_code=400, detail="confidence must be one of: 0.90, 0.95, 0.99")

    ensure_cohort_tables(connection)
    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
    ).fetchone()[0]
    
    total_buckets = max_day + 1 if granularity == "day" else (max_day * 24)
    
    if not scoped_exists:
        res: dict[str, Any] = {"max_day": int(max_day), "retention_event": retention_event or "any", "retention_table": []}
        if granularity == "hour":
            res["max_hour"] = total_buckets
        return res

    end_timer = time_block("retention_query")
    cohorts, cohort_sizes = build_active_cohort_base(connection)
    if not cohorts:
        end_timer(max_day=max_day, retention_event=retention_event, cohort_count=0)
        res = {"max_day": int(max_day), "retention_event": retention_event or "any", "retention_table": []}
        if granularity == "hour":
            res["max_hour"] = total_buckets
        return res

    active_rows = fetch_retention_active_rows(connection, max_day, retention_event, granularity, retention_type)
    eligibility_rows = fetch_eligibility_rows(connection, max_day, granularity)

    active_by_bucket = {(int(c), int(b)): int(a) for c, b, a in active_rows}
    eligible_by_bucket = {(int(c), int(b)): int(a) for c, b, a in eligibility_rows}

    retention_table: list[dict[str, Any]] = []
    for cohort_id, cohort_name in cohorts:
        cohort_id = int(cohort_id)
        cohort_size = cohort_sizes.get(cohort_id, 0)
        retention: dict[str, float | None] = {}
        availability: dict[str, dict[str, int]] = {}
        retention_ci: dict[str, dict[str, float | None]] = {}
        for bucket_number in range(total_buckets):
            active_users = active_by_bucket.get((cohort_id, bucket_number), 0)
            eligible_users = eligible_by_bucket.get((cohort_id, bucket_number), 0)
            
            percent: float | None = None
            if cohort_size > 0:
                percent = active_users / cohort_size * 100.0
            
            retention[str(bucket_number)] = float(percent) if percent is not None else None
            
            availability[str(bucket_number)] = {
                "eligible_users": int(eligible_users),
                "cohort_size": int(cohort_size)
            }

            if include_ci:
                lower, upper = wilson_ci(active_users, cohort_size, confidence)
                retention_ci[str(bucket_number)] = {
                    "lower": (float(lower) * 100.0) if lower is not None else None,
                    "upper": (float(upper) * 100.0) if upper is not None else None,
                }

        row: dict[str, Any] = {
            "cohort_id": cohort_id,
            "cohort_name": str(cohort_name),
            "size": int(cohort_size),
            "retention": retention,
            "availability": availability,
        }
        if include_ci:
            row["retention_ci"] = retention_ci
        retention_table.append(row)

    detected_max_day = max_day
    if granularity == "day":
        THRESHOLD = 1.0
        detected_max_day = 0
        for day_number in range(max_day + 1):
            all_below_threshold = True
            for row_data in retention_table:
                val = row_data["retention"].get(str(day_number))
                if val is None:
                    val = 0.0
                if val >= THRESHOLD:
                    all_below_threshold = False
                    break
            if all_below_threshold:
                break
            detected_max_day = day_number
        detected_max_day = max(1, detected_max_day)

    end_timer(
        max_day=detected_max_day,
        retention_event=retention_event,
        cohort_count=len(cohorts)
    )

    result_payload: dict[str, Any] = {
        "max_day": int(detected_max_day),
        "retention_event": retention_event or "any",
        "retention_table": retention_table,
        "observation_end_time": get_observation_end_time(connection).isoformat() if get_observation_end_time(connection) else None
    }
    if granularity == "hour":
        result_payload["max_hour"] = total_buckets
    return result_payload
