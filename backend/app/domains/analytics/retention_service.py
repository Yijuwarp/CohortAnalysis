"""
Short summary: service for computing user retention across cohorts.
"""
import duckdb
from fastapi import HTTPException
from app.utils.perf import time_block
from app.utils.math_utils import Z_SCORES, wilson_ci
from app.domains.cohorts.cohort_service import ensure_cohort_tables
from app.queries.retention_queries import fetch_retention_active_rows

def build_active_cohort_base(connection: duckdb.DuckDBPyConnection) -> tuple[list[tuple[int, str]], dict[int, int]]:
    cohorts = connection.execute(
        """
        SELECT cohort_id, name
        FROM cohorts
        WHERE is_active = TRUE AND hidden = FALSE
        ORDER BY cohort_id
        """
    ).fetchall()
    cohort_sizes = {
        int(row[0]): int(row[1])
        for row in connection.execute(
            """
            SELECT c.cohort_id, COUNT(DISTINCT cm.user_id) AS cohort_size
            FROM cohorts c
            LEFT JOIN cohort_membership cm ON c.cohort_id = cm.cohort_id
            LEFT JOIN events_scoped es ON cm.user_id = es.user_id
            WHERE c.is_active = TRUE AND c.hidden = FALSE AND es.user_id IS NOT NULL
            GROUP BY c.cohort_id
            """
        ).fetchall()
    }
    return cohorts, cohort_sizes


def get_retention(
    connection: duckdb.DuckDBPyConnection,
    max_day: int,
    retention_event: str | None = None,
    include_ci: bool = False,
    confidence: float = 0.95,
) -> dict[str, int | str | list[dict[str, object]]]:
    confidence = round(confidence, 2)
    if confidence not in Z_SCORES:
        raise HTTPException(status_code=400, detail="confidence must be one of: 0.90, 0.95, 0.99")

    ensure_cohort_tables(connection)
    scoped_exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
    ).fetchone()[0]
    if not scoped_exists:
        return {"max_day": int(max_day), "retention_event": retention_event or "any", "retention_table": []}

    end_timer = time_block("retention_query")
    cohorts, cohort_sizes = build_active_cohort_base(connection)
    if not cohorts:
        end_timer(max_day=max_day, retention_event=retention_event, cohort_count=0)
        return {"max_day": int(max_day), "retention_event": retention_event or "any", "retention_table": []}

    active_rows = fetch_retention_active_rows(connection, max_day, retention_event)

    active_by_day = {(int(c), int(d)): int(a) for c, d, a in active_rows}

    retention_table = []
    for cohort_id, cohort_name in cohorts:
        cohort_id = int(cohort_id)
        cohort_size = cohort_sizes.get(cohort_id, 0)
        retention = {}
        retention_ci = {}
        for day_number in range(max_day + 1):
            active_users = active_by_day.get((cohort_id, day_number), 0)
            if cohort_size == 0:
                percent = None
            else:
                percent = active_users / cohort_size * 100.0
            retention[str(day_number)] = float(percent) if percent is not None else None
            if include_ci:
                lower, upper = wilson_ci(active_users, cohort_size, confidence)
                retention_ci[str(day_number)] = {
                    "lower": (float(lower) * 100.0) if lower is not None else None,
                    "upper": (float(upper) * 100.0) if upper is not None else None,
                }

        row = {
            "cohort_id": cohort_id,
            "cohort_name": str(cohort_name),
            "size": int(cohort_size),
            "retention": retention,
        }
        if include_ci:
            row["retention_ci"] = retention_ci
        retention_table.append(row)

    THRESHOLD = 1.0

    detected_max_day = 0

    for day_number in range(max_day + 1):
        all_below_threshold = True
        
        for row in retention_table:
            val = row["retention"].get(str(day_number), 0)
            if val is None:
                val = 0

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

    return {"max_day": int(detected_max_day), "retention_event": retention_event or "any", "retention_table": retention_table}
