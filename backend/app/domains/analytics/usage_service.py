"""
Short summary: service for computing event usage and frequency.
"""
import logging
import duckdb
from fastapi import HTTPException

logger = logging.getLogger(__name__)
from app.utils.perf import time_block
from app.utils.sql import quote_identifier, classify_column
from app.utils.db_utils import check_table_exists
from app.domains.cohorts.cohort_service import ensure_cohort_tables
from app.domains.analytics.retention_service import build_active_cohort_base
from app.utils.time_boundary import get_observation_end_time
from app.queries.usage_queries import build_usage_property_filter_clause

def list_events(connection: duckdb.DuckDBPyConnection) -> dict[str, list[str]]:
    if not check_table_exists(connection, "events_scoped"):
        return {"events": []}

    rows = connection.execute("SELECT DISTINCT event_name FROM events_scoped ORDER BY event_name").fetchall()
    return {"events": [str(row[0]) for row in rows]}


def get_event_properties(connection: duckdb.DuckDBPyConnection, event_name: str) -> dict[str, list[str]]:
    if not check_table_exists(connection, "events_scoped"):
        return {"properties": []}

    event_exists = connection.execute("SELECT 1 FROM events_scoped WHERE event_name = ? LIMIT 1", [event_name]).fetchone()
    if event_exists is None:
        raise HTTPException(status_code=404, detail=f"Unknown event: {event_name}")

    columns = [
        str(row[0])
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'events_scoped'
            ORDER BY ordinal_position
            """
        ).fetchall()
    ]
    properties = [column for column in columns if classify_column(column) == "property"]
    return {"properties": properties}


def get_event_property_values(
    connection: duckdb.DuckDBPyConnection,
    event_name: str,
    property: str,
    limit: int = 25,
) -> dict[str, object]:
    if not check_table_exists(connection, "events_scoped"):
        return {"values": [], "total_distinct": 0}

    event_exists = connection.execute("SELECT 1 FROM events_scoped WHERE event_name = ? LIMIT 1", [event_name]).fetchone()
    if event_exists is None:
        raise HTTPException(status_code=404, detail=f"Unknown event: {event_name}")

    columns = {
        str(row[0])
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'events_scoped'
            """
        ).fetchall()
    }
    if property not in columns or classify_column(property) != "property":
        raise HTTPException(status_code=400, detail=f"Unknown property: {property}")

    property_ref = quote_identifier(property)
    rows = connection.execute(
        f"""
        SELECT CAST({property_ref} AS VARCHAR) AS property_value, COUNT(*) AS frequency
        FROM events_scoped
        WHERE event_name = ?
          AND {property_ref} IS NOT NULL
        GROUP BY property_value
        ORDER BY frequency DESC, property_value ASC
        LIMIT ?
        """,
        [event_name, limit],
    ).fetchall()

    total_distinct = int(connection.execute(
        f"""
        SELECT COUNT(DISTINCT CAST({property_ref} AS VARCHAR))
        FROM events_scoped
        WHERE event_name = ?
          AND {property_ref} IS NOT NULL
        """,
        [event_name],
    ).fetchone()[0] or 0)

    return {"values": [str(value) for value, _ in rows], "total_distinct": total_distinct}


def get_usage(
    connection: duckdb.DuckDBPyConnection,
    event: str,
    max_day: int = 7,
    retention_event: str | None = None,
    property: str | None = None,
    operator: str = "=",
    values: list[str] | None = None,
) -> dict[str, object]:
    retention_event = retention_event or "any"
    ensure_cohort_tables(connection)
    
    empty_response = {
        "max_day": int(max_day),
        "event": event,
        "retention_event": retention_event or "any",
        "property_filter": {"property": property, "operator": operator, "values": values} if property else None,
        "usage_volume_table": [],
        "usage_users_table": [],
        "usage_adoption_table": [],
        "retained_users_table": [],
    }

    if not check_table_exists(connection, "events_scoped"):
        return empty_response

    end_timer = time_block("usage_query")
    cohorts, cohort_sizes = build_active_cohort_base(connection)
    if not cohorts:
        end_timer(event=event, max_day=max_day, retention_event=retention_event, cohort_count=0)
        return empty_response

    event_exists = connection.execute("SELECT 1 FROM events_scoped WHERE event_name = ? LIMIT 1", [event]).fetchone()
    if event_exists is None:
        end_timer(event=event, max_day=max_day, retention_event=retention_event, error="event_not_found")
        return empty_response

    known_columns = {
        row[0]
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'events_scoped'
            """
        ).fetchall()
    }
    if property and (
        property not in known_columns
        or classify_column(property) != "property"
    ):
        raise HTTPException(status_code=400, detail=f"Unknown property: {property}")

    property_clause, property_params = build_usage_property_filter_clause(
        property=property,
        operator=operator,
        values=values,
    )

    from app.domains.analytics.metric_builders.usage_vectors import build_usage_vector_sql
    from app.domains.analytics.metric_builders.retention_vectors import build_retention_vector_sql
    observation_end_time = get_observation_end_time(connection)

    usage_volume_table = []
    usage_users_table = []
    usage_adoption_table = []
    retained_users_table = []
    
    for cohort_id, cohort_name, join_type in cohorts:
        cohort_id = int(cohort_id)
        cohort_size = cohort_sizes.get(cohort_id, 0)
        
        # 1. Volume & Unique Vectors
        join_type = next((c[2] for c in cohorts if c[0] == cohort_id), "condition_met")
        vol_sql, vol_params = build_usage_vector_sql(
            cohort_id=cohort_id,
            max_day=max_day,
            join_type=join_type,
            event_name=event,
            metric="volume",
            property_clause=property_clause,
            property_params=property_params,
            observation_end_time=observation_end_time
        )
        # Fetch volume and eligibility
        vol_agg_sql = f"SELECT day_offset, SUM(value), SUM(is_eligible::INTEGER) FROM ({vol_sql}) GROUP BY 1"
        vol_rows = connection.execute(vol_agg_sql, vol_params).fetchall()
        volume_by_day = {int(d): int(v) for d, v, e in vol_rows}
        eligible_by_day = {int(d): int(e) for d, v, e in vol_rows}
        
        # 2. Adoption (Cumulative Unique)
        unique_sql, unique_params = build_usage_vector_sql(
            cohort_id=cohort_id,
            max_day=max_day,
            join_type=join_type,
            event_name=event,
            metric="unique",
            property_clause=property_clause,
            property_params=property_params,
            observation_end_time=observation_end_time
        )
        agg_sql = f"""
        WITH active_users AS (
            SELECT user_id, day_offset,
                   ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY day_offset) as rn
            FROM ({unique_sql})
            WHERE value > 0
        )
        SELECT 
            day_offset,
            COUNT(*) as distinct_users,
            SUM((rn = 1)::INTEGER) as newly_adopted_users
        FROM active_users
        GROUP BY day_offset
        """
        agg_rows = connection.execute(agg_sql, unique_params).fetchall()
        
        distinct_users_by_day = {d: 0 for d in range(max_day + 1)}
        adoption_increment = {d: 0 for d in range(max_day + 1)}
        
        for day, dist, newly_adopted in agg_rows:
            day = int(day)
            if day <= max_day:
                distinct_users_by_day[day] = int(dist)
                adoption_increment[day] = int(newly_adopted)
        
        # 3. Retained Users
        join_type = next((c[2] for c in cohorts if c[0] == cohort_id), "condition_met")
        ret_sql, ret_params = build_retention_vector_sql(
            cohort_id=cohort_id,
            max_day=max_day,
            retention_event=retention_event,
            observation_end_time=observation_end_time,
            join_type=join_type
        )
        ret_agg_sql = f"SELECT day_offset, SUM(value::INTEGER), SUM(is_eligible::INTEGER) FROM ({ret_sql}) GROUP BY 1"
        ret_rows = connection.execute(ret_agg_sql, ret_params).fetchall()
        retained_by_day = {int(d): int(a) for d, a, e in ret_rows}
        ret_eligible_by_day = {int(d): int(e) for d, a, e in ret_rows}

        # 4. Assemble Tables
        volume_values = {}
        user_values = {}
        adoption_values = {}
        retained_values = {}
        availability = {}
        ret_availability = {}
        cumulative_adoption = 0
        
        for day_number in range(max_day + 1):
            day_str = str(day_number)
            volume_values[day_str] = volume_by_day.get(day_number, 0)
            user_values[day_str] = distinct_users_by_day.get(day_number, 0)
            
            cumulative_adoption += adoption_increment.get(day_number, 0)
            adoption_values[day_str] = cumulative_adoption
            
            retained_values[day_str] = retained_by_day.get(day_number, 0)
            
            eligible = eligible_by_day.get(day_number, 0)
            availability[day_str] = {
                "eligible_users": int(eligible),
                "cohort_size": int(cohort_size)
            }
            
            ret_eligible = ret_eligible_by_day.get(day_number, 0)
            ret_availability[day_str] = {
                "eligible_users": int(ret_eligible),
                "cohort_size": int(cohort_size)
            }

        common_metadata = {"cohort_id": cohort_id, "cohort_name": str(cohort_name), "size": int(cohort_size)}
        usage_volume_table.append({**common_metadata, "values": volume_values, "availability": availability})
        usage_users_table.append({**common_metadata, "values": user_values, "availability": availability})
        usage_adoption_table.append({**common_metadata, "values": adoption_values, "availability": availability})
        retained_users_table.append({**common_metadata, "values": retained_values, "availability": ret_availability})

    end_timer(
        event=event,
        max_day=max_day,
        retention_event=retention_event,
        cohort_count=len(cohorts)
    )

    return {
        "event": event,
        "max_day": int(max_day),
        "retention_event": retention_event,
        "usage_volume_table": usage_volume_table,
        "usage_users_table": usage_users_table,
        "usage_adoption_table": usage_adoption_table,
        "retained_users_table": retained_users_table,
        "observation_end_time": observation_end_time.isoformat() if observation_end_time else None
    }


def get_usage_frequency(
    connection: duckdb.DuckDBPyConnection,
    event: str,
    property: str | None = None,
    operator: str = "=",
    values: list[str] | None = None,
) -> dict[str, object]:
    ensure_cohort_tables(connection)

    if not check_table_exists(connection, "events_scoped"):
        return {"buckets": [], "cohort_sizes": []}

    cohorts, cohort_sizes_map = build_active_cohort_base(connection)
    if not cohorts:
        return {"buckets": [], "cohort_sizes": []}

    cohort_sizes = [{"cohort_id": c[0], "name": str(c[1]), "size": cohort_sizes_map.get(c[0], 0)} for c in cohorts]

    known_columns = {
        row[0]
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'events_scoped'
            """
        ).fetchall()
    }
    if property and (
        property not in known_columns
        or classify_column(property) != "property"
    ):
        raise HTTPException(status_code=400, detail=f"Unknown property: {property}")

    property_clause, property_params = build_usage_property_filter_clause(
        property=property,
        operator=operator,
        values=values,
        table_alias="e",
    )

    rows = connection.execute(
        """
        WITH deduped_membership AS (
            SELECT DISTINCT cohort_id, user_id, join_time
            FROM cohort_membership
        ),
        filtered_membership AS (
            SELECT dm.*
            FROM deduped_membership dm
            JOIN cohorts c ON c.cohort_id = dm.cohort_id
            WHERE c.hidden = FALSE
        ),
        user_event_counts AS (
            SELECT
                cm.cohort_id,
                cm.user_id,
                SUM(e.event_count) AS event_count
            FROM filtered_membership cm
            JOIN events_scoped e
                ON e.user_id = cm.user_id
                AND e.event_name = ?
                AND e.event_time >= cm.join_time{property_clause}
            GROUP BY cm.cohort_id, cm.user_id
        ),
        non_zero_buckets AS (
            SELECT
                cohort_id,
                CASE
                    WHEN event_count = 1 THEN '1'
                    WHEN event_count BETWEEN 2 AND 5 THEN '2-5'
                    WHEN event_count BETWEEN 6 AND 10 THEN '6-10'
                    WHEN event_count BETWEEN 11 AND 20 THEN '11-20'
                    ELSE '20+'
                END AS bucket,
                COUNT(DISTINCT user_id) AS users
            FROM user_event_counts
            GROUP BY cohort_id, bucket
        ),
        zero_bucket AS (
            SELECT
                cm.cohort_id,
                '0' AS bucket,
                COUNT(DISTINCT cm.user_id) AS users
            FROM filtered_membership cm
            LEFT JOIN user_event_counts u
                ON cm.user_id = u.user_id
                AND cm.cohort_id = u.cohort_id
            WHERE u.user_id IS NULL
            GROUP BY cm.cohort_id
        ),
        all_buckets AS (
            SELECT * FROM zero_bucket
            UNION ALL
            SELECT * FROM non_zero_buckets
        )
        SELECT * FROM all_buckets
        ORDER BY
            cohort_id,
            CASE
                WHEN bucket = '0' THEN 0
                WHEN bucket = '1' THEN 1
                WHEN bucket = '2-5' THEN 2
                WHEN bucket = '6-10' THEN 3
                WHEN bucket = '11-20' THEN 4
                ELSE 5
            END
        """.format(property_clause=property_clause),
        [event, *property_params]
    ).fetchall()

    bucket_order = ["0", "1", "2-5", "6-10", "11-20", "20+"]
    
    bucket_data = {b: {c[0]: 0 for c in cohorts} for b in bucket_order}
    
    for cohort_id, bucket, users in rows:
        if bucket in bucket_data:
            bucket_data[bucket][cohort_id] = users

    # Validation: Buckets sum to cohort size
    for cid, name, jtype in cohorts:
        size = cohort_sizes_map.get(cid, 0)
        total_bucket_users = sum(bucket_data[b].get(cid, 0) for b in bucket_order)
        if total_bucket_users != size:
            logger.error(
                f"[Frequency Validation Failed] cohort={name} "
                f"expected={size} got={total_bucket_users}"
            )
            
    buckets = []
    for b in bucket_order:
        cohorts_list = [{"cohort_id": cid, "users": count} for cid, count in bucket_data[b].items()]
        cohorts_list.sort(key=lambda x: x["cohort_id"])
        buckets.append({
            "bucket": b,
            "cohorts": cohorts_list
        })

    return {
        "buckets": buckets,
        "cohort_sizes": cohort_sizes
    }
