from pathlib import Path

import duckdb
import pandas as pd
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="Behavioral Cohort Analysis API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_PATH = Path(__file__).resolve().parent.parent / "cohort_analysis.duckdb"


class ColumnMappingRequest(BaseModel):
    user_id_column: str
    event_name_column: str
    event_time_column: str


class CreateCohortRequest(BaseModel):
    name: str
    event_name: str
    min_event_count: int


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DATABASE_PATH))


def ensure_cohort_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cohorts (
            cohort_id INTEGER PRIMARY KEY,
            name TEXT,
            event_name TEXT,
            min_event_count INTEGER
        )
        """
    )
    connection.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS cohorts_id_sequence START 1
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cohort_membership (
            user_id TEXT,
            cohort_id INTEGER,
            join_time TIMESTAMP
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cohort_activity_snapshot (
            cohort_id INTEGER,
            user_id TEXT,
            event_time TIMESTAMP
        )
        """
    )


def quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


@app.get("/")
def read_root() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)) -> dict[str, int | list[str]]:
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    try:
        dataframe = pd.read_csv(
            file.file,
            keep_default_na=False,
            na_values=[""],
        )
    except Exception as exc:  # pragma: no cover - defensive parsing guard
        raise HTTPException(status_code=400, detail="Invalid CSV file") from exc
    finally:
        await file.close()

    if len(dataframe.columns) < 3:
        raise HTTPException(status_code=400, detail="CSV must contain at least 3 columns")

    connection = get_connection()
    try:
        connection.register("uploaded_events", dataframe)
        connection.execute("CREATE OR REPLACE TABLE events AS SELECT * FROM uploaded_events")
    finally:
        connection.close()

    return {
        "rows_imported": int(len(dataframe)),
        "columns": [str(column) for column in dataframe.columns.tolist()],
    }


@app.post("/map-columns")
def map_columns(mapping: ColumnMappingRequest) -> dict[str, str | int]:
    connection = get_connection()
    try:
        table_exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events'"
        ).fetchone()
        if not table_exists or table_exists[0] == 0:
            raise HTTPException(status_code=400, detail="No uploaded CSV found. Upload a CSV first.")

        existing_columns = [
            row[0]
            for row in connection.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'events'
                ORDER BY ordinal_position
                """
            ).fetchall()
        ]

        requested_columns = {
            mapping.user_id_column,
            mapping.event_name_column,
            mapping.event_time_column,
        }

        missing_columns = sorted(requested_columns - set(existing_columns))
        if missing_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Mapped columns not found in uploaded CSV: {', '.join(missing_columns)}",
            )

        remaining_columns = [column for column in existing_columns if column not in requested_columns]
        if remaining_columns:
            json_struct_fields = ", ".join(
                f"'{column}', {quote_identifier(column)}" for column in sorted(remaining_columns)
            )
            raw_data_sql = f"json_object({json_struct_fields})::JSON"
        else:
            raw_data_sql = "'{}'::JSON"

        user_id_column = quote_identifier(mapping.user_id_column)
        event_name_column = quote_identifier(mapping.event_name_column)
        event_time_column = quote_identifier(mapping.event_time_column)

        connection.execute(
            f"""
            CREATE OR REPLACE TABLE events_normalized AS
            SELECT
                CAST({user_id_column} AS TEXT) AS user_id,
                CAST({event_name_column} AS TEXT) AS event_name,
                CAST({event_time_column} AS TIMESTAMP) AS event_time,
                {raw_data_sql} AS raw_data
            FROM events
            """
        )

        row_count = connection.execute("SELECT COUNT(*) FROM events_normalized").fetchone()[0]
    except duckdb.ConversionException as exc:
        raise HTTPException(status_code=400, detail="Failed to convert event_time column to TIMESTAMP") from exc
    finally:
        connection.close()

    return {"status": "normalized", "row_count": int(row_count)}


@app.post("/cohorts")
def create_cohort(payload: CreateCohortRequest) -> dict[str, int]:
    if payload.min_event_count < 1:
        raise HTTPException(status_code=400, detail="min_event_count must be at least 1")

    connection = get_connection()
    try:
        normalized_exists = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized'"
        ).fetchone()
        if not normalized_exists or normalized_exists[0] == 0:
            raise HTTPException(
                status_code=400,
                detail="No normalized events found. Upload a CSV and map columns first.",
            )

        ensure_cohort_tables(connection)

        cohort_id = connection.execute(
            """
            INSERT INTO cohorts (cohort_id, name, event_name, min_event_count)
            VALUES (nextval('cohorts_id_sequence'), ?, ?, ?)
            RETURNING cohort_id
            """,
            [payload.name, payload.event_name, payload.min_event_count],
        ).fetchone()[0]

        connection.execute(
            """
            INSERT INTO cohort_membership (user_id, cohort_id, join_time)
            WITH ranked_events AS (
                SELECT
                    user_id,
                    event_time,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time) AS event_rank
                FROM events_normalized
                WHERE event_name = ?
            )
            SELECT
                user_id,
                ?,
                event_time
            FROM ranked_events
            WHERE event_rank = ?
            """,
            [payload.event_name, cohort_id, payload.min_event_count],
        )

        connection.execute(
            """
            INSERT INTO cohort_activity_snapshot (cohort_id, user_id, event_time)
            SELECT
                ?,
                e.user_id,
                e.event_time
            FROM events_normalized e
            JOIN cohort_membership cm
                ON cm.user_id = e.user_id
               AND cm.cohort_id = ?
            """,
            [cohort_id, cohort_id],
        )

        users_joined = connection.execute(
            "SELECT COUNT(*) FROM cohort_membership WHERE cohort_id = ?",
            [cohort_id],
        ).fetchone()[0]
    finally:
        connection.close()

    return {"cohort_id": int(cohort_id), "users_joined": int(users_joined)}


@app.get("/retention")
def get_retention(max_day: int = Query(7, ge=0)) -> dict[str, int | list[dict[str, object]]]:
    connection = get_connection()
    try:
        ensure_cohort_tables(connection)

        cohorts = connection.execute(
            """
            SELECT cohort_id, name
            FROM cohorts
            ORDER BY cohort_id
            """
        ).fetchall()
        if not cohorts:
            return {"max_day": int(max_day), "retention_table": []}

        cohort_sizes = {
            row[0]: int(row[1])
            for row in connection.execute(
                """
                SELECT c.cohort_id, COUNT(cm.user_id) AS cohort_size
                FROM cohorts c
                LEFT JOIN cohort_membership cm ON c.cohort_id = cm.cohort_id
                GROUP BY c.cohort_id
                """
            ).fetchall()
        }

        active_by_day: dict[tuple[int, int], int] = {}
        active_rows = connection.execute(
            """
            WITH activity_deltas AS (
                SELECT
                    cm.cohort_id,
                    cm.user_id,
                    DATE_DIFF('day', cm.join_time::DATE, cas.event_time::DATE) AS day_number
                FROM cohort_membership cm
                JOIN cohort_activity_snapshot cas
                  ON cm.cohort_id = cas.cohort_id
                 AND cm.user_id = cas.user_id
                WHERE DATE_DIFF('day', cm.join_time::DATE, cas.event_time::DATE) BETWEEN 0 AND ?
            )
            SELECT
                cohort_id,
                day_number,
                COUNT(DISTINCT user_id) AS active_users
            FROM activity_deltas
            GROUP BY cohort_id, day_number
            """,
            [max_day],
        ).fetchall()

        active_by_day = {
            (int(cohort_id), int(day_number)): int(active_users)
            for cohort_id, day_number, active_users in active_rows
        }

        retention_table = []
        for cohort_id, cohort_name in cohorts:
            cohort_size = cohort_sizes.get(cohort_id, 0)
            retention = {}
            for day_number in range(max_day + 1):
                active_users = active_by_day.get((cohort_id, day_number), 0)
                percent = (active_users / cohort_size * 100.0) if cohort_size > 0 else 0.0
                retention[str(day_number)] = float(percent)

            retention_table.append(
                {
                    "cohort_id": int(cohort_id),
                    "cohort_name": str(cohort_name),
                    "size": int(cohort_size),
                    "retention": retention,
                }
            )

        return {"max_day": int(max_day), "retention_table": retention_table}
    finally:
        connection.close()
