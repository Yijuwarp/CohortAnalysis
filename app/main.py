from pathlib import Path

import duckdb
import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from pydantic import BaseModel

app = FastAPI(title="Behavioral Cohort Analysis API")

DATABASE_PATH = Path(__file__).resolve().parent.parent / "cohort_analysis.duckdb"


class ColumnMappingRequest(BaseModel):
    user_id_column: str
    event_name_column: str
    event_time_column: str


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DATABASE_PATH))


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
        dataframe = pd.read_csv(file.file)
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
