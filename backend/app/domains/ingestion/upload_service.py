"""
Short summary: handles CSV upload workflow for ingestion.
"""
import os
import tempfile
import duckdb
from fastapi import File, HTTPException, UploadFile
from app.utils.perf import time_block
from app.utils.sql import reset_application_state
from app.domains.ingestion.type_detection import detect_column_type
from app.domains.ingestion.mapping_service import suggest_column_mapping

async def upload_csv(connection: duckdb.DuckDBPyConnection, file: UploadFile) -> dict[str, object]:
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    end_timer = time_block("csv_upload")
    tmp_path = None
    try:
        file_size = 0

        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            while chunk := await file.read(1024 * 1024):
                file_size += len(chunk)
                tmp.write(chunk)
            tmp_path = tmp.name

        try:
            input_rows = connection.execute(
                "SELECT COUNT(*) FROM read_csv(?, auto_detect=true, ignore_errors=false)",
                [tmp_path],
            ).fetchone()[0]

            connection.execute("DROP TABLE IF EXISTS events")
            reset_application_state(connection)
            try:
                connection.execute(
                    """
                    CREATE TABLE events AS
                    SELECT *
                    FROM read_csv(
                        ?,
                        auto_detect=true,
                        sample_size=10000,
                        quote='"',
                        escape='"',
                        ignore_errors=true,
                        maximum_line_size=20000000,  -- allow very large text/JSON fields in CSV rows
                        parallel=true
                    )
                    """,
                    [tmp_path],
                )
            except Exception as exc:
                end_timer(error=str(exc))
                raise HTTPException(status_code=400, detail=f"Invalid CSV file: {str(exc)}") from exc

            row_count = int(connection.execute("SELECT COUNT(*) FROM events").fetchone()[0])
            skipped_rows = max(input_rows - row_count, 0)
            column_info = connection.execute("PRAGMA table_info('events')").fetchall()
            column_names = [row[1] for row in column_info]

            if len(column_names) < 3:
                end_timer(error="insufficient_columns")
                raise HTTPException(status_code=400, detail="CSV must contain at least 3 columns")

            # Get a sample for type detection to avoid materializing the entire dataset in Pandas
            sample_df = connection.execute("SELECT * FROM events LIMIT 1000").df()
            detected_types = {
                str(column): detect_column_type(sample_df[column])
                for column in column_names
            }
            mapping_suggestions = suggest_column_mapping(column_names)

            end_timer(
                row_count=row_count,
                column_count=len(column_names),
                file_size=file_size
            )

            return {
                "rows_imported": row_count,
                "skipped_rows": skipped_rows,
                "columns": column_names,
                "detected_types": detected_types,
                "mapping_suggestions": mapping_suggestions,
            }
        except Exception as exc:
            end_timer(error=str(exc))
            raise
    finally:
        await file.close()
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
