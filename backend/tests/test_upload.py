from __future__ import annotations

from fastapi.testclient import TestClient
import pandas as pd

from app.main import detect_column_type
from tests.utils import csv_upload


def test_upload_valid_csv_inserts_rows_and_returns_columns(
    client: TestClient,
    db_connection,
) -> None:
    csv_text = "user,event,time,plan\nu1,signup,2024-01-01,free\nu2,signup,2024-01-02,pro\n"

    response = csv_upload(client, csv_text=csv_text)

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["rows_imported"] == 2, "Upload should report two imported rows"
    assert payload["skipped_rows"] == 0, "Well-formed CSV should not skip rows"
    assert payload["columns"] == ["user", "event", "time", "plan"], "Returned columns should match CSV order"
    assert payload["detected_types"]["user"] == "TEXT"

    table_exists = db_connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events'"
    ).fetchone()[0]
    assert table_exists == 1, "events table should be created after a successful upload"

    inserted_rows = db_connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    assert inserted_rows == 2, "events should contain exactly the uploaded rows"


def test_upload_rejects_non_csv_file(client: TestClient) -> None:
    response = csv_upload(
        client,
        csv_text="not,really,csv\n",
        filename="events.json",
        content_type="application/json",
    )

    assert response.status_code == 400, "Non-CSV uploads should be rejected"
    assert response.json()["detail"] == "Only CSV files are supported"


def test_upload_requires_minimum_three_columns(client: TestClient) -> None:
    response = csv_upload(client, csv_text="user,event\nu1,signup\n")

    assert response.status_code == 400, "Uploads with fewer than three columns should fail"
    assert response.json()["detail"] == "CSV must contain at least 3 columns"


def test_detect_column_type_detects_float_numeric() -> None:
    series = pd.Series(["9.99", "-2.25", "0", None, ""])

    assert detect_column_type(series) == "NUMERIC"


def test_upload_multiline_csv_field_counts_records_not_lines(client: TestClient) -> None:
    csv_text = 'user,event,time\nu1,"signup\nmobile",2024-01-01\n'

    response = csv_upload(client, csv_text=csv_text)

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["rows_imported"] == 1
    assert payload["skipped_rows"] == 0
