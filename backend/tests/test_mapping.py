from __future__ import annotations

from fastapi.testclient import TestClient

from tests.utils import csv_upload


def _upload_base_csv(client: TestClient) -> None:
    csv_text = (
        "uid,event,timestamp,plan,region\n"
        "u1,signup,2024-01-01 10:00:00,free,NA\n"
        "u2,click,2024-01-02 11:00:00,pro,EU\n"
    )
    response = csv_upload(client, csv_text=csv_text)
    assert response.status_code == 200, f"Precondition failed: upload endpoint returned {response.text}"


def test_valid_mapping_creates_normalized_table_with_timestamp_and_same_row_count(
    client: TestClient,
    db_connection,
) -> None:
    _upload_base_csv(client)

    response = client.post(
        "/map-columns",
        json={
            "user_id_column": "uid",
            "event_name_column": "event",
            "event_time_column": "timestamp",
        },
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["status"] == "normalized"
    assert payload["row_count"] == 2, "Mapping should preserve the original row count"

    exists = db_connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_normalized'"
    ).fetchone()[0]
    assert exists == 1, "events_normalized table should exist after successful column mapping"

    row_count = db_connection.execute("SELECT COUNT(*) FROM events_normalized").fetchone()[0]
    assert row_count == 2, "events_normalized row count should match uploaded events"

    event_time_type = db_connection.execute(
        """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_name = 'events_normalized' AND column_name = 'event_time'
        """
    ).fetchone()[0]
    assert event_time_type == "TIMESTAMP", f"event_time should be TIMESTAMP, got {event_time_type}"


def test_mapping_rejects_unknown_column_names(client: TestClient) -> None:
    _upload_base_csv(client)

    response = client.post(
        "/map-columns",
        json={
            "user_id_column": "unknown_user",
            "event_name_column": "event",
            "event_time_column": "timestamp",
        },
    )

    assert response.status_code == 400, "Mapping should fail when a requested column does not exist"
    assert "Mapped columns not found" in response.json()["detail"]


def test_mapping_promotes_mapped_columns_as_first_class_columns(
    client: TestClient,
    db_connection,
) -> None:
    _upload_base_csv(client)

    response = client.post(
        "/map-columns",
        json={
            "user_id_column": "uid",
            "event_name_column": "event",
            "event_time_column": "timestamp",
        },
    )
    assert response.status_code == 200, response.text

    columns = {
        row[0]: row[1]
        for row in db_connection.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'events_normalized'
            """
        ).fetchall()
    }

    assert "plan" in columns
    assert "region" in columns
    assert columns["user_id"] == "VARCHAR"
    assert columns["event_name"] == "VARCHAR"


def test_mapping_allows_empty_timestamp_cells_as_null(
    client: TestClient,
    db_connection,
) -> None:
    csv_text = (
        "uid,event,timestamp,plan,region\n"
        "u1,signup,,free,NA\n"
        "u2,click,2024-01-02 11:00:00,pro,EU\n"
    )
    upload = csv_upload(client, csv_text=csv_text)
    assert upload.status_code == 200, f"Precondition failed: upload endpoint returned {upload.text}"

    response = client.post(
        "/map-columns",
        json={
            "user_id_column": "uid",
            "event_name_column": "event",
            "event_time_column": "timestamp",
        },
    )

    assert response.status_code == 200, response.text
    event_time = db_connection.execute(
        "SELECT event_time FROM events_normalized WHERE user_id = 'u1'"
    ).fetchone()[0]
    assert event_time is None, "Blank timestamp cells should be ingested as NULL"
