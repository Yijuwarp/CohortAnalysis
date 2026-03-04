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


def test_valid_mapping_creates_normalized_table_with_timestamp_and_event_count(
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
            "event_count_column": None,
            "column_types": {
                "uid": "TEXT",
                "event": "TEXT",
                "timestamp": "TIMESTAMP",
                "plan": "TEXT",
                "region": "TEXT",
            },
        },
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["status"] == "normalized"
    assert payload["row_count"] == 2

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
    assert str(columns["event_time"]).startswith("TIMESTAMP")
    assert columns["event_count"] in {"INTEGER", "BIGINT"}


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

    assert response.status_code == 400
    assert "Mapped columns not found" in response.json()["detail"]


def test_mapping_rejects_empty_event_timestamp(client: TestClient) -> None:
    csv_text = (
        "uid,event,timestamp,plan,region\n"
        "u1,signup,,free,NA\n"
        "u2,click,2024-01-02 11:00:00,pro,EU\n"
    )
    upload = csv_upload(client, csv_text=csv_text)
    assert upload.status_code == 200

    response = client.post(
        "/map-columns",
        json={
            "user_id_column": "uid",
            "event_name_column": "event",
            "event_time_column": "timestamp",
            "column_types": {"uid": "TEXT", "event": "TEXT", "timestamp": "TIMESTAMP", "plan": "TEXT", "region": "TEXT"},
        },
    )

    assert response.status_code == 400
    assert "Timestamp value cannot be null" in response.json()["detail"]


def test_mapping_deduplicates_and_aggregates_event_count(client: TestClient, db_connection) -> None:
    csv_text = (
        "uid,event,timestamp,count\n"
        "u1,purchase,2024-01-01,5\n"
        "u1,purchase,2024-01-01,3\n"
    )
    assert csv_upload(client, csv_text=csv_text).status_code == 200

    response = client.post(
        "/map-columns",
        json={
            "user_id_column": "uid",
            "event_name_column": "event",
            "event_time_column": "timestamp",
            "event_count_column": "count",
            "column_types": {"uid": "TEXT", "event": "TEXT", "timestamp": "TIMESTAMP", "count": "NUMERIC"},
        },
    )
    assert response.status_code == 200, response.text

    rows = db_connection.execute(
        "SELECT user_id, event_name, event_count FROM events_normalized"
    ).fetchall()
    assert rows == [("u1", "purchase", 8)]


def test_mapping_rejects_float_event_count(client: TestClient) -> None:
    csv_text = "uid,event,timestamp,count\nu1,purchase,2024-01-01,2.5\n"
    assert csv_upload(client, csv_text=csv_text).status_code == 200

    response = client.post(
        "/map-columns",
        json={
            "user_id_column": "uid",
            "event_name_column": "event",
            "event_time_column": "timestamp",
            "event_count_column": "count",
            "column_types": {"uid": "TEXT", "event": "TEXT", "timestamp": "TIMESTAMP", "count": "NUMERIC"},
        },
    )
    assert response.status_code == 400
    assert "Invalid integer value" in response.json()["detail"]


def test_mapping_normalizes_coarse_timestamps(client: TestClient, db_connection) -> None:
    csv_text = (
        "uid,event,timestamp\n"
        "u1,signup,2024-01-01\n"
        "u2,signup,2024-01-01 09\n"
    )
    assert csv_upload(client, csv_text=csv_text).status_code == 200

    response = client.post(
        "/map-columns",
        json={
            "user_id_column": "uid",
            "event_name_column": "event",
            "event_time_column": "timestamp",
            "column_types": {"uid": "TEXT", "event": "TEXT", "timestamp": "TIMESTAMP"},
        },
    )
    assert response.status_code == 200, response.text

    rows = db_connection.execute(
        "SELECT user_id, strftime(event_time, '%Y-%m-%d %H:%M:%S') FROM events_normalized ORDER BY user_id"
    ).fetchall()
    assert rows == [
        ("u1", "2024-01-01 00:00:00"),
        ("u2", "2024-01-01 09:00:00"),
    ]


def test_mapping_sets_revenue_default_to_zero_when_not_mapped(client: TestClient, db_connection) -> None:
    csv_text = "uid,event,timestamp\nu1,signup,2024-01-01\n"
    assert csv_upload(client, csv_text=csv_text).status_code == 200

    response = client.post(
        "/map-columns",
        json={
            "user_id_column": "uid",
            "event_name_column": "event",
            "event_time_column": "timestamp",
            "column_types": {"uid": "TEXT", "event": "TEXT", "timestamp": "TIMESTAMP"},
        },
    )
    assert response.status_code == 200, response.text

    rows = db_connection.execute("SELECT revenue_amount FROM events_normalized").fetchall()
    assert rows == [(0.0,)]


def test_mapping_deduplicates_and_aggregates_revenue_amount(client: TestClient, db_connection) -> None:
    csv_text = (
        "uid,event,timestamp,count,revenue\n"
        "u1,purchase,2024-01-01,2,10.5\n"
        "u1,purchase,2024-01-01,3,-2.25\n"
    )
    assert csv_upload(client, csv_text=csv_text).status_code == 200

    response = client.post(
        "/map-columns",
        json={
            "user_id_column": "uid",
            "event_name_column": "event",
            "event_time_column": "timestamp",
            "event_count_column": "count",
            "revenue_column": "revenue",
            "column_types": {"uid": "TEXT", "event": "TEXT", "timestamp": "TIMESTAMP", "count": "NUMERIC", "revenue": "NUMERIC"},
        },
    )
    assert response.status_code == 200, response.text

    rows = db_connection.execute(
        "SELECT user_id, event_name, event_count, revenue_amount FROM events_normalized"
    ).fetchall()
    assert rows == [("u1", "purchase", 5, 8.25)]


def test_revenue_events_empty_when_revenue_not_mapped(client: TestClient) -> None:
    csv_text = "uid,event,timestamp\nu1,signup,2024-01-01\n"
    assert csv_upload(client, csv_text=csv_text).status_code == 200

    response = client.post(
        "/map-columns",
        json={
            "user_id_column": "uid",
            "event_name_column": "event",
            "event_time_column": "timestamp",
            "column_types": {"uid": "TEXT", "event": "TEXT", "timestamp": "TIMESTAMP"},
        },
    )
    assert response.status_code == 200, response.text

    events_response = client.get('/revenue-events')
    assert events_response.status_code == 200, events_response.text
    payload = events_response.json()
    assert payload['has_revenue_mapping'] is False
    assert payload['events'] == []
