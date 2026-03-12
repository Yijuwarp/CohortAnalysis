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
    assert payload["status"] == "ok"
    assert payload["total_users"] == 2
    assert payload["total_events"] == 2

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
    assert columns["original_event_count"] in {"INTEGER", "BIGINT"}
    assert columns["modified_event_count"] in {"INTEGER", "BIGINT"}


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
    assert "event_time must not be empty" in response.json()["detail"]


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
        "SELECT user_id, event_name, original_event_count FROM events_normalized"
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
    assert "event_count must be integer >= 1" in response.json()["detail"]


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

    rows = db_connection.execute("SELECT original_revenue, modified_revenue FROM events_normalized").fetchall()
    assert rows == [(0.0, 0.0)]


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
        "SELECT user_id, event_name, original_event_count, original_revenue, modified_event_count, modified_revenue FROM events_normalized"
    ).fetchall()
    assert rows == [("u1", "purchase", 5, 8.25, 5, 8.25)]


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


def test_events_normalized_revenue_defaults_to_zero_on_manual_insert(client: TestClient, db_connection) -> None:
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

    db_connection.execute(
        """
        INSERT INTO events_normalized (user_id, event_name, event_time, original_event_count, modified_event_count)
        VALUES ('u2', 'session_start', TIMESTAMP '2024-01-02 00:00:00', 1, 1)
        """
    )

    inserted = db_connection.execute(
        """
        SELECT original_revenue, modified_revenue
        FROM events_normalized
        WHERE user_id = 'u2' AND event_name = 'session_start'
        """
    ).fetchone()
    assert inserted == (0.0, 0.0)


def test_revenue_config_events_available_when_revenue_not_mapped(client: TestClient) -> None:
    csv_text = "uid,event,timestamp\nu1,signup,2024-01-01\nu1,session_start,2024-01-02\n"
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

    config_response = client.get('/revenue-config-events')
    assert config_response.status_code == 200, config_response.text
    payload = config_response.json()
    assert payload['has_revenue_mapping'] is True

    config_by_event = {event['event_name']: event for event in payload['events']}
    assert config_by_event == {}
    assert payload['addable_events'] == ['session_start', 'signup']

def test_mapping_handles_quoted_timestamps(client: TestClient, db_connection):
    csv_text = (
        'uid,event,timestamp\n'
        'u1,signup,"2024-01-01 09:30:10"\n'
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

    assert response.status_code == 200


def test_mapping_handles_iso_timestamps(client: TestClient, db_connection):
    csv_text = (
        "uid,event,timestamp\n"
        "u1,signup,2024-01-01T09:30:10\n"
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

    assert response.status_code == 200


def test_mapping_handles_millisecond_timestamps(client: TestClient, db_connection):
    csv_text = (
        "uid,event,timestamp\n"
        "u1,signup,2024-01-01 09:30:10.123\n"
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

    assert response.status_code == 200


def test_mapping_handles_hour_precision_timestamp(client: TestClient, db_connection):
    csv_text = (
        "uid,event,timestamp\n"
        "u1,signup,2024-01-01 09\n"
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

    assert response.status_code == 200
