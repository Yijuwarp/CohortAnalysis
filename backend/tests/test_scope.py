from __future__ import annotations

from fastapi.testclient import TestClient

from tests.utils import csv_upload


def _prepare_scoped_fixture(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time,country,device,campaign_id\n"
        "u1,signup,2026-01-01 09:00:00,US,Android,c1\n"
        "u1,open,2026-01-02 09:00:00,US,Android,c1\n"
        "u1,purchase,2026-01-03 09:00:00,US,iOS,c2\n"
        "u2,signup,2026-01-01 10:00:00,CA,Android,c2\n"
        "u2,open,2026-01-04 10:00:00,CA,Android,c2\n"
        "u3,signup,2026-02-01 08:00:00,US,Web,c3\n"
        "u3,open,2026-02-02 08:00:00,US,Web,c3\n"
    )
    upload = csv_upload(client, csv_text=csv_text)
    assert upload.status_code == 200, upload.text

    mapped = client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
        },
    )
    assert mapped.status_code == 200, mapped.text


def test_filter_reduces_dataset_and_reset_restores_dataset(client: TestClient, db_connection) -> None:
    _prepare_scoped_fixture(client)

    original_count = db_connection.execute("SELECT COUNT(*) FROM events_normalized").fetchone()[0]

    filtered = client.post(
        "/apply-filters",
        json={"date_range": None, "filters": [{"column": "country", "operator": "=", "value": "US"}]},
    )
    assert filtered.status_code == 200, filtered.text

    scoped_count = db_connection.execute("SELECT COUNT(*) FROM events_scoped").fetchone()[0]
    assert scoped_count < original_count

    reset = client.post("/apply-filters", json={"date_range": None, "filters": []})
    assert reset.status_code == 200, reset.text
    restored = db_connection.execute("SELECT COUNT(*) FROM events_scoped").fetchone()[0]
    assert restored == original_count


def test_cohort_inactivation_and_overlay_metrics_use_scoped_dataset(client: TestClient) -> None:
    _prepare_scoped_fixture(client)

    cohort = client.post(
        "/cohorts",
        json={"name": "purchase_only", "logic_operator": "AND", "conditions": [{"event_name": "purchase", "min_event_count": 1}]},
    )
    assert cohort.status_code == 200, cohort.text

    # Keep only CA users to remove web_only members from scoped overlay.
    filtered = client.post(
        "/apply-filters",
        json={"date_range": None, "filters": [{"column": "country", "operator": "=", "value": "CA"}]},
    )
    assert filtered.status_code == 200, filtered.text

    cohorts_response = client.get("/cohorts")
    assert cohorts_response.status_code == 200, cohorts_response.text
    cohorts = {row["cohort_name"]: row for row in cohorts_response.json()["cohorts"]}
    assert cohorts["purchase_only"]["is_active"] is False

    retention = client.get("/retention?max_day=3")
    assert retention.status_code == 200, retention.text
    retention_rows = {row["cohort_name"]: row for row in retention.json()["retention_table"]}
    assert "purchase_only" not in retention_rows
    assert retention_rows["All Users"]["size"] == 1

    usage = client.get("/usage?event=open&max_day=3")
    assert usage.status_code == 200, usage.text
    usage_rows = {row["cohort_name"]: row for row in usage.json()["usage_volume_table"]}
    assert "purchase_only" not in usage_rows
    assert usage_rows["All Users"]["values"]["3"] == 1


def test_date_range_filter_and_scope_metadata_persistence(client: TestClient, db_connection) -> None:
    _prepare_scoped_fixture(client)

    response = client.post(
        "/apply-filters",
        json={
            "date_range": {"start": "2026-01-01", "end": "2026-01-31"},
            "filters": [{"column": "device", "operator": "IN", "value": ["Android", "iOS"]}],
        },
    )
    assert response.status_code == 200, response.text

    out_of_range_count = db_connection.execute(
        """
        SELECT COUNT(*)
        FROM events_scoped
        WHERE event_time < '2026-01-01'::TIMESTAMP OR event_time >= '2026-02-01'::TIMESTAMP
        """
    ).fetchone()[0]
    assert out_of_range_count == 0

    scope = db_connection.execute(
        "SELECT id, filters_json, total_rows, filtered_rows FROM dataset_scope"
    ).fetchone()
    assert scope[0] == 1
    assert scope[2] == db_connection.execute("SELECT COUNT(*) FROM events_normalized").fetchone()[0]
    assert scope[3] == db_connection.execute("SELECT COUNT(*) FROM events_scoped").fetchone()[0]
    assert '"date_range"' in scope[1]


def test_all_users_effective_size_changes_under_scope(client: TestClient) -> None:
    _prepare_scoped_fixture(client)

    baseline = client.get("/retention?max_day=0")
    assert baseline.status_code == 200, baseline.text
    baseline_size = {row["cohort_name"]: row for row in baseline.json()["retention_table"]}["All Users"]["size"]
    assert baseline_size == 3

    filtered = client.post(
        "/apply-filters",
        json={"date_range": None, "filters": [{"column": "country", "operator": "=", "value": "US"}]},
    )
    assert filtered.status_code == 200, filtered.text

    scoped = client.get("/retention?max_day=0")
    assert scoped.status_code == 200, scoped.text
    scoped_size = {row["cohort_name"]: row for row in scoped.json()["retention_table"]}["All Users"]["size"]
    assert scoped_size == 2


def test_date_range_includes_end_of_day_events(client: TestClient, db_connection) -> None:
    _prepare_scoped_fixture(client)

    db_connection.execute(
        """
        INSERT INTO events_normalized (user_id, event_name, event_time, country, device, campaign_id)
        VALUES ('u9', 'open', '2026-01-31 18:00:00', 'US', 'Android', 'c9')
        """
    )

    reset_scope = client.post('/apply-filters', json={'date_range': None, 'filters': []})
    assert reset_scope.status_code == 200, reset_scope.text

    response = client.post(
        '/apply-filters',
        json={
            'date_range': {'start': '2026-01-01', 'end': '2026-01-31'},
            'filters': [],
        },
    )
    assert response.status_code == 200, response.text

    included_count = db_connection.execute(
        """
        SELECT COUNT(*)
        FROM events_scoped
        WHERE user_id = 'u9' AND event_time = '2026-01-31 18:00:00'::TIMESTAMP
        """
    ).fetchone()[0]
    assert included_count == 1


def test_invalid_date_range_returns_400(client: TestClient) -> None:
    _prepare_scoped_fixture(client)

    response = client.post(
        '/apply-filters',
        json={
            'date_range': {'start': '2026-02-01', 'end': '2026-01-01'},
            'filters': [],
        },
    )

    assert response.status_code == 400
    assert response.json()['detail'] == 'Invalid date range: start must be before or equal to end'


def test_operator_type_validation_rejects_text_comparison_operator(client: TestClient) -> None:
    _prepare_scoped_fixture(client)

    response = client.post(
        '/apply-filters',
        json={
            'date_range': None,
            'filters': [{'column': 'country', 'operator': '>', 'value': 'US'}],
        },
    )

    assert response.status_code == 400
    assert response.json()['detail'] == "Operator '>' not allowed for column type TEXT"


def test_retention_overlay_handles_same_timestamp_different_events_without_double_count(client: TestClient) -> None:
    csv_text = (
        'user_id,event_name,event_time,channel\n'
        'u1,signup,2026-01-01 09:00:00,paid\n'
        'u1,open,2026-01-02 10:00:00,paid\n'
        'u1,purchase,2026-01-02 10:00:00,organic\n'
    )
    upload = csv_upload(client, csv_text=csv_text)
    assert upload.status_code == 200, upload.text

    mapped = client.post(
        '/map-columns',
        json={
            'user_id_column': 'user_id',
            'event_name_column': 'event_name',
            'event_time_column': 'event_time',
        },
    )
    assert mapped.status_code == 200, mapped.text

    cohort = client.post(
        '/cohorts',
        json={'name': 'signup_once', 'logic_operator': 'AND', 'conditions': [{'event_name': 'signup', 'min_event_count': 1}]},
    )
    assert cohort.status_code == 200, cohort.text

    response = client.get('/retention?max_day=2')
    assert response.status_code == 200, response.text

    row = {entry['cohort_name']: entry for entry in response.json()['retention_table']}['signup_once']
    assert row['size'] == 1
    assert row['retention']['1'] == 100.0

    # If overlay join duplicated same-timestamp records ambiguously, this day can inflate under bad joins.
    assert all(value <= 100.0 for value in row['retention'].values())
