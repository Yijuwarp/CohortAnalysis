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


def test_cohort_membership_recomputed_under_scope(client: TestClient, db_connection) -> None:
    csv_text = (
        'user_id,event_name,event_time,country\n'
        'u1,search,2026-01-01 09:00:00,US\n'
        'u1,open,2026-01-01 10:00:00,US\n'
        'u2,search,2026-01-02 09:00:00,CA\n'
        'u2,open,2026-01-02 10:00:00,CA\n'
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
        json={
            'name': 'searchers',
            'logic_operator': 'AND',
            'conditions': [{'event_name': 'search', 'min_event_count': 1}],
        },
    )
    assert cohort.status_code == 200, cohort.text

    filtered = client.post(
        '/apply-filters',
        json={
            'date_range': None,
            'filters': [{'column': 'event_name', 'operator': '!=', 'value': 'search'}],
        },
    )
    assert filtered.status_code == 200, filtered.text

    scoped_members = db_connection.execute(
        '''
        SELECT COUNT(*)
        FROM cohort_membership cm
        JOIN cohorts c ON c.cohort_id = cm.cohort_id
        WHERE c.name = 'searchers'
        '''
    ).fetchone()[0]
    assert scoped_members == 0

    cohort_state = db_connection.execute(
        "SELECT is_active FROM cohorts WHERE name = 'searchers'"
    ).fetchone()[0]
    assert cohort_state is False

    retention = client.get('/retention?max_day=0')
    assert retention.status_code == 200, retention.text
    assert 'searchers' not in {row['cohort_name'] for row in retention.json()['retention_table']}


def test_all_users_membership_rebuilt_from_scoped_with_join_time_refresh(client: TestClient, db_connection) -> None:
    csv_text = (
        'user_id,event_name,event_time,country\n'
        'u1,open,2026-01-01 09:00:00,US\n'
        'u1,open,2026-01-05 09:00:00,US\n'
        'u2,open,2026-01-02 09:00:00,CA\n'
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

    filtered = client.post(
        '/apply-filters',
        json={
            'date_range': {'start': '2026-01-03', 'end': '2026-01-31'},
            'filters': [],
        },
    )
    assert filtered.status_code == 200, filtered.text

    all_users_membership = db_connection.execute(
        """
        SELECT cm.user_id, cm.join_time
        FROM cohort_membership cm
        JOIN cohorts c ON c.cohort_id = cm.cohort_id
        WHERE c.name = 'All Users'
        ORDER BY cm.user_id
        """
    ).fetchall()
    assert len(all_users_membership) == 1
    assert all_users_membership[0][0] == 'u1'
    assert all_users_membership[0][1].isoformat(sep=' ') == '2026-01-05 09:00:00'

    scoped_users = db_connection.execute('SELECT COUNT(DISTINCT user_id) FROM events_scoped').fetchone()[0]
    assert scoped_users == 1

    retention = client.get('/retention?max_day=0')
    assert retention.status_code == 200, retention.text
    all_users_row = next(row for row in retention.json()['retention_table'] if row['cohort_name'] == 'All Users')
    assert all_users_row['size'] == scoped_users


def test_all_users_empty_scope_becomes_inactive_and_hidden_in_retention(client: TestClient, db_connection) -> None:
    _prepare_scoped_fixture(client)

    filtered = client.post(
        '/apply-filters',
        json={
            'date_range': None,
            'filters': [{'column': 'event_name', 'operator': '=', 'value': '__no_such_event__'}],
        },
    )
    assert filtered.status_code == 200, filtered.text

    scoped_rows = db_connection.execute('SELECT COUNT(*) FROM events_scoped').fetchone()[0]
    assert scoped_rows == 0

    all_users_state = db_connection.execute(
        "SELECT is_active FROM cohorts WHERE name = 'All Users'"
    ).fetchone()[0]
    assert all_users_state is False

    all_users_members = db_connection.execute(
        """
        SELECT COUNT(*)
        FROM cohort_membership cm
        JOIN cohorts c ON c.cohort_id = cm.cohort_id
        WHERE c.name = 'All Users'
        """
    ).fetchone()[0]
    assert all_users_members == 0

    retention = client.get('/retention?max_day=0')
    assert retention.status_code == 200, retention.text
    assert 'All Users' not in {row['cohort_name'] for row in retention.json()['retention_table']}


def test_date_range_includes_end_of_day_events(client: TestClient, db_connection) -> None:
    _prepare_scoped_fixture(client)

    db_connection.execute(
        """
        INSERT INTO events_normalized (user_id, event_name, event_time, original_event_count, modified_event_count, original_revenue, modified_revenue, country, device, campaign_id)
        VALUES ('u9', 'open', '2026-01-31 18:00:00', 1, 1, 0, 0, 'US', 'Android', 'c9')
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


def test_columns_includes_data_type(client: TestClient) -> None:
    _prepare_scoped_fixture(client)

    response = client.get('/columns')
    assert response.status_code == 200, response.text

    columns = response.json()['columns']
    event_time = next((column for column in columns if column['name'] == 'event_time'), None)
    assert event_time is not None
    assert str(event_time['data_type']).startswith('TIMESTAMP')


def test_column_values_returns_distinct_values(client: TestClient) -> None:
    _prepare_scoped_fixture(client)

    response = client.get('/column-values?column=country')
    assert response.status_code == 200, response.text

    payload = response.json()
    assert payload['values'] == ['CA', 'US']
    assert payload['total_distinct'] == 2


def test_column_values_scoped_by_event_name(client: TestClient) -> None:
    csv_text = (
        'user_id,event_name,event_time,version\n'
        'u1,search,2026-01-01 09:00:00,1.0\n'
        'u2,search,2026-01-01 10:00:00,1.1\n'
        'u3,signup,2026-01-01 11:00:00,2.0\n'
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

    response = client.get('/column-values?column=version&event_name=search')
    assert response.status_code == 200, response.text

    payload = response.json()
    assert payload['values'] == ['1.0', '1.1']
    assert payload['total_distinct'] == 2


def test_column_values_global_fallback_without_event_name(client: TestClient) -> None:
    csv_text = (
        'user_id,event_name,event_time,version\n'
        'u1,search,2026-01-01 09:00:00,1.0\n'
        'u2,search,2026-01-01 10:00:00,1.1\n'
        'u3,signup,2026-01-01 11:00:00,2.0\n'
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

    response = client.get('/column-values?column=version')
    assert response.status_code == 200, response.text

    payload = response.json()
    assert payload['values'] == ['1.0', '1.1', '2.0']
    assert payload['total_distinct'] == 3


def test_column_values_global_validation_uses_events_normalized_when_scoped_table_missing(
    client: TestClient,
    db_connection,
) -> None:
    csv_text = (
        'user_id,event_name,event_time,version\n'
        'u1,search,2026-01-01 09:00:00,1.0\n'
        'u2,signup,2026-01-01 10:00:00,2.0\n'
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

    db_connection.execute('DROP VIEW IF EXISTS events_scoped')

    response = client.get('/column-values?column=version')
    assert response.status_code == 200, response.text

    payload = response.json()
    assert payload['values'] == ['1.0', '2.0']
    assert payload['total_distinct'] == 2


def test_column_values_respects_100_limit(client: TestClient, db_connection) -> None:
    _prepare_scoped_fixture(client)

    rows = [
        (f'u_extra_{index}', 'open', f'2026-02-10 12:{index % 60:02d}:00', 1, 1, 0, 0, f'code_{index}', 'Web', f'cx_{index}')
        for index in range(150)
    ]
    db_connection.executemany(
        """
        INSERT INTO events_normalized (user_id, event_name, event_time, original_event_count, modified_event_count, original_revenue, modified_revenue, country, device, campaign_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

    reset_scope = client.post('/apply-filters', json={'date_range': None, 'filters': []})
    assert reset_scope.status_code == 200, reset_scope.text

    response = client.get('/column-values?column=country')
    assert response.status_code == 200, response.text

    payload = response.json()
    assert len(payload['values']) == 100
    assert payload['total_distinct'] == 152


def test_column_values_not_affected_by_scope(client: TestClient) -> None:
    _prepare_scoped_fixture(client)

    filtered = client.post(
        '/apply-filters',
        json={'date_range': None, 'filters': [{'column': 'country', 'operator': '=', 'value': 'US'}]},
    )
    assert filtered.status_code == 200, filtered.text

    response = client.get('/column-values?column=country')
    assert response.status_code == 200, response.text

    payload = response.json()
    assert set(payload['values']) == {'CA', 'US'}
    assert payload['total_distinct'] == 2


def test_date_range_returns_min_max(client: TestClient) -> None:
    _prepare_scoped_fixture(client)

    response = client.get('/date-range')
    assert response.status_code == 200, response.text

    payload = response.json()
    assert payload == {
        'min_date': '2026-01-01',
        'max_date': '2026-02-02',
    }
