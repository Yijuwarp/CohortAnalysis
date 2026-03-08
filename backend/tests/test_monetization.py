from __future__ import annotations

from fastapi.testclient import TestClient

from tests.utils import csv_upload


def _prepare_monetization_fixture(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time,revenue,region\n"
        "u1,signup,2026-01-01 09:00:00,0,NA\n"
        "u1,purchase,2026-01-01 10:00:00,10.50,NA\n"
        "u1,refund,2026-01-02 10:00:00,-2.25,NA\n"
        "u2,signup,2026-01-01 09:30:00,0,EU\n"
        "u2,purchase,2026-01-02 08:00:00,5.00,EU\n"
    )
    assert csv_upload(client, csv_text=csv_text).status_code == 200

    mapped = client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
            "revenue_column": "revenue",
            "column_types": {
                "user_id": "TEXT",
                "event_name": "TEXT",
                "event_time": "TIMESTAMP",
                "revenue": "NUMERIC",
                "region": "TEXT",
            },
        },
    )
    assert mapped.status_code == 200, mapped.text


def test_revenue_events_include_only_non_zero_revenue_events_by_default(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time,revenue\n"
        "u1,signup,2026-01-01 09:00:00,0\n"
        "u1,purchase,2026-01-01 10:00:00,10.50\n"
        "u1,refund,2026-01-02 10:00:00,-2.25\n"
        "u2,session_start,2026-01-01 09:45:00,0\n"
    )
    assert csv_upload(client, csv_text=csv_text).status_code == 200

    mapped = client.post(
        '/map-columns',
        json={
            'user_id_column': 'user_id',
            'event_name_column': 'event_name',
            'event_time_column': 'event_time',
            'revenue_column': 'revenue',
            'column_types': {
                'user_id': 'TEXT',
                'event_name': 'TEXT',
                'event_time': 'TIMESTAMP',
                'revenue': 'NUMERIC',
            },
        },
    )
    assert mapped.status_code == 200, mapped.text

    response = client.get('/revenue-events')
    assert response.status_code == 200, response.text
    events = {row['event_name'] for row in response.json()['events']}

    assert 'purchase' in events
    assert 'refund' in events
    assert 'signup' not in events
    assert 'session_start' not in events




def test_revenue_config_events_show_only_configured_revenue_events_and_return_addable_events(client: TestClient) -> None:
    _prepare_monetization_fixture(client)

    response = client.get('/revenue-config-events')
    assert response.status_code == 200, response.text
    payload = response.json()

    assert payload['has_revenue_mapping'] is True
    config_by_event = {event['event_name']: event for event in payload['events']}
    assert set(config_by_event) == {'purchase', 'refund'}
    assert config_by_event['purchase']['included'] is True
    assert config_by_event['refund']['included'] is True
    assert payload['addable_events'] == ['signup']

def test_revenue_events_default_to_included_and_can_be_toggled(client: TestClient) -> None:
    _prepare_monetization_fixture(client)

    response = client.get('/revenue-events')
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload['has_revenue_mapping'] is True
    events = {row['event_name']: row['is_included'] for row in payload['events']}
    assert events['purchase'] is True

    update = client.post('/update-revenue-config', json={'revenue_config': {'purchase': {'included': False, 'override': None}}})
    assert update.status_code == 200, update.text
    purchase = next(event for event in update.json()['events'] if event['event_name'] == 'purchase')
    assert purchase['included'] is False


def test_monetization_returns_daily_raw_values_and_day_buckets(client: TestClient) -> None:
    _prepare_monetization_fixture(client)

    response = client.get('/monetization?max_day=1')
    assert response.status_code == 200, response.text
    payload = response.json()

    all_users = [r for r in payload['revenue_table'] if r['cohort_name'] == 'All Users']
    by_day = {row['day_number']: row['revenue'] for row in all_users}
    assert by_day == {0: 10.5, 1: 2.75}


def test_monetization_respects_event_selection_and_scope(client: TestClient) -> None:
    _prepare_monetization_fixture(client)

    update = client.post('/update-revenue-config', json={'revenue_config': {'purchase': {'included': True, 'override': None}, 'refund': {'included': False, 'override': None}}})
    assert update.status_code == 200, update.text

    scoped = client.post('/apply-filters', json={'filters': [{'column': 'region', 'operator': '=', 'value': 'EU'}]})
    assert scoped.status_code == 200, scoped.text

    response = client.get('/monetization?max_day=1')
    assert response.status_code == 200, response.text
    payload = response.json()

    all_users = [r for r in payload['revenue_table'] if r['cohort_name'] == 'All Users']
    by_day = {row['day_number']: row['revenue'] for row in all_users}
    assert by_day == {1: 5.0}


def test_monetization_negative_revenue_included(client: TestClient) -> None:
    _prepare_monetization_fixture(client)
    response = client.get('/monetization?max_day=1')
    payload = response.json()
    day_one = next(row for row in payload['revenue_table'] if row['cohort_name'] == 'All Users' and row['day_number'] == 1)
    assert day_one['revenue'] == 2.75


def test_monetization_excludes_hidden_cohorts(client: TestClient) -> None:
    _prepare_monetization_fixture(client)

    created = client.post(
        "/cohorts",
        json={"name": "signup_users", "logic_operator": "AND", "conditions": [{"event_name": "signup", "min_event_count": 1}]},
    )
    assert created.status_code == 200, created.text

    hidden = client.patch(f"/cohorts/{created.json()['cohort_id']}/hide")
    assert hidden.status_code == 200, hidden.text

    response = client.get('/monetization?max_day=1')
    assert response.status_code == 200, response.text

    cohort_names = {row['cohort_name'] for row in response.json()['cohort_sizes']}
    assert 'signup_users' not in cohort_names


def test_update_revenue_config_override_clear_and_reapply(client: TestClient) -> None:
    _prepare_monetization_fixture(client)

    override = client.post('/update-revenue-config', json={
        'revenue_config': {
            'purchase': {'included': True, 'override': 10},
            'refund': {'included': True, 'override': None},
        },
    })
    assert override.status_code == 200, override.text

    payload = client.get('/monetization?max_day=1').json()
    all_users = [r for r in payload['revenue_table'] if r['cohort_name'] == 'All Users']
    by_day = {row['day_number']: row['revenue'] for row in all_users}
    assert by_day == {0: 10.0, 1: 7.75}

    cleared = client.post('/update-revenue-config', json={
        'revenue_config': {
            'purchase': {'included': True, 'override': None},
            'refund': {'included': True, 'override': None},
        },
    })
    assert cleared.status_code == 200, cleared.text

    payload = client.get('/monetization?max_day=1').json()
    all_users = [r for r in payload['revenue_table'] if r['cohort_name'] == 'All Users']
    by_day = {row['day_number']: row['revenue'] for row in all_users}
    assert by_day == {0: 10.5, 1: 2.75}

    override_again = client.post('/update-revenue-config', json={
        'revenue_config': {
            'purchase': {'included': True, 'override': 5},
            'refund': {'included': True, 'override': None},
        },
    })
    assert override_again.status_code == 200, override_again.text

    payload = client.get('/monetization?max_day=1').json()
    all_users = [r for r in payload['revenue_table'] if r['cohort_name'] == 'All Users']
    by_day = {row['day_number']: row['revenue'] for row in all_users}
    assert by_day == {0: 5.0, 1: 2.75}


def test_partial_revenue_config_payload_does_not_reset_other_events(client: TestClient) -> None:
    _prepare_monetization_fixture(client)

    response = client.post('/update-revenue-config', json={
        'revenue_config': {
            'purchase': {'included': True, 'override': 10},
        },
    })
    assert response.status_code == 200, response.text

    config_by_event = {event['event_name']: event for event in response.json()['events']}
    assert config_by_event['purchase']['override'] == 10
    assert config_by_event['refund']['included'] is True

    payload = client.get('/monetization?max_day=1').json()
    all_users = [r for r in payload['revenue_table'] if r['cohort_name'] == 'All Users']
    by_day = {row['day_number']: row['revenue'] for row in all_users}
    assert by_day == {0: 10.0, 1: 7.75}


def test_revenue_config_events_returns_persisted_included_and_override_state(client: TestClient) -> None:
    _prepare_monetization_fixture(client)

    update = client.post('/update-revenue-config', json={
        'revenue_config': {
            'purchase': {'included': True, 'override': 12},
            'refund': {'included': False, 'override': None},
        },
    })
    assert update.status_code == 200, update.text

    response = client.get('/revenue-config-events')
    assert response.status_code == 200, response.text
    config_by_event = {event['event_name']: event for event in response.json()['events']}

    assert config_by_event['purchase']['included'] is True
    assert config_by_event['purchase']['override'] == 12
    assert config_by_event['refund']['included'] is False
    assert config_by_event['refund']['override'] is None
    assert response.json()['addable_events'] == ['signup']


def test_update_revenue_config_rejects_empty_payload(client: TestClient) -> None:
    _prepare_monetization_fixture(client)

    response = client.post('/update-revenue-config', json={'revenue_config': {}})
    assert response.status_code == 400
    assert response.json()['detail'] == 'revenue_config cannot be empty'


def test_revenue_override_handles_large_values(client: TestClient) -> None:
    csv_text = (
        "uid,event,timestamp,count\n"
        "u1,search,2024-01-01 10:00:00,3\n"
    )

    assert csv_upload(client, csv_text=csv_text).status_code == 200

    client.post(
        "/map-columns",
        json={
            "user_id_column": "uid",
            "event_name_column": "event",
            "event_time_column": "timestamp",
            "event_count_column": "count",
            "column_types": {
                "uid": "TEXT",
                "event": "TEXT",
                "timestamp": "TIMESTAMP",
                "count": "NUMERIC",
            },
        },
    )

    response = client.post(
        "/update-revenue-config",
        json={
            "events": [
                {"event_name": "search", "include": True, "override": 5}
            ]
        },
    )

    assert response.status_code == 200


def test_revenue_override_handles_high_override_values(client: TestClient) -> None:
    csv_text = (
        "uid,event,timestamp,count\n"
        "u1,search,2024-01-01 10:00:00,2\n"
    )

    assert csv_upload(client, csv_text=csv_text).status_code == 200

    client.post(
        "/map-columns",
        json={
            "user_id_column": "uid",
            "event_name_column": "event",
            "event_time_column": "timestamp",
            "event_count_column": "count",
            "column_types": {
                "uid": "TEXT",
                "event": "TEXT",
                "timestamp": "TIMESTAMP",
                "count": "NUMERIC",
            },
        },
    )

    response = client.post(
        "/update-revenue-config",
        json={
            "events": [
                {"event_name": "search", "include": True, "override": 100}
            ]
        },
    )

    assert response.status_code == 200
