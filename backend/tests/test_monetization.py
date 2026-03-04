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


def test_revenue_events_default_to_included_and_can_be_toggled(client: TestClient) -> None:
    _prepare_monetization_fixture(client)

    response = client.get('/revenue-events')
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload['has_revenue_mapping'] is True
    events = {row['event_name']: row['is_included'] for row in payload['events']}
    assert events['purchase'] is True

    update = client.put('/revenue-events', json={'events': [{'event_name': 'purchase', 'is_included': False}]})
    assert update.status_code == 200, update.text
    toggled = {row['event_name']: row['is_included'] for row in update.json()['events']}
    assert toggled['purchase'] is False


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

    update = client.put('/revenue-events', json={'events': [{'event_name': 'refund', 'is_included': False}]})
    assert update.status_code == 200, update.text

    scoped = client.post('/apply-filters', json={'filters': [{'column': 'region', 'operator': '=', 'value': 'EU'}]})
    assert scoped.status_code == 200, scoped.text

    response = client.get('/monetization?max_day=1')
    assert response.status_code == 200, response.text
    payload = response.json()

    all_users = [r for r in payload['revenue_table'] if r['cohort_name'] == 'All Users']
    assert all_users == [
        {'cohort_id': 1, 'cohort_name': 'All Users', 'day_number': 0, 'revenue': 0.0},
        {'cohort_id': 1, 'cohort_name': 'All Users', 'day_number': 1, 'revenue': 5.0},
    ]


def test_monetization_negative_revenue_included(client: TestClient) -> None:
    _prepare_monetization_fixture(client)
    response = client.get('/monetization?max_day=1')
    payload = response.json()
    day_one = next(row for row in payload['revenue_table'] if row['cohort_name'] == 'All Users' and row['day_number'] == 1)
    assert day_one['revenue'] == 2.75
