from __future__ import annotations

from tests.utils import csv_upload


def _prepare_fixture(client):
    csv_text = (
        "user_id,event_name,event_time,country,plan,event_count\n"
        "u1,signup,2026-01-01 09:00:00,US,free,1\n"
        "u1,open,2026-01-01 10:00:00,US,free,2\n"
        "u1,purchase,2026-01-01 11:00:00,US,pro,3\n"
        "u1,open,2026-01-02 10:00:00,CA,pro,4\n"
        "u2,signup,2026-01-01 09:30:00,US,free,1\n"
    )
    upload = csv_upload(client, csv_text=csv_text)
    assert upload.status_code == 200, upload.text

    mapped = client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
            "event_count_column": "event_count",
        },
    )
    assert mapped.status_code == 200, mapped.text


def test_users_search_returns_scoped_users(client):
    _prepare_fixture(client)

    response = client.get('/users/search?query=u&limit=20')
    assert response.status_code == 200, response.text

    assert response.json() == [{"user_id": "u1"}, {"user_id": "u2"}]


def test_user_explorer_returns_summary_timeline_and_navigation(client):
    _prepare_fixture(client)

    created = client.post(
        "/cohorts",
        json={"name": "Purchasers", "logic_operator": "AND", "conditions": [{"event_name": "purchase", "min_event_count": 1}]},
    )
    assert created.status_code == 200, created.text

    response = client.get('/user-explorer?user_id=u1&page=1&page_size=2')
    assert response.status_code == 200, response.text

    payload = response.json()
    assert payload['summary']['first_event_time'].startswith('2026-01-01T09:00:00')
    assert payload['summary']['last_event_time'].startswith('2026-01-02T10:00:00')
    assert payload['summary']['total_events'] == 10
    assert payload['summary']['properties']['country'] == 'CA'
    assert payload['pagination'] == {'page': 1, 'total_pages': 2, 'total_events': 4}
    assert [row['event_name'] for row in payload['events']] == ['signup', 'open']

    nav_next = client.get(
        '/user-explorer?user_id=u1&page_size=2&event_search=open&direction=next&from_event_time=2026-01-01T10:00:00'
    )
    assert nav_next.status_code == 200, nav_next.text
    next_payload = nav_next.json()
    assert next_payload['pagination']['page'] == 2
    assert next_payload['cursor']['current_event_time'].startswith('2026-01-02T10:00:00')


def test_user_explorer_jump_datetime_and_cohort_join_tagging(client):
    _prepare_fixture(client)
    created = client.post(
        "/cohorts",
        json={"name": "Purchasers", "logic_operator": "AND", "conditions": [{"event_name": "purchase", "min_event_count": 1}]},
    )
    assert created.status_code == 200, created.text

    jumped = client.get('/user-explorer?user_id=u1&page_size=10&jump_datetime=2026-01-01')
    assert jumped.status_code == 200, jumped.text
    payload = jumped.json()
    tagged = [event for event in payload['events'] if event['event_name'] == 'purchase']
    assert tagged
    assert tagged[0]['cohort_joins'] == ['Purchasers']
