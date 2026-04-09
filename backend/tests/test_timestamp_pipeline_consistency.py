from fastapi.testclient import TestClient

import io


def test_scope_and_cohort_timestamp_logic_consistent_for_on(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time,country\n"
        "u1,open,2026-01-01 01:00:00,US\n"
        "u2,open,2026-01-01 12:00:00,US\n"
        "u3,open,2026-01-02 12:00:00,CA\n"
    )
    assert client.post(
        "/upload?user_id=abcdef12",
        files={"file": ("events.csv", io.BytesIO(csv_text.encode("utf-8")), "text/csv")},
    ).status_code == 200
    assert client.post('/map-columns?user_id=abcdef12', json={
        'user_id_column': 'user_id',
        'event_name_column': 'event_name',
        'event_time_column': 'event_time',
    }).status_code == 200

    assert client.post('/apply-filters?user_id=abcdef12', json={
        'date_range': None,
        'filters': [{'column': 'event_time', 'operator': 'on', 'value': {'date': '2026-01-01'}}],
    }).status_code == 200
    scoped_users = client.get('/scope?user_id=abcdef12').json()['filtered_rows']

    resp = client.post('/cohorts?user_id=abcdef12', json={
        'name': 'open-jan1',
        'logic_operator': 'AND',
        'conditions': [
            {
                'event_name': 'open',
                'min_event_count': 1,
                'property_filter': {
                    'column': 'event_time',
                    'operator': 'on',
                    'values': {'date': '2026-01-01'},
                },
            }
        ],
    })
    assert resp.status_code == 200, resp.text

    cohort_users = resp.json()['users_joined']
    assert cohort_users == scoped_users == 2
