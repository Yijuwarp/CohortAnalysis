from fastapi.testclient import TestClient

import io


def test_create_cohort_with_timestamp_on_property_filter(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time,country\n"
        "u1,purchase,2026-01-01 09:00:00,US\n"
        "u2,purchase,2026-01-02 09:00:00,CA\n"
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

    resp = client.post('/cohorts?user_id=abcdef12', json={
        'name': 'jan1-buyers',
        'logic_operator': 'AND',
        'conditions': [
            {
                'event_name': 'purchase',
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

    assert resp.json()["users_joined"] == 1
