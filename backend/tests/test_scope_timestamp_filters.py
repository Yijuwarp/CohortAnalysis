from fastapi.testclient import TestClient

import io


def test_apply_scope_timestamp_on_filter(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time,country\n"
        "u1,open,2026-01-01 09:00:00,US\n"
        "u2,open,2026-01-02 09:00:00,CA\n"
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

    response = client.post('/apply-filters?user_id=abcdef12', json={
        'date_range': None,
        'filters': [{'column': 'event_time', 'operator': 'on', 'value': {'date': '2026-01-01'}}],
    })
    assert response.status_code == 200, response.text

    assert response.json()["filtered_rows"] == 1
