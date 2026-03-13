from __future__ import annotations

from fastapi.testclient import TestClient

from tests.utils import csv_upload


def _prepare_property_fixture(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time,source,device_type\n"
        "u1,open,2026-01-01 10:00:00,homescreen,phone\n"
        "u1,open,2026-01-02 10:00:00,homescreen,phone\n"
        "u2,open,2026-01-01 12:00:00,appdrawer,tablet\n"
        "u2,open,2026-01-02 12:00:00,appdrawer,tablet\n"
        "u3,open,2026-01-01 15:00:00,homescreen,tablet\n"
        "u4,signup,2026-01-01 09:00:00,ads,phone\n"
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


def _all_users_values(payload: dict[str, object], table_key: str) -> dict[str, int]:
    row = next(entry for entry in payload[table_key] if entry["cohort_name"] == "All Users")
    return row["values"]


def test_event_property_discovery_and_value_suggestions(client: TestClient) -> None:
    _prepare_property_fixture(client)

    properties = client.get("/events/open/properties")
    assert properties.status_code == 200, properties.text
    assert properties.json()["properties"] == ["source", "device_type"]

    values = client.get("/events/open/properties/source/values?limit=2")
    assert values.status_code == 200, values.text
    payload = values.json()
    assert payload["values"] == ["homescreen", "appdrawer"]
    assert payload["total_distinct"] == 2


def test_usage_tables_apply_property_filter(client: TestClient) -> None:
    _prepare_property_fixture(client)

    response = client.get("/usage?event=open&max_day=1&property=source&operator=%3D&value=homescreen")
    assert response.status_code == 200, response.text

    payload = response.json()
    assert _all_users_values(payload, "usage_volume_table") == {"0": 2, "1": 1}
    assert _all_users_values(payload, "usage_users_table") == {"0": 2, "1": 1}
    assert _all_users_values(payload, "usage_adoption_table") == {"0": 2, "1": 2}


def test_usage_frequency_applies_property_filter(client: TestClient) -> None:
    _prepare_property_fixture(client)

    response = client.get("/usage-frequency?event=open&property=source&operator=%3D&value=homescreen")
    assert response.status_code == 200, response.text

    buckets = {row["bucket"]: row["cohorts"][0]["users"] for row in response.json()["buckets"]}
    assert buckets["0"] == 2
    assert buckets["1"] == 1
    assert buckets["2-5"] == 1


def test_usage_filter_requires_value_when_property_selected(client: TestClient) -> None:
    _prepare_property_fixture(client)

    response = client.get("/usage?event=open&max_day=1&property=source&operator=%3D")
    assert response.status_code == 400, response.text
