from __future__ import annotations

from fastapi.testclient import TestClient

from tests.utils import csv_upload


def _prepare_usage_fixture(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,signup,2026-01-01 09:00:00\n"
        "u1,open,2026-01-01 10:00:00\n"
        "u1,open,2026-01-01 11:00:00\n"
        "u1,open,2026-01-02 10:00:00\n"
        "u2,signup,2026-01-01 12:00:00\n"
        "u2,open,2026-01-01 13:00:00\n"
        "u2,purchase,2026-01-01 14:00:00\n"
        "u3,signup,2026-01-01 15:00:00\n"
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


def test_usage_returns_raw_count_tables_and_retained_users(client: TestClient) -> None:
    _prepare_usage_fixture(client)

    response = client.get("/usage?event=open&max_day=1")
    assert response.status_code == 200, response.text

    payload = response.json()
    assert payload["retention_event"] == "any"
    assert _all_users_values(payload, "usage_volume_table") == {"0": 3, "1": 1}
    assert _all_users_values(payload, "usage_users_table") == {"0": 2, "1": 1}
    assert _all_users_values(payload, "usage_adoption_table") == {"0": 2, "1": 2}
    assert _all_users_values(payload, "retained_users_table") == {"0": 3, "1": 1}


def test_usage_retained_users_table_honors_selected_retention_event(client: TestClient) -> None:
    _prepare_usage_fixture(client)

    response = client.get("/usage?event=open&max_day=1&retention_event=purchase")
    assert response.status_code == 200, response.text

    payload = response.json()
    assert payload["retention_event"] == "purchase"
    assert _all_users_values(payload, "retained_users_table") == {"0": 1, "1": 0}


def test_usage_returns_empty_tables_when_event_absent(client: TestClient) -> None:
    _prepare_usage_fixture(client)

    response = client.get("/usage?event=nonexistent&max_day=1&retention_event=open")
    assert response.status_code == 200, response.text

    payload = response.json()
    assert payload["usage_volume_table"] == []
    assert payload["usage_users_table"] == []
    assert payload["usage_adoption_table"] == []
    assert payload["retained_users_table"] == []


def test_usage_adoption_counts_distinct_users_cumulatively(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,signup,2026-01-01 09:00:00\n"
        "u1,open,2026-01-01 10:00:00\n"
        "u1,open,2026-01-02 10:00:00\n"
        "u2,signup,2026-01-01 12:00:00\n"
        "u2,open,2026-01-02 13:00:00\n"
        "u3,signup,2026-01-01 15:00:00\n"
        "u3,open,2026-01-03 11:00:00\n"
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

    response = client.get("/usage?event=open&max_day=3")
    assert response.status_code == 200, response.text

    payload = response.json()
    assert _all_users_values(payload, "usage_users_table") == {"0": 1, "1": 2, "2": 1, "3": 0}
    assert _all_users_values(payload, "usage_adoption_table") == {"0": 1, "1": 2, "2": 3, "3": 3}




def test_usage_adoption_with_aggregated_rows_counts_distinct_users(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time,event_count\n"
        "u1,signup,2026-01-01 09:00:00,1\n"
        "u1,open,2026-01-01 10:00:00,5\n"
        "u1,open,2026-01-02 10:00:00,3\n"
        "u2,signup,2026-01-01 12:00:00,1\n"
        "u2,open,2026-01-02 13:00:00,7\n"
        "u3,signup,2026-01-01 15:00:00,1\n"
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

    response = client.get("/usage?event=open&max_day=2")
    assert response.status_code == 200, response.text

    payload = response.json()
    assert _all_users_values(payload, "usage_users_table") == {"0": 1, "1": 2, "2": 0}
    assert _all_users_values(payload, "usage_adoption_table") == {"0": 1, "1": 2, "2": 2}


def test_usage_volume_with_aggregated_rows_uses_event_count_not_row_count_or_overrides(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time,event_count\n"
        "u1,open,2026-01-01 10:00:00,5\n"
        "u1,open,2026-01-02 10:00:00,3\n"
        "u2,open,2026-01-01 12:00:00,2\n"
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

    override = client.post(
        "/update-revenue-config",
        json={"revenue_config": {"open": {"included": True, "override": 100}}},
    )
    assert override.status_code == 200, override.text

    response = client.get("/usage?event=open&max_day=1")
    assert response.status_code == 200, response.text

    payload = response.json()
    assert _all_users_values(payload, "usage_volume_table") == {"0": 7, "1": 3}

def test_usage_excludes_hidden_cohorts(client: TestClient) -> None:
    _prepare_usage_fixture(client)

    created = client.post(
        "/cohorts",
        json={"name": "signup_users", "logic_operator": "AND", "conditions": [{"event_name": "signup", "min_event_count": 1}]},
    )
    assert created.status_code == 200, created.text

    hidden = client.patch(f"/cohorts/{created.json()['cohort_id']}/hide")
    assert hidden.status_code == 200, hidden.text

    response = client.get("/usage?event=open&max_day=1")
    assert response.status_code == 200, response.text

    cohort_names = {row["cohort_name"] for row in response.json()["usage_volume_table"]}
    assert "signup_users" not in cohort_names
