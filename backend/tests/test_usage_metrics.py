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


def test_usage_returns_raw_count_tables_and_retained_users_relative_window(client: TestClient) -> None:
    # Uses 24-hour relative window (not calendar day)
    # u1 joins at 09:00. 
    # Event at 10:00, 11:00 (D0)
    # Event at 10:00 next day (+25h) -> D1
    _prepare_usage_fixture(client)

    response = client.get("/usage?event=open&max_day=1")
    assert response.status_code == 200, response.text

    payload = response.json()
    assert payload["retention_event"] == "any"
    # D0: u1 (2 events), u2 (1 event) -> Total 3
    # D1: u1 (1 event) -> Total 1
    assert _all_users_values(payload, "usage_volume_table") == {"0": 3, "1": 1}
    # Users D0: u1, u2 -> 2
    # Users D1: u1 -> 1
    assert _all_users_values(payload, "usage_users_table") == {"0": 2, "1": 1}
    # Adoption: u1, u2 at D0 (2), u1 already counted at D0 so Adoption at D1 remains 2
    # Wait, u1 first event is at 10:00 (D0). u2 first event is at 13:00 (D0).
    # So by end of D0, 2 users adopted.
    # By end of D1, still 2 users adopted.
    assert _all_users_values(payload, "usage_adoption_table") == {"0": 2, "1": 2}
    # Retained users (Denominator based)
    # u1, u2, u3 joined. u1, u2 active on D0 (3)
    # u1 active on D1 (1)
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
    # u1 join 09:00. Event 10:00 (D0), Event 10:00 next day (D1).
    # u2 join 12:00. Event 13:00 next day (D1). (+25h)
    # u3 join 15:00. Event 11:00 Day 3. Jan 1 15:00 -> Jan 3 11:00 is (24+20) = 44h -> Day 1!
    # Wait: Jan 1 15:00 -> Jan 2 15:00 (24h), Jan 3 15:00 (48h). 
    # Jan 3 11:00 is between 24h and 48h -> Day 1.
    
    # Let's re-verify:
    # u1: D0, D1
    # u2: D1
    # u3: D1
    
    # usage_users_table:
    # D0: u1 (1)
    # D1: u1, u2, u3 (3)
    # D2: 0
    # D3: 0
    assert _all_users_values(payload, "usage_users_table") == {"0": 1, "1": 3, "2": 0, "3": 0}
    # usage_adoption_table:
    # D0: u1 (1)
    # D1: u1, u2, u3 (3)
    # D2: 3
    # D3: 3
    assert _all_users_values(payload, "usage_adoption_table") == {"0": 1, "1": 3, "2": 3, "3": 3}




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
    # u1: D0, D1
    # u2: D1
    # usage_users_table:
    # D0: u1 (1)
    # D1: u1, u2 (2)
    # D2: 0
    assert _all_users_values(payload, "usage_users_table") == {"0": 1, "1": 2, "2": 0}
    # usage_adoption_table:
    # D0: 1
    # D1: 2
    # D2: 2
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
