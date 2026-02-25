from __future__ import annotations

from fastapi.testclient import TestClient

from tests.utils import csv_upload


def _prepare_events(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time,channel\n"
        "u1,signup,2024-01-01 09:00:00,ads\n"
        "u1,open,2024-01-02 09:00:00,ads\n"
        "u1,purchase,2024-01-03 09:00:00,email\n"
        "u1,open,2024-01-04 09:00:00,email\n"
        "u1,legacy,2023-12-31 09:00:00,organic\n"
        "u2,signup,2024-01-01 10:00:00,ads\n"
        "u2,open,2024-01-03 10:00:00,ads\n"
        "u3,purchase,2024-01-02 12:00:00,organic\n"
        "u3,open,2024-01-03 12:00:00,organic\n"
    )
    upload = csv_upload(client, csv_text=csv_text)
    assert upload.status_code == 200, f"Precondition failed: upload returned {upload.text}"

    mapping = client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
        },
    )
    assert mapping.status_code == 200, f"Precondition failed: map-columns returned {mapping.text}"


def test_retention_basic_case_has_day_zero_and_expected_drop(client: TestClient) -> None:
    _prepare_events(client)

    cohort = client.post(
        "/cohorts",
        json={"name": "signup_once", "event_name": "signup", "min_event_count": 1},
    )
    assert cohort.status_code == 200, cohort.text

    response = client.get("/retention")
    assert response.status_code == 200, response.text

    payload = response.json()
    assert payload["max_day"] == 7
    assert len(payload["retention_table"]) == 1

    row = payload["retention_table"][0]
    assert row["cohort_name"] == "signup_once"
    assert row["size"] == 2
    assert row["retention"]["0"] == 100.0
    assert row["retention"]["1"] == 50.0
    assert row["retention"]["2"] == 100.0
    assert row["retention"]["3"] == 50.0


def test_retention_handles_multiple_cohorts(client: TestClient) -> None:
    _prepare_events(client)

    signup = client.post(
        "/cohorts",
        json={"name": "signup_once", "event_name": "signup", "min_event_count": 1},
    )
    assert signup.status_code == 200, signup.text

    purchase = client.post(
        "/cohorts",
        json={"name": "purchase_once", "event_name": "purchase", "min_event_count": 1},
    )
    assert purchase.status_code == 200, purchase.text

    response = client.get("/retention?max_day=2")
    assert response.status_code == 200, response.text

    table = response.json()["retention_table"]
    assert len(table) == 2

    by_name = {row["cohort_name"]: row for row in table}

    assert by_name["signup_once"]["size"] == 2
    assert by_name["signup_once"]["retention"] == {"0": 100.0, "1": 50.0, "2": 100.0}

    assert by_name["purchase_once"]["size"] == 2
    assert by_name["purchase_once"]["retention"] == {"0": 100.0, "1": 100.0, "2": 0.0}


def test_retention_respects_max_day_parameter(client: TestClient) -> None:
    _prepare_events(client)

    cohort = client.post(
        "/cohorts",
        json={"name": "signup_once", "event_name": "signup", "min_event_count": 1},
    )
    assert cohort.status_code == 200, cohort.text

    response = client.get("/retention?max_day=1")
    assert response.status_code == 200, response.text

    payload = response.json()
    row = payload["retention_table"][0]
    assert payload["max_day"] == 1
    assert row["retention"] == {"0": 100.0, "1": 50.0}


def test_retention_returns_empty_when_no_cohorts_exist(client: TestClient) -> None:
    response = client.get("/retention")

    assert response.status_code == 200, response.text
    assert response.json() == {"max_day": 7, "retention_table": []}
