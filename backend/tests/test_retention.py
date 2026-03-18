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
        json={"name": "signup_once", "logic_operator": "AND", "conditions": [{"event_name": "signup", "min_event_count": 1}]},
    )
    assert cohort.status_code == 200, cohort.text

    response = client.get("/retention")
    assert response.status_code == 200, response.text

    payload = response.json()
    assert payload["max_day"] == 4

    by_name = {entry["cohort_name"]: entry for entry in payload["retention_table"]}
    assert "All Users" in by_name
    all_users = by_name["All Users"]
    assert all_users["size"] == 3
    assert all_users["retention"]["0"] == 100.0

    row = by_name["signup_once"]
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
        json={"name": "signup_once", "logic_operator": "AND", "conditions": [{"event_name": "signup", "min_event_count": 1}]},
    )
    assert signup.status_code == 200, signup.text

    purchase = client.post(
        "/cohorts",
        json={"name": "purchase_once", "logic_operator": "AND", "conditions": [{"event_name": "purchase", "min_event_count": 1}]},
    )
    assert purchase.status_code == 200, purchase.text

    response = client.get("/retention?max_day=2")
    assert response.status_code == 200, response.text

    table = response.json()["retention_table"]

    by_name = {row["cohort_name"]: row for row in table}

    assert by_name["signup_once"]["size"] == 2
    assert by_name["signup_once"]["retention"] == {"0": 100.0, "1": 50.0, "2": 100.0}

    assert by_name["purchase_once"]["size"] == 2
    assert by_name["purchase_once"]["retention"] == {"0": 100.0, "1": 100.0, "2": 0.0}




def test_first_event_join_type_shifts_retention_anchor(client: TestClient) -> None:
    _prepare_events(client)

    cohort = client.post(
        "/cohorts",
        json={
            "name": "purchase_first_event",
            "logic_operator": "AND",
            "join_type": "first_event",
            "conditions": [{"event_name": "purchase", "min_event_count": 1}],
        },
    )
    assert cohort.status_code == 200, cohort.text

    response = client.get("/retention?max_day=3")
    assert response.status_code == 200, response.text

    by_name = {row["cohort_name"]: row for row in response.json()["retention_table"]}
    row = by_name["purchase_first_event"]
    assert row["size"] == 2
    assert row["retention"] == {"0": 100.0, "1": 100.0, "2": 50.0, "3": 50.0}

def test_retention_respects_max_day_parameter(client: TestClient) -> None:
    _prepare_events(client)

    cohort = client.post(
        "/cohorts",
        json={"name": "signup_once", "logic_operator": "AND", "conditions": [{"event_name": "signup", "min_event_count": 1}]},
    )
    assert cohort.status_code == 200, cohort.text

    response = client.get("/retention?max_day=1")
    assert response.status_code == 200, response.text

    payload = response.json()
    by_name = {row["cohort_name"]: row for row in payload["retention_table"]}
    row = by_name["signup_once"]
    assert payload["max_day"] == 1
    assert row["retention"] == {"0": 100.0, "1": 50.0}


def test_map_columns_creates_all_users_only_once(client: TestClient) -> None:
    _prepare_events(client)

    remap = client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
        },
    )
    assert remap.status_code == 200, remap.text

    response = client.get("/retention?max_day=1")
    assert response.status_code == 200, response.text

    all_users_rows = [
        row for row in response.json()["retention_table"] if row["cohort_name"] == "All Users"
    ]
    assert len(all_users_rows) == 1
    assert all_users_rows[0]["size"] == 3
    assert all_users_rows[0]["retention"]["0"] == 100.0


def test_retention_returns_empty_when_no_cohorts_exist(client: TestClient) -> None:
    response = client.get("/retention")

    assert response.status_code == 200, response.text
    assert response.json() == {
        "max_day": 7,
        "retention_event": "any",
        "retention_table": [],
        }


def test_retention_rebuilds_from_new_mapping_after_remap(client: TestClient) -> None:
    _prepare_events(client)

    cohort = client.post(
        "/cohorts",
        json={"name": "signup_once", "logic_operator": "AND", "conditions": [{"event_name": "signup", "min_event_count": 1}]},
    )
    assert cohort.status_code == 200, cohort.text

    baseline = client.get("/retention?max_day=2")
    assert baseline.status_code == 200, baseline.text
    baseline_row = {
        row["cohort_name"]: row for row in baseline.json()["retention_table"]
    }["signup_once"]
    assert baseline_row["retention"] == {"0": 100.0, "1": 50.0, "2": 100.0}

    replacement_csv = (
        "user_id,event_name,event_time\n"
        "z1,signup,2024-02-01 09:00:00\n"
        "z1,open,2024-02-02 09:00:00\n"
    )
    upload = csv_upload(client, csv_text=replacement_csv)
    assert upload.status_code == 200, upload.text

    remap = client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
        },
    )
    assert remap.status_code == 200, remap.text

    response = client.get("/retention?max_day=2")
    assert response.status_code == 200, response.text
    by_name = {row["cohort_name"]: row for row in response.json()["retention_table"]}
    assert list(by_name.keys()) == ["All Users"]
    assert by_name["All Users"]["size"] == 1


def test_retention_excludes_hidden_cohorts(client: TestClient) -> None:
    _prepare_events(client)

    created = client.post(
        "/cohorts",
        json={"name": "signup_once", "logic_operator": "AND", "conditions": [{"event_name": "signup", "min_event_count": 1}]},
    )
    assert created.status_code == 200, created.text

    hidden = client.patch(f"/cohorts/{created.json()['cohort_id']}/hide")
    assert hidden.status_code == 200, hidden.text

    response = client.get('/retention?max_day=1')
    assert response.status_code == 200, response.text

    cohort_names = {row['cohort_name'] for row in response.json()['retention_table']}
    assert 'signup_once' not in cohort_names
