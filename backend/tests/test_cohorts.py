from __future__ import annotations

from datetime import datetime

from fastapi.testclient import TestClient

from tests.utils import csv_upload


def _prepare_normalized_events(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time,channel\n"
        "u1,purchase,2024-01-01 09:00:00,ads\n"
        "u1,purchase,2024-01-03 09:00:00,email\n"
        "u2,purchase,2024-01-02 09:00:00,ads\n"
        "u2,signup,2024-01-05 09:00:00,organic\n"
        "u3,purchase,2024-01-04 09:00:00,organic\n"
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


def test_basic_cohort_creation_inserts_expected_users_and_join_times(
    client: TestClient,
    db_connection,
) -> None:
    _prepare_normalized_events(client)

    response = client.post(
        "/cohorts",
        json={"name": "purchase_once", "logic_operator": "AND", "conditions": [{"event_name": "purchase", "min_event_count": 1}]},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["users_joined"] == 3, "Three users have at least one purchase event"

    membership = db_connection.execute(
        """
        SELECT user_id, join_time
        FROM cohort_membership
        WHERE cohort_id = ?
        ORDER BY user_id
        """,
        [payload["cohort_id"]],
    ).fetchall()

    assert membership == [
        ("u1", datetime(2024, 1, 1, 9, 0, 0)),
        ("u2", datetime(2024, 1, 2, 9, 0, 0)),
        ("u3", datetime(2024, 1, 4, 9, 0, 0)),
    ], f"join_time should match each user's first qualifying event, got {membership}"


def test_nth_event_logic_uses_min_event_count_as_join_time(
    client: TestClient,
    db_connection,
) -> None:
    _prepare_normalized_events(client)

    response = client.post(
        "/cohorts",
        json={"name": "purchase_twice", "logic_operator": "AND", "conditions": [{"event_name": "purchase", "min_event_count": 2}]},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["users_joined"] == 1, "Only one user has a second purchase event"

    rows = db_connection.execute(
        "SELECT user_id, join_time FROM cohort_membership WHERE cohort_id = ?",
        [payload["cohort_id"]],
    ).fetchall()
    assert rows == [
        ("u1", datetime(2024, 1, 3, 9, 0, 0))
    ], f"The cohort join_time should be the second purchase timestamp, got {rows}"


def test_cohort_creation_with_no_matching_users_inserts_zero(client: TestClient, db_connection) -> None:
    _prepare_normalized_events(client)

    response = client.post(
        "/cohorts",
        json={"name": "no_users", "logic_operator": "AND", "conditions": [{"event_name": "refund", "min_event_count": 1}]},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["users_joined"] == 0, "No users should join when the event_name does not exist"

    count = db_connection.execute(
        "SELECT COUNT(*) FROM cohort_membership WHERE cohort_id = ?",
        [payload["cohort_id"]],
    ).fetchone()[0]
    assert count == 0, "cohort_membership should have no rows for the unmatched cohort"


def test_cohort_creation_with_same_payload_creates_new_membership_rows(client: TestClient, db_connection) -> None:
    _prepare_normalized_events(client)

    first = client.post(
        "/cohorts",
        json={"name": "purchase_once", "logic_operator": "AND", "conditions": [{"event_name": "purchase", "min_event_count": 1}]},
    )
    assert first.status_code == 200, first.text

    before_second_run = db_connection.execute("SELECT COUNT(*) FROM cohort_membership").fetchone()[0]

    second = client.post(
        "/cohorts",
        json={"name": "purchase_once", "logic_operator": "AND", "conditions": [{"event_name": "purchase", "min_event_count": 1}]},
    )
    assert second.status_code == 200, second.text

    after_second_run = db_connection.execute("SELECT COUNT(*) FROM cohort_membership").fetchone()[0]
    assert (
        after_second_run == before_second_run + 3
    ), "Running cohort creation twice with identical payload should create three new membership rows"


def test_structural_integrity_tables_exist_and_row_counts_are_stable(
    client: TestClient,
    db_connection,
) -> None:
    _prepare_normalized_events(client)

    cohort = client.post(
        "/cohorts",
        json={"name": "purchase_once", "logic_operator": "AND", "conditions": [{"event_name": "purchase", "min_event_count": 1}]},
    )
    assert cohort.status_code == 200, cohort.text

    expected_tables = {"events", "events_normalized", "cohort_membership"}
    found_tables = {
        row[0]
        for row in db_connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name IN ('events', 'events_normalized', 'cohort_membership')
            """
        ).fetchall()
    }
    assert found_tables == expected_tables, f"Expected tables {expected_tables}, found {found_tables}"

    events_count = db_connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    normalized_count = db_connection.execute("SELECT COUNT(*) FROM events_normalized").fetchone()[0]
    membership_count = db_connection.execute("SELECT COUNT(*) FROM cohort_membership").fetchone()[0]

    assert events_count == 5, f"events row count changed unexpectedly: {events_count}"
    assert normalized_count == 5, f"events_normalized row count changed unexpectedly: {normalized_count}"
    assert membership_count == 6, f"cohort_membership should include All Users and first-purchase rows, got {membership_count}"


def test_delete_cohort_removes_related_rows_and_hides_it_from_retention(
    client: TestClient,
    db_connection,
) -> None:
    _prepare_normalized_events(client)

    first = client.post(
        "/cohorts",
        json={"name": "purchase_once", "logic_operator": "AND", "conditions": [{"event_name": "purchase", "min_event_count": 1}]},
    )
    assert first.status_code == 200, first.text
    first_id = first.json()["cohort_id"]

    second = client.post(
        "/cohorts",
        json={"name": "purchase_twice", "logic_operator": "AND", "conditions": [{"event_name": "purchase", "min_event_count": 2}]},
    )
    assert second.status_code == 200, second.text
    second_id = second.json()["cohort_id"]

    delete_response = client.delete(f"/cohorts/{first_id}")
    assert delete_response.status_code == 200, delete_response.text
    assert delete_response.json() == {"deleted": True, "cohort_id": first_id}

    cohorts_remaining = db_connection.execute("SELECT cohort_id FROM cohorts ORDER BY cohort_id").fetchall()
    assert cohorts_remaining == [(1,), (second_id,)]

    membership_count = db_connection.execute(
        "SELECT COUNT(*) FROM cohort_membership WHERE cohort_id = ?",
        [first_id],
    ).fetchone()[0]
    assert membership_count == 0

    activity_count = db_connection.execute(
        "SELECT COUNT(*) FROM cohort_activity_snapshot WHERE cohort_id = ?",
        [first_id],
    ).fetchone()[0]
    assert activity_count == 0

    retention_response = client.get("/retention?max_day=3")
    assert retention_response.status_code == 200, retention_response.text
    cohort_ids_in_retention = [row["cohort_id"] for row in retention_response.json()["retention_table"]]
    assert cohort_ids_in_retention == [1, second_id]


def test_delete_cohort_returns_404_for_unknown_cohort(client: TestClient) -> None:
    response = client.delete("/cohorts/99999")

    assert response.status_code == 404
    assert response.json() == {"detail": "Cohort not found"}


def test_delete_cohort_succeeds_when_cohort_has_no_members(client: TestClient, db_connection) -> None:
    _prepare_normalized_events(client)

    cohort = client.post(
        "/cohorts",
        json={"name": "no_users", "logic_operator": "AND", "conditions": [{"event_name": "refund", "min_event_count": 1}]},
    )
    assert cohort.status_code == 200, cohort.text
    cohort_id = cohort.json()["cohort_id"]

    before_delete = db_connection.execute(
        "SELECT COUNT(*) FROM cohort_membership WHERE cohort_id = ?",
        [cohort_id],
    ).fetchone()[0]
    assert before_delete == 0

    response = client.delete(f"/cohorts/{cohort_id}")
    assert response.status_code == 200, response.text
    assert response.json() == {"deleted": True, "cohort_id": cohort_id}

    exists_after = db_connection.execute(
        "SELECT COUNT(*) FROM cohorts WHERE cohort_id = ?",
        [cohort_id],
    ).fetchone()[0]
    assert exists_after == 0


def test_list_cohorts_returns_logic_and_conditions(client: TestClient) -> None:
    _prepare_normalized_events(client)

    created = client.post(
        "/cohorts",
        json={
            "name": "searchers",
            "logic_operator": "AND",
            "conditions": [{"event_name": "purchase", "min_event_count": 1}],
        },
    )
    assert created.status_code == 200, created.text

    response = client.get("/cohorts")
    assert response.status_code == 200, response.text

    cohorts = response.json()["cohorts"]
    created_cohort = next(row for row in cohorts if row["cohort_id"] == created.json()["cohort_id"])
    assert created_cohort["logic_operator"] == "AND"
    assert created_cohort["conditions"] == [{"event_name": "purchase", "min_event_count": 1, "property_filter": None}]


def test_update_cohort_replaces_conditions_and_rebuilds_membership(client: TestClient, db_connection) -> None:
    _prepare_normalized_events(client)

    created = client.post(
        "/cohorts",
        json={
            "name": "purchase_once",
            "logic_operator": "AND",
            "conditions": [{"event_name": "purchase", "min_event_count": 1}],
        },
    )
    assert created.status_code == 200, created.text
    cohort_id = created.json()["cohort_id"]

    before_update_members = db_connection.execute(
        "SELECT user_id FROM cohort_membership WHERE cohort_id = ? ORDER BY user_id",
        [cohort_id],
    ).fetchall()
    assert before_update_members == [("u1",), ("u2",), ("u3",)]

    updated = client.put(
        f"/cohorts/{cohort_id}",
        json={
            "name": "signup_once",
            "logic_operator": "AND",
            "conditions": [{"event_name": "signup", "min_event_count": 1}],
        },
    )
    assert updated.status_code == 200, updated.text
    assert updated.json() == {"cohort_id": cohort_id, "users_joined": 1}

    condition_rows = db_connection.execute(
        "SELECT event_name, min_event_count FROM cohort_conditions WHERE cohort_id = ?",
        [cohort_id],
    ).fetchall()
    assert condition_rows == [("signup", 1)]

    membership_rows = db_connection.execute(
        "SELECT user_id FROM cohort_membership WHERE cohort_id = ? ORDER BY user_id",
        [cohort_id],
    ).fetchall()
    assert membership_rows == [("u2",)]

    activity_rows = db_connection.execute(
        "SELECT COUNT(*) FROM cohort_activity_snapshot WHERE cohort_id = ?",
        [cohort_id],
    ).fetchone()[0]
    assert activity_rows == 2


def test_update_cohort_returns_404_for_unknown_cohort(client: TestClient) -> None:
    response = client.put(
        "/cohorts/99999",
        json={
            "name": "missing",
            "logic_operator": "AND",
            "conditions": [{"event_name": "signup", "min_event_count": 1}],
        },
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Cohort not found"}


def test_update_cohort_rejects_empty_conditions(client: TestClient) -> None:
    _prepare_normalized_events(client)

    created = client.post(
        "/cohorts",
        json={
            "name": "purchase_once",
            "logic_operator": "AND",
            "conditions": [{"event_name": "purchase", "min_event_count": 1}],
        },
    )
    assert created.status_code == 200, created.text

    response = client.put(
        f"/cohorts/{created.json()['cohort_id']}",
        json={"name": "empty", "logic_operator": "AND", "conditions": []},
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "At least one condition is required"}


def test_update_all_users_is_forbidden(client: TestClient) -> None:
    _prepare_normalized_events(client)

    response = client.put(
        "/cohorts/1",
        json={
            "name": "All Users Updated",
            "logic_operator": "AND",
            "conditions": [{"event_name": "purchase", "min_event_count": 1}],
        },
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "All Users cohort cannot be updated"}



def _prepare_property_filter_events(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time,version,amount\n"
        "u1,search,2024-01-01 09:00:00,3.9.1,10\n"
        "u1,search,2024-01-02 09:00:00,3.9.1,11\n"
        "u1,search,2024-01-03 09:00:00,4.0.0,12\n"
        "u2,search,2024-01-01 10:00:00,3.9.1,20\n"
        "u2,search,2024-01-04 10:00:00,3.8.0,21\n"
        "u3,search,2024-01-02 11:00:00,3.9.1,30\n"
        "u3,search,2024-01-03 11:00:00,3.9.1,31\n"
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


def test_create_cohort_without_property_filter_membership_correct_phase1(client: TestClient) -> None:
    _prepare_property_filter_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "search_twice",
            "logic_operator": "AND",
            "conditions": [{"event_name": "search", "min_event_count": 2}],
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["users_joined"] == 3


def test_create_cohort_with_property_filter_membership_correct_phase1(client: TestClient, db_connection) -> None:
    _prepare_property_filter_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "search_391_twice",
            "logic_operator": "AND",
            "conditions": [
                {
                    "event_name": "search",
                    "min_event_count": 2,
                    "property_filter": {"column": "version", "operator": "=", "value": "3.9.1"},
                }
            ],
        },
    )

    assert response.status_code == 200, response.text
    cohort_id = response.json()["cohort_id"]
    assert response.json()["users_joined"] == 2

    members = db_connection.execute(
        "SELECT user_id FROM cohort_membership WHERE cohort_id = ? ORDER BY user_id",
        [cohort_id],
    ).fetchall()
    assert members == [("u1",), ("u3",)]


def test_update_cohort_remove_property_filter_recalculates_membership(client: TestClient, db_connection) -> None:
    _prepare_property_filter_events(client)

    created = client.post(
        "/cohorts",
        json={
            "name": "search_391_twice",
            "logic_operator": "AND",
            "conditions": [
                {
                    "event_name": "search",
                    "min_event_count": 2,
                    "property_filter": {"column": "version", "operator": "=", "value": "3.9.1"},
                }
            ],
        },
    )
    assert created.status_code == 200, created.text
    cohort_id = created.json()["cohort_id"]

    updated = client.put(
        f"/cohorts/{cohort_id}",
        json={
            "name": "search_twice",
            "logic_operator": "AND",
            "conditions": [{"event_name": "search", "min_event_count": 2, "property_filter": None}],
        },
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["users_joined"] == 3

    members = db_connection.execute(
        "SELECT user_id FROM cohort_membership WHERE cohort_id = ? ORDER BY user_id",
        [cohort_id],
    ).fetchall()
    assert members == [("u1",), ("u2",), ("u3",)]


def test_create_cohort_rejects_invalid_property_filter_operator(client: TestClient) -> None:
    _prepare_property_filter_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "invalid_operator",
            "logic_operator": "AND",
            "conditions": [
                {
                    "event_name": "search",
                    "min_event_count": 1,
                    "property_filter": {"column": "version", "operator": ">", "value": "3.9.1"},
                }
            ],
        },
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "Operator '>' not allowed for column type TEXT"}


def test_create_cohort_rejects_unknown_property_filter_column(client: TestClient) -> None:
    _prepare_property_filter_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "invalid_column",
            "logic_operator": "AND",
            "conditions": [
                {
                    "event_name": "search",
                    "min_event_count": 1,
                    "property_filter": {"column": "missing_col", "operator": "=", "value": "3.9.1"},
                }
            ],
        },
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "Unknown filter column: missing_col"}


def test_property_filter_applies_before_aggregation(client: TestClient, db_connection) -> None:
    _prepare_property_filter_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "search_400_twice",
            "logic_operator": "AND",
            "conditions": [
                {
                    "event_name": "search",
                    "min_event_count": 2,
                    "property_filter": {"column": "version", "operator": "=", "value": "4.0.0"},
                }
            ],
        },
    )

    assert response.status_code == 200, response.text
    cohort_id = response.json()["cohort_id"]
    assert response.json()["users_joined"] == 0

    condition_row = db_connection.execute(
        """
        SELECT property_column, property_operator, property_value
        FROM cohort_conditions
        WHERE cohort_id = ?
        """,
        [cohort_id],
    ).fetchone()
    assert condition_row == ("version", "=", "4.0.0")
