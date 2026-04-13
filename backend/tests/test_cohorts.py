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





def test_cohort_threshold_uses_sum_event_count_for_aggregated_rows(client: TestClient, db_connection) -> None:
    csv_text = (
        "user_id,event_name,event_time,event_count\n"
        "u1,purchase,2024-01-01,5\n"
        "u2,purchase,2024-01-01,1\n"
        "u2,purchase,2024-01-02,1\n"
    )
    assert csv_upload(client, csv_text=csv_text).status_code == 200
    mapping = client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
            "event_count_column": "event_count",
            "column_types": {
                "user_id": "TEXT",
                "event_name": "TEXT",
                "event_time": "TIMESTAMP",
                "event_count": "NUMERIC",
            },
        },
    )
    assert mapping.status_code == 200, mapping.text

    response = client.post(
        "/cohorts",
        json={"name": "purchase_twice", "logic_operator": "AND", "conditions": [{"event_name": "purchase", "min_event_count": 2}]},
    )
    assert response.status_code == 200, response.text

    rows = db_connection.execute(
        "SELECT user_id, join_time FROM cohort_membership WHERE cohort_id = ? ORDER BY user_id",
        [response.json()["cohort_id"]],
    ).fetchall()
    assert rows == [
        ("u1", datetime(2024, 1, 1, 0, 0, 0)),
        ("u2", datetime(2024, 1, 2, 0, 0, 0)),
    ]

def test_cohort_join_type_defaults_to_condition_met_when_omitted(client: TestClient, db_connection) -> None:
    _prepare_normalized_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "purchase_twice_default_join",
            "logic_operator": "AND",
            "conditions": [{"event_name": "purchase", "min_event_count": 2}],
        },
    )

    assert response.status_code == 200, response.text
    cohort_id = response.json()["cohort_id"]

    cohort_row = db_connection.execute(
        "SELECT join_type FROM cohorts WHERE cohort_id = ?",
        [cohort_id],
    ).fetchone()
    assert cohort_row == ("condition_met",)

    rows = db_connection.execute(
        "SELECT user_id, join_time FROM cohort_membership WHERE cohort_id = ?",
        [cohort_id],
    ).fetchall()
    assert rows == [("u1", datetime(2024, 1, 3, 9, 0, 0))]


def test_first_event_join_type_overrides_join_time_not_membership(client: TestClient, db_connection) -> None:
    _prepare_normalized_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "purchase_twice_first_event",
            "logic_operator": "AND",
            "join_type": "first_event",
            "conditions": [{"event_name": "purchase", "min_event_count": 2}],
        },
    )

    assert response.status_code == 200, response.text
    cohort_id = response.json()["cohort_id"]
    assert response.json()["users_joined"] == 1

    rows = db_connection.execute(
        "SELECT user_id, join_time FROM cohort_membership WHERE cohort_id = ?",
        [cohort_id],
    ).fetchall()
    assert rows == [("u1", datetime(2024, 1, 1, 9, 0, 0))]




def test_create_cohort_normalizes_uppercase_join_type(client: TestClient, db_connection) -> None:
    _prepare_normalized_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "purchase_upper_join",
            "logic_operator": "AND",
            "join_type": "FIRST_EVENT",
            "conditions": [{"event_name": "purchase", "min_event_count": 2}],
        },
    )

    assert response.status_code == 200, response.text
    cohort_id = response.json()["cohort_id"]

    cohort_row = db_connection.execute(
        "SELECT join_type FROM cohorts WHERE cohort_id = ?",
        [cohort_id],
    ).fetchone()
    assert cohort_row == ("first_event",)


def test_all_users_cohort_defaults_to_first_event_join_type(client: TestClient, db_connection) -> None:
    _prepare_normalized_events(client)

    all_users = db_connection.execute(
        "SELECT join_type FROM cohorts WHERE name = 'All Users'",
    ).fetchone()
    assert all_users == ("first_event",)


def test_create_cohort_rejects_invalid_join_type(client: TestClient) -> None:
    _prepare_normalized_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "invalid_join",
            "logic_operator": "AND",
            "join_type": "invalid",
            "conditions": [{"event_name": "purchase", "min_event_count": 1}],
        },
    )

    assert response.status_code == 422
    assert response.json()["detail"][0]["msg"] == "Value error, join_type must be 'condition_met' or 'first_event'"


def test_update_cohort_can_change_join_type(client: TestClient, db_connection) -> None:
    _prepare_normalized_events(client)

    created = client.post(
        "/cohorts",
        json={
            "name": "purchase_twice",
            "logic_operator": "AND",
            "conditions": [{"event_name": "purchase", "min_event_count": 2}],
        },
    )
    assert created.status_code == 200, created.text
    cohort_id = created.json()["cohort_id"]

    updated = client.put(
        f"/cohorts/{cohort_id}",
        json={
            "name": "purchase_twice",
            "logic_operator": "AND",
            "join_type": "first_event",
            "conditions": [{"event_name": "purchase", "min_event_count": 2}],
        },
    )
    assert updated.status_code == 200, updated.text

    cohort_row = db_connection.execute(
        "SELECT join_type FROM cohorts WHERE cohort_id = ?",
        [cohort_id],
    ).fetchone()
    assert cohort_row == ("first_event",)

    rows = db_connection.execute(
        "SELECT user_id, join_time FROM cohort_membership WHERE cohort_id = ?",
        [cohort_id],
    ).fetchall()
    assert rows == [("u1", datetime(2024, 1, 1, 9, 0, 0))]


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
    assert created_cohort["conditions"] == [{"event_name": "purchase", "min_event_count": 1, "property_filter": None, "is_negated": False}]


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
        "user_id,event_name,event_time,version,amount,is_premium\n"
        "u1,search,2024-01-01 09:00:00,3.9.1,10,true\n"
        "u1,search,2024-01-02 09:00:00,3.9.1,11,true\n"
        "u1,search,2024-01-03 09:00:00,4.0.0,12,true\n"
        "u2,search,2024-01-01 10:00:00,3.9.1,20,false\n"
        "u2,search,2024-01-04 10:00:00,3.8.0,21,false\n"
        "u3,search,2024-01-02 11:00:00,3.9.1,30,true\n"
        "u3,search,2024-01-03 11:00:00,3.9.1,31,true\n"
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
                    "property_filter": {"column": "version", "operator": "=", "values": "3.9.1"},
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
                    "property_filter": {"column": "version", "operator": "=", "values": "3.9.1"},
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
                    "property_filter": {"column": "version", "operator": ">", "values": "3.9.1"},
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
                    "property_filter": {"column": "missing_col", "operator": "=", "values": "3.9.1"},
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
                    "property_filter": {"column": "version", "operator": "=", "values": "4.0.0"},
                }
            ],
        },
    )

    assert response.status_code == 200, response.text
    cohort_id = response.json()["cohort_id"]
    assert response.json()["users_joined"] == 0

    condition_row = db_connection.execute(
        """
        SELECT property_column, property_operator, property_values
        FROM cohort_conditions
        WHERE cohort_id = ?
        """,
        [cohort_id],
    ).fetchone()
    assert condition_row == ("version", "=", '["4.0.0"]')


def test_create_cohort_with_in_operator_multiple_values(client: TestClient) -> None:
    _prepare_property_filter_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "search_version_in",
            "logic_operator": "AND",
            "conditions": [{
                "event_name": "search",
                "min_event_count": 1,
                "property_filter": {"column": "version", "operator": "IN", "values": ["3.9.1", "3.8.0"]},
            }],
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["users_joined"] == 3


def test_create_cohort_with_not_in_operator_multiple_values(client: TestClient) -> None:
    _prepare_property_filter_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "search_version_not_in",
            "logic_operator": "AND",
            "conditions": [{
                "event_name": "search",
                "min_event_count": 1,
                "property_filter": {"column": "version", "operator": "NOT IN", "values": ["3.9.1"]},
            }],
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["users_joined"] == 2


def test_create_cohort_with_numeric_gt_filter(client: TestClient) -> None:
    _prepare_property_filter_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "search_amount_gt",
            "logic_operator": "AND",
            "conditions": [{
                "event_name": "search",
                "min_event_count": 1,
                "property_filter": {"column": "amount", "operator": ">", "values": 25},
            }],
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["users_joined"] == 1


def test_numeric_in_operator_accepts_string_numbers(client: TestClient) -> None:
    _prepare_property_filter_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "search_amount_in",
            "logic_operator": "AND",
            "conditions": [{
                "event_name": "search",
                "min_event_count": 1,
                "property_filter": {"column": "amount", "operator": "IN", "values": ["20", "30"]},
            }],
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["users_joined"] == 2

    listed = client.get('/cohorts')
    assert listed.status_code == 200, listed.text
    cohort = next(row for row in listed.json()['cohorts'] if row['cohort_id'] == response.json()['cohort_id'])
    assert cohort['conditions'][0]['property_filter']['values'] == [20, 30]




def test_numeric_not_in_operator_accepts_string_numbers(client: TestClient) -> None:
    _prepare_property_filter_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "search_amount_not_in",
            "logic_operator": "AND",
            "conditions": [{
                "event_name": "search",
                "min_event_count": 1,
                "property_filter": {"column": "amount", "operator": "NOT IN", "values": ["20", "21"]},
            }],
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["users_joined"] == 2

def test_create_cohort_with_boolean_equals_filter(client: TestClient, db_connection) -> None:
    _prepare_property_filter_events(client)
    db_connection.execute("""
        ALTER TABLE events_normalized
        ALTER COLUMN is_premium TYPE BOOLEAN
        USING CAST(is_premium AS BOOLEAN)
    """)

    response = client.post(
        "/cohorts",
        json={
            "name": "search_premium",
            "logic_operator": "AND",
            "conditions": [{
                "event_name": "search",
                "min_event_count": 1,
                "property_filter": {"column": "is_premium", "operator": "=", "values": True},
            }],
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["users_joined"] == 2


def test_create_cohort_rejects_empty_in_values(client: TestClient) -> None:
    _prepare_property_filter_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "invalid_in",
            "logic_operator": "AND",
            "conditions": [{
                "event_name": "search",
                "min_event_count": 1,
                "property_filter": {"column": "version", "operator": "IN", "values": []},
            }],
        },
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "Operator IN requires a non-empty array value"}


def test_create_cohort_rejects_non_numeric_value_for_numeric_operator(client: TestClient) -> None:
    _prepare_property_filter_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "invalid_numeric",
            "logic_operator": "AND",
            "conditions": [{
                "event_name": "search",
                "min_event_count": 1,
                "property_filter": {"column": "amount", "operator": ">", "values": "abc"},
            }],
        },
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "Numeric operators require numeric values"}


def test_create_cohort_with_timestamp_in_filter(client: TestClient) -> None:
    _prepare_property_filter_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "search_time_in",
            "logic_operator": "AND",
            "conditions": [{
                "event_name": "search",
                "min_event_count": 1,
                "property_filter": {
                    "column": "event_time",
                    "operator": "IN",
                    "values": ["2024-01-01 09:00:00", "2024-01-01 10:00:00"],
                },
            }],
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["users_joined"] == 2


def test_create_cohort_rejects_non_string_timestamp_values(client: TestClient) -> None:
    _prepare_property_filter_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "invalid_timestamp",
            "logic_operator": "AND",
            "conditions": [{
                "event_name": "search",
                "min_event_count": 1,
                "property_filter": {"column": "event_time", "operator": ">=", "values": 123},
            }],
        },
    )

    assert response.status_code == 400
    # The error depends on the operator. Now >= is not in allowed timestamp ops.
    assert "not allowed for column type TIMESTAMP" in response.json()["detail"]




def test_create_cohort_rejects_boolean_value_for_numeric_operator(client: TestClient) -> None:
    _prepare_property_filter_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "invalid_numeric_bool",
            "logic_operator": "AND",
            "conditions": [{
                "event_name": "search",
                "min_event_count": 1,
                "property_filter": {"column": "amount", "operator": ">", "values": True},
            }],
        },
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "Numeric operators require numeric values"}


def test_create_cohort_normalizes_datetime_local_timestamp_values(client: TestClient) -> None:
    _prepare_property_filter_events(client)

    created = client.post(
        "/cohorts",
        json={
            "name": "search_time_local",
            "logic_operator": "AND",
            "conditions": [{
                "event_name": "search",
                "min_event_count": 1,
                "property_filter": {"column": "event_time", "operator": "=", "values": "2024-01-01T09:00"},
            }],
        },
    )
    assert created.status_code == 200, created.text

    listed = client.get('/cohorts')
    assert listed.status_code == 200, listed.text
    cohort = next(row for row in listed.json()['cohorts'] if row['cohort_id'] == created.json()['cohort_id'])
    assert cohort['conditions'][0]['property_filter']['values'] == {'date': '2024-01-01'}



def test_create_cohort_rejects_empty_timestamp_string(client: TestClient) -> None:
    _prepare_property_filter_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "invalid_timestamp_empty",
            "logic_operator": "AND",
            "conditions": [{
                "event_name": "search",
                "min_event_count": 1,
                "property_filter": {"column": "event_time", "operator": "=", "values": "   "},
            }],
        },
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "Timestamp value cannot be empty"}



def test_create_cohort_rejects_empty_timestamp_string_in_list(client: TestClient) -> None:
    _prepare_property_filter_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "invalid_timestamp_list_empty",
            "logic_operator": "AND",
            "conditions": [{
                "event_name": "search",
                "min_event_count": 1,
                "property_filter": {
                    "column": "event_time",
                    "operator": "IN",
                    "values": ["2024-01-01 09:00:00", " "],
                },
            }],
        },
    )

    assert response.status_code == 400
    assert "Invalid timestamp value in IN" in response.json()["detail"]



def test_create_cohort_rejects_invalid_timestamp_format(client: TestClient) -> None:
    _prepare_property_filter_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "invalid_timestamp_format",
            "logic_operator": "AND",
            "conditions": [{
                "event_name": "search",
                "min_event_count": 1,
                "property_filter": {"column": "event_time", "operator": "=", "values": "not-a-date"},
            }],
        },
    )

    assert response.status_code == 400
    assert "Invalid timestamp format" in response.json()["detail"]



def test_toggle_hide_updates_cohort_visibility_flag(client: TestClient, db_connection) -> None:
    _prepare_normalized_events(client)

    created = client.post(
        "/cohorts",
        json={"name": "purchase_once", "logic_operator": "AND", "conditions": [{"event_name": "purchase", "min_event_count": 1}]},
    )
    assert created.status_code == 200, created.text
    cohort_id = created.json()["cohort_id"]

    hide_response = client.patch(f"/cohorts/{cohort_id}/hide")
    assert hide_response.status_code == 200, hide_response.text
    assert hide_response.json() == {"cohort_id": cohort_id, "hidden": True}

    listed = client.get("/cohorts")
    assert listed.status_code == 200, listed.text
    toggled = next(row for row in listed.json()["cohorts"] if row["cohort_id"] == cohort_id)
    assert toggled["hidden"] is True

    unhide_response = client.patch(f"/cohorts/{cohort_id}/hide")
    assert unhide_response.status_code == 200, unhide_response.text
    assert unhide_response.json() == {"cohort_id": cohort_id, "hidden": False}

def test_random_split_creates_four_child_cohorts_and_assigns_members(client: TestClient, db_connection) -> None:
    csv_text = "user_id,event_name,event_time\n" + "".join(
        f"u{i},signup,2024-01-01 00:00:{i:02d}\n" for i in range(1, 13)
    )
    assert csv_upload(client, csv_text=csv_text).status_code == 200
    mapping = client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
        },
    )
    assert mapping.status_code == 200, mapping.text

    created = client.post(
        "/cohorts",
        json={
            "name": "signup cohort",
            "logic_operator": "AND",
            "conditions": [{"event_name": "signup", "min_event_count": 1}],
        },
    )
    assert created.status_code == 200, created.text
    parent_id = created.json()["cohort_id"]

    split = client.post(f"/cohorts/{parent_id}/random_split")
    assert split.status_code == 200, split.text
    assert split.json() == {"created": 4}

    children = db_connection.execute(
        """
        SELECT cohort_id, split_group_index, split_group_total
        FROM cohorts
        WHERE split_parent_cohort_id = ?
        ORDER BY split_group_index
        """,
        [parent_id],
    ).fetchall()
    assert len(children) == 4
    assert [row[1] for row in children] == [0, 1, 2, 3]
    assert all(row[2] == 4 for row in children)

    child_ids = [row[0] for row in children]
    assigned = db_connection.execute(
        """
        SELECT user_id, COUNT(*)
        FROM cohort_membership
        WHERE cohort_id IN (?, ?, ?, ?)
        GROUP BY user_id
        ORDER BY user_id
        """,
        child_ids,
    ).fetchall()
    assert len(assigned) == 12
    assert all(row[1] == 1 for row in assigned)

    size_rows = db_connection.execute(
        """
        SELECT cohort_id, COUNT(*) AS cohort_size
        FROM cohort_membership
        WHERE cohort_id IN (?, ?, ?, ?)
        GROUP BY cohort_id
        ORDER BY cohort_id
        """,
        child_ids,
    ).fetchall()
    sizes = [int(row[1]) for row in size_rows]
    assert len(sizes) == 4
    assert max(sizes) - min(sizes) <= 1

    join_times = db_connection.execute(
        """
        SELECT COUNT(*)
        FROM cohort_membership
        WHERE cohort_id IN (?, ?, ?, ?)
          AND join_time IS NULL
        """,
        child_ids,
    ).fetchone()[0]
    assert join_times == 0

    parent_join_times = {
        str(user_id): join_time
        for user_id, join_time in db_connection.execute(
            "SELECT user_id, join_time FROM cohort_membership WHERE cohort_id = ?",
            [parent_id],
        ).fetchall()
    }
    child_join_rows = db_connection.execute(
        """
        SELECT cm.user_id, cm.join_time
        FROM cohort_membership cm
        WHERE cm.cohort_id IN (?, ?, ?, ?)
        ORDER BY cm.user_id
        """,
        child_ids,
    ).fetchall()
    assert len(child_join_rows) == len(parent_join_times)
    assert all(parent_join_times[str(user_id)] == join_time for user_id, join_time in child_join_rows)

    snapshot_count = db_connection.execute(
        """
        SELECT COUNT(*)
        FROM cohort_activity_snapshot
        WHERE cohort_id IN (?, ?, ?, ?)
        """,
        child_ids,
    ).fetchone()[0]
    assert snapshot_count == 12


def test_random_split_requires_minimum_parent_size(client: TestClient) -> None:
    _prepare_normalized_events(client)

    created = client.post(
        "/cohorts",
        json={
            "name": "small cohort",
            "logic_operator": "AND",
            "conditions": [{"event_name": "purchase", "min_event_count": 1}],
        },
    )
    assert created.status_code == 200, created.text

    split = client.post(f"/cohorts/{created.json()['cohort_id']}/random_split")
    assert split.status_code == 400
    assert split.json()["detail"] == "Minimum 8 users required"



def test_random_split_rejects_sub_cohort(client: TestClient, db_connection) -> None:
    csv_text = "user_id,event_name,event_time\n" + "".join(
        f"u{i},signup,2024-01-01 00:00:{i:02d}\n" for i in range(1, 13)
    )
    assert csv_upload(client, csv_text=csv_text).status_code == 200
    assert client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
        },
    ).status_code == 200

    parent_created = client.post(
        "/cohorts",
        json={
            "name": "signup cohort",
            "logic_operator": "AND",
            "conditions": [{"event_name": "signup", "min_event_count": 1}],
        },
    )
    assert parent_created.status_code == 200, parent_created.text
    parent_id = parent_created.json()["cohort_id"]

    first_split = client.post(f"/cohorts/{parent_id}/random_split")
    assert first_split.status_code == 200, first_split.text

    child_id = db_connection.execute(
        """
        SELECT cohort_id
        FROM cohorts
        WHERE split_parent_cohort_id = ?
        ORDER BY split_group_index
        LIMIT 1
        """,
        [parent_id],
    ).fetchone()[0]

    split_child = client.post(f"/cohorts/{child_id}/random_split")
    assert split_child.status_code == 400
    assert split_child.json()["detail"] == "Cannot split sub-cohort"


def test_random_split_rejects_hidden_cohort(client: TestClient) -> None:
    csv_text = "user_id,event_name,event_time\n" + "".join(
        f"u{i},signup,2024-01-01 00:00:{i:02d}\n" for i in range(1, 13)
    )
    assert csv_upload(client, csv_text=csv_text).status_code == 200
    assert client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
        },
    ).status_code == 200

    created = client.post(
        "/cohorts",
        json={
            "name": "signup cohort",
            "logic_operator": "AND",
            "conditions": [{"event_name": "signup", "min_event_count": 1}],
        },
    )
    assert created.status_code == 200, created.text
    cohort_id = created.json()["cohort_id"]

    hide_response = client.patch(f"/cohorts/{cohort_id}/hide")
    assert hide_response.status_code == 200, hide_response.text

    split = client.post(f"/cohorts/{cohort_id}/random_split")
    assert split.status_code == 400
    assert split.json()["detail"] == "Cannot split hidden cohort"


def test_delete_parent_cohort_cascades_split_children(client: TestClient, db_connection) -> None:
    csv_text = "user_id,event_name,event_time\n" + "".join(
        f"u{i},signup,2024-01-01 00:00:{i:02d}\n" for i in range(1, 13)
    )
    assert csv_upload(client, csv_text=csv_text).status_code == 200
    assert client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
        },
    ).status_code == 200

    created = client.post(
        "/cohorts",
        json={
            "name": "signup cohort",
            "logic_operator": "AND",
            "conditions": [{"event_name": "signup", "min_event_count": 1}],
        },
    )
    assert created.status_code == 200, created.text
    parent_id = created.json()["cohort_id"]

    split = client.post(f"/cohorts/{parent_id}/random_split")
    assert split.status_code == 200, split.text

    children_before = db_connection.execute(
        "SELECT COUNT(*) FROM cohorts WHERE split_parent_cohort_id = ?",
        [parent_id],
    ).fetchone()[0]
    assert children_before == 4

    deleted = client.delete(f"/cohorts/{parent_id}")
    assert deleted.status_code == 200, deleted.text

    children_after = db_connection.execute(
        "SELECT COUNT(*) FROM cohorts WHERE split_parent_cohort_id = ?",
        [parent_id],
    ).fetchone()[0]
    assert children_after == 0

    child_memberships_after = db_connection.execute(
        "SELECT COUNT(*) FROM cohort_membership cm JOIN cohorts c ON cm.cohort_id = c.cohort_id WHERE c.split_parent_cohort_id = ?",
        [parent_id],
    ).fetchone()[0]
    assert child_memberships_after == 0


# ─────────────────────────────────────────────
# Negation tests (DID / DIDN'T)
# ─────────────────────────────────────────────

def _prepare_negation_events(client: TestClient) -> None:
    """
    Dataset:
      u1 – purchased (once)
      u2 – searched (once)
      u3 – purchased (once) AND searched (once)
    """
    csv_text = (
        "user_id,event_name,event_time,channel\n"
        "u1,purchase,2024-01-01 09:00:00,ads\n"
        "u2,search,2024-01-02 09:00:00,organic\n"
        "u3,purchase,2024-01-03 09:00:00,email\n"
        "u3,search,2024-01-03 10:00:00,email\n"
    )
    from tests.utils import csv_upload
    upload = csv_upload(client, csv_text=csv_text)
    assert upload.status_code == 200, f"Upload failed: {upload.text}"
    mapping = client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
        },
    )
    assert mapping.status_code == 200, f"Mapping failed: {mapping.text}"


def test_negated_condition_returns_users_who_never_performed_event(
    client: TestClient,
    db_connection,
) -> None:
    """DIDN'T purchase → {u2} (u1 and u3 purchased, u2 never did)"""
    _prepare_negation_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "never_purchased",
            "logic_operator": "AND",
            "conditions": [{"event_name": "purchase", "min_event_count": 1, "is_negated": True}],
        },
    )
    assert response.status_code == 200, response.text
    cohort_id = response.json()["cohort_id"]
    assert response.json()["users_joined"] == 1, "Only u2 never purchased"

    members = db_connection.execute(
        "SELECT user_id FROM cohort_membership WHERE cohort_id = ? ORDER BY user_id",
        [cohort_id],
    ).fetchall()
    assert members == [("u2",)], f"Expected [u2] but got {members}"


def test_did_and_didnt_conditions_with_and_logic(
    client: TestClient,
    db_connection,
) -> None:
    """DID search AND DIDN'T purchase → {u2} (u3 did both, u1 only purchased, u2 only searched)"""
    _prepare_negation_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "searched_not_purchased",
            "logic_operator": "AND",
            "conditions": [
                {"event_name": "search", "min_event_count": 1, "is_negated": False},
                {"event_name": "purchase", "min_event_count": 1, "is_negated": True},
            ],
        },
    )
    assert response.status_code == 200, response.text
    cohort_id = response.json()["cohort_id"]
    assert response.json()["users_joined"] == 1, "Only u2 searched and did NOT purchase"

    members = db_connection.execute(
        "SELECT user_id FROM cohort_membership WHERE cohort_id = ? ORDER BY user_id",
        [cohort_id],
    ).fetchall()
    assert members == [("u2",)], f"Expected [u2] but got {members}"


def test_negated_condition_with_property_filter(
    client: TestClient,
    db_connection,
) -> None:
    """DIDN'T purchase where channel=ads → users who did not purchase via ads.
       u1 purchased via ads, u3 via email. So DIDN'T purchase (channel=ads) = {u2, u3}."""
    _prepare_negation_events(client)

    response = client.post(
        "/cohorts",
        json={
            "name": "not_purchased_via_ads",
            "logic_operator": "AND",
            "conditions": [
                {
                    "event_name": "purchase",
                    "min_event_count": 1,
                    "is_negated": True,
                    "property_filter": {"column": "channel", "operator": "=", "values": "ads"},
                }
            ],
        },
    )
    assert response.status_code == 200, response.text
    cohort_id = response.json()["cohort_id"]
    assert response.json()["users_joined"] == 2, "u2 and u3 did NOT purchase via ads channel"

    members = db_connection.execute(
        "SELECT user_id FROM cohort_membership WHERE cohort_id = ? ORDER BY user_id",
        [cohort_id],
    ).fetchall()
    assert members == [("u2",), ("u3",)], f"Expected [u2, u3] but got {members}"

