from __future__ import annotations

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
        json={"name": "purchase_once", "event_name": "purchase", "min_event_count": 1},
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
        ("u1", "2024-01-01 09:00:00"),
        ("u2", "2024-01-02 09:00:00"),
        ("u3", "2024-01-04 09:00:00"),
    ], f"join_time should match each user's first qualifying event, got {membership}"


def test_nth_event_logic_uses_min_event_count_as_join_time(
    client: TestClient,
    db_connection,
) -> None:
    _prepare_normalized_events(client)

    response = client.post(
        "/cohorts",
        json={"name": "purchase_twice", "event_name": "purchase", "min_event_count": 2},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["users_joined"] == 1, "Only one user has a second purchase event"

    rows = db_connection.execute(
        "SELECT user_id, join_time FROM cohort_membership WHERE cohort_id = ?",
        [payload["cohort_id"]],
    ).fetchall()
    assert rows == [
        ("u1", "2024-01-03 09:00:00")
    ], f"The cohort join_time should be the second purchase timestamp, got {rows}"


def test_cohort_creation_with_no_matching_users_inserts_zero(client: TestClient, db_connection) -> None:
    _prepare_normalized_events(client)

    response = client.post(
        "/cohorts",
        json={"name": "no_users", "event_name": "refund", "min_event_count": 1},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["users_joined"] == 0, "No users should join when the event_name does not exist"

    count = db_connection.execute(
        "SELECT COUNT(*) FROM cohort_membership WHERE cohort_id = ?",
        [payload["cohort_id"]],
    ).fetchone()[0]
    assert count == 0, "cohort_membership should have no rows for the unmatched cohort"


def test_cohort_creation_is_idempotent_for_same_payload(client: TestClient, db_connection) -> None:
    _prepare_normalized_events(client)

    first = client.post(
        "/cohorts",
        json={"name": "purchase_once", "event_name": "purchase", "min_event_count": 1},
    )
    assert first.status_code == 200, first.text

    before_second_run = db_connection.execute("SELECT COUNT(*) FROM cohort_membership").fetchone()[0]

    second = client.post(
        "/cohorts",
        json={"name": "purchase_once", "event_name": "purchase", "min_event_count": 1},
    )
    assert second.status_code == 200, second.text

    after_second_run = db_connection.execute("SELECT COUNT(*) FROM cohort_membership").fetchone()[0]
    assert (
        after_second_run == before_second_run
    ), "Running cohort creation twice with identical payload should not duplicate membership rows"


def test_structural_integrity_tables_exist_and_row_counts_are_stable(
    client: TestClient,
    db_connection,
) -> None:
    _prepare_normalized_events(client)

    cohort = client.post(
        "/cohorts",
        json={"name": "purchase_once", "event_name": "purchase", "min_event_count": 1},
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
    assert membership_count == 3, f"cohort_membership should have three rows for first-purchase cohort, got {membership_count}"
