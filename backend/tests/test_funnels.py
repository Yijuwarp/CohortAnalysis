"""
Backend unit tests for the Funnels feature.
Covers: funnel creation, execution correctness, and validity detection.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from tests.utils import csv_upload


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _upload_funnel_dataset(client: TestClient) -> None:
    """Upload a dataset with signup → search(query=amazon) → purchase."""
    csv_text = (
        "user_id,event_name,event_time,query,country\n"
        # u1: completes all 3 steps
        "u1,signup,2024-01-01 08:00:00,,US\n"
        "u1,search,2024-01-01 09:00:00,amazon,US\n"
        "u1,purchase,2024-01-01 10:00:00,,US\n"
        # u2: signup + search but no purchase
        "u2,signup,2024-01-01 08:00:00,,US\n"
        "u2,search,2024-01-01 09:00:00,amazon,US\n"
        # u3: signup only
        "u3,signup,2024-01-01 08:00:00,,US\n"
        # u4: signup → search (google, not amazon) → purchase
        "u4,signup,2024-01-01 08:00:00,,US\n"
        "u4,search,2024-01-01 09:00:00,google,US\n"
        "u4,purchase,2024-01-01 10:00:00,,US\n"
        # u5: signup → (one search before signup, ignored) → search after → purchase
        "u5,signup,2024-01-01 08:00:00,,GB\n"
        "u5,search,2024-01-01 06:00:00,amazon,GB\n"   # BEFORE signup — must be ignored
        "u5,search,2024-01-01 09:30:00,amazon,GB\n"   # AFTER signup — valid
        "u5,purchase,2024-01-01 10:00:00,,GB\n"
    )
    r = csv_upload(client, csv_text=csv_text)
    assert r.status_code == 200, r.text
    m = client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
        },
    )
    assert m.status_code == 200, m.text


# ---------------------------------------------------------------------------
# 1. Funnel creation
# ---------------------------------------------------------------------------

def test_create_funnel_returns_id_and_name(client: TestClient) -> None:
    r = client.post(
        "/funnels",
        json={
            "name": "signup_to_purchase",
            "steps": [
                {"event_name": "signup", "filters": []},
                {"event_name": "purchase", "filters": []},
            ],
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert "id" in body
    assert body["name"] == "signup_to_purchase"


def test_create_funnel_rejects_fewer_than_two_steps(client: TestClient) -> None:
    r = client.post(
        "/funnels",
        json={
            "name": "one_step",
            "steps": [{"event_name": "signup", "filters": []}],
        },
    )
    assert r.status_code == 422  # Pydantic min_length=2


def test_create_funnel_rejects_more_than_ten_steps(client: TestClient) -> None:
    steps = [{"event_name": f"event_{i}", "filters": []} for i in range(11)]
    r = client.post("/funnels", json={"name": "too_many", "steps": steps})
    assert r.status_code == 422  # Pydantic max_length=10


def test_create_funnel_with_property_filters(client: TestClient) -> None:
    r = client.post(
        "/funnels",
        json={
            "name": "filtered_funnel",
            "steps": [
                {"event_name": "signup", "filters": []},
                {
                    "event_name": "search",
                    "filters": [{"property_key": "query", "property_value": "amazon"}],
                },
                {"event_name": "purchase", "filters": []},
            ],
        },
    )
    assert r.status_code == 200, r.text
    assert r.json()["name"] == "filtered_funnel"


def test_list_funnels_returns_created_funnel(client: TestClient) -> None:
    client.post(
        "/funnels",
        json={
            "name": "my_funnel",
            "steps": [
                {"event_name": "signup", "filters": []},
                {"event_name": "purchase", "filters": []},
            ],
        },
    )
    r = client.get("/funnels")
    assert r.status_code == 200, r.text
    funnels = r.json()["funnels"]
    assert any(f["name"] == "my_funnel" for f in funnels)


def test_delete_funnel_removes_it_from_list(client: TestClient) -> None:
    create = client.post(
        "/funnels",
        json={
            "name": "to_delete",
            "steps": [
                {"event_name": "signup", "filters": []},
                {"event_name": "purchase", "filters": []},
            ],
        },
    )
    assert create.status_code == 200
    fid = create.json()["id"]

    delete = client.delete(f"/funnels/{fid}")
    assert delete.status_code == 200
    assert delete.json()["deleted"] is True

    funnels = client.get("/funnels").json()["funnels"]
    assert not any(f["id"] == fid for f in funnels)


def test_delete_unknown_funnel_returns_404(client: TestClient) -> None:
    r = client.delete("/funnels/99999")
    assert r.status_code == 404


def test_update_funnel_returns_id_and_name(client: TestClient) -> None:
    create = client.post(
        "/funnels",
        json={
            "name": "to_update",
            "steps": [
                {"event_name": "signup", "filters": []},
                {"event_name": "search", "filters": []},
            ],
        },
    )
    fid = create.json()["id"]

    update = client.put(
        f"/funnels/{fid}",
        json={
            "name": "updated_funnel",
            "steps": [
                {"event_name": "signup", "filters": []},
                {"event_name": "purchase", "filters": [{"property_key": "country", "property_value": "US"}]},
            ],
        },
    )
    assert update.status_code == 200, update.text
    assert update.json()["name"] == "updated_funnel"

    # Verify list funnels returns the updated data including steps
    funnels = client.get("/funnels").json()["funnels"]
    target = next(f for f in funnels if f["id"] == fid)
    assert target["name"] == "updated_funnel"
    assert len(target["steps"]) == 2
    assert target["steps"][1]["event_name"] == "purchase"
    assert target["steps"][1]["filters"][0]["property_key"] == "country"
    assert target["steps"][1]["filters"][0]["property_value"] == "US"


# ---------------------------------------------------------------------------
# 2. Funnel execution correctness
# ---------------------------------------------------------------------------

def _create_simple_funnel(client: TestClient, name: str = "signup_search_purchase") -> int:
    r = client.post(
        "/funnels",
        json={
            "name": name,
            "steps": [
                {"event_name": "signup", "filters": []},
                {"event_name": "search", "filters": []},
                {"event_name": "purchase", "filters": []},
            ],
        },
    )
    assert r.status_code == 200, r.text
    return r.json()["id"]


def test_run_funnel_step_counts_are_correct(client: TestClient) -> None:
    _upload_funnel_dataset(client)
    fid = _create_simple_funnel(client)

    run = client.post("/funnels/run", json={"funnel_id": fid})
    assert run.status_code == 200, run.text

    body = run.json()
    all_users_result = next(
        r for r in body["results"] if r["cohort_name"] == "All Users"
    )
    steps = all_users_result["steps"]

    # Step 0: all 5 users signed up
    assert steps[0]["users"] == 5, f"Step 0 should be 5, got {steps[0]['users']}"

    # Step 1 (search, no filter): u1, u2, u4, u5 searched AFTER signup; u3 never searched
    # u5's early search is before signup so doesn't count, but u5's later search does
    assert steps[1]["users"] == 4, f"Step 1 should be 4, got {steps[1]['users']}"

    # Step 2 (purchase): u1, u4, u5 purchased after searching; u2 did not
    assert steps[2]["users"] == 3, f"Step 2 should be 3, got {steps[2]['users']}"


def test_run_funnel_ordering_is_enforced(client: TestClient) -> None:
    """Users who complete step 2 BEFORE step 1 (in time) must not be counted at step 2."""
    csv_text = (
        "user_id,event_name,event_time\n"
        # u1: purchase BEFORE signup → should NOT reach step 2
        "u1,purchase,2024-01-01 08:00:00\n"
        "u1,signup,2024-01-01 09:00:00\n"
        # u2: signup → purchase → correctly completes
        "u2,signup,2024-01-01 08:00:00\n"
        "u2,purchase,2024-01-01 09:00:00\n"
    )
    r = csv_upload(client, csv_text=csv_text)
    assert r.status_code == 200
    client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
        },
    )

    fid = client.post(
        "/funnels",
        json={
            "name": "order_test",
            "steps": [
                {"event_name": "signup", "filters": []},
                {"event_name": "purchase", "filters": []},
            ],
        },
    ).json()["id"]

    run = client.post("/funnels/run", json={"funnel_id": fid})
    assert run.status_code == 200, run.text

    all_users = next(
        r for r in run.json()["results"] if r["cohort_name"] == "All Users"
    )
    assert all_users["steps"][0]["users"] == 2   # both signed up
    assert all_users["steps"][1]["users"] == 1   # only u2 (u1's purchase was before signup)


def test_run_funnel_same_timestamp_events_are_not_counted_for_next_step(client: TestClient) -> None:
    """
    Same-timestamp events should not count for the next step because each step must be strictly later.
    """
    csv_text = (
        "user_id,event_name,event_time\n"
        # u1: signup and search at the EXACT same timestamp
        "u1,signup,2024-01-01 08:00:00\n"
        "u1,search,2024-01-01 08:00:00\n"   # same second as signup
        "u1,purchase,2024-01-01 09:00:00\n"
        # u2: normal ordering
        "u2,signup,2024-01-01 08:00:00\n"
        "u2,search,2024-01-01 09:00:00\n"
    )
    r = csv_upload(client, csv_text=csv_text)
    assert r.status_code == 200
    client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
        },
    )

    fid = client.post(
        "/funnels",
        json={
            "name": "same_ts_test",
            "steps": [
                {"event_name": "signup", "filters": []},
                {"event_name": "search", "filters": []},
                {"event_name": "purchase", "filters": []},
            ],
        },
    ).json()["id"]

    run = client.post("/funnels/run", json={"funnel_id": fid})
    assert run.status_code == 200, run.text

    all_users = next(
        r for r in run.json()["results"] if r["cohort_name"] == "All Users"
    )
    steps = all_users["steps"]
    # u1 and u2 both signed up, but only u2 searched after signup
    assert steps[0]["users"] == 2
    assert steps[1]["users"] == 1, f"Same-timestamp search should not count; got {steps[1]['users']}"
    # No one purchased after a qualifying search
    assert steps[2]["users"] == 0


def test_run_funnel_property_filter_reduces_step_count(client: TestClient) -> None:
    _upload_funnel_dataset(client)

    fid = client.post(
        "/funnels",
        json={
            "name": "filtered_search",
            "steps": [
                {"event_name": "signup", "filters": []},
                {
                    "event_name": "search",
                    "filters": [{"property_key": "query", "property_value": "amazon"}],
                },
                {"event_name": "purchase", "filters": []},
            ],
        },
    ).json()["id"]

    run = client.post("/funnels/run", json={"funnel_id": fid})
    assert run.status_code == 200, run.text

    all_users = next(
        r for r in run.json()["results"] if r["cohort_name"] == "All Users"
    )
    steps = all_users["steps"]
    # Step 0: 5 signed up
    assert steps[0]["users"] == 5
    # Step 1: u1, u2, u5 searched amazon AFTER signup (u4 searched google)
    assert steps[1]["users"] == 3
    # Step 2: u1 and u5 purchased after amazon search
    assert steps[2]["users"] == 2


def test_run_funnel_property_filter_type_safety(client: TestClient) -> None:
    """Issue #5: filters use CAST(col AS VARCHAR) for type-safe comparison."""
    csv_text = (
        "user_id,event_name,event_time,category\n"
        # category is a string column — CAST(category AS VARCHAR) = 'premium' should match
        "u1,start,2024-01-01 08:00:00,premium\n"
        "u1,finish,2024-01-01 09:00:00,premium\n"
        "u2,start,2024-01-01 08:00:00,free\n"
        "u2,finish,2024-01-01 09:00:00,free\n"
    )
    r = csv_upload(client, csv_text=csv_text)
    assert r.status_code == 200
    client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
        },
    )

    fid = client.post(
        "/funnels",
        json={
            "name": "type_safety_test",
            "steps": [
                {
                    "event_name": "start",
                    "filters": [{"property_key": "category", "property_value": "premium"}],
                },
                {"event_name": "finish", "filters": []},
            ],
        },
    ).json()["id"]

    run = client.post("/funnels/run", json={"funnel_id": fid})
    assert run.status_code == 200, run.text

    all_users = next(
        r for r in run.json()["results"] if r["cohort_name"] == "All Users"
    )
    # Only u1 (category=premium) qualifies at step 0; u2 (category=free) does not
    assert all_users["steps"][0]["users"] == 1, f"Expected 1, got {all_users['steps'][0]['users']}"
    assert all_users["steps"][1]["users"] == 1


def test_run_funnel_zero_users_at_step_is_handled(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,signup,2024-01-01 08:00:00\n"
    )
    r = csv_upload(client, csv_text=csv_text)
    assert r.status_code == 200
    client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
        },
    )

    fid = client.post(
        "/funnels",
        json={
            "name": "dead_end_funnel",
            "steps": [
                {"event_name": "signup", "filters": []},
                {"event_name": "purchase", "filters": []},
            ],
        },
    ).json()["id"]

    run = client.post("/funnels/run", json={"funnel_id": fid})
    assert run.status_code == 200
    all_users = next(
        r for r in run.json()["results"] if r["cohort_name"] == "All Users"
    )
    assert all_users["steps"][0]["users"] == 1
    assert all_users["steps"][1]["users"] == 0
    assert all_users["steps"][1]["conversion_pct"] == 0.0
    assert all_users["steps"][1]["dropoff_pct"] == 100.0


def test_run_funnel_conversion_and_dropoff_math(client: TestClient) -> None:
    _upload_funnel_dataset(client)
    fid = _create_simple_funnel(client, "math_test")

    run = client.post("/funnels/run", json={"funnel_id": fid})
    assert run.status_code == 200
    all_users = next(
        r for r in run.json()["results"] if r["cohort_name"] == "All Users"
    )
    steps = all_users["steps"]
    # Step 0: 100% conversion (base), 0% dropoff
    assert steps[0]["conversion_pct"] == 100.0
    assert steps[0]["dropoff_pct"] == 0.0

    # Step 1: 4/5 = 80% conversion from step 0; 1/5 = 20% dropoff
    assert abs(steps[1]["conversion_pct"] - 80.0) < 0.01
    assert abs(steps[1]["dropoff_pct"] - 20.0) < 0.01

    # Step 2: 3/5 = 60% conversion from step 0; 1/4 = 25% dropoff
    assert abs(steps[2]["conversion_pct"] - 60.0) < 0.01
    assert abs(steps[2]["dropoff_pct"] - 25.0) < 0.1


def test_run_funnel_returns_per_cohort_results(client: TestClient) -> None:
    _upload_funnel_dataset(client)

    client.post(
        "/cohorts",
        json={
            "name": "GB Users",
            "logic_operator": "AND",
            "conditions": [{"event_name": "signup", "min_event_count": 1}],
        },
    )

    fid = _create_simple_funnel(client, "cohort_test")
    run = client.post("/funnels/run", json={"funnel_id": fid})
    assert run.status_code == 200

    cohort_names = [r["cohort_name"] for r in run.json()["results"]]
    assert "All Users" in cohort_names
    assert "GB Users" in cohort_names


def test_run_unknown_funnel_returns_404(client: TestClient) -> None:
    r = client.post("/funnels/run", json={"funnel_id": 99999})
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# 3. Validity detection  (Issue #7: only event + property key, not values)
# ---------------------------------------------------------------------------

def test_funnel_is_invalid_when_event_not_in_dataset(client: TestClient) -> None:
    _upload_funnel_dataset(client)

    client.post(
        "/funnels",
        json={
            "name": "invalid_event_funnel",
            "steps": [
                {"event_name": "signup", "filters": []},
                {"event_name": "nonexistent_event_xyz", "filters": []},
            ],
        },
    )

    funnels = client.get("/funnels").json()["funnels"]
    target = next(f for f in funnels if f["name"] == "invalid_event_funnel")
    assert target["is_valid"] is False


def test_funnel_is_invalid_when_filter_property_key_not_in_dataset(client: TestClient) -> None:
    """Issue #7: invalid when filter key (column) is missing — not based on value sampling."""
    _upload_funnel_dataset(client)

    client.post(
        "/funnels",
        json={
            "name": "invalid_prop_key_funnel",
            "steps": [
                {"event_name": "signup", "filters": []},
                {
                    "event_name": "search",
                    "filters": [
                        {"property_key": "nonexistent_column_xyz", "property_value": "anything"}
                    ],
                },
            ],
        },
    )

    funnels = client.get("/funnels").json()["funnels"]
    target = next(f for f in funnels if f["name"] == "invalid_prop_key_funnel")
    assert target["is_valid"] is False


def test_funnel_is_valid_when_all_events_and_property_keys_exist(client: TestClient) -> None:
    _upload_funnel_dataset(client)

    client.post(
        "/funnels",
        json={
            "name": "valid_funnel",
            "steps": [
                {"event_name": "signup", "filters": []},
                {
                    "event_name": "search",
                    # "query" column exists in dataset — is_valid should be True
                    "filters": [{"property_key": "query", "property_value": "amazon"}],
                },
                {"event_name": "purchase", "filters": []},
            ],
        },
    )

    funnels = client.get("/funnels").json()["funnels"]
    target = next(f for f in funnels if f["name"] == "valid_funnel")
    # Property key "query" exists → valid regardless of whether "amazon" was sampled
    assert target["is_valid"] is True


def test_funnel_validity_does_not_depend_on_property_value(client: TestClient) -> None:
    """
    Issue #7: A funnel with a filter whose value is rare (wouldn't appear in a 500-sample)
    should still be marked VALID as long as the property KEY exists.
    """
    _upload_funnel_dataset(client)

    client.post(
        "/funnels",
        json={
            "name": "rare_value_funnel",
            "steps": [
                {"event_name": "signup", "filters": []},
                {
                    "event_name": "search",
                    "filters": [
                        # "query" column exists; value "nonexistent_val_xyz" does NOT appear in data
                        # but validity should still be True (key exists)
                        {"property_key": "query", "property_value": "nonexistent_val_xyz"}
                    ],
                },
            ],
        },
    )

    funnels = client.get("/funnels").json()["funnels"]
    target = next(f for f in funnels if f["name"] == "rare_value_funnel")
    # Valid because "query" is a real column — value existence is not checked
    assert target["is_valid"] is True


def test_list_funnels_without_dataset_marks_funnels_as_invalid(client: TestClient) -> None:
    """Funnels created before any data upload should gracefully show as invalid."""
    r = client.post(
        "/funnels",
        json={
            "name": "pre_upload_funnel",
            "steps": [
                {"event_name": "signup", "filters": []},
                {"event_name": "purchase", "filters": []},
            ],
        },
    )
    assert r.status_code == 200

    funnels = client.get("/funnels").json()["funnels"]
    target = next((f for f in funnels if f["name"] == "pre_upload_funnel"), None)
    assert target is not None
    assert target["is_valid"] is False


def test_create_and_list_funnel_with_conversion_window(client: TestClient) -> None:
    create = client.post(
        "/funnels",
        json={
            "name": "windowed_funnel",
            "conversion_window": {"value": 10, "unit": "minute"},
            "steps": [
                {"event_name": "signup", "filters": []},
                {"event_name": "purchase", "filters": []},
            ],
        },
    )
    assert create.status_code == 200, create.text

    funnels = client.get("/funnels").json()["funnels"]
    target = next(f for f in funnels if f["name"] == "windowed_funnel")
    assert target["conversion_window"] == {"value": 10, "unit": "minute"}


def test_run_funnel_with_conversion_window_within_limit_progresses(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,a,2024-01-01 10:00:00\n"
        "u1,b,2024-01-01 10:05:00\n"
        "u1,c,2024-01-01 10:09:00\n"
    )
    assert csv_upload(client, csv_text=csv_text).status_code == 200
    client.post(
        "/map-columns",
        json={"user_id_column": "user_id", "event_name_column": "event_name", "event_time_column": "event_time"},
    )

    fid = client.post(
        "/funnels",
        json={
            "name": "window_within",
            "conversion_window": {"value": 10, "unit": "minute"},
            "steps": [{"event_name": "a", "filters": []}, {"event_name": "b", "filters": []}, {"event_name": "c", "filters": []}],
        },
    ).json()["id"]
    run = client.post("/funnels/run", json={"funnel_id": fid})
    all_users = next(r for r in run.json()["results"] if r["cohort_name"] == "All Users")
    assert [s["users"] for s in all_users["steps"]] == [1, 1, 1]


def test_run_funnel_with_conversion_window_exceeds_limit_drops_user(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,a,2024-01-01 10:00:00\n"
        "u1,b,2024-01-01 10:05:00\n"
        "u1,c,2024-01-01 10:30:00\n"
    )
    assert csv_upload(client, csv_text=csv_text).status_code == 200
    client.post(
        "/map-columns",
        json={"user_id_column": "user_id", "event_name_column": "event_name", "event_time_column": "event_time"},
    )

    fid = client.post(
        "/funnels",
        json={
            "name": "window_exceeds",
            "conversion_window": {"value": 10, "unit": "minute"},
            "steps": [{"event_name": "a", "filters": []}, {"event_name": "b", "filters": []}, {"event_name": "c", "filters": []}],
        },
    ).json()["id"]
    run = client.post("/funnels/run", json={"funnel_id": fid})
    all_users = next(r for r in run.json()["results"] if r["cohort_name"] == "All Users")
    assert [s["users"] for s in all_users["steps"]] == [1, 1, 0]


def test_conversion_window_first_valid_path_picks_earliest_valid_match(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,a,2024-01-01 10:00:00\n"
        "u1,b,2024-01-01 10:20:00\n"
        "u1,b,2024-01-01 10:08:00\n"
        "u1,c,2024-01-01 10:12:00\n"
    )
    assert csv_upload(client, csv_text=csv_text).status_code == 200
    client.post(
        "/map-columns",
        json={"user_id_column": "user_id", "event_name_column": "event_name", "event_time_column": "event_time"},
    )
    fid = client.post(
        "/funnels",
        json={
            "name": "first_valid",
            "conversion_window": {"value": 10, "unit": "minute"},
            "steps": [{"event_name": "a", "filters": []}, {"event_name": "b", "filters": []}, {"event_name": "c", "filters": []}],
        },
    ).json()["id"]
    run = client.post("/funnels/run", json={"funnel_id": fid})
    all_users = next(r for r in run.json()["results"] if r["cohort_name"] == "All Users")
    assert [s["users"] for s in all_users["steps"]] == [1, 1, 1]
