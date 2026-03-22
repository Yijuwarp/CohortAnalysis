"""
Tests for the Flow Analytics module (GET /flow/l1, GET /flow/l2).

Covers:
  1. Basic forward L1 – correct top events and percentages
  2. Reverse L1 – correct previous events
  3. L2 expansion – correct second step, respects parent_event
  4. Multi-cohort correctness – different cohorts return different values
  5. First-occurrence logic – multiple start_events per user → only first counted
  6. Self-loop exclusion – event → same event not present
  7. "Other" calculation – correct remainder, excluded from expansion
  8. Sorting – sorted by pct desc
  9. Empty cases – no transitions → empty rows
 10. Invalid direction → 400 error
"""
from __future__ import annotations

from fastapi.testclient import TestClient

from tests.utils import csv_upload


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

def _upload_and_map(client: TestClient, csv_text: str) -> None:
    """Upload CSV and perform column mapping."""
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
    assert mapping.status_code == 200, f"Map-columns failed: {mapping.text}"


def _make_cohort(client: TestClient, name: str, event: str) -> int:
    """Create a basic cohort by event name; return cohort_id."""
    resp = client.post(
        "/cohorts",
        json={
            "name": name,
            "logic_operator": "AND",
            "conditions": [{"event_name": event, "min_event_count": 1}],
        },
    )
    assert resp.status_code == 200, resp.text
    return int(resp.json()["cohort_id"])


def _value_pct(value: dict) -> float:
    parent_users = float(value.get("parent_users", 0) or 0)
    if parent_users <= 0:
        return 0.0
    return float(value.get("user_count", 0) or 0) / parent_users


# ---------------------------------------------------------------------------
# Test 1: Basic forward L1
# ---------------------------------------------------------------------------

def test_l1_forward_basic_top_events_and_percentages(client: TestClient) -> None:
    """
    3 users do 'search'. After search:
      u1 → product_view
      u2 → product_view
      u3 → checkout

    Expected L1 forward from 'search':
      product_view: 2/3 ≈ 0.666667
      checkout:     1/3 ≈ 0.333333
    """
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,search,2024-01-01 10:00:00\n"
        "u1,product_view,2024-01-01 10:01:00\n"
        "u2,search,2024-01-01 10:00:00\n"
        "u2,product_view,2024-01-01 10:02:00\n"
        "u3,search,2024-01-01 10:00:00\n"
        "u3,checkout,2024-01-01 10:03:00\n"
    )
    _upload_and_map(client, csv_text)

    resp = client.get("/flow/l1?start_event=search&direction=forward")
    assert resp.status_code == 200, resp.text
    payload = resp.json()

    rows = payload["rows"]
    assert len(rows) >= 2, f"Expected at least 2 rows, got: {rows}"

    # Find All Users cohort_id (should be the first cohort created by map-columns)
    # Get all cohorts to find ID
    cohorts_resp = client.get("/cohorts")
    cohorts = cohorts_resp.json()["cohorts"]
    all_users = next(c for c in cohorts if c["cohort_name"] == "All Users")
    cid = str(all_users["cohort_id"])

    by_event = {row["path"][-1]: row for row in rows if row["path"][-1] != "Other"}

    assert "product_view" in by_event, f"Expected product_view in rows: {[r['path'] for r in rows]}"
    assert "checkout" in by_event, f"Expected checkout in rows: {[r['path'] for r in rows]}"

    pv_row = by_event["product_view"]
    co_row = by_event["checkout"]

    assert pv_row["values"][cid]["user_count"] == 2
    assert abs(_value_pct(pv_row["values"][cid]) - 2 / 3) < 1e-4

    assert co_row["values"][cid]["user_count"] == 1
    assert abs(_value_pct(co_row["values"][cid]) - 1 / 3) < 1e-4

    # product_view should appear first (higher pct)
    event_names = [r["path"][-1] for r in rows if r["path"][-1] != "Other"]
    assert event_names.index("product_view") < event_names.index("checkout")


# ---------------------------------------------------------------------------
# Test 2: Reverse L1
# ---------------------------------------------------------------------------

def test_l1_reverse_correct_previous_events(client: TestClient) -> None:
    """
    Before 'checkout':
      u1: login → search → checkout  →  most recent previous = search
      u2: login → checkout           →  most recent previous = login
      u3: search → checkout          →  most recent previous = search

    Expected L1 reverse from 'checkout':
      search: 2/3
      login:  1/3
    """
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,login,2024-01-01 09:00:00\n"
        "u1,search,2024-01-01 09:30:00\n"
        "u1,checkout,2024-01-01 10:00:00\n"
        "u2,login,2024-01-01 09:00:00\n"
        "u2,checkout,2024-01-01 10:00:00\n"
        "u3,search,2024-01-01 09:00:00\n"
        "u3,checkout,2024-01-01 10:00:00\n"
    )
    _upload_and_map(client, csv_text)

    resp = client.get("/flow/l1?start_event=checkout&direction=reverse")
    assert resp.status_code == 200, resp.text
    rows = resp.json()["rows"]

    cohorts_resp = client.get("/cohorts")
    all_users = next(c for c in cohorts_resp.json()["cohorts"] if c["cohort_name"] == "All Users")
    cid = str(all_users["cohort_id"])

    by_event = {r["path"][-1]: r for r in rows if r["path"][-1] != "Other"}

    assert "search" in by_event
    assert by_event["search"]["values"][cid]["user_count"] == 2
    assert abs(_value_pct(by_event["search"]["values"][cid]) - 2 / 3) < 1e-4

    assert "login" in by_event
    assert by_event["login"]["values"][cid]["user_count"] == 1
    assert abs(_value_pct(by_event["login"]["values"][cid]) - 1 / 3) < 1e-4


# ---------------------------------------------------------------------------
# Test 3: L2 expansion
# ---------------------------------------------------------------------------

def test_l2_expansion_respects_parent_event(client: TestClient) -> None:
    """
    3 users: search → product_view → <next>
      u1: search → product_view → checkout
      u2: search → product_view → add_to_cart
      u3: search → checkout       (does NOT pass through product_view)

    L2(start=search, parent=product_view, forward):
      checkout:   1/2 = 0.5
      add_to_cart: 1/2 = 0.5
    """
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,search,2024-01-01 10:00:00\n"
        "u1,product_view,2024-01-01 10:01:00\n"
        "u1,checkout,2024-01-01 10:02:00\n"
        "u2,search,2024-01-01 10:00:00\n"
        "u2,product_view,2024-01-01 10:01:00\n"
        "u2,add_to_cart,2024-01-01 10:02:00\n"
        "u3,search,2024-01-01 10:00:00\n"
        "u3,checkout,2024-01-01 10:01:00\n"
    )
    _upload_and_map(client, csv_text)

    resp = client.get(
        "/flow/l2?start_event=search&parent_event=product_view&direction=forward"
    )
    assert resp.status_code == 200, resp.text
    payload = resp.json()

    assert payload["parent_path"] == ["search", "product_view"]
    rows = payload["rows"]

    cohorts_resp = client.get("/cohorts")
    all_users = next(c for c in cohorts_resp.json()["cohorts"] if c["cohort_name"] == "All Users")
    cid = str(all_users["cohort_id"])

    by_event = {r["path"][-1]: r for r in rows if r["path"][-1] != "Other"}

    assert "checkout" in by_event, f"Expected checkout in L2: {[r['path'] for r in rows]}"
    assert "add_to_cart" in by_event, f"Expected add_to_cart in L2: {[r['path'] for r in rows]}"

    assert by_event["checkout"]["values"][cid]["user_count"] == 1
    assert abs(_value_pct(by_event["checkout"]["values"][cid]) - 0.5) < 1e-4

    assert by_event["add_to_cart"]["values"][cid]["user_count"] == 1
    assert abs(_value_pct(by_event["add_to_cart"]["values"][cid]) - 0.5) < 1e-4

    # u3 should NOT be included (their L1 was checkout, not product_view)
    total_users = sum(r["values"][cid]["user_count"] for r in rows)
    assert total_users == 2, f"Expected 2 total users in L2 expansion, got {total_users}"


# ---------------------------------------------------------------------------
# Test 4: Multi-cohort correctness
# ---------------------------------------------------------------------------

def test_l1_forward_multi_cohort_different_values(client: TestClient) -> None:
    """
    Two cohorts:
      - Cohort A (did 'login'):  u1, u2
      - Cohort B (did 'signup'): u3

    After 'search':
      u1: search → purchase   (in cohort A)
      u2: search → purchase   (in cohort A)
      u3: search → browse     (in cohort B)

    Expected:
      Cohort A: purchase pct ≈ 1.0 (2/2)
      Cohort B: browse pct ≈ 1.0   (1/1)
    """
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,login,2024-01-01 09:00:00\n"
        "u1,search,2024-01-01 10:00:00\n"
        "u1,purchase,2024-01-01 10:01:00\n"
        "u2,login,2024-01-01 09:00:00\n"
        "u2,search,2024-01-01 10:00:00\n"
        "u2,purchase,2024-01-01 10:01:00\n"
        "u3,signup,2024-01-01 09:00:00\n"
        "u3,search,2024-01-01 10:00:00\n"
        "u3,browse,2024-01-01 10:01:00\n"
    )
    _upload_and_map(client, csv_text)

    cid_a = _make_cohort(client, "login_users", "login")
    cid_b = _make_cohort(client, "signup_users", "signup")

    resp = client.get("/flow/l1?start_event=search&direction=forward")
    assert resp.status_code == 200, resp.text
    rows = resp.json()["rows"]

    by_event = {r["path"][-1]: r for r in rows if r["path"][-1] != "Other"}

    # Cohort A sees purchase
    if "purchase" in by_event:
        cid_a_str = str(cid_a)
        assert by_event["purchase"]["values"][cid_a_str]["user_count"] == 2
        assert abs(_value_pct(by_event["purchase"]["values"][cid_a_str]) - 1.0) < 1e-4

    # Cohort B sees browse
    if "browse" in by_event:
        cid_b_str = str(cid_b)
        assert by_event["browse"]["values"][cid_b_str]["user_count"] == 1
        assert abs(_value_pct(by_event["browse"]["values"][cid_b_str]) - 1.0) < 1e-4


# ---------------------------------------------------------------------------
# Test 5: First-occurrence logic
# ---------------------------------------------------------------------------

def test_l1_forward_first_occurrence_only(client: TestClient) -> None:
    """
    u1 performs 'search' three times.
    Only the FIRST occurrence should be used as the anchor.
      - After first search: product_view
      - After second search: checkout
      - After third search: purchase

    Expected: only product_view (the event immediately after first search)
    is counted. The anchor count is 1 (not 3).
    """
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,search,2024-01-01 10:00:00\n"
        "u1,product_view,2024-01-01 10:01:00\n"
        "u1,search,2024-01-01 10:02:00\n"
        "u1,checkout,2024-01-01 10:03:00\n"
        "u1,search,2024-01-01 10:04:00\n"
        "u1,purchase,2024-01-01 10:05:00\n"
    )
    _upload_and_map(client, csv_text)

    resp = client.get("/flow/l1?start_event=search&direction=forward")
    assert resp.status_code == 200, resp.text
    rows = resp.json()["rows"]

    cohorts_resp = client.get("/cohorts")
    all_users = next(c for c in cohorts_resp.json()["cohorts"] if c["cohort_name"] == "All Users")
    cid = str(all_users["cohort_id"])

    # Only the transition from the FIRST search occurrence should count
    by_event = {r["path"][-1]: r for r in rows if r["path"][-1] != "Other"}

    # product_view is the first event after the first search
    assert "product_view" in by_event
    assert by_event["product_view"]["values"][cid]["user_count"] == 1
    # pct should be 1.0 (1/1 users)
    assert abs(_value_pct(by_event["product_view"]["values"][cid]) - 1.0) < 1e-4

    # Total transitions should equal 1 (only one user, one transition)
    total = sum(r["values"][cid]["user_count"] for r in rows)
    assert total == 1, f"Expected 1 total transition (first occurrence only), got {total}"


# ---------------------------------------------------------------------------
# Test 6: Self-loop exclusion
# ---------------------------------------------------------------------------

def test_l1_forward_excludes_self_loops(client: TestClient) -> None:
    """
    u1: search → search → product_view
    The first transition after 'search' must NOT be another 'search'.
    Expected: product_view (skip the self-loop).
    """
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,search,2024-01-01 10:00:00\n"
        "u1,search,2024-01-01 10:00:30\n"
        "u1,product_view,2024-01-01 10:01:00\n"
    )
    _upload_and_map(client, csv_text)

    resp = client.get("/flow/l1?start_event=search&direction=forward")
    assert resp.status_code == 200, resp.text
    rows = resp.json()["rows"]

    event_names = [r["path"][-1] for r in rows]
    assert "search" not in event_names, \
        f"Self-loop 'search' should not appear in rows: {event_names}"


# ---------------------------------------------------------------------------
# Test 7: "Other" calculation
# ---------------------------------------------------------------------------

def test_l1_forward_other_bucket_correct_and_not_expandable(client: TestClient) -> None:
    """
    4 users after 'start':
      u1 → event_a (top 1)
      u2 → event_b (top 2)
      u3 → event_c (top 3)
      u4 → event_d (goes to Other)

    "Other" count = 1, pct = 1/4 = 0.25, expandable = False.
    """
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,start,2024-01-01 10:00:00\n"
        "u1,event_a,2024-01-01 10:01:00\n"
        "u2,start,2024-01-01 10:00:00\n"
        "u2,event_b,2024-01-01 10:01:00\n"
        "u3,start,2024-01-01 10:00:00\n"
        "u3,event_c,2024-01-01 10:01:00\n"
        "u4,start,2024-01-01 10:00:00\n"
        "u4,event_d,2024-01-01 10:01:00\n"
    )
    _upload_and_map(client, csv_text)

    resp = client.get("/flow/l1?start_event=start&direction=forward")
    assert resp.status_code == 200, resp.text
    rows = resp.json()["rows"]

    cohorts_resp = client.get("/cohorts")
    all_users = next(c for c in cohorts_resp.json()["cohorts"] if c["cohort_name"] == "All Users")
    cid = str(all_users["cohort_id"])

    other_rows = [r for r in rows if r["path"][-1] == "Other"]
    assert len(other_rows) == 1, f"Expected exactly one 'Other' row, got: {[r['path'] for r in rows]}"

    other_row = other_rows[0]
    assert other_row["expandable"] is False
    assert other_row["values"][cid]["user_count"] == 1
    assert abs(_value_pct(other_row["values"][cid]) - 0.25) < 1e-4

    # Named rows should include top 3 + No further action
    named_rows = [r for r in rows if r["path"][-1] != "Other"]
    assert len(named_rows) == 4, f"Expected 4 named rows (including No further action), got {len(named_rows)}"

    # Top rows should be expandable; No further action should not.
    for r in named_rows:
        if r["path"][-1] == "No further action":
            assert r["expandable"] is False
        else:
            assert r["expandable"] is True


# ---------------------------------------------------------------------------
# Test 8: Sorting by pct desc
# ---------------------------------------------------------------------------

def test_l1_forward_sorted_by_pct_descending(client: TestClient) -> None:
    """
    4 users after 'home':
      u1, u2, u3 → search   (3/4 = 0.75)
      u4          → browse   (1/4 = 0.25)

    Rows should be sorted: search first, browse second.
    """
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,home,2024-01-01 09:00:00\n"
        "u1,search,2024-01-01 09:01:00\n"
        "u2,home,2024-01-01 09:00:00\n"
        "u2,search,2024-01-01 09:01:00\n"
        "u3,home,2024-01-01 09:00:00\n"
        "u3,search,2024-01-01 09:01:00\n"
        "u4,home,2024-01-01 09:00:00\n"
        "u4,browse,2024-01-01 09:01:00\n"
    )
    _upload_and_map(client, csv_text)

    resp = client.get("/flow/l1?start_event=home&direction=forward")
    assert resp.status_code == 200, resp.text
    rows = resp.json()["rows"]

    event_names = [r["path"][-1] for r in rows]
    assert event_names[0] == "search", \
        f"Expected 'search' first (highest pct), got: {event_names}"
    assert event_names[1] == "browse", \
        f"Expected 'browse' second, got: {event_names}"


# ---------------------------------------------------------------------------
# Test 9: Empty cases
# ---------------------------------------------------------------------------

def test_l1_forward_empty_when_no_transitions(client: TestClient) -> None:
    """
    Only one event per user – no transitions possible.
    """
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,search,2024-01-01 10:00:00\n"
        "u2,search,2024-01-01 10:00:00\n"
    )
    _upload_and_map(client, csv_text)

    resp = client.get("/flow/l1?start_event=search&direction=forward")
    assert resp.status_code == 200, resp.text
    assert resp.json()["rows"] == []


def test_l1_forward_empty_when_no_cohorts(client: TestClient) -> None:
    """No data uploaded → empty rows."""
    resp = client.get("/flow/l1?start_event=search&direction=forward")
    assert resp.status_code == 200, resp.text
    assert resp.json()["rows"] == []


def test_l2_empty_when_parent_event_not_reached(client: TestClient) -> None:
    """
    All users go search → checkout directly (no product_view).
    L2 with parent=product_view should return empty rows.
    """
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,search,2024-01-01 10:00:00\n"
        "u1,checkout,2024-01-01 10:01:00\n"
    )
    _upload_and_map(client, csv_text)

    resp = client.get(
        "/flow/l2?start_event=search&parent_event=product_view&direction=forward"
    )
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload["parent_path"] == ["search", "product_view"]
    assert payload["rows"] == []


# ---------------------------------------------------------------------------
# Test 10: Invalid direction
# ---------------------------------------------------------------------------

def test_l1_invalid_direction_returns_400(client: TestClient) -> None:
    """Non-existent direction should return HTTP 400."""
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,search,2024-01-01 10:00:00\n"
    )
    _upload_and_map(client, csv_text)

    resp = client.get("/flow/l1?start_event=search&direction=sideways")
    assert resp.status_code == 400, resp.text


def test_l2_invalid_direction_returns_400(client: TestClient) -> None:
    """Non-existent direction should return HTTP 400."""
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,search,2024-01-01 10:00:00\n"
    )
    _upload_and_map(client, csv_text)

    resp = client.get(
        "/flow/l2?start_event=search&parent_event=login&direction=left"
    )
    assert resp.status_code == 400, resp.text


# ---------------------------------------------------------------------------
# Test 11: L1 path structure is correct
# ---------------------------------------------------------------------------

def test_l1_path_structure_is_correct(client: TestClient) -> None:
    """Verify that forward L1 paths have exactly 2 elements: [start_event, next_event]."""
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,home,2024-01-01 10:00:00\n"
        "u1,search,2024-01-01 10:01:00\n"
    )
    _upload_and_map(client, csv_text)

    resp = client.get("/flow/l1?start_event=home&direction=forward")
    assert resp.status_code == 200, resp.text
    rows = resp.json()["rows"]

    assert len(rows) >= 1
    for row in rows:
        assert len(row["path"]) == 2, f"L1 path should have 2 elements: {row['path']}"
        assert row["path"][0] == "home", f"First path element should be start_event: {row['path']}"


# ---------------------------------------------------------------------------
# Test 12: L2 path structure is correct
# ---------------------------------------------------------------------------

def test_l2_path_structure_is_correct(client: TestClient) -> None:
    """Verify that L2 paths have exactly 3 elements: [start, parent, next]."""
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,home,2024-01-01 10:00:00\n"
        "u1,search,2024-01-01 10:01:00\n"
        "u1,product_view,2024-01-01 10:02:00\n"
    )
    _upload_and_map(client, csv_text)

    resp = client.get(
        "/flow/l2?start_event=home&parent_event=search&direction=forward"
    )
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    rows = payload["rows"]

    assert len(rows) >= 1
    for row in rows:
        assert len(row["path"]) == 3, f"L2 path should have 3 elements: {row['path']}"
        assert row["path"][0] == "home"
        assert row["path"][1] == "search"


# ---------------------------------------------------------------------------
# Test 13: Expandability flags
# ---------------------------------------------------------------------------

def test_l1_named_rows_are_expandable_other_is_not(client: TestClient) -> None:
    """
    Ensure top-3 rows have expandable=True, Other has expandable=False.
    """
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,start,2024-01-01 10:00:00\n"
        "u1,a,2024-01-01 10:01:00\n"
        "u2,start,2024-01-01 10:00:00\n"
        "u2,b,2024-01-01 10:01:00\n"
        "u3,start,2024-01-01 10:00:00\n"
        "u3,c,2024-01-01 10:01:00\n"
        "u4,start,2024-01-01 10:00:00\n"
        "u4,d,2024-01-01 10:01:00\n"
    )
    _upload_and_map(client, csv_text)

    resp = client.get("/flow/l1?start_event=start&direction=forward")
    assert resp.status_code == 200, resp.text
    rows = resp.json()["rows"]

    for row in rows:
        if row["path"][-1] == "Other":
            assert row["expandable"] is False, "Other row must not be expandable"
        elif row["path"][-1] == "No further action":
            assert row["expandable"] is False, "No further action row must not be expandable"
        else:
            assert row["expandable"] is True, f"Named row {row['path']} should be expandable"


# ---------------------------------------------------------------------------
# Test 14: Hidden cohorts are excluded
# ---------------------------------------------------------------------------

def test_l1_forward_excludes_hidden_cohorts(client: TestClient) -> None:
    """Hidden cohorts must not appear in flow values."""
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,search,2024-01-01 10:00:00\n"
        "u1,product_view,2024-01-01 10:01:00\n"
    )
    _upload_and_map(client, csv_text)

    cid = _make_cohort(client, "searchers", "search")
    hide = client.patch(f"/cohorts/{cid}/hide")
    assert hide.status_code == 200, hide.text

    resp = client.get("/flow/l1?start_event=search&direction=forward")
    assert resp.status_code == 200, resp.text
    rows = resp.json()["rows"]

    for row in rows:
        assert str(cid) not in row["values"], \
            f"Hidden cohort {cid} should not appear in values"


# ---------------------------------------------------------------------------
# Test 15: L2 denominator is based on L1 parent users (not start_event users)
# ---------------------------------------------------------------------------

def test_l2_denominator_is_l1_parent_users(client: TestClient) -> None:
    """
    4 users all do 'search'.
    2 go to 'product_view' (these become the L2 denominator).
    1 of those 2 then goes to 'purchase'.

    Expected L2 purchase pct = 1/2 = 0.5, NOT 1/4 = 0.25.
    """
    csv_text = (
        "user_id,event_name,event_time\n"
        "u1,search,2024-01-01 10:00:00\n"
        "u1,product_view,2024-01-01 10:01:00\n"
        "u1,purchase,2024-01-01 10:02:00\n"
        "u2,search,2024-01-01 10:00:00\n"
        "u2,product_view,2024-01-01 10:01:00\n"
        "u3,search,2024-01-01 10:00:00\n"
        "u3,checkout,2024-01-01 10:01:00\n"
        "u4,search,2024-01-01 10:00:00\n"
        "u4,checkout,2024-01-01 10:01:00\n"
    )
    _upload_and_map(client, csv_text)

    resp = client.get(
        "/flow/l2?start_event=search&parent_event=product_view&direction=forward"
    )
    assert resp.status_code == 200, resp.text
    rows = resp.json()["rows"]

    cohorts_resp = client.get("/cohorts")
    all_users = next(c for c in cohorts_resp.json()["cohorts"] if c["cohort_name"] == "All Users")
    cid = str(all_users["cohort_id"])

    by_event = {r["path"][-1]: r for r in rows if r["path"][-1] != "Other"}

    assert "purchase" in by_event
    purchase_pct = _value_pct(by_event["purchase"]["values"][cid])
    # Should be 1/2 (only 2 users reached product_view)
    assert abs(purchase_pct - 0.5) < 1e-4, \
        f"Expected pct ≈ 0.5 (1/2), got {purchase_pct}"
