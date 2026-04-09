from __future__ import annotations
import pytest
from fastapi.testclient import TestClient
from tests.utils import csv_upload

# Valid user_id according to re.match(r"^[a-f0-9]{8}$", user_id)
VALID_USER_ID = "abcdef12"

def _prepare_data(client: TestClient) -> None:
    csv_text = (
        "user_id,event_name,event_time,source\n"
        "u1,open,2026-01-01 10:00:00,web\n"
        "u2,open,2026-01-01 10:00:00,app\n"
    )
    # Upload data
    client.post(
        f"/upload?user_id={VALID_USER_ID}",
        files={"file": ("events.csv", csv_text.encode("utf-8"), "text/csv")},
    )
    # Map columns
    client.post(
        f"/map-columns?user_id={VALID_USER_ID}",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
        },
    )

def test_scalar_list_validation_fix(client: TestClient) -> None:
    _prepare_data(client)
    
    # Send a request with a list containing a single value for "=" operator
    payload = {
        "name": "Test Scalar List Fix",
        "logic_operator": "AND",
        "conditions": [
            {
                "event_name": "open",
                "min_event_count": 1,
                "property_filter": {
                    "column": "source",
                    "operator": "=",
                    "values": ["web"]
                }
            }
        ]
    }
    
    # 1. Test estimation
    resp = client.post(f"/cohorts/estimate?user_id={VALID_USER_ID}", json=payload)
    assert resp.status_code == 200, resp.text
    assert resp.json()["estimated_users"] == 1
    
    # 2. Test saving
    saved_resp = client.post(f"/saved-cohorts?user_id={VALID_USER_ID}", json=payload)
    assert saved_resp.status_code == 200, saved_resp.text
    
    # 3. Test filter service (global filter)
    filter_payload = {
        "date_range": None,
        "filters": [
            {
                "column": "source",
                "operator": "=",
                "value": ["app"]
            }
        ]
    }
    filter_resp = client.post(f"/apply-filters?user_id={VALID_USER_ID}", json=filter_payload)
    assert filter_resp.status_code == 200, filter_resp.text
