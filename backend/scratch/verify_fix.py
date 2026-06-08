from __future__ import annotations
import json
from fastapi.testclient import TestClient
from app.main import app
from tests.utils import csv_upload, DeterministicTestClient

def verify_all():
    client = DeterministicTestClient(app)
    
    # 1. Upload and Map
    csv_text = (
        "user_id,event_name,event_time,country\n"
        "u1,signup,2026-01-01 09:00:00,US\n"
        "u1,search,2026-01-01 10:00:00,US\n"
        "u1,purchase,2026-01-01 11:00:00,US\n"
        "u2,signup,2026-01-01 09:30:00,CA\n"
        "u2,search,2026-01-01 10:30:00,CA\n"
    )
    upload = csv_upload(client, csv_text=csv_text)
    assert upload.status_code == 200
    upload_data = upload.json()
    
    print("Verification: Columns after upload:", upload_data["columns"])
    assert "row_id" not in upload_data["columns"], "row_id should be hidden in upload response"
    
    mapped = client.post(
        "/map-columns",
        json={
            "user_id_column": "user_id",
            "event_name_column": "event_name",
            "event_time_column": "event_time",
        },
    )
    assert mapped.status_code == 200
    
    # 2. Check /columns metadata
    cols_resp = client.get("/columns")
    assert cols_resp.status_code == 200
    cols_data = cols_resp.json()
    col_names = [c["name"] for c in cols_data["columns"]]
    print("Verification: Columns in metadata:", col_names)
    assert "row_id" not in col_names, "row_id should be hidden in /columns metadata"
    
    # 3. Check Flow L1
    flow_resp = client.get("/flow/l1?start_event=signup&direction=forward&depth=5&include_top_k=true&limit=3")
    print(f"Verification: Flow L1 status: {flow_resp.status_code}")
    if flow_resp.status_code != 200:
        print(f"Error detail: {flow_resp.text}")
    assert flow_resp.status_code == 200
    
    flow_data = flow_resp.json()
    print(f"Verification: Flow L1 top level nodes: {[r['path'] for r in flow_data['rows']]}")
    assert len(flow_data["rows"]) > 0
    
    print("Verification SUCCESS: All checks passed.")

if __name__ == "__main__":
    verify_all()
