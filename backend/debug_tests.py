from fastapi.testclient import TestClient
from app.main import app
import json

client = TestClient(app)

def csv_upload(client, csv_text):
    return client.post("/upload-csv", files={"file": ("test.csv", csv_text)})

# u1: joins at 10:00. 
csv_text = (
    "user_id,event_name,event_time,revenue\n"
    "u1,registration,2026-01-01 10:00:00,0\n"
    "u1,app_open,2026-01-01 09:59:59,0\n"
    "u1,app_open,2026-01-01 10:00:01,0\n"
    "u1,app_open,2026-01-01 23:00:00,0\n"
    "u1,purchase,2026-01-01 11:00:00,10.0\n"
    "u1,app_open,2026-01-02 09:59:59,0\n"
    "u1,app_open,2026-01-02 10:00:01,0\n"
    "u2,registration,2026-01-01 10:00:00,0\n"
)
resp = csv_upload(client, csv_text)
print(f"UPLOAD: {resp.status_code}")

mapped = client.post("/map-columns", json={
    "user_id_column": "user_id",
    "event_name_column": "event_name",
    "event_time_column": "event_time",
    "revenue_column": "revenue",
    "column_types": {
        "user_id": "TEXT",
        "event_name": "TEXT",
        "event_time": "TIMESTAMP",
        "revenue": "NUMERIC"
    }
})
print(f"MAPPED: {mapped.status_code}")

client.post("/cohorts", json={
    "name": "Cohort A",
    "logic_operator": "AND",
    "conditions": [{"event_name": "purchase", "min_event_count": 1}]
})

client.post("/cohorts", json={
    "name": "Cohort B Precise",
    "logic_operator": "AND",
    "conditions": [
        {"event_name": "purchase", "min_event_count": 0, "max_event_count": 0}
    ]
})

cohorts = client.get("/cohorts").json()
print(f"COHORTS: {cohorts}")

try:
    ret_resp = client.get("/retention?max_day=1&retention_event=app_open")
    print(f"RETENTION: {ret_resp.status_code}")
    print(f"RETENTION BODY: {ret_resp.json()}")
except Exception as e:
    print(f"RETENTION FAILED: {e}")
