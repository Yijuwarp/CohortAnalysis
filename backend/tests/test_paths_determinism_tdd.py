import pytest
import duckdb
import pandas as pd
from app.domains.paths.paths_service import (
    ensure_path_tables, run_paths, PathStep, PathStepGroup
)
from app.domains.ingestion.upload_service import upload_csv
from app.domains.ingestion.mapping_service import map_columns
from app.models.ingestion_models import ColumnMappingRequest
from app.domains.cohorts.cohort_service import ensure_cohort_tables
from fastapi import UploadFile
import io

@pytest.fixture
def conn():
    c = duckdb.connect(":memory:")
    ensure_cohort_tables(c)
    ensure_path_tables(c)
    return c

def create_upload_file(content: str, filename: str):
    return UploadFile(filename=filename, file=io.BytesIO(content.encode('utf-8')))

import asyncio

def test_aba_sequence_determinism(conn):
    """
    Test Case: A -> B -> A at the same timestamp.
    Current expected outcome: FAILURE (A -> B -> A will NOT be found because of aggregation leak).
    Target outcome: SUCCESS (After fix, each event is preserved and sequence is found).
    """
    csv_content = (
        "user_id,event_name,event_time\n"
        "u1,A,2024-01-01 10:00:00\n"
        "u1,B,2024-01-01 10:00:00\n"
        "u1,A,2024-01-01 10:00:00\n"
    )
    
    # 1. Upload
    asyncio.run(upload_csv(conn, create_upload_file(csv_content, "test.csv")))
    
    # 2. Map Columns (Aggregation happens here currently)
    mapping = ColumnMappingRequest(
        user_id_column="user_id",
        event_name_column="event_name",
        event_time_column="event_time",
        column_types={
            "user_id": "TEXT",
            "event_name": "TEXT",
            "event_time": "TIMESTAMP"
        }
    )
    map_columns(conn, mapping)
    
    # 3. Verify Aggregation Leak (Red Part)
    # If the leak exists, events_normalized will have only 2 rows (one for A with count=2, one for B)
    norm_rows = conn.execute("SELECT event_name, event_count FROM events_normalized ORDER BY event_name").fetchall()
    print(f"\nNormalized rows: {norm_rows}")
    
    # 4. Run Path Analysis: A -> B -> A
    steps = [
        PathStep(step_order=0, groups=[PathStepGroup(event_name='A')]),
        PathStep(step_order=1, groups=[PathStepGroup(event_name='B')]),
        PathStep(step_order=2, groups=[PathStepGroup(event_name='A')])
    ]
    
    res = run_paths(conn, steps)
    
    # We expect 1 user to reach Step 3
    # CURRENT BEHAVIOR: Step 3 will have 0 users.
    reached_step_3 = 0
    if res["results"]:
        reached_step_3 = res["results"][0].steps[2].users
        
    print(f"Users reached step 3: {reached_step_3}")
    
    # This assertion will FAIL before the fix
    assert reached_step_3 == 1, "Should find A -> B -> A sequence even at the same timestamp"

