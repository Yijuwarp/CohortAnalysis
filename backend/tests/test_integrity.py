import pytest
import duckdb
import asyncio
from app.domains.ingestion.upload_service import upload_csv
from app.domains.ingestion.mapping_service import map_columns
from app.models.ingestion_models import ColumnMappingRequest
from app.domains.cohorts.cohort_service import ensure_cohort_tables, create_cohort
from app.models.cohort_models import CreateCohortRequest, CohortCondition
from app.domains.paths.paths_service import ensure_path_tables, run_paths, PathStep, PathStepGroup
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

def test_pipeline_integrity(conn):
    """
    Test Case:
    1. ABA sequence at same timestamp (Separation check)
    2. Cohort size with duplicates (Inflation check)
    """
    csv_content = (
        "user_id,event_name,event_time\n"
        "u1,A,2024-01-01 10:00:00\n"
        "u1,B,2024-01-01 10:00:00\n"
        "u1,A,2024-01-01 10:00:00\n"
    )
    
    # --- INGESTION ---
    asyncio.run(upload_csv(conn, create_upload_file(csv_content, "test.csv")))
    mapping = ColumnMappingRequest(
        user_id_column="user_id",
        event_name_column="event_name",
        event_time_column="event_time",
        column_types={"user_id": "TEXT", "event_name": "TEXT", "event_time": "TIMESTAMP"}
    )
    map_columns(conn, mapping)
    
    # --- DEBUG TABLE CONTENT ---
    print("\n--- EVENTS_RAW ---")
    print(conn.execute("SELECT * FROM events_raw").df())
    print("\n--- EVENTS_NORMALIZED ---")
    print(conn.execute("SELECT * FROM events_normalized").df())
    
    # --- PART 1: SEQUENCING (Expect Success) ---

    steps = [
        PathStep(step_order=0, groups=[PathStepGroup(event_name='A')]),
        PathStep(step_order=1, groups=[PathStepGroup(event_name='B')]),
        PathStep(step_order=2, groups=[PathStepGroup(event_name='A')])
    ]
    res = run_paths(conn, steps)
    reached_step_3 = res["results"][0].steps[2].users if res["results"] else 0
    print(f"\nSequencing: A -> B -> A found {reached_step_3} users (Expected: 1)")
    assert reached_step_3 == 1, "Sequencing failed to find ABA"

    # --- PART 2: COHORT INTEGRITY (Expect No Inflation) ---
    # Create a cohort for event A.
    # Total A's in raw = 2
    # Total A's in normalized = 1 (with count=2)
    # Correct Cohort Engine logic should treat this as ONE match per user if using aggregated logic.
    payload = CreateCohortRequest(
        name="Users with A",
        conditions=[CohortCondition(event_name='A', min_event_count=1)],
        logic_operator="AND",
        join_type="first_event"
    )

    cohort_id = create_cohort(conn, payload)["cohort_id"]
    
    # Check Cohort Size for our specific cohort
    cohort_size = conn.execute("SELECT COUNT(*) FROM cohort_membership WHERE cohort_id = ?", [cohort_id]).fetchone()[0]
    print(f"Cohort Size for ID {cohort_id}: {cohort_size} (Expected: 1)")

    
    # Check if events_scoped is aggregated
    scoped_count = conn.execute("SELECT COUNT(*) FROM events_scoped").fetchone()[0]
    raw_count = conn.execute("SELECT COUNT(*) FROM events_raw").fetchone()[0]
    print(f"Events Scoped Count: {scoped_count} (Aggregated)")
    print(f"Events Raw Count: {raw_count} (True Raw)")
    
    assert cohort_size == 1, "Cohort size inflated (likely using row-level data for logic)"
    assert scoped_count == 2, "events_scoped should be aggregated (A + B)"
    assert raw_count == 3, "events_raw should have all rows (A + B + A)"
