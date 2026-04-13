import pytest
import duckdb
import asyncio
from app.domains.ingestion.upload_service import upload_csv
from app.domains.ingestion.mapping_service import map_columns
from app.models.ingestion_models import ColumnMappingRequest
from fastapi import UploadFile
import io

@pytest.fixture
def conn():
    return duckdb.connect(":memory:")

def create_upload_file(content: str, filename: str):
    return UploadFile(filename=filename, file=io.BytesIO(content.encode('utf-8')))

def test_revenue_recompute_raw(conn):
    """
    Test Case: Verify that revenue recomputation works for events_raw
    """
    csv_content = (
        "user_id,event_name,event_time,rev\n"
        "u1,A,2024-01-01 10:00:00,10.0\n"
        "u1,B,2024-01-01 10:00:00,20.0\n"
    )
    
    # --- INGESTION ---
    asyncio.run(upload_csv(conn, create_upload_file(csv_content, "test.csv")))
    mapping = ColumnMappingRequest(
        user_id_column="user_id",
        event_name_column="event_name",
        event_time_column="event_time",
        revenue_column="rev",
        column_types={
            "user_id": "TEXT", 
            "event_name": "TEXT", 
            "event_time": "TIMESTAMP",
            "rev": "NUMERIC"
        }
    )
    
    # This should NOT fail anymore with ValueError: Unsupported table
    map_columns(conn, mapping)
    
    raw_revs = conn.execute("SELECT event_name, original_revenue, modified_revenue FROM events_raw").fetchall()
    print(f"\nRaw Revenues: {raw_revs}")
    
    assert len(raw_revs) == 2
    for name, orig, mod in raw_revs:
        assert mod == orig, f"Initial modified revenue should match original for {name}"
