from fastapi import APIRouter, Depends, File, UploadFile
import duckdb
from app.db.connection import get_connection
from app.domains.ingestion.upload_service import upload_csv

router = APIRouter()

@router.post("/upload")
async def upload_endpoint(
    file: UploadFile = File(...),
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return await upload_csv(conn, file)
