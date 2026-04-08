from fastapi import APIRouter, File, UploadFile
from app.db.connection import async_run_query
from app.domains.ingestion.upload_service import upload_csv

router = APIRouter()

@router.post("/upload")
async def upload_endpoint(
    user_id: str,
    file: UploadFile = File(...),
):
    return await async_run_query(user_id, lambda conn: upload_csv(conn, file))
