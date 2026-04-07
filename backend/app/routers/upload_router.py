from fastapi import APIRouter, File, UploadFile
from app.db.connection import get_connection
from app.utils.db_utils import get_user_lock
from app.domains.ingestion.upload_service import upload_csv

router = APIRouter()

@router.post("/upload")
async def upload_endpoint(
    user_id: str,
    file: UploadFile = File(...),
):
    with get_user_lock(user_id):
        with get_connection(user_id) as conn:
            return await upload_csv(conn, file)
