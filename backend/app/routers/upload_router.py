"""
Short summary: exposes upload endpoint and delegates to ingestion services.
"""
from fastapi import APIRouter, File, UploadFile

from app.domains import legacy_api

router = APIRouter()


@router.get("/")
def read_root() -> dict[str, str]:
    return legacy_api.read_root()


@router.post("/upload")
async def upload_csv(file: UploadFile = File(...)) -> dict[str, object]:
    return await legacy_api.upload_csv(file)
