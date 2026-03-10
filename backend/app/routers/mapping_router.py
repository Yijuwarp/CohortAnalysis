"""
Short summary: exposes column mapping endpoint and delegates normalization.
"""
from fastapi import APIRouter

from app.domains import legacy_api
from app.models.ingestion_models import ColumnMappingRequest

router = APIRouter()


@router.post("/map-columns")
def map_columns(mapping: ColumnMappingRequest) -> dict[str, str | int]:
    return legacy_api.map_columns(mapping)
