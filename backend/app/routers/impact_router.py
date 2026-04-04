from fastapi import APIRouter, Depends, HTTPException
import duckdb
from typing import List, Optional
from pydantic import BaseModel, Field
from app.db.connection import get_connection
from app.domains.analytics.impact_service import run_impact_analysis

router = APIRouter()

class ImpactRequest(BaseModel):
    baseline_cohort_id: int
    variant_cohort_ids: List[int]
    start_day: int = Field(default=0, ge=0)
    end_day: int = Field(default=7, ge=0)
    exposure_events: List[str] = Field(min_length=1)
    interaction_events: List[str] = Field(min_length=1)
    impact_events: List[str] = []

@router.post("/impact/run")
async def impact_run_endpoint(
    payload: ImpactRequest,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection)
):
    try:
        return run_impact_analysis(
            conn,
            baseline_cohort_id=payload.baseline_cohort_id,
            variant_cohort_ids=payload.variant_cohort_ids,
            start_day=payload.start_day,
            end_day=payload.end_day,
            exposure_events=payload.exposure_events,
            interaction_events=payload.interaction_events,
            impact_events=payload.impact_events or []
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
