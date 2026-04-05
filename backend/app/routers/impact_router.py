from fastapi import APIRouter, Depends, HTTPException
import duckdb
from typing import List, Optional
from pydantic import BaseModel, Field
from app.db.connection import get_connection
from app.domains.analytics.impact_service import run_impact_analysis, IMPACT_RUN_CACHE

router = APIRouter()

class EventFilter(BaseModel):
    property: str
    operator: str = "="
    value: str

class EventConfig(BaseModel):
    event_name: str
    filters: List[EventFilter] = []

class ImpactRequest(BaseModel):
    baseline_cohort_id: int
    variant_cohort_ids: List[int]
    start_day: int = Field(default=0, ge=0)
    end_day: int = Field(default=7, ge=0)
    exposure_events: List[EventConfig] = Field(min_length=1)
    interaction_events: List[EventConfig] = Field(min_length=1)
    impact_events: List[EventConfig] = []
    monetization_events: List[EventConfig] = []
    retention_event: Optional[str] = None

class ImpactStatsRequest(BaseModel):
    run_id: str

@router.post("/impact/run")
async def impact_run_endpoint(
    payload: ImpactRequest,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection)
):
    if not payload.retention_event:
        raise HTTPException(status_code=400, detail="retention_event is required")
    try:
        return run_impact_analysis(
            connection=conn,
            baseline_cohort_id=payload.baseline_cohort_id,
            variant_cohort_ids=payload.variant_cohort_ids,
            start_day=payload.start_day,
            end_day=payload.end_day,
            exposure_events=payload.exposure_events,
            interaction_events=payload.interaction_events,
            impact_events=payload.impact_events or [],
            monetization_events=payload.monetization_events or [],
            retention_event=payload.retention_event
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/impact/stats")
async def impact_stats_endpoint(
    payload: ImpactStatsRequest,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection)
):
    """Lazy statistical significance computation using cached run data."""
    cached = IMPACT_RUN_CACHE.get(payload.run_id)
    if not cached:
        raise HTTPException(status_code=404, detail="Run expired or invalid")

    try:
        from app.domains.analytics.impact_stats_service import compute_all_stats
        stats = compute_all_stats(conn, cached)
        return {"stats": stats}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
