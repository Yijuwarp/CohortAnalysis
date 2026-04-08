from __future__ import annotations
from fastapi import APIRouter
from pydantic import BaseModel
from app.db.connection import run_query
from app.domains.analytics.comparison_service import compare_cohorts

router = APIRouter()

class CompareCohortRequest(BaseModel):
    cohort_a: int
    cohort_b: int
    tab: str          # retention | usage | monetization
    metric: str
    day: int
    max_day: int | None = None
    event: str | None = None
    granularity: str = "day"
    retention_type: str = "classic"
    property: str | None = None
    operator: str = "="
    value: str | None = None

@router.post("/compare-cohorts")
async def compare_cohorts_endpoint(
    user_id: str,
    payload: CompareCohortRequest,
):
    return run_query(user_id, lambda conn: compare_cohorts(
        conn=conn,
        cohort_a=payload.cohort_a,
        cohort_b=payload.cohort_b,
        tab=payload.tab,
        metric=payload.metric,
        day=payload.day,
        max_day=payload.max_day,
        event=payload.event,
        granularity=payload.granularity,
        retention_type=payload.retention_type,
        property=payload.property,
        operator=payload.operator,
        value=payload.value,
    ))
