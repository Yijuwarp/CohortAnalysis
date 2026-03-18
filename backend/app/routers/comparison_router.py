"""
Short summary: router for the POST /compare-cohorts endpoint.
"""
from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.db.connection import get_connection
from app.domains.analytics.comparison_service import compare_cohorts

router = APIRouter()


class CompareCohortRequest(BaseModel):
    cohort_a: int
    cohort_b: int
    tab: str          # retention | usage | monetization
    metric: str
    day: int
    event: str | None = None
    granularity: str = "day"
    retention_type: str = "classic"


@router.post("/compare-cohorts")
async def compare_cohorts_endpoint(
    payload: CompareCohortRequest,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return compare_cohorts(
        conn=conn,
        cohort_a=payload.cohort_a,
        cohort_b=payload.cohort_b,
        tab=payload.tab,
        metric=payload.metric,
        day=payload.day,
        event=payload.event,
        granularity=payload.granularity,
        retention_type=payload.retention_type,
    )
