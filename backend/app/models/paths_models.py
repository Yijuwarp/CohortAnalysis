"""
Short summary: Pydantic models for Paths (Sequence Analysis) request/response payloads.
"""
from typing import List, Optional
from pydantic import BaseModel, Field

MIN_PATHS_STEPS = 2
MAX_PATHS_STEPS = 10

class RunPathsRequest(BaseModel):
    steps: List[str] = Field(..., min_items=MIN_PATHS_STEPS, max_items=MAX_PATHS_STEPS)

class CreateDropOffCohortRequest(BaseModel):
    cohort_id: int
    step_index: int
    steps: List[str]
    cohort_name: Optional[str] = None

class CreateReachedCohortRequest(BaseModel):
    cohort_id: int
    step_index: int
    steps: List[str]
    cohort_name: Optional[str] = None

class PathsStepResult(BaseModel):
    step: int
    event: str
    users: int
    conversion_pct: float
    drop_off_pct: Optional[float] = None
    mean_time: Optional[float] = None
    p20: Optional[float] = None
    p80: Optional[float] = None

class PathsCohortResult(BaseModel):
    cohort_id: int
    cohort_name: str
    cohort_size: int
    steps: List[PathsStepResult]
    insights: List[str] = Field(default_factory=list)

class PathsResponse(BaseModel):
    steps: List[str]
    results: List[PathsCohortResult]
    global_insights: List[str] = Field(default_factory=list)
