"""
Short summary: Pydantic models for Paths (Sequence Analysis) request/response payloads.
"""
from typing import List, Optional, Union
from pydantic import BaseModel, Field

MIN_PATHS_STEPS = 2
MAX_PATHS_STEPS = 10

class PathStepFilter(BaseModel):
    property_key: str
    property_value: Union[str, int, float]

class PathStep(BaseModel):
    step_order: int
    event_name: str
    filters: List[PathStepFilter] = Field(default_factory=list)

class CreatePathRequest(BaseModel):
    name: str
    steps: List[PathStep] = Field(..., min_items=MIN_PATHS_STEPS, max_items=MAX_PATHS_STEPS)

class UpdatePathRequest(BaseModel):
    name: str
    steps: List[PathStep] = Field(..., min_items=MIN_PATHS_STEPS, max_items=MAX_PATHS_STEPS)

class PathDetail(BaseModel):
    id: int
    name: str
    steps: List[PathStep]
    is_valid: bool
    invalid_reason: Optional[str] = None
    created_at: str

class RunPathsRequest(BaseModel):
    # Support both raw string steps (backward compat) and complex PathStep objects
    steps: Union[List[str], List[PathStep]] = Field(..., min_items=MIN_PATHS_STEPS, max_items=MAX_PATHS_STEPS)

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
