"""
Short summary: Pydantic models for funnel request/response payloads.
"""
from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional

MIN_FUNNEL_STEPS = 2
MAX_FUNNEL_STEPS = 10


class FunnelStepFilter(BaseModel):
    property_key: str
    property_value: str
    # v1: only "equals" supported
    operator: str = "equals"


class FunnelStep(BaseModel):
    event_name: str
    filters: list[FunnelStepFilter] = Field(default_factory=list)


class ConversionWindow(BaseModel):
    value: int = Field(ge=1)
    unit: str = Field(default="minute")


class CreateFunnelRequest(BaseModel):
    name: str
    steps: list[FunnelStep] = Field(min_length=MIN_FUNNEL_STEPS, max_length=MAX_FUNNEL_STEPS)
    conversion_window: Optional[ConversionWindow] = None


class RunFunnelRequest(BaseModel):
    funnel_id: int
