"""
Short summary: Pydantic models for funnel request/response payloads.
"""
from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional


class FunnelStepFilter(BaseModel):
    property_key: str
    property_value: str
    # v1: only "equals" supported
    operator: str = "equals"


class FunnelStep(BaseModel):
    event_name: str
    filters: list[FunnelStepFilter] = Field(default_factory=list)


class CreateFunnelRequest(BaseModel):
    name: str
    steps: list[FunnelStep] = Field(min_length=2, max_length=5)


class RunFunnelRequest(BaseModel):
    funnel_id: int
