"""
Short summary: defines revenue configuration request models.
"""
from pydantic import BaseModel, Field

class RevenueEventSelectionItem(BaseModel):
    event_name: str
    is_included: bool
    override: float | None = None


class RevenueEventSelectionRequest(BaseModel):
    events: list[RevenueEventSelectionItem] = Field(default_factory=list)


class RevenueConfigItem(BaseModel):
    included: bool
    override: float | None = None


class RevenueEventSelectionPayloadItem(BaseModel):
    """Item format used in the events-list variant of UpdateRevenueConfigRequest."""
    event_name: str
    include: bool
    override: float | None = None


class UpdateRevenueConfigRequest(BaseModel):
    revenue_config: dict[str, RevenueConfigItem] = Field(default_factory=dict)
    events: list[RevenueEventSelectionPayloadItem] = Field(default_factory=list)
