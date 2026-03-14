"""
Short summary: defines filtering models used for scoped dataset operations.
"""
from pydantic import BaseModel, Field

class DateRange(BaseModel):
    start: str
    end: str

class ScopeFilter(BaseModel):
    column: str
    operator: str
    value: str | float | int | list[str] | list[float] | list[int]

class ApplyFiltersRequest(BaseModel):
    date_range: DateRange | None = None
    filters: list[ScopeFilter] = Field(default_factory=list)
