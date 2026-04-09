"""
Short summary: defines filtering models used for scoped dataset operations.
"""
from typing import Any
from pydantic import BaseModel, Field, model_validator
from app.utils.filter_normalization import normalize_filter_value

class DateRange(BaseModel):
    start: str
    end: str

class ScopeFilter(BaseModel):
    column: str
    operator: str
    value: str | float | int | list[str] | list[float] | list[int]

    @model_validator(mode="before")
    @classmethod
    def normalize_and_validate(cls, data: Any) -> Any:
        if isinstance(data, dict):
            op = data.get("operator")
            val = data.get("value")
            data["value"] = normalize_filter_value(val, op)
        return data

class ApplyFiltersRequest(BaseModel):
    date_range: DateRange | None = None
    filters: list[ScopeFilter] = Field(default_factory=list)
