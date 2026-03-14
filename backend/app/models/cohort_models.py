"""
Short summary: defines cohort request and configuration models.
"""
from pydantic import BaseModel, Field, field_validator

class CohortPropertyFilter(BaseModel):
    column: str
    operator: str
    values: str | float | int | bool | list[str] | list[float] | list[int] | list[bool]

class CohortCondition(BaseModel):
    event_name: str
    min_event_count: int = Field(ge=1)
    property_filter: CohortPropertyFilter | None = None

    @field_validator("property_filter")
    @classmethod
    def validate_property_filter(cls, value: CohortPropertyFilter | None) -> CohortPropertyFilter | None:
        if value is None:
            return value
        if value.values is None or (isinstance(value.values, str) and value.values == ""):
            raise ValueError("property_filter.values is required")
        return value

class CreateCohortRequest(BaseModel):
    name: str = Field(min_length=1)
    logic_operator: str
    condition_logic: str | None = None
    join_type: str = "condition_met"
    conditions: list[CohortCondition] = Field(max_length=5)

    @field_validator("logic_operator")
    @classmethod
    def validate_logic_operator(cls, value: str) -> str:
        normalized = value.upper()
        if normalized not in {"AND", "OR"}:
            raise ValueError("logic_operator must be either AND or OR")
        return normalized

    @field_validator("condition_logic")
    @classmethod
    def validate_condition_logic(cls, value: str | None) -> str | None:
        if value is None:
            return value
        normalized = value.upper()
        if normalized not in {"AND", "OR"}:
            raise ValueError("condition_logic must be either AND or OR")
        return normalized

    @field_validator("join_type")
    @classmethod
    def validate_join_type(cls, value: str) -> str:
        normalized = value.lower()
        if normalized not in {"condition_met", "first_event"}:
            raise ValueError("join_type must be 'condition_met' or 'first_event'")
        return normalized
