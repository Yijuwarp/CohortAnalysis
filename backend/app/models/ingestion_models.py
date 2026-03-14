"""
Short summary: defines ingestion request models for upload and mapping.
"""
from pydantic import BaseModel, Field

class ColumnMappingRequest(BaseModel):
    user_id_column: str
    event_name_column: str
    event_time_column: str
    event_count_column: str | None = None
    revenue_column: str | None = None
    column_types: dict[str, str] = Field(default_factory=dict)
