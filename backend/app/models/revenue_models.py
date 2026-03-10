"""
Short summary: defines revenue configuration request models.
"""
from app.domains.legacy_api import (
    RevenueConfigItem,
    RevenueEventSelectionItem,
    RevenueEventSelectionPayloadItem,
    RevenueEventSelectionRequest,
    UpdateRevenueConfigRequest,
)

__all__ = [
    "RevenueEventSelectionItem",
    "RevenueEventSelectionRequest",
    "RevenueConfigItem",
    "RevenueEventSelectionPayloadItem",
    "UpdateRevenueConfigRequest",
]
