"""
Short summary: exports API routers grouped by capability.
"""
from . import analytics_router, cohorts_router, filters_router, mapping_router, metadata_router, revenue_router, upload_router

__all__ = [
    "upload_router",
    "mapping_router",
    "filters_router",
    "cohorts_router",
    "analytics_router",
    "revenue_router",
    "metadata_router",
]
