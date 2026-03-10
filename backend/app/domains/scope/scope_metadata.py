"""
Short summary: reads and writes scoped dataset metadata.
"""
from app.domains.legacy_api import get_scope, upsert_dataset_scope

__all__ = ["get_scope", "upsert_dataset_scope"]
