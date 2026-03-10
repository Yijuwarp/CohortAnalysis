"""
Short summary: defines database schema initialization helpers.
"""
from app.domains.legacy_api import ensure_cohort_tables, ensure_dataset_metadata_table, ensure_scope_tables

__all__ = ["ensure_cohort_tables", "ensure_scope_tables", "ensure_dataset_metadata_table"]
