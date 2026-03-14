"""
Short summary: defines database schema initialization helpers.
"""
from app.domains.cohorts.cohort_service import ensure_cohort_tables
from app.domains.ingestion.mapping_service import ensure_dataset_metadata_table
from app.domains.scope.scope_metadata import ensure_scope_tables

__all__ = ["ensure_cohort_tables", "ensure_scope_tables", "ensure_dataset_metadata_table"]
