"""
Short summary: performs cohort CRUD and list operations.
"""
from app.domains.legacy_api import create_cohort, delete_cohort, list_cohorts, toggle_cohort_hide, update_cohort

__all__ = ["create_cohort", "list_cohorts", "update_cohort", "delete_cohort", "toggle_cohort_hide"]
