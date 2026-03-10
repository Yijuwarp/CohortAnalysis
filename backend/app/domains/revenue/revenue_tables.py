"""
Short summary: initializes and validates revenue support tables.
"""
from app.domains.legacy_api import ensure_revenue_event_selection_table, initialize_revenue_event_selection

__all__ = ["ensure_revenue_event_selection_table", "initialize_revenue_event_selection"]
