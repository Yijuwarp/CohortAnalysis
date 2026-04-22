"""
Short summary: contains raw SQL queries for usage analytics.
"""
def build_usage_property_filter_clause(
    property: str | None,
    operator: str,
    values: list[str] | str | None,
    table_alias: str = "es",
) -> tuple[str, list[object]]:
    from fastapi import HTTPException
    from app.utils.sql import quote_identifier

    if not property:
        return "", []

    # Handle case where a single string might be passed instead of a list
    if isinstance(values, str):
        values = [values]
    
    if values is None or len(values) == 0:
        raise HTTPException(status_code=400, detail="Property value is required when property filter is used")

    column_ref = quote_identifier(property)
    operator_lower = operator.lower()

    if operator_lower in {"=", "!="}:
        comparator = "=" if operator_lower == "=" else "!="
        return f" AND CAST({table_alias}.{column_ref} AS VARCHAR) {comparator} ?", [values[0]]
    
    if operator_lower in {"in", "not in"}:
        placeholders = ", ".join(["?" for _ in values])
        comparator = "IN" if operator_lower == "in" else "NOT IN"
        return f" AND CAST({table_alias}.{column_ref} AS VARCHAR) {comparator} ({placeholders})", values

    raise HTTPException(status_code=400, detail=f"Unsupported operator: {operator}. Allowed: =, !=, in, not in")
