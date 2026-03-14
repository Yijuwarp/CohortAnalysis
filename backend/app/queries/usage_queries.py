"""
Short summary: contains raw SQL queries for usage analytics.
"""
def build_usage_property_filter_clause(
    property: str | None,
    operator: str,
    value: str | None,
    table_alias: str = "es",
) -> tuple[str, list[object]]:
    from fastapi import HTTPException
    from app.utils.sql import quote_identifier

    if not property:
        return "", []
    if operator not in {"=", "!="}:
        raise HTTPException(status_code=400, detail="Unsupported operator. Allowed operators: =, !=")
    if value is None or value == "":
        raise HTTPException(status_code=400, detail="Property value is required when property filter is used")

    column_ref = quote_identifier(property)
    comparator = "=" if operator == "=" else "!="
    return f" AND CAST({table_alias}.{column_ref} AS VARCHAR) {comparator} ?", [value]
