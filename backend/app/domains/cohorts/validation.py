"""
Short summary: validates cohort logic and filters before execution.
"""
import duckdb
from fastapi import HTTPException
from app.utils.sql import get_column_type_map, get_column_kind, get_allowed_operators
from app.utils.timestamp import TIMESTAMP_OPERATORS, normalize_timestamp_filter_value, validate_timestamp_payload, migrate_legacy_timestamp_filter
from app.models.cohort_models import CohortCondition, CohortPropertyFilter

def validate_cohort_property_filter_value(property_filter: CohortPropertyFilter, column_kind: str) -> None:
    operator = property_filter.operator.upper()
    values = property_filter.values
    if operator in {"IN", "NOT IN"}:
        if not isinstance(values, list) or not values:
            raise HTTPException(status_code=400, detail=f"Operator {operator} requires a non-empty array value")
    else:
        if isinstance(values, list):
            raise HTTPException(status_code=400, detail=f"Operator {operator} requires a scalar value")

    scalar_values = values if isinstance(values, list) else [values]
    if column_kind == "NUMERIC":
        normalized_values: list[int | float] = []
        for value in scalar_values:
            if isinstance(value, bool):
                raise HTTPException(status_code=400, detail="Numeric operators require numeric values")
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="Numeric operators require numeric values") from None

            if parsed.is_integer():
                normalized_values.append(int(parsed))
            else:
                normalized_values.append(parsed)

        property_filter.values = normalized_values if isinstance(values, list) else normalized_values[0]
    elif column_kind == "TIMESTAMP":
        if operator in TIMESTAMP_OPERATORS:
            property_filter.values = validate_timestamp_payload(operator, values)
        else:
            normalized_values: list[str] = []
            for value in scalar_values:
                if not isinstance(value, str):
                    raise HTTPException(status_code=400, detail="Timestamp filters require string values")
                normalized = normalize_timestamp_filter_value(value)
                if not normalized:
                    raise HTTPException(status_code=400, detail="Timestamp filters require non-empty string values")
                normalized_values.append(normalized)

            property_filter.values = normalized_values if isinstance(values, list) else normalized_values[0]
    elif column_kind == "BOOLEAN":
        if operator in {"IN", "NOT IN"}:
            raise HTTPException(status_code=400, detail=f"Operator '{operator}' not allowed for column type BOOLEAN")
        for value in scalar_values:
            if not isinstance(value, bool):
                raise HTTPException(status_code=400, detail="Boolean filters only accept true/false values")


def validate_cohort_conditions(
    connection: duckdb.DuckDBPyConnection,
    source_table: str,
    conditions: list[CohortCondition],
) -> None:
    column_types = get_column_type_map(connection, source_table)
    if not column_types:
        raise HTTPException(status_code=400, detail="No normalized events found. Upload a CSV and map columns first.")

    for condition in conditions:
        property_filter = condition.property_filter
        if property_filter is None:
            continue

        if property_filter.column not in column_types:
            raise HTTPException(status_code=400, detail=f"Unknown filter column: {property_filter.column}")

        operator = property_filter.operator.upper()
        column_kind = get_column_kind(column_types[property_filter.column])
        if column_kind == "TIMESTAMP":
            migrated_operator, migrated_value = migrate_legacy_timestamp_filter(operator, property_filter.values)
            property_filter.operator = migrated_operator
            property_filter.values = migrated_value
            operator = migrated_operator
        allowed_ops = TIMESTAMP_OPERATORS if column_kind == "TIMESTAMP" else get_allowed_operators(column_kind)
        if operator not in allowed_ops:
            raise HTTPException(
                status_code=400,
                detail=f"Operator '{operator}' not allowed for column type {column_kind}",
            )
        validate_cohort_property_filter_value(property_filter, column_kind)
