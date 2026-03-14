"""
Short summary: detects semantic column types from CSV samples.
"""
import pandas as pd
from fastapi import HTTPException
from app.utils.timestamp import normalize_event_timestamp_value
from app.utils.parsing import parse_bool_value

def detect_column_type(values: pd.Series) -> str:
    cleaned = values.dropna().astype(str).str.strip()
    cleaned = cleaned[cleaned != ""]

    if cleaned.empty:
        return "TEXT"

    non_null = cleaned.tolist()

    # BOOLEAN
    try:
        for value in non_null:
            parse_bool_value(value)
        return "BOOLEAN"
    except ValueError:
        pass

    # TIMESTAMP
    try:
        for value in non_null:
            normalize_event_timestamp_value(value, allow_empty=False)
        return "TIMESTAMP"
    except HTTPException:
        pass

    # NUMERIC
    try:
        cleaned.astype(float)
        return "NUMERIC"
    except ValueError:
        pass

    return "TEXT"
