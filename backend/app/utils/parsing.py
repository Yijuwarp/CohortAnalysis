"""
Short summary: contains primitive parser helpers.
"""
import re

def parse_bool_value(value: object) -> bool:
    normalized = str(value).strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError("Expected true/false")


def parse_int_value(value: object) -> int:
    normalized = str(value).strip()
    if not re.fullmatch(r"[+-]?\d+", normalized):
        raise ValueError("Expected integer")
    return int(normalized)


def parse_max_day(value: object, default: int = 7) -> int:
    try:
        if value is None or str(value).strip() == "":
            return default
        # Match legacy behavior where 3.9 becomes 3, and strings like "abc" or "NaN" become default.
        # Also, non-positive integers (0, -1, etc.) default to 7 based on tests.
        parsed = int(float(str(value)))
        if parsed <= 0:
            return default
        return parsed
    except (ValueError, TypeError):
        return default
