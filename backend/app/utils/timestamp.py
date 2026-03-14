"""
Short summary: contains timestamp parsing and normalization helpers.
"""
import re
from datetime import datetime
from fastapi import HTTPException

TIMESTAMP_INPUT_FORMATS: tuple[str, ...] = (
    "%Y-%m-%d",
    "%Y-%m-%d %H",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
)

_TZ_OFFSET_RE = re.compile(r"[+-]\d{2}:?\d{2}$")


def normalize_timestamp_filter_value(value: str) -> str:
    normalized = value.strip().replace("T", " ")
    if not normalized:
        return ""

    try:
        parsed = datetime.fromisoformat(normalized.replace(" ", "T"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid timestamp format") from exc

    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def normalize_event_timestamp_value(value: object, *, allow_empty: bool) -> datetime | None:
    if value is None:
        if allow_empty:
            return None
        raise HTTPException(status_code=400, detail="Timestamp value cannot be null")

    raw = str(value).strip()

    if raw == "":
        if allow_empty:
            return None
        raise HTTPException(status_code=400, detail="Timestamp value cannot be empty")

    # 1. Strip timezone suffixes first (before T replacement, so "UTC" T isn't touched)
    if raw.upper().endswith("UTC"):
        raw = raw[:-3].strip()
    elif raw.endswith("Z"):
        raw = raw[:-1]
    else:
        raw = _TZ_OFFSET_RE.sub("", raw).strip()

    # 2. Now safe to replace ISO T separator
    raw = raw.replace("T", " ")

    for fmt in TIMESTAMP_INPUT_FORMATS:
        try:
            return datetime.strptime(raw, fmt).replace(microsecond=0)
        except ValueError:
            continue

    raise HTTPException(status_code=400, detail=f"Invalid timestamp format: {raw!r}")
