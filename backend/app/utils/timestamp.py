"""
Short summary: contains timestamp parsing, migration, and SQL helpers.
"""
import re
from datetime import date, datetime, time, timedelta
from typing import Any
from fastapi import HTTPException

TIMESTAMP_INPUT_FORMATS: tuple[str, ...] = (
    "%Y-%m-%d",
    "%Y-%m-%d %H",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
)

_TIMESTAMP_TIME_FORMATS: tuple[str, ...] = (
    "%H",
    "%H:%M",
    "%H:%M:%S",
)

_TZ_OFFSET_RE = re.compile(r"[+-]\d{2}:?\d{2}$")

TIMESTAMP_OPERATORS = {"BEFORE", "AFTER", "ON", "BETWEEN", "IN", "NOT IN"}


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


def _parse_date(raw: Any, *, field_name: str) -> date:
    if not isinstance(raw, str) or not raw.strip():
        raise HTTPException(status_code=400, detail=f"{field_name} is required")
    try:
        return date.fromisoformat(raw.strip())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name} format") from exc


def _parse_time(raw: Any, *, field_name: str) -> time:
    if raw is None:
        return time(0, 0, 0)
    if not isinstance(raw, str) or not raw.strip():
        return time(0, 0, 0)
    candidate = raw.strip()
    for fmt in _TIMESTAMP_TIME_FORMATS:
        try:
            parsed = datetime.strptime(candidate, fmt)
            return time(parsed.hour, parsed.minute, parsed.second)
        except ValueError:
            continue
    raise HTTPException(status_code=400, detail=f"Invalid {field_name} format")


def validate_timestamp_payload(operator: str, value: Any) -> dict[str, str]:
    op = str(operator or "").upper()
    if op not in TIMESTAMP_OPERATORS:
        raise HTTPException(status_code=400, detail=f"Unsupported timestamp operator: {operator}")
    if op in {"IN", "NOT IN"}:
        if not isinstance(value, list) or len(value) == 0:
            raise HTTPException(
                status_code=400,
                detail=f"Operator {operator} requires a non-empty list value"
            )
        
        validated_values = []
        for v in value:
            try:
                # This also validates format via normalize_event_timestamp_value
                dt = normalize_event_timestamp_value(v, allow_empty=False)
                if not dt:
                    raise ValueError
                validated_values.append(dt.strftime("%Y-%m-%d %H:%M:%S"))
            except Exception:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid timestamp value in {operator}: {v!r}"
                )
        return validated_values

    if hasattr(value, "model_dump"):
        value = value.model_dump()
    if not isinstance(value, dict):
        raise HTTPException(status_code=400, detail="Timestamp filter requires a structured object value or list for IN/NOT IN")

    if op in {"BEFORE", "AFTER"}:
        d = _parse_date(value.get("date"), field_name="date")
        t = _parse_time(value.get("time"), field_name="time")
        return {"date": d.isoformat(), "time": t.strftime("%H:%M:%S")}

    if op == "ON":
        d = _parse_date(value.get("date"), field_name="date")
        return {"date": d.isoformat()}

    # BETWEEN
    start_d = _parse_date(value.get("startDate"), field_name="startDate")
    end_d = _parse_date(value.get("endDate"), field_name="endDate")
    start_t = _parse_time(value.get("startTime"), field_name="startTime")

    end_time_raw = value.get("endTime")
    if isinstance(end_time_raw, str) and end_time_raw.strip() == "":
        end_time_raw = None
    end_t = _parse_time(end_time_raw, field_name="endTime") if end_time_raw is not None else None
    if end_t is None:
        end_dt = datetime.combine(end_d + timedelta(days=1), time(0, 0, 0))
    else:
        end_dt = datetime.combine(end_d, end_t)

    start_dt = datetime.combine(start_d, start_t)
    if end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="Timestamp BETWEEN range must satisfy end > start")

    payload: dict[str, str] = {
        "startDate": start_d.isoformat(),
        "endDate": end_d.isoformat(),
        "startTime": start_t.strftime("%H:%M:%S"),
    }
    if end_time_raw is not None:
        payload["endTime"] = end_t.strftime("%H:%M:%S") if end_t is not None else "00:00:00"
    return payload


def migrate_legacy_timestamp_filter(operator: str, value: Any) -> tuple[str, Any]:
    op = str(operator or "").upper()
    if op == "=" and isinstance(value, str):
        parsed = normalize_event_timestamp_value(value, allow_empty=False)
        assert parsed is not None
        return "ON", {"date": parsed.date().isoformat()}
    return op, value


def _combine_date_time(d: str, t: str) -> datetime:
    return datetime.combine(date.fromisoformat(d), time.fromisoformat(t))


def build_sql_clause(column_sql: str, operator: str, value: Any, *, parameterized: bool) -> tuple[str, list[object]]:
    payload = validate_timestamp_payload(operator, value)

    params: list[object] = []

    def emit_param(ts: datetime) -> str:
        rendered = ts.strftime("%Y-%m-%d %H:%M:%S")
        if parameterized:
            params.append(rendered)
            return "?"
        return f"TIMESTAMP '{rendered}'"

    op = str(operator or "").upper()
    if op == "BEFORE":
        threshold = _combine_date_time(payload["date"], payload["time"])
        return f"{column_sql} < {emit_param(threshold)}", params

    if op == "AFTER":
        threshold = _combine_date_time(payload["date"], payload["time"])
        return f"{column_sql} >= {emit_param(threshold)}", params

    if op == "ON":
        start = datetime.combine(date.fromisoformat(payload["date"]), time(0, 0, 0))
        end = start + timedelta(days=1)
        return f"({column_sql} >= {emit_param(start)} AND {column_sql} < {emit_param(end)})", params

    if op in {"IN", "NOT IN"}:
        if not payload:
             raise HTTPException(status_code=400, detail="Empty IN list not allowed")
             
        quoted_values = []
        for v in payload:

            if isinstance(v, datetime):
                quoted_values.append(emit_param(v))
            else:
                dt = normalize_event_timestamp_value(v, allow_empty=False)
                if not dt:
                     raise HTTPException(status_code=400, detail=f"Invalid timestamp: {v}")
                quoted_values.append(emit_param(dt))
        
        if not quoted_values:
             raise HTTPException(status_code=400, detail="Empty IN list not allowed")
        
        clause_op = "IN" if op == "IN" else "NOT IN"
        return f"{column_sql} {clause_op} ({', '.join(quoted_values)})", params

    start = _combine_date_time(payload["startDate"], payload["startTime"])
    if "endTime" in payload:
        end = _combine_date_time(payload["endDate"], payload["endTime"])
    else:
        end = datetime.combine(date.fromisoformat(payload["endDate"]) + timedelta(days=1), time(0, 0, 0))

    return f"({column_sql} >= {emit_param(start)} AND {column_sql} < {emit_param(end)})", params
