from app.utils.timestamp import (
    migrate_legacy_timestamp_filter,
    validate_timestamp_payload,
    build_sql_clause,
)


def test_migrate_legacy_scalar_equality_to_on() -> None:
    op, value = migrate_legacy_timestamp_filter("=", "2026-01-03 10:15")
    assert op == "ON"
    assert value == {"date": "2026-01-03"}


def test_validate_between_defaults_and_half_open() -> None:
    payload = validate_timestamp_payload("between", {"startDate": "2026-01-01", "endDate": "2026-01-03"})
    assert payload["startTime"] == "00:00:00"
    assert "endTime" not in payload


def test_build_sql_clause_for_on_operator() -> None:
    sql, params = build_sql_clause('"event_time"', "on", {"date": "2026-01-05"}, parameterized=True)
    assert '"event_time" >=' in sql and '"event_time" <' in sql
    assert params == ["2026-01-05 00:00:00", "2026-01-06 00:00:00"]


def test_validate_between_treats_blank_end_time_as_missing() -> None:
    payload = validate_timestamp_payload(
        "between",
        {"startDate": "2026-01-01", "endDate": "2026-01-01", "startTime": "00:00:00", "endTime": ""},
    )
    assert "endTime" not in payload
