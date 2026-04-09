from app.utils.sql import get_allowed_operators


def test_timestamp_allowed_operators_keep_legacy_and_structured() -> None:
    allowed = get_allowed_operators("TIMESTAMP")
    assert "=" in allowed
    assert ">=" in allowed
    assert "ON" in allowed
    assert "BETWEEN" in allowed
