from __future__ import annotations

from fastapi.testclient import TestClient
import pytest

from app import main
# We need to patch the actual service modules because they are imported into the routers
import app.domains.analytics.retention_service as retention_service
from app.utils.math_utils import wilson_ci
from tests.test_retention import _prepare_events


def test_wilson_ci_deterministic_example() -> None:
    lower, upper = wilson_ci(400, 1000, 0.95)

    assert lower is not None
    assert upper is not None
    assert lower == pytest.approx(0.3701, abs=1e-4)
    assert upper == pytest.approx(0.4307, abs=1e-4)


def test_wilson_ci_zero_cohort_size_returns_none_bounds() -> None:
    assert wilson_ci(0, 0, 0.95) == (None, None)


def test_wilson_ci_zero_successes_has_zero_lower_and_positive_upper() -> None:
    lower, upper = wilson_ci(0, 100, 0.95)

    assert lower == 0.0
    assert upper is not None
    assert upper > 0


def test_wilson_ci_confidence_levels_adjust_interval_width() -> None:
    lower_90, upper_90 = wilson_ci(400, 1000, 0.90)
    lower_95, upper_95 = wilson_ci(400, 1000, 0.95)
    lower_99, upper_99 = wilson_ci(400, 1000, 0.99)

    width_90 = upper_90 - lower_90
    width_95 = upper_95 - lower_95
    width_99 = upper_99 - lower_99

    assert width_90 < width_95 < width_99


def test_retention_endpoint_includes_ci_only_when_requested(client: TestClient) -> None:
    _prepare_events(client)

    without_ci = client.get('/retention?max_day=1')
    assert without_ci.status_code == 200, without_ci.text
    assert 'retention_ci' not in without_ci.json()['retention_table'][0]

    with_ci = client.get('/retention?max_day=1&include_ci=true&confidence=0.95')
    assert with_ci.status_code == 200, with_ci.text
    row = with_ci.json()['retention_table'][0]
    assert 'retention_ci' in row
    assert set(row['retention_ci']['0'].keys()) == {'lower', 'upper'}


def test_retention_endpoint_rounds_confidence_before_validation(client: TestClient) -> None:
    _prepare_events(client)

    response = client.get('/retention?include_ci=true&confidence=0.9500001')

    assert response.status_code == 200, response.text


def test_retention_endpoint_rejects_invalid_confidence(client: TestClient) -> None:
    response = client.get('/retention?include_ci=true&confidence=0.93')

    assert response.status_code == 400
    assert response.json()['detail'] == 'confidence must be one of: 0.90, 0.95, 0.99'


def test_retention_returns_null_retention_and_ci_for_zero_sized_cohort(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeConnection:
        def execute(self, *args, **kwargs):
            class _Result:
                def __init__(self, data):
                    self.data = data
                    # description is required for to_dicts Utility
                    self.description = [("col", None, None, None, None, None, None)] if data else []
                def fetchall(self):
                    # Basic detection for cohort list needed by retention service
                    query = args[0] if args else ""
                    if "FROM cohorts" in query:
                        return [(99, "Empty Cohort", "condition_met")]
                    return self.data
                def fetchone(self):
                    # Default return for scoped_exists check
                    return [1]
            
            # Return empty data by default unless it's the cohort list query
            return _Result([])
        def cursor(self):
            return self

        @staticmethod
        def close() -> None:
            return None

    import app.db.connection
    from threading import Lock

    # Patch the service module directly
    monkeypatch.setattr(app.db.connection, 'get_connection', lambda *args: (_FakeConnection(), Lock()))
    import app.db.schema_init
    monkeypatch.setattr(app.db.schema_init, 'ensure_base_schema', lambda _conn: None)
    monkeypatch.setattr(retention_service, 'ensure_cohort_tables', lambda _connection: None)
    
    # We must patch build_active_cohort_base inside the retention_service module
    monkeypatch.setattr(retention_service, 'build_active_cohort_base', lambda _connection: ([(99, 'Empty Cohort', 'condition_met')], {99: 0}))
    # Patch the vector builder at its source
    monkeypatch.setattr('app.domains.analytics.metric_builders.retention_vectors.build_retention_vector_sql', lambda *args, **kwargs: ("SELECT 99 as cohort_id, 'u1' as user_id, 0 as day_offset, 1 as value, 1 as is_eligible WHERE 1=0", []))

    try:
        response = client.get('/retention?max_day=1&include_ci=true&confidence=0.95')

        assert response.status_code == 200, response.text
        row = response.json()['retention_table'][0]
        assert row['size'] == 0
        assert row['retention'] == {'0': None, '1': None}
        assert row['retention_ci'] == {
            '0': {'lower': None, 'upper': None},
            '1': {'lower': None, 'upper': None},
        }
    finally:
        main.app.dependency_overrides.clear()
