from __future__ import annotations

from fastapi.testclient import TestClient
import pytest

from app import main
from app.main import wilson_ci
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
        def execute(self, *_args, **_kwargs):
            class _Result:
                @staticmethod
                def fetchone():
                    return [1]

            return _Result()

        @staticmethod
        def close() -> None:
            return None

    monkeypatch.setattr(main, 'get_connection', lambda: _FakeConnection())
    monkeypatch.setattr(main, 'ensure_cohort_tables', lambda _connection: None)
    monkeypatch.setattr(main, 'build_active_cohort_base', lambda _connection: ([(99, 'Empty Cohort')], {99: 0}))
    monkeypatch.setattr(main, 'fetch_retention_active_rows', lambda _connection, _max_day, _retention_event: [])

    response = client.get('/retention?max_day=1&include_ci=true&confidence=0.95')

    assert response.status_code == 200, response.text
    row = response.json()['retention_table'][0]
    assert row['size'] == 0
    assert row['retention'] == {'0': None, '1': None}
    assert row['retention_ci'] == {
        '0': {'lower': None, 'upper': None},
        '1': {'lower': None, 'upper': None},
    }
