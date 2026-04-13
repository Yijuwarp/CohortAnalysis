
from app.models.filter_models import ScopeFilter
from app.models.cohort_models import CohortPropertyFilter
from app.utils.timestamp import validate_timestamp_payload
import pydantic
from fastapi import HTTPException

def test_pydantic_models():
    print("Testing ScopeFilter with list...")
    try:
        sf = ScopeFilter(column="install_date", operator="in", value=["2026-03-18 00:00:00"])
        print("ScopeFilter success:", sf.model_dump())
    except Exception as e:
        print("ScopeFilter failed:", e)

    print("\nTesting CohortPropertyFilter with lowercase 'in'...")
    try:
        cpf = CohortPropertyFilter(column="install_date", operator="in", values=["2026-03-18 00:00:00"])
        print("CohortPropertyFilter success:", cpf.model_dump())
    except Exception as e:
        print("CohortPropertyFilter failed:", e)

def test_validation_logic():
    print("\nTesting validate_timestamp_payload with empty list...")
    try:
        validate_timestamp_payload("IN", [])
        print("FAIL: Expected error for empty list")
    except HTTPException as e:
        print(f"SUCCESS: Caught expected error: {e.detail}")

    print("\nTesting validate_timestamp_payload with invalid timestamp...")
    try:
        validate_timestamp_payload("IN", ["not-a-timestamp"])
        print("FAIL: Expected error for invalid timestamp")
    except HTTPException as e:
        print(f"SUCCESS: Caught expected error: {e.detail}")

    print("\nTesting validate_timestamp_payload with valid list...")
    try:
        res = validate_timestamp_payload("IN", ["2026-03-18 00:00:00"])
        print(f"SUCCESS: Validated list: {res}")
    except HTTPException as e:
        print(f"FAIL: Unexpected error: {e.detail}")

if __name__ == "__main__":
    test_pydantic_models()
    test_validation_logic()
