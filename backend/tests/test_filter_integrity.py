from __future__ import annotations
import pytest
from pydantic import ValidationError
from app.models.filter_models import ScopeFilter
from app.models.cohort_models import CohortPropertyFilter
from app.utils.filter_normalization import normalize_filter_value

def test_normalization_contract_scalar():
    # Fixable
    assert normalize_filter_value(["web"], "=") == "web"
    assert normalize_filter_value([123], ">") == 123
    
    # Reject multi-item
    with pytest.raises(Exception) as excinfo:
        normalize_filter_value(["a", "b"], "=")
    assert "requires a scalar value" in str(excinfo.value)

def test_normalization_contract_array():
    # Expect list
    assert normalize_filter_value(["a", "b"], "IN") == ["a", "b"]
    
    # Reject scalar
    with pytest.raises(Exception) as excinfo:
        normalize_filter_value("a", "IN")
    assert "requires a list value" in str(excinfo.value)
    
    # Reject empty list
    with pytest.raises(Exception) as excinfo:
        normalize_filter_value([], "IN")
    assert "requires a non-empty list value" in str(excinfo.value)

def test_normalization_reject_nested():
    with pytest.raises(Exception) as excinfo:
        normalize_filter_value([["a"]], "=")
    assert "Nested arrays are not supported" in str(excinfo.value)

def test_model_normalization_at_entry():
    # ScopeFilter
    sf = ScopeFilter(column="source", operator="=", value=["web"])
    assert sf.value == "web"
    
    # CohortPropertyFilter
    cpf = CohortPropertyFilter(column="source", operator="=", values=["web"])
    assert cpf.values == "web"

def test_pydantic_validation_error_on_bad_shape():
    # This should raise 400 (HTTPException) inside the validator, which FastAPI converts to 400.
    # In a pure pydantic context, it might be an HTTPException or ValueError depending on how it's raised.
    # Since I used 'from fastapi import HTTPException', it will raise that.
    from fastapi import HTTPException
    
    with pytest.raises(HTTPException) as excinfo:
        ScopeFilter(column="source", operator="=", value=["a", "b"])
    assert excinfo.value.status_code == 400
