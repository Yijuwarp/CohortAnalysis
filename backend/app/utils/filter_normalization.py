import logging
from typing import Any

logger = logging.getLogger(__name__)

SCALAR_OPS = {"=", "!=", ">", "<", ">=", "<="}
ARRAY_OPS = {"IN", "NOT IN"}

def normalize_filter_value(value: Any, operator: str) -> Any:
    """
    Normalizes filter values based on the operator.
    - Resolves list-of-1 to scalar for scalar operators.
    - Rejects nested lists.
    - Rejects multi-item lists for scalar operators.
    """
    if operator is None:
        return value

    op = operator.upper()
    
    # Reject nested lists
    if isinstance(value, list) and any(isinstance(v, list) for v in value):
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="Nested arrays are not supported in filter values")

    if op in SCALAR_OPS:
        if isinstance(value, list):
            if len(value) == 1:
                logger.info(f"Auto-normalizing list-wrapped scalar for operator '{op}'")
                return value[0]
            elif len(value) > 1:
                from fastapi import HTTPException
                raise HTTPException(status_code=400, detail=f"Operator {op} requires a scalar value, but received a list of length {len(value)}")
        return value
    
    if op in ARRAY_OPS:
        if not isinstance(value, list):
            # We strictly expect a list for IN/NOT IN
            from fastapi import HTTPException
            raise HTTPException(status_code=400, detail=f"Operator {op} requires a list value")
        if not value:
            from fastapi import HTTPException
            raise HTTPException(status_code=400, detail=f"Operator {op} requires a non-empty list value")
        return value

    return value
