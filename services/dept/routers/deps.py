"""
routers/deps.py
Shared application-level dependencies: env config, generator instance, and
the dept_guard() helper used by every router to gate its endpoints.
"""

import os
from fastapi import HTTPException
from generators import DepartmentGenerator, get_generator_for_dept

SUB_ID: str = os.getenv("SUBSIDIARY_ID", "ABC")
DEPT_TYPE: str = os.getenv("DEPT_TYPE", "finance")

try:
    gen: DepartmentGenerator = get_generator_for_dept(DEPT_TYPE, subsidiary_id=SUB_ID)
except ValueError as exc:
    raise RuntimeError(f"Invalid DEPT_TYPE '{DEPT_TYPE}': {exc}") from exc


def dept_guard(*allowed: str) -> None:
    """Raise 404 if DEPT_TYPE is not one of the allowed values."""
    if DEPT_TYPE not in allowed:
        allowed_str = " or ".join(f"'{d}'" for d in allowed)
        raise HTTPException(
            status_code=404,
            detail=f"Endpoint only available for {allowed_str} department",
        )
