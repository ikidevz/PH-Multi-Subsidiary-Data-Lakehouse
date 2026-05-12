"""
routers/it_audit.py
IT Audit department endpoints: audit log, access events, system incidents.
All read-only (generator-backed).
"""

from fastapi import APIRouter, Query

from .deps import gen, dept_guard

router = APIRouter(tags=["IT Audit"])


@router.get("/audit-log")
def audit_log(days: int = Query(30, ge=1, le=365)):
    dept_guard("it_audit")
    return gen.audit_log(days=days)


@router.get("/access-events")
def access_events(days: int = Query(30, ge=1, le=365)):
    dept_guard("it_audit")
    return gen.access_events(days=days)


@router.get("/system-incidents")
def system_incidents(days: int = Query(30, ge=1, le=365)):
    dept_guard("it_audit")
    return gen.system_incidents(days=days)
