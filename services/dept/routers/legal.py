"""
routers/legal.py
Legal department endpoints: SEC filings, stockholders, board resolutions, officers.
All read-only (generator-backed).
"""

from fastapi import APIRouter

from .deps import gen, dept_guard

router = APIRouter(tags=["Legal"])


@router.get("/sec-filings")
def sec_filings():
    dept_guard("legal")
    return gen.sec_filings()


@router.get("/stockholders")
def stockholders():
    dept_guard("legal")
    return gen.stockholders()


@router.get("/board-resolutions")
def board_resolutions():
    dept_guard("legal")
    return gen.board_resolutions()


@router.get("/officers")
def officers():
    dept_guard("legal")
    return gen.officers()
