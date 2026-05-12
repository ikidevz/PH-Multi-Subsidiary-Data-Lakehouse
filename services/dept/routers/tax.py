"""
routers/tax.py
Tax department endpoints: VAT returns, WHT filings, BIR filings log, alphalist.
All read-only (generator-backed).
"""

from fastapi import APIRouter, Query

from .deps import gen, dept_guard

router = APIRouter(tags=["Tax"])


@router.get("/vat-returns")
def vat_returns(months: int = Query(3, ge=1, le=12)):
    dept_guard("tax")
    return gen.vat_returns(months=months)


@router.get("/wht-filings")
def wht_filings(days: int = Query(30, ge=1, le=365)):
    dept_guard("tax")
    return gen.wht_filings(days=days)


@router.get("/bir-filings-log")
def bir_filings_log(days: int = Query(30, ge=1, le=365)):
    dept_guard("tax")
    return gen.bir_filings_log(days=days)


@router.get("/alphalist")
def alphalist():
    dept_guard("tax")
    return gen.alphalist()
