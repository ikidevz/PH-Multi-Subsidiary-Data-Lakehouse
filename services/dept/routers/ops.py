"""
routers/ops.py
Ops department endpoints: inventory movements, purchase orders, sales orders, stock summary.
Purchase orders are also accessible by procurement (guarded per endpoint).
All read-only (generator-backed).
"""

from fastapi import APIRouter, Query

from .deps import gen, dept_guard

router = APIRouter(tags=["Ops"])


@router.get("/inventory-movements")
def inventory_movements(days: int = Query(30, ge=1, le=365)):
    dept_guard("ops")
    return gen.inventory_movements(days=days)


@router.get("/purchase-orders")
def purchase_orders(days: int = Query(30, ge=1, le=365)):
    # Both ops and procurement expose purchase orders
    dept_guard("ops", "procurement")
    return gen.purchase_orders(days=days)


@router.get("/sales-orders")
def sales_orders(days: int = Query(30, ge=1, le=365)):
    dept_guard("ops")
    return gen.sales_orders(days=days)


@router.get("/stock-summary")
def stock_summary(as_of_date: str | None = Query(None)):
    dept_guard("ops")
    return gen.stock_summary(as_of_date=as_of_date)
