"""
routers/sales.py
Sales department endpoints: transactions (CRUD), customers (CRUD), campaigns, revenue summary.
"""

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from database import get_db, SalesTransaction, Customer
from schemas import (
    SalesTransactionCreate, SalesTransactionUpdate, SalesTransactionResponse,
    CustomerCreate, CustomerUpdate, CustomerResponse,
)
from .deps import SUB_ID, gen, dept_guard

router = APIRouter(tags=["Sales"])


# ── Sales Transactions ────────────────────────────────────────────────────────

@router.get("/sales-transactions")
def sales_transactions(days: int = Query(30, ge=1, le=365)):
    dept_guard("sales")
    return gen.sales_transactions(days=days)


@router.post("/sales-transactions", response_model=SalesTransactionResponse, status_code=status.HTTP_201_CREATED)
def create_sales_transaction(txn: SalesTransactionCreate, db: Session = Depends(get_db)):
    """Create sales transaction (CRUD INSERT)"""
    dept_guard("sales")
    db_txn = SalesTransaction(**txn.dict(), subsidiary_id=SUB_ID)
    db.add(db_txn)
    db.commit()
    db.refresh(db_txn)
    return db_txn


@router.patch("/sales-transactions/{txn_id}", response_model=SalesTransactionResponse)
def update_sales_transaction(txn_id: int, update: SalesTransactionUpdate, db: Session = Depends(get_db)):
    """Update sales transaction"""
    dept_guard("sales")
    txn = (
        db.query(SalesTransaction)
        .filter(SalesTransaction.id == txn_id, SalesTransaction.is_deleted == False, SalesTransaction.subsidiary_id == SUB_ID)
        .first()
    )
    if not txn:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Transaction not found")
    for key, value in update.dict(exclude_unset=True).items():
        setattr(txn, key, value)
    db.commit()
    db.refresh(txn)
    return txn


# ── Customers ─────────────────────────────────────────────────────────────────

@router.get("/customers")
def customers():
    dept_guard("sales")
    return gen.customers()


@router.post("/customers", response_model=CustomerResponse, status_code=status.HTTP_201_CREATED)
def create_customer(cust: CustomerCreate, db: Session = Depends(get_db)):
    """Create customer"""
    dept_guard("sales")
    db_cust = Customer(**cust.dict(), subsidiary_id=SUB_ID)
    db.add(db_cust)
    db.commit()
    db.refresh(db_cust)
    return db_cust


@router.patch("/customers/{cust_id}", response_model=CustomerResponse)
def update_customer(cust_id: int, update: CustomerUpdate, db: Session = Depends(get_db)):
    """Update customer"""
    dept_guard("sales")
    cust = (
        db.query(Customer)
        .filter(Customer.id == cust_id, Customer.is_deleted == False, Customer.subsidiary_id == SUB_ID)
        .first()
    )
    if not cust:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Customer not found")
    for key, value in update.dict(exclude_unset=True).items():
        setattr(cust, key, value)
    db.commit()
    db.refresh(cust)
    return cust


# ── Campaigns & Revenue ───────────────────────────────────────────────────────

@router.get("/campaigns")
def campaigns():
    dept_guard("sales")
    return gen.campaigns()


@router.get("/revenue-summary")
def revenue_summary(month: int = Query(0, ge=0, le=12), year: int | None = Query(None, ge=2000)):
    dept_guard("sales")
    return gen.revenue_summary(month=month, year=year)
