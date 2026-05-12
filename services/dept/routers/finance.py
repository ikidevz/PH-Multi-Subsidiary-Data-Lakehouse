"""
routers/finance.py
Finance department endpoints: journal entries (full CRUD), AP invoices, AR ledger, trial balance.
"""

from datetime import date

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from database import get_db, JournalEntry, APInvoice
from schemas import (
    JournalEntryCreate, JournalEntryUpdate, JournalEntryResponse,
    APInvoiceCreate, APInvoiceUpdate, APInvoiceResponse,
)
from .deps import SUB_ID, gen, dept_guard

router = APIRouter(tags=["Finance"])


# ── Journal Entries ───────────────────────────────────────────────────────────

@router.post("/journal-entries", response_model=JournalEntryResponse, status_code=status.HTTP_201_CREATED)
def create_journal_entry(entry: JournalEntryCreate, db: Session = Depends(get_db)):
    """Create a new journal entry (POST creates CDC INSERT event)"""
    dept_guard("finance")
    db_entry = JournalEntry(**entry.dict(), subsidiary_id=SUB_ID)
    db.add(db_entry)
    db.commit()
    db.refresh(db_entry)
    return db_entry


@router.get("/journal-entries")
def list_journal_entries(days: int = Query(30, ge=1, le=365), db: Session = Depends(get_db)):
    """List journal entries (READ from PostgreSQL instead of in-memory)"""
    dept_guard("finance")
    return (
        db.query(JournalEntry)
        .filter(JournalEntry.is_deleted == False, JournalEntry.subsidiary_id == SUB_ID)
        .limit(days * 5)
        .all()
    )


@router.get("/journal-entries/{entry_id}", response_model=JournalEntryResponse)
def get_journal_entry(entry_id: int, db: Session = Depends(get_db)):
    """Get a specific journal entry"""
    dept_guard("finance")
    entry = (
        db.query(JournalEntry)
        .filter(JournalEntry.id == entry_id, JournalEntry.is_deleted == False, JournalEntry.subsidiary_id == SUB_ID)
        .first()
    )
    if not entry:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Journal entry not found")
    return entry


@router.patch("/journal-entries/{entry_id}", response_model=JournalEntryResponse)
def update_journal_entry(entry_id: int, update: JournalEntryUpdate, db: Session = Depends(get_db)):
    """Update a journal entry (PATCH creates CDC UPDATE event)"""
    dept_guard("finance")
    entry = (
        db.query(JournalEntry)
        .filter(JournalEntry.id == entry_id, JournalEntry.is_deleted == False, JournalEntry.subsidiary_id == SUB_ID)
        .first()
    )
    if not entry:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Journal entry not found")
    for key, value in update.dict(exclude_unset=True).items():
        setattr(entry, key, value)
    db.commit()
    db.refresh(entry)
    return entry


@router.delete("/journal-entries/{entry_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_journal_entry(entry_id: int, db: Session = Depends(get_db)):
    """Soft-delete a journal entry (creates CDC UPDATE event)"""
    dept_guard("finance")
    entry = (
        db.query(JournalEntry)
        .filter(JournalEntry.id == entry_id, JournalEntry.is_deleted == False, JournalEntry.subsidiary_id == SUB_ID)
        .first()
    )
    if not entry:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Journal entry not found")
    entry.is_deleted = True
    entry.deleted_at = date.today()
    db.commit()


# ── AP Invoices ───────────────────────────────────────────────────────────────

@router.get("/ap-invoices")
def ap_invoices(days: int = Query(30, ge=1, le=365)):
    dept_guard("finance")
    return gen.ap_invoices(days=days)


@router.post("/ap-invoices", response_model=APInvoiceResponse, status_code=status.HTTP_201_CREATED)
def create_ap_invoice(invoice: APInvoiceCreate, db: Session = Depends(get_db)):
    """Create AP invoice"""
    dept_guard("finance")
    db_invoice = APInvoice(**invoice.dict(), subsidiary_id=SUB_ID)
    db.add(db_invoice)
    db.commit()
    db.refresh(db_invoice)
    return db_invoice


@router.patch("/ap-invoices/{invoice_id}", response_model=APInvoiceResponse)
def update_ap_invoice(invoice_id: int, update: APInvoiceUpdate, db: Session = Depends(get_db)):
    """Update AP invoice"""
    dept_guard("finance")
    invoice = (
        db.query(APInvoice)
        .filter(APInvoice.id == invoice_id, APInvoice.is_deleted == False, APInvoice.subsidiary_id == SUB_ID)
        .first()
    )
    if not invoice:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Invoice not found")
    for key, value in update.dict(exclude_unset=True).items():
        setattr(invoice, key, value)
    db.commit()
    db.refresh(invoice)
    return invoice


# ── AR Ledger & Trial Balance ─────────────────────────────────────────────────

@router.get("/ar-ledger")
def ar_ledger(days: int = Query(30, ge=1, le=365)):
    dept_guard("finance")
    return gen.ar_ledger(days=days)


@router.get("/trial-balance")
def trial_balance(days: int = Query(30, ge=1, le=365)):
    dept_guard("finance")
    return gen.trial_balance(days=days)
