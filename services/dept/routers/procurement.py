"""
routers/procurement.py
Procurement department endpoints: vendors (CRUD), WHT certificates, vendor scorecard, AP aging.
Purchase orders are shared with ops (handled in ops.py).
"""

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from database import get_db, Vendor
from schemas import VendorCreate, VendorUpdate, VendorResponse
from .deps import SUB_ID, gen, dept_guard

router = APIRouter(tags=["Procurement"])


# ── Vendors ───────────────────────────────────────────────────────────────────

@router.get("/vendors")
def vendors():
    dept_guard("procurement")
    return gen.vendors()


@router.post("/vendors", response_model=VendorResponse, status_code=status.HTTP_201_CREATED)
def create_vendor(vendor: VendorCreate, db: Session = Depends(get_db)):
    """Create vendor"""
    dept_guard("procurement")
    db_vendor = Vendor(**vendor.dict(), subsidiary_id=SUB_ID)
    db.add(db_vendor)
    db.commit()
    db.refresh(db_vendor)
    return db_vendor


@router.patch("/vendors/{vendor_id}", response_model=VendorResponse)
def update_vendor(vendor_id: int, update: VendorUpdate, db: Session = Depends(get_db)):
    """Update vendor"""
    dept_guard("procurement")
    vendor = (
        db.query(Vendor)
        .filter(Vendor.id == vendor_id, Vendor.is_deleted == False, Vendor.subsidiary_id == SUB_ID)
        .first()
    )
    if not vendor:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Vendor not found")
    for key, value in update.dict(exclude_unset=True).items():
        setattr(vendor, key, value)
    db.commit()
    db.refresh(vendor)
    return vendor


# ── WHT Certificates, Scorecard, AP Aging ────────────────────────────────────

@router.get("/wht-certs")
def wht_certs():
    dept_guard("procurement")
    return gen.wht_certificates()


@router.get("/vendor-scorecard")
def vendor_scorecard(month: int = Query(0, ge=0, le=12), year: int | None = Query(None, ge=2000)):
    dept_guard("procurement")
    return gen.vendor_scorecard(month=month, year=year)


@router.get("/ap-aging-detail")
def ap_aging_detail(as_of_date: str | None = Query(None)):
    dept_guard("procurement")
    return gen.ap_aging_detail(as_of_date=as_of_date)
