"""
Pydantic models for request/response validation
Used by FastAPI CRUD endpoints
"""

from datetime import date, datetime
from decimal import Decimal
from pydantic import BaseModel, Field
from typing import Optional


# ============================================================================
# Finance Schemas
# ============================================================================

class JournalEntryBase(BaseModel):
    entry_date: date
    entry_number: str
    account_code: str
    account_name: Optional[str] = None
    debit_amount: Decimal = Field(default=0, decimal_places=2)
    credit_amount: Decimal = Field(default=0, decimal_places=2)
    description: Optional[str] = None
    reference_doc: Optional[str] = None
    is_interco: bool = False
    counterpart_sub_id: Optional[str] = None
    cost_center: Optional[str] = None
    posted_by: Optional[str] = None
    approved_by: Optional[str] = None


class JournalEntryCreate(JournalEntryBase):
    pass


class JournalEntryUpdate(BaseModel):
    entry_date: Optional[date] = None
    account_code: Optional[str] = None
    debit_amount: Optional[Decimal] = None
    credit_amount: Optional[Decimal] = None
    description: Optional[str] = None
    status: Optional[str] = None
    posted_by: Optional[str] = None
    approved_by: Optional[str] = None


class JournalEntryResponse(JournalEntryBase):
    id: int
    subsidiary_id: str
    created_at: datetime
    updated_at: datetime
    is_deleted: bool

    class Config:
        from_attributes = True


# ============================================================================
# AP Invoice Schemas
# ============================================================================

class APInvoiceBase(BaseModel):
    invoice_number: str
    vendor_id: Optional[str] = None
    vendor_name: Optional[str] = None
    vendor_tin: Optional[str] = None
    invoice_date: date
    due_date: Optional[date] = None
    gross_amount: Decimal = Field(default=0, decimal_places=2)
    vat_amount: Decimal = Field(default=0, decimal_places=2)
    net_amount: Decimal = Field(default=0, decimal_places=2)
    wht_amount: Decimal = Field(default=0, decimal_places=2)
    payment_amount: Optional[Decimal] = None
    payment_date: Optional[date] = None
    status: str = "unpaid"


class APInvoiceCreate(APInvoiceBase):
    pass


class APInvoiceUpdate(BaseModel):
    status: Optional[str] = None
    payment_amount: Optional[Decimal] = None
    payment_date: Optional[date] = None
    approved_by: Optional[str] = None


class APInvoiceResponse(APInvoiceBase):
    id: int
    subsidiary_id: str
    created_at: datetime
    updated_at: datetime
    is_deleted: bool

    class Config:
        from_attributes = True


# ============================================================================
# Sales Transaction Schemas
# ============================================================================

class SalesTransactionBase(BaseModel):
    invoice_number: str
    customer_name: Optional[str] = None
    transaction_date: date
    gross_amount: Decimal = Field(default=0, decimal_places=2)
    discount_amount: Decimal = Field(default=0, decimal_places=2)
    vat_amount: Decimal = Field(default=0, decimal_places=2)
    net_amount: Decimal = Field(default=0, decimal_places=2)
    vat_classification: Optional[str] = None
    is_interco: bool = False


class SalesTransactionCreate(SalesTransactionBase):
    pass


class SalesTransactionUpdate(BaseModel):
    transaction_date: Optional[date] = None
    gross_amount: Optional[Decimal] = None
    discount_amount: Optional[Decimal] = None
    vat_amount: Optional[Decimal] = None
    net_amount: Optional[Decimal] = None
    status: Optional[str] = None


class SalesTransactionResponse(SalesTransactionBase):
    id: int
    subsidiary_id: str
    created_at: datetime
    updated_at: datetime
    is_deleted: bool

    class Config:
        from_attributes = True


# ============================================================================
# Employee Schemas
# ============================================================================

class EmployeeBase(BaseModel):
    employee_id: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    tin: Optional[str] = None
    department: Optional[str] = None
    position: Optional[str] = None
    employment_type: Optional[str] = None
    date_hired: Optional[date] = None
    basic_salary: Optional[Decimal] = None


class EmployeeCreate(EmployeeBase):
    pass


class EmployeeUpdate(BaseModel):
    position: Optional[str] = None
    basic_salary: Optional[Decimal] = None
    department: Optional[str] = None


class EmployeeResponse(EmployeeBase):
    id: int
    subsidiary_id: str
    created_at: datetime
    updated_at: datetime
    is_deleted: bool

    class Config:
        from_attributes = True


# ============================================================================
# Customer Schemas
# ============================================================================

class CustomerBase(BaseModel):
    customer_id: str
    customer_name: Optional[str] = None
    customer_tin: Optional[str] = None
    contact_email: Optional[str] = None
    phone: Optional[str] = None
    is_active: bool = True


class CustomerCreate(CustomerBase):
    pass


class CustomerUpdate(BaseModel):
    customer_name: Optional[str] = None
    contact_email: Optional[str] = None
    phone: Optional[str] = None
    is_active: Optional[bool] = None


class CustomerResponse(CustomerBase):
    id: int
    subsidiary_id: str
    created_at: datetime
    updated_at: datetime
    is_deleted: bool

    class Config:
        from_attributes = True


# ============================================================================
# Vendor Schemas
# ============================================================================

class VendorBase(BaseModel):
    vendor_id: str
    vendor_name: Optional[str] = None
    vendor_tin: Optional[str] = None
    vendor_type: Optional[str] = None
    payment_terms: Optional[str] = None
    is_accredited: bool = False


class VendorCreate(VendorBase):
    pass


class VendorUpdate(BaseModel):
    vendor_name: Optional[str] = None
    is_accredited: Optional[bool] = None
    payment_terms: Optional[str] = None


class VendorResponse(VendorBase):
    id: int
    subsidiary_id: str
    created_at: datetime
    updated_at: datetime
    is_deleted: bool

    class Config:
        from_attributes = True


# ============================================================================
# Generic Error Response
# ============================================================================

class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
    status_code: int
