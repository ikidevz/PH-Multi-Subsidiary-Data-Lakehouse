"""
Department-specific PostgreSQL database setup for CRUD operations
Supports CDC via logical replication (Debezium)
"""

import os
from datetime import datetime
from typing import Generator
from sqlalchemy import create_engine, Column, Integer, String, Numeric, Date, DateTime, Boolean, JSON, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

# Configuration
DEPT_TYPE = os.getenv("DEPT_TYPE", "finance")
SUBSIDIARY_ID = os.getenv("SUBSIDIARY_ID", "ABC")

# Department-specific database URL
POSTGRES_HOST = os.getenv("POSTGRES_DEPT_HOST", "postgres-dept")
POSTGRES_PORT = os.getenv("POSTGRES_DEPT_PORT", 5432)
POSTGRES_USER = os.getenv("POSTGRES_DEPT_USER", "postgres")
POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_DEPT_PASSWORD", "multi_subsidiary_dept_password")
POSTGRES_DB = f"dept_{DEPT_TYPE}_{SUBSIDIARY_ID.lower()}"

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# SQLAlchemy setup
Base = declarative_base()
engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


def get_db():
    """Dependency for FastAPI to get DB session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Create all tables and enable logical replication"""
    Base.metadata.create_all(bind=engine)

    # Enable logical replication on the database
    with engine.connect() as conn:
        try:
            conn.execute(text("CREATE PUBLICATION pub_cdc FOR ALL TABLES;"))
            conn.commit()
        except Exception as e:
            # Publication may already exist
            pass


# ============================================================================
# Base audit columns (all tables inherit these)
# ============================================================================

class AuditMixin:
    """Mixin to add audit columns to all models"""
    id = Column(Integer, primary_key=True, autoincrement=True)
    subsidiary_id = Column(String(10), nullable=False, default=SUBSIDIARY_ID)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False,
                        default=datetime.utcnow, onupdate=datetime.utcnow)
    is_deleted = Column(Boolean, nullable=False, default=False)
    deleted_at = Column(DateTime, nullable=True)


# ============================================================================
# Finance Models
# ============================================================================

class JournalEntry(Base, AuditMixin):
    __tablename__ = "journal_entries"

    entry_date = Column(Date, nullable=False)
    entry_number = Column(String(100), nullable=False, unique=True)
    account_code = Column(String(20), nullable=False)
    account_name = Column(String(255))
    debit_amount = Column(Numeric(18, 2), nullable=False, default=0)
    credit_amount = Column(Numeric(18, 2), nullable=False, default=0)
    description = Column(String(500))
    reference_doc = Column(String(100))
    is_interco = Column(Boolean, nullable=False, default=False)
    counterpart_sub_id = Column(String(10))
    cost_center = Column(String(50))
    posted_by = Column(String(100))
    approved_by = Column(String(100))


class APInvoice(Base, AuditMixin):
    __tablename__ = "ap_invoices"

    invoice_number = Column(String(100), nullable=False, unique=True)
    vendor_id = Column(String(50))
    vendor_name = Column(String(255))
    vendor_tin = Column(String(20))
    invoice_date = Column(Date, nullable=False)
    due_date = Column(Date)
    gross_amount = Column(Numeric(18, 2), nullable=False, default=0)
    vat_amount = Column(Numeric(18, 2), nullable=False, default=0)
    net_amount = Column(Numeric(18, 2), nullable=False, default=0)
    wht_amount = Column(Numeric(18, 2), nullable=False, default=0)
    payment_amount = Column(Numeric(18, 2))
    payment_date = Column(Date)
    status = Column(String(20), nullable=False, default="unpaid")
    is_interco = Column(Boolean, nullable=False, default=False)
    counterpart_sub_id = Column(String(10))


class ARLedger(Base, AuditMixin):
    __tablename__ = "ar_ledger"

    invoice_number = Column(String(100), nullable=False, unique=True)
    customer_id = Column(String(50))
    customer_name = Column(String(255))
    customer_tin = Column(String(20))
    invoice_date = Column(Date, nullable=False)
    due_date = Column(Date)
    gross_amount = Column(Numeric(18, 2), nullable=False, default=0)
    vat_amount = Column(Numeric(18, 2), nullable=False, default=0)
    net_amount = Column(Numeric(18, 2), nullable=False, default=0)
    collected_amount = Column(Numeric(18, 2))
    collection_date = Column(Date)
    status = Column(String(20), nullable=False, default="open")
    is_interco = Column(Boolean, nullable=False, default=False)
    counterpart_sub_id = Column(String(10))


# ============================================================================
# HR Models
# ============================================================================

class Employee(Base, AuditMixin):
    __tablename__ = "employees"

    employee_id = Column(String(50), nullable=False, unique=True)
    first_name = Column(String(100))
    last_name = Column(String(100))
    tin = Column(String(20))
    sss_number = Column(String(20))
    philhealth_number = Column(String(20))
    pagibig_number = Column(String(20))
    department = Column(String(100))
    position = Column(String(100))
    employment_type = Column(String(30))
    date_hired = Column(Date)
    basic_salary = Column(Numeric(18, 2))


class PayrollRun(Base, AuditMixin):
    __tablename__ = "payroll_runs"

    payroll_period = Column(String(20), nullable=False)
    employee_id = Column(String(50), nullable=False)
    gross_pay = Column(Numeric(18, 2), nullable=False, default=0)
    total_deductions = Column(Numeric(18, 2), nullable=False, default=0)
    net_pay = Column(Numeric(18, 2), nullable=False, default=0)
    payroll_date = Column(Date)


# ============================================================================
# Sales Models
# ============================================================================

class SalesTransaction(Base, AuditMixin):
    __tablename__ = "sales_transactions"

    invoice_number = Column(String(100), nullable=False, unique=True)
    customer_name = Column(String(255))
    transaction_date = Column(Date, nullable=False)
    gross_amount = Column(Numeric(18, 2), nullable=False, default=0)
    discount_amount = Column(Numeric(18, 2), nullable=False, default=0)
    vat_amount = Column(Numeric(18, 2), nullable=False, default=0)
    net_amount = Column(Numeric(18, 2), nullable=False, default=0)
    vat_classification = Column(String(50))
    is_interco = Column(Boolean, nullable=False, default=False)
    counterpart_sub_id = Column(String(10))


class Customer(Base, AuditMixin):
    __tablename__ = "customers"

    customer_id = Column(String(50), nullable=False, unique=True)
    customer_name = Column(String(255))
    customer_tin = Column(String(20))
    contact_email = Column(String(255))
    phone = Column(String(50))
    is_active = Column(Boolean, nullable=False, default=True)


# ============================================================================
# Ops Models
# ============================================================================

class InventoryMovement(Base, AuditMixin):
    __tablename__ = "inventory_movements"

    movement_date = Column(Date, nullable=False)
    movement_type = Column(String(50), nullable=False)
    item_code = Column(String(100))
    quantity = Column(Integer)
    unit_cost = Column(Numeric(18, 2))
    total_cost = Column(Numeric(18, 2))
    warehouse_location = Column(String(100))
    reference_doc = Column(String(100))


class PurchaseOrder(Base, AuditMixin):
    __tablename__ = "purchase_orders"

    po_number = Column(String(100), nullable=False, unique=True)
    vendor_id = Column(String(50))
    order_date = Column(Date)
    expected_date = Column(Date)
    status = Column(String(30), nullable=False, default="open")
    total_amount = Column(Numeric(18, 2), nullable=False, default=0)


class SalesOrder(Base, AuditMixin):
    __tablename__ = "sales_orders"

    so_number = Column(String(100), nullable=False, unique=True)
    customer_id = Column(String(50))
    order_date = Column(Date)
    ship_date = Column(Date)
    status = Column(String(30), nullable=False, default="pending")
    total_amount = Column(Numeric(18, 2), nullable=False, default=0)


# ============================================================================
# Tax Models
# ============================================================================

class VATReturn(Base, AuditMixin):
    __tablename__ = "vat_returns"

    period_month = Column(Integer, nullable=False)
    period_year = Column(Integer, nullable=False)
    form_type = Column(String(20), nullable=False)
    gross_sales = Column(Numeric(18, 2), nullable=False, default=0)
    taxable_sales = Column(Numeric(18, 2), nullable=False, default=0)
    output_vat = Column(Numeric(18, 2), nullable=False, default=0)
    input_vat = Column(Numeric(18, 2), nullable=False, default=0)
    vat_payable = Column(Numeric(18, 2), nullable=False, default=0)
    filing_date = Column(Date)
    status = Column(String(20), nullable=False, default="pending")


# ============================================================================
# Legal Models
# ============================================================================

class Officer(Base, AuditMixin):
    __tablename__ = "officers"

    officer_id = Column(String(50), nullable=False, unique=True)
    name = Column(String(255))
    position = Column(String(100))
    start_date = Column(Date)
    end_date = Column(Date)


class Stockholder(Base, AuditMixin):
    __tablename__ = "stockholders"

    stockholder_id = Column(String(50), nullable=False, unique=True)
    name = Column(String(255))
    ownership_pct = Column(Numeric(6, 2))
    is_foreign = Column(Boolean, nullable=False, default=False)


# ============================================================================
# Procurement Models
# ============================================================================

class Vendor(Base, AuditMixin):
    __tablename__ = "vendors"

    vendor_id = Column(String(50), nullable=False, unique=True)
    vendor_name = Column(String(255))
    vendor_tin = Column(String(20))
    vendor_type = Column(String(50))
    payment_terms = Column(String(50))
    is_accredited = Column(Boolean, nullable=False, default=False)


# ============================================================================
# IT Audit Models
# ============================================================================

class AuditLog(Base, AuditMixin):
    __tablename__ = "audit_log"

    event_timestamp = Column(DateTime, nullable=False)
    user_id = Column(String(50))
    action = Column(String(50))
    result = Column(String(50))
    risk_level = Column(String(50))


class AccessEvent(Base, AuditMixin):
    __tablename__ = "access_events"

    event_timestamp = Column(DateTime, nullable=False)
    user_id = Column(String(50))
    action = Column(String(50))
    resource = Column(String(100))
    resource_id = Column(String(100))
    ip_address = Column(String(50))
    result = Column(String(50))
