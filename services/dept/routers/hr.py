"""
routers/hr.py
HR department endpoints: employees (CRUD), payroll runs, statutory contributions, DOLE report.
"""

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from database import get_db, Employee
from schemas import EmployeeCreate, EmployeeUpdate, EmployeeResponse
from .deps import SUB_ID, gen, dept_guard

router = APIRouter(tags=["HR"])


# ── Employees ─────────────────────────────────────────────────────────────────

@router.get("/employees")
def employees():
    dept_guard("hr")
    return gen.employees()


@router.post("/employees", response_model=EmployeeResponse, status_code=status.HTTP_201_CREATED)
def create_employee(emp: EmployeeCreate, db: Session = Depends(get_db)):
    """Create employee (CRUD INSERT)"""
    dept_guard("hr")
    db_emp = Employee(**emp.dict(), subsidiary_id=SUB_ID)
    db.add(db_emp)
    db.commit()
    db.refresh(db_emp)
    return db_emp


@router.patch("/employees/{emp_id}", response_model=EmployeeResponse)
def update_employee(emp_id: int, update: EmployeeUpdate, db: Session = Depends(get_db)):
    """Update employee"""
    dept_guard("hr")
    emp = (
        db.query(Employee)
        .filter(Employee.id == emp_id, Employee.is_deleted == False, Employee.subsidiary_id == SUB_ID)
        .first()
    )
    if not emp:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Employee not found")
    for key, value in update.dict(exclude_unset=True).items():
        setattr(emp, key, value)
    db.commit()
    db.refresh(emp)
    return emp


# ── Payroll & Contributions ───────────────────────────────────────────────────

@router.get("/payroll-runs")
def payroll_runs(days: int = Query(30, ge=1, le=365)):
    dept_guard("hr")
    return gen.payroll_runs(days=days)


@router.get("/statutory-contributions")
def statutory_contributions(days: int = Query(30, ge=1, le=365)):
    dept_guard("hr")
    return gen.statutory_contributions(days=days)


@router.get("/dole-report")
def dole_report(year: int | None = Query(None, ge=2000)):
    dept_guard("hr")
    return gen.dole_report(year=year)
