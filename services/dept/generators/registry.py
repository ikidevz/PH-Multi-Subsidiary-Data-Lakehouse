from typing import Any

from .base import DepartmentGenerator
from .finance_generator import FinanceGenerator
from .tax_generator import TaxGenerator
from .hr_generator import HRGenerator
from .ops_generator import OpsGenerator
from .sales_generator import SalesGenerator
from .procurement_generator import ProcurementGenerator
from .legal_generator import LegalGenerator
from .it_audit_generator import ITAuditGenerator

_REGISTRY: dict[str, type[DepartmentGenerator]] = {
    "finance": FinanceGenerator,
    "tax": TaxGenerator,
    "hr": HRGenerator,
    "ops": OpsGenerator,
    "sales": SalesGenerator,
    "procurement": ProcurementGenerator,
    "legal": LegalGenerator,
    "it_audit": ITAuditGenerator,
}


def get_generator_for_dept(dept_type: str, subsidiary_id: str, **kwargs: Any) -> DepartmentGenerator:
    dept_cls = _REGISTRY.get(dept_type)
    if not dept_cls:
        raise ValueError(f"Unsupported department type: {dept_type}")
    return dept_cls(subsidiary_id=subsidiary_id, **kwargs)
