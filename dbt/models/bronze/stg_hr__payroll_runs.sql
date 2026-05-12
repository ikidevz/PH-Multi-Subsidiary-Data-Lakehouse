select
    subsidiary_id,
    employee_id,
    payroll_period,
    gross_pay,
    total_deductions,
    net_pay,
    sss_ee,
    philhealth_ee,
    pagibig_ee,
    wht,
    ingested_at
from bronze.payroll_runs
