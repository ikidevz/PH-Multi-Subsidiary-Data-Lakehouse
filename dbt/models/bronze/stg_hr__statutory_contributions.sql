select
    subsidiary_id,
    period_month,
    period_year,
    sss_er_amount,
    philhealth_er_amount,
    pagibig_er_amount,
    ec_life_insurance_amount,
    remittance_date,
    status,
    ingested_at
from bronze.statutory_contributions
