select
    subsidiary_id,
    form_type,
    period_month,
    period_year,
    payee_tin,
    payee_name,
    income_payment,
    atc_code,
    tax_rate,
    wht_amount,
    filing_date,
    efps_confirmation,
    status,
    ingested_at
from bronze.wht_filings
