select
    subsidiary_id,
    form_type,
    period_covered,
    due_date,
    filing_date,
    amount_paid,
    surcharge_amount,
    interest_amount,
    compromise_amount,
    penalty_amount,
    efps_ref,
    payment_mode,
    status,
    ingested_at
from bronze.bir_filings_log
