select
    subsidiary_id,
    filing_type,
    due_date,
    filing_date,
    status,
    reference_number,
    ingested_at
from bronze.sec_filings
