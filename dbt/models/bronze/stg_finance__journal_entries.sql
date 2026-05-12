select
    subsidiary_id,
    entry_date,
    entry_number,
    account_code,
    account_name,
    debit_amount,
    credit_amount,
    description,
    reference_doc,
    is_interco,
    counterpart_sub_id,
    cost_center,
    posted_by,
    approved_by,
    ingested_at
from bronze.journal_entries
