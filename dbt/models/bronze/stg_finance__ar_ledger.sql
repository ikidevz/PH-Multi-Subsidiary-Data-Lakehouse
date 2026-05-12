select
    subsidiary_id,
    invoice_number,
    customer_id,
    customer_name,
    customer_tin,
    invoice_date,
    due_date,
    gross_amount,
    vat_amount,
    net_amount,
    collected_amount,
    collection_date,
    status,
    is_interco,
    counterpart_sub_id,
    ingested_at
from bronze.ar_ledger
