select
    subsidiary_id,
    vendor_id,
    certificate_number,
    income_payment,
    tax_withheld,
    tax_rate,
    cert_date,
    status,
    ingested_at
from bronze.wht_certificates
