select
    subsidiary_id,
    customer_id,
    customer_name,
    tin,
    address,
    industry,
    credit_limit,
    is_active,
    ingested_at
from bronze.customers
