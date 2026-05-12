select
    subsidiary_id,
    so_number,
    customer_id,
    so_date,
    delivery_date,
    total_amount,
    status,
    ingested_at
from bronze.sales_orders
