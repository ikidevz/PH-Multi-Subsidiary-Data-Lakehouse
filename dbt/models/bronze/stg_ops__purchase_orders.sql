select
    subsidiary_id,
    po_number,
    vendor_id,
    po_date,
    delivery_date,
    total_amount,
    status,
    ingested_at
from bronze.purchase_orders
