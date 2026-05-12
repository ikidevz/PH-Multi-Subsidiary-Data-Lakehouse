select
    subsidiary_id,
    movement_date,
    movement_type,
    item_code,
    quantity,
    unit_cost,
    total_cost,
    warehouse_location,
    reference_doc,
    ingested_at
from bronze.inventory_movements
