select
    subsidiary_id,
    stockholder_name,
    tin,
    address,
    shares_owned,
    ownership_pct,
    is_foreign,
    ingested_at
from bronze.stockholders
