select
    subsidiary_id,
    event_timestamp,
    user_id,
    action,
    resource,
    resource_id,
    ip_address,
    result,
    ingested_at
from bronze.access_events
