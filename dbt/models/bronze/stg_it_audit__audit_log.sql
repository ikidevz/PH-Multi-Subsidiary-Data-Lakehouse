select
    subsidiary_id,
    event_timestamp,
    user_id,
    action,
    result,
    risk_level,
    ingested_at
from bronze.audit_log
