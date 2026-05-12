select
    subsidiary_id,
    incident_id,
    incident_date,
    incident_type,
    severity,
    description,
    resolved_date,
    ingested_at
from bronze.system_incidents
