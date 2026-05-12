select
    subsidiary_id,
    campaign_id,
    campaign_name,
    campaign_type,
    channel,
    start_date,
    end_date,
    budget_amount,
    actual_spend,
    leads_generated,
    conversions,
    revenue_attributed,
    status,
    ingested_at
from bronze.campaigns
