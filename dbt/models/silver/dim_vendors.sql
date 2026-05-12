with vendors as (
    select
        subsidiary_id,
        vendor_id,
        vendor_name,
        vendor_tin,
        vendor_type,
        payment_terms,
        is_accredited,
        ingested_at
    from {{ ref('stg_procurement__vendors') }}
)

select * from vendors
