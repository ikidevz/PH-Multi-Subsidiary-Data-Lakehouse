with ar as (
    select
        subsidiary_id,
        invoice_number,
        customer_id,
        customer_name,
        invoice_date,
        due_date,
        gross_amount,
        vat_amount,
        net_amount,
        collected_amount,
        collection_date,
        status,
        is_interco,
        counterpart_sub_id,
        ingested_at,
        extract(year from invoice_date) as fiscal_year,
        extract(month from invoice_date) as fiscal_month
    from {{ ref('stg_finance__ar_ledger') }}
)

select * from ar
