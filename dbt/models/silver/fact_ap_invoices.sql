with ap as (
    select
        subsidiary_id,
        invoice_number,
        vendor_id,
        vendor_name,
        invoice_date,
        due_date,
        gross_amount,
        vat_amount,
        net_amount,
        wht_amount,
        payment_amount,
        payment_date,
        status,
        is_interco,
        counterpart_sub_id,
        ingested_at,
        extract(year from invoice_date) as fiscal_year,
        extract(month from invoice_date) as fiscal_month
    from {{ ref('stg_finance__ap_invoices') }}
)

select * from ap
