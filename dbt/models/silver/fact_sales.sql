with sales as (
    select
        subsidiary_id,
        invoice_number,
        customer_name,
        invoice_date,
        gross_amount,
        discount_amount,
        vat_amount,
        net_amount,
        vat_classification,
        is_interco,
        counterpart_sub_id,
        ingested_at,
        extract(year from invoice_date) as fiscal_year,
        extract(month from invoice_date) as fiscal_month
    from {{ ref('stg_sales_transactions') }}
)

select * from sales
