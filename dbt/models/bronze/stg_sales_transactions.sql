with source as (
    select
        subsidiary_id,
        invoice_number,
        customer_name,
        transaction_date as invoice_date,
        gross_amount,
        discount_amount,
        vat_amount,
        net_amount,
        vat_classification,
        is_interco,
        counterpart_sub_id,
        ingested_at
    from bronze.sales_transactions
)

select * from source
