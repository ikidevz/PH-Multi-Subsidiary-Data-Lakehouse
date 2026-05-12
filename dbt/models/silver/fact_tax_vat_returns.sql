with vat_returns as (
    select
        subsidiary_id,
        period_month,
        period_year,
        form_type,
        gross_sales,
        exempt_sales,
        zero_rated_sales,
        taxable_sales,
        output_vat,
        input_vat,
        vat_payable,
        filing_date,
        status,
        ingested_at
    from {{ ref('stg_tax__vat_returns') }}
)

select * from vat_returns
