with sales as (
    select
        subsidiary_id,
        fiscal_year,
        fiscal_month,
        sum(gross_amount) as revenue_gross,
        sum(net_amount) as revenue_net,
        sum(vat_amount) as output_vat
    from {{ ref('fact_sales') }}
    group by subsidiary_id, fiscal_year, fiscal_month
)

select
    subsidiary_id,
    fiscal_year,
    fiscal_month,
    revenue_gross,
    revenue_net,
    output_vat
from sales
