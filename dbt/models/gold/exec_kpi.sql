with sales as (
    select
        subsidiary_id,
        fiscal_year,
        fiscal_month,
        sum(net_amount) as net_revenue
    from {{ ref('fact_sales') }}
    group by subsidiary_id, fiscal_year, fiscal_month
)

select
    subsidiary_id,
    fiscal_year,
    fiscal_month,
    net_revenue as revenue_net
from sales
