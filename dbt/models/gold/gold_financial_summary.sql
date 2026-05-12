with ap as (
    select
        subsidiary_id,
        fiscal_year,
        fiscal_month,
        sum(gross_amount) as total_ap_gross,
        sum(vat_amount) as total_ap_vat,
        sum(net_amount) as total_ap_net,
        sum(payment_amount) as total_ap_paid
    from {{ ref('fact_ap_invoices') }}
    group by subsidiary_id, fiscal_year, fiscal_month
),

ar as (
    select
        subsidiary_id,
        fiscal_year,
        fiscal_month,
        sum(gross_amount) as total_ar_gross,
        sum(vat_amount) as total_ar_vat,
        sum(net_amount) as total_ar_net,
        sum(collected_amount) as total_ar_collected
    from {{ ref('fact_ar_ledger') }}
    group by subsidiary_id, fiscal_year, fiscal_month
),

vat as (
    select
        subsidiary_id,
        period_year as fiscal_year,
        period_month as fiscal_month,
        sum(taxable_sales) as total_taxable_sales,
        sum(output_vat) as total_output_vat,
        sum(input_vat) as total_input_vat,
        sum(vat_payable) as total_vat_payable
    from {{ ref('fact_tax_vat_returns') }}
    group by subsidiary_id, period_year, period_month
)

select
    coalesce(ap.subsidiary_id, ar.subsidiary_id, vat.subsidiary_id) as subsidiary_id,
    coalesce(ap.fiscal_year, ar.fiscal_year, vat.fiscal_year) as fiscal_year,
    coalesce(ap.fiscal_month, ar.fiscal_month, vat.fiscal_month) as fiscal_month,
    ap.total_ap_gross,
    ap.total_ap_vat,
    ap.total_ap_net,
    ap.total_ap_paid,
    ar.total_ar_gross,
    ar.total_ar_vat,
    ar.total_ar_net,
    ar.total_ar_collected,
    vat.total_taxable_sales,
    vat.total_output_vat,
    vat.total_input_vat,
    vat.total_vat_payable
from ap
full outer join ar on ap.subsidiary_id = ar.subsidiary_id and ap.fiscal_year = ar.fiscal_year and ap.fiscal_month = ar.fiscal_month
full outer join vat on coalesce(ap.subsidiary_id, ar.subsidiary_id) = vat.subsidiary_id
    and coalesce(ap.fiscal_year, ar.fiscal_year) = vat.fiscal_year
    and coalesce(ap.fiscal_month, ar.fiscal_month) = vat.fiscal_month
