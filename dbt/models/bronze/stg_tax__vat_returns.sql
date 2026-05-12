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
    efps_confirmation,
    status,
    ingested_at
from bronze.vat_returns
