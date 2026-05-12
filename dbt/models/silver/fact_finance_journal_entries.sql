with journal_entries as (
    select
        subsidiary_id,
        entry_date,
        entry_number,
        account_code,
        account_name,
        debit_amount,
        credit_amount,
        description,
        reference_doc,
        is_interco,
        counterpart_sub_id,
        cost_center,
        posted_by,
        approved_by,
        ingested_at,
        extract(year from entry_date) as fiscal_year,
        extract(month from entry_date) as fiscal_month
    from {{ ref('stg_finance__journal_entries') }}
)

select * from journal_entries
