with inventory as (
    select
        subsidiary_id,
        movement_date,
        movement_type,
        item_code,
        quantity,
        unit_cost,
        total_cost,
        warehouse_location,
        reference_doc,
        ingested_at,
        extract(year from movement_date) as fiscal_year,
        extract(month from movement_date) as fiscal_month
    from {{ ref('stg_ops__inventory_movements') }}
)

select * from inventory
