with inventory_summary as (
    select
        subsidiary_id,
        movement_type,
        sum(quantity) as total_quantity,
        sum(total_cost) as total_cost,
        extract(year from movement_date) as fiscal_year,
        extract(month from movement_date) as fiscal_month
    from {{ ref('fact_inventory_movements') }}
    group by subsidiary_id, movement_type, fiscal_year, fiscal_month
)

select * from inventory_summary
