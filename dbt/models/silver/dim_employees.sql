with employees as (
    select
        subsidiary_id,
        employee_id,
        first_name,
        last_name,
        {{ mask_tin('tin') }} as tin_hashed,
        department,
        position,
        employment_type,
        date_hired,
        basic_salary
    from {{ ref('stg_employees') }}
)

select * from employees
