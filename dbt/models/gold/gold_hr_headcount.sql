with employee_counts as (
    select
        subsidiary_id,
        count(*) as headcount,
        avg(basic_salary) as average_salary,
        min(basic_salary) as min_salary,
        max(basic_salary) as max_salary
    from {{ ref('dim_employees') }}
    group by subsidiary_id
)

select * from employee_counts
