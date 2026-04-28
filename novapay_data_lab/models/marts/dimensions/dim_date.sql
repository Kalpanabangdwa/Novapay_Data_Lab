{{ config(materialized='table', schema='DIMENSIONAL') }}

with date_spine as (

    select
        dateadd(day, seq4(), '2020-01-01') as date_day
    from table(generator(rowcount => 2557))  -- ~7 years

)

select
    date_day,
    year(date_day) as year,
    month(date_day) as month,
    quarter(date_day) as quarter,

    -- fiscal quarter (example: April start)
    case
        when month(date_day) in (4,5,6) then 1
        when month(date_day) in (7,8,9) then 2
        when month(date_day) in (10,11,12) then 3
        else 4
    end as fiscal_quarter,

    dayofweek(date_day) in (6,7) as is_weekend

from date_spine