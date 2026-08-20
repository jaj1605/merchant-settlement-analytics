-- Conformed date dimension shared by every fact table, so facts at different grains
-- (orders, weekly payouts, error events) can be compared on one calendar.
-- Spine is generated from the observed data range rather than hardcoded.

with bounds as (
    select
        least(
            (select min(order_date)   from {{ ref('int_order_transactions') }}),
            (select min(payout_date)  from {{ ref('stg_payout_summary') }})
        ) as min_date,
        greatest(
            (select max(order_date)   from {{ ref('int_order_transactions') }}),
            (select max(payout_date)  from {{ ref('stg_payout_summary') }})
        ) as max_date
    from (select 1) _
),

spine as (
    select unnest(generate_series(
        (select min_date from bounds),
        (select max_date from bounds),
        interval 1 day
    ))::date as date_day
)

select
    date_day                                                    as date_key,
    date_day,
    extract(year    from date_day)                              as year_number,
    extract(quarter from date_day)                              as quarter_number,
    extract(month   from date_day)                              as month_number,
    strftime(date_day, '%B')                                    as month_name,
    strftime(date_day, '%Y-%m')                                 as year_month,
    extract(week    from date_day)                              as week_number,
    extract(dayofweek from date_day)                            as day_of_week,
    strftime(date_day, '%A')                                    as day_name,
    extract(dayofweek from date_day) in (0, 6)                  as is_weekend
from spine
