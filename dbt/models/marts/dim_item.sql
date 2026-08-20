-- Menu item dimension, with the error exposure attributes needed for quality analysis.
-- Grain: one row per item per channel.

select
    md5(item_name || '|' || coalesce(channel, ''))      as item_key,
    item_name,
    channel                                             as channel_key,
    is_popular_item,
    first_sold_date,
    last_sold_date,
    gross_sales,
    discounts,
    units_sold,
    item_error_count,
    item_error_charges,

    -- error rate per item. Deliberately NOT the ranking metric on its own: items with
    -- small unit counts produce unstable rates, so the confidence interval matters.
    case when units_sold > 0
         then round(item_error_count * 100.0 / units_sold, 4)
    end                                                 as error_rate_pct,
    case when units_sold > 0
         then round(item_error_charges / units_sold, 4)
    end                                                 as error_charge_per_unit
from {{ ref('stg_product_mix') }}
