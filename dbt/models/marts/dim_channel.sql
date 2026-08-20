-- Sales channel dimension. Small, but conformed: keeping channel in a dimension
-- rather than as free text on each fact means a channel rename happens in one place.

with channels as (
    select distinct channel from {{ ref('stg_transactions') }} where channel is not null
    union
    select distinct channel from {{ ref('stg_payout_summary') }} where channel is not null
)

select
    channel                                     as channel_key,
    channel                                     as channel_name,
    case channel
        when 'Marketplace' then 'Platform-sourced demand; platform takes commission'
        when 'Storefront'  then 'Merchant-sourced demand; payment processing fee only'
        else 'Unclassified'
    end                                         as channel_description
from channels
