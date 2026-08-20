-- Marketing campaign dimension. Grain: one row per campaign.

with daily as (
    select * from {{ ref('stg_marketing_campaigns') }}
)

select
    campaign_id                                 as campaign_key,
    max(campaign_name)                          as campaign_name,
    max(promotion_type)                         as promotion_type,
    bool_or(is_self_serve)                      as is_self_serve,
    min(campaign_start_date)                    as campaign_start_date,
    max(campaign_end_date)                      as campaign_end_date,
    min(activity_date)                          as first_activity_date,
    max(activity_date)                          as last_activity_date,
    count(*)                                    as active_days
from daily
group by campaign_id
