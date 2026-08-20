-- Grain: one row per campaign per day.

with source as (
    select * from {{ source('raw', 'marketing_promotion') }}
),

renamed as (
    select
        "Campaign ID"                                                                       as campaign_id,
        {{ clean_string('"Campaign name"') }}                                               as campaign_name,
        {{ clean_string('"Type of promotion"') }}                                           as promotion_type,
        lower(coalesce("Is self serve campaign", 'false')) = 'true'                         as is_self_serve,
        cast("Date" as date)                                                                as activity_date,
        cast("Campaign start date" as date)                                                 as campaign_start_date,
        cast("Campaign end date" as date)                                                   as campaign_end_date,
        try_cast("Orders" as integer)                                                       as orders,
        {{ to_amount('"Sales"') }}                                                          as sales,
        {{ to_amount('"Customer discounts from marketing | (Funded by you)"') }}             as discounts_merchant_funded,
        {{ to_amount('"Customer discounts from marketing | (Funded by DoorDash)"') }}        as discounts_platform_funded,
        {{ to_amount('"Marketing fees | (including any applicable taxes)"') }}               as marketing_fees,
        {{ to_amount('"DoorDash marketing credit"') }}                                      as marketing_credit,
        {{ to_amount('"Average order value"') }}                                            as average_order_value,
        {{ to_amount('"ROAS"') }}                                                           as platform_reported_roas,
        try_cast("New customers acquired" as integer)                                       as new_customers,
        try_cast("Existing customers acquired" as integer)                                  as existing_customers,
        try_cast("Total customers acquired" as integer)                                     as total_customers,

        _source_file,
        _loaded_at,
        _row_hash
    from source
)

select * from renamed
