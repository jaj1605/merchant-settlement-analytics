-- Grain: one row per menu item per channel, aggregated over the export period.

with source as (
    select * from {{ source('raw', 'product_mix') }}
),

renamed as (
    select
        {{ clean_string('"Item name"') }}           as item_name,
        {{ clean_string('"Channel"') }}             as channel,
        coalesce("Popular item", '0') = '1'         as is_popular_item,
        cast("Start date" as date)                  as first_sold_date,
        cast("End date" as date)                    as last_sold_date,
        {{ to_amount('"Gross sales"') }}            as gross_sales,
        {{ to_amount('"Discounts"') }}              as discounts,
        try_cast("Total sold" as integer)           as units_sold,
        try_cast("Total item errors" as integer)    as item_error_count,
        {{ to_amount('"Total error charges"') }}    as item_error_charges,

        _source_file,
        _loaded_at,
        _row_hash
    from source
)

select * from renamed
