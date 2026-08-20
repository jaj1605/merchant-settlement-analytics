-- Grain: one row per order in the platform's order feed.
--
-- IMPORTANT SOURCE LIMITATION: this export contains no order identifier. It cannot
-- be joined row-by-row to the transaction feed — only compared in aggregate. We
-- therefore generate a surrogate key from the natural business key (placed
-- timestamp + subtotal + channel) purely to give the table a stable primary key.
--
-- This model exists as an INDEPENDENT source for cross-checking order counts and
-- gross sales against the transaction feed. That independence is what makes the
-- downstream comparison a reconciliation rather than a self-consistency check.

with source as (
    select * from {{ source('raw', 'orders') }}
),

renamed as (
    select
        md5(
            coalesce("Order placed date", '') || '|' ||
            coalesce("Order placed time", '') || '|' ||
            coalesce("Subtotal", '')          || '|' ||
            coalesce("Channel", '')
        )                                                       as order_feed_key,

        {{ clean_string('"Channel"') }}                         as channel,
        cast("Order placed date" as date)                       as order_placed_date,
        try_cast("Order placed time" as time)                   as order_placed_time,
        try_cast("Pickup timestamp" as timestamp)               as pickup_at,
        try_cast("Delivery date" as date)                       as delivery_date,
        try_cast("Delivery time" as time)                       as delivery_time,
        lower(coalesce("Is cancelled", 'false')) = 'true'       as is_cancelled,
        {{ to_amount('"Subtotal"') }}                           as subtotal,
        {{ to_amount('"Error charge"') }}                       as error_charge,
        {{ clean_string('"Customer rating"') }}                 as customer_rating,
        {{ clean_string('"Currency"') }}                        as currency,
        {{ clean_string('"Timezone"') }}                        as store_timezone,

        _source_file,
        _loaded_at,
        _row_hash
    from source
)

select * from renamed
