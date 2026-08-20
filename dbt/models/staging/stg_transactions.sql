-- Grain: one row per financial transaction event.
--
-- NOTE ON GRAIN: this is deliberately NOT at order grain. A single order can produce
-- an order row plus later error-charge and adjustment rows, all sharing an order id.
-- Treating this table as one-row-per-order is the most likely modelling mistake with
-- this source, so the distinction is documented here and enforced by tests.

with source as (
    select * from {{ source('raw', 'transactions_detailed') }}
),

renamed as (
    select
        "DoorDash transaction ID"                                                        as transaction_id,
        "DoorDash order ID"                                                              as order_id,
        {{ clean_string('"Delivery UUID"') }}                                            as delivery_uuid,
        {{ clean_string('"Transaction type"') }}                                         as transaction_type,
        {{ clean_string('"Channel"') }}                                                  as channel,
        {{ clean_string('"Final order status"') }}                                       as final_order_status,
        {{ clean_string('"Description"') }}                                              as description,
        try_cast("Timestamp local time" as timestamp)                                    as event_at,
        try_cast("Timestamp local date" as date)                                         as event_date,
        cast("Payout date" as date)                                                      as payout_date,

        {{ to_amount('"Subtotal"') }}                                                    as subtotal,
        {{ to_amount('"Subtotal tax passed to merchant"') }}                             as subtotal_tax_passed,
        {{ to_amount('"Staff tip"') }}                                                   as staff_tip,
        {{ to_amount('"Commission"') }}                                                  as commission,
        {{ to_amount('"Payment processing fee"') }}                                      as payment_processing_fee,
        {{ to_amount('"Marketing fees | (including any applicable taxes)"') }}           as marketing_fees,
        {{ to_amount('"Customer discounts from marketing | (funded by you)"') }}          as discounts_merchant_funded,
        {{ to_amount('"Customer discounts from marketing | (funded by DoorDash)"') }}     as discounts_platform_funded,
        {{ to_amount('"DoorDash marketing credit"') }}                                   as marketing_credit,
        {{ to_amount('"Third-party contribution"') }}                                    as thirdparty_contribution,
        {{ to_amount('"Error charges"') }}                                               as error_charges,
        {{ to_amount('"Adjustments"') }}                                                 as adjustments,

        _source_file,
        _loaded_at,
        _row_hash
    from source
)

select * from renamed
