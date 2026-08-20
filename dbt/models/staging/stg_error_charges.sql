-- Grain: one row per error charge or adjustment credit.
-- Charges are deductions for order problems; adjustments are later credits that
-- reverse them. Both live in one source table distinguished by transaction type.

with source as (
    select * from {{ source('raw', 'error_charges') }}
),

renamed as (
    select
        "DoorDash transaction ID"                       as transaction_id,
        "DoorDash order ID"                             as order_id,
        {{ clean_string('"Delivery UUID"') }}           as delivery_uuid,
        {{ clean_string('"Transaction type"') }}        as transaction_type,
        {{ clean_string('"Channel"') }}                 as channel,
        {{ clean_string('"Description"') }}             as description,
        try_cast("Timestamp local time" as timestamp)   as event_at,
        cast("Payout date" as date)                     as payout_date,
        {{ to_amount('"Error charges"') }}              as error_charge_amount,
        {{ to_amount('"Adjustments"') }}                as adjustment_amount,

        _source_file,
        _loaded_at,
        _row_hash
    from source
)

select * from renamed
