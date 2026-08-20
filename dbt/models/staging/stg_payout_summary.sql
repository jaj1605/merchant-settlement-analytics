-- Grain: one row per payout date per channel.
-- This is the settlement side of the reconciliation.

with source as (
    select * from {{ source('raw', 'payout_summary') }}
),

renamed as (
    select
        "Payout ID"                                                                     as payout_id,
        cast("Payout date" as date)                                                     as payout_date,
        {{ clean_string('"Channel"') }}                                                 as channel,
        {{ clean_string('"Payout status"') }}                                           as payout_status,
        {{ clean_string('"Currency"') }}                                                as currency,

        {{ to_amount('"Subtotal"') }}                                                   as subtotal,
        {{ to_amount('"Subtotal tax passed to merchant"') }}                            as subtotal_tax_passed,
        {{ to_amount('"Staff tip"') }}                                                  as staff_tip,
        {{ to_amount('"Commission"') }}                                                 as commission,
        {{ to_amount('"Payment processing fee"') }}                                     as payment_processing_fee,
        {{ to_amount('"Marketing fees | (including any applicable taxes)"') }}          as marketing_fees,
        {{ to_amount('"Customer discounts from marketing | (funded by you)"') }}         as discounts_merchant_funded,
        {{ to_amount('"Customer discounts from marketing | (funded by DoorDash)"') }}    as discounts_platform_funded,
        {{ to_amount('"Customer discounts from marketing | (funded by a third-party)"') }} as discounts_thirdparty_funded,
        {{ to_amount('"DoorDash marketing credit"') }}                                  as marketing_credit,
        {{ to_amount('"Third-party contribution"') }}                                   as thirdparty_contribution,
        {{ to_amount('"Error charges"') }}                                              as error_charges,
        {{ to_amount('"Adjustments"') }}                                                as adjustments,
        {{ to_amount('"Net total"') }}                                                  as net_total,

        _source_file,
        _loaded_at,
        _row_hash
    from source
)

select * from renamed
