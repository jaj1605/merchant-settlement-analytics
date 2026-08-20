-- Collapses the transaction feed to order grain.
--
-- The transaction feed records reversal cycles: an order can post, reverse, and
-- re-book under the same order id within minutes. Taking the first or last row would
-- misstate revenue in both directions, so the net economic value of an order is the
-- SUM across its cycle. Orders that went through a reversal are flagged so the
-- behaviour stays visible downstream rather than being silently smoothed away.

with txns as (
    select * from {{ ref('stg_transactions') }}
    where transaction_type = 'Order'
),

aggregated as (
    select
        order_id,
        max(channel)                                            as channel,
        min(event_at)                                           as first_event_at,
        max(event_at)                                           as last_event_at,
        cast(min(event_at) as date)                             as order_date,
        max(payout_date)                                        as payout_date,
        max(final_order_status)                                 as final_order_status,

        count(*)                                                as transaction_count,
        count(*) > 1                                            as has_reversal,
        count_if(subtotal < 0)                                  as reversal_count,

        sum(subtotal)                                           as subtotal,
        sum(staff_tip)                                          as staff_tip,
        sum(commission)                                         as commission,
        sum(payment_processing_fee)                             as payment_processing_fee,
        sum(marketing_fees)                                     as marketing_fees,
        sum(discounts_merchant_funded)                          as discounts_merchant_funded,
        sum(discounts_platform_funded)                          as discounts_platform_funded,
        sum(marketing_credit)                                   as marketing_credit,
        sum(thirdparty_contribution)                            as thirdparty_contribution
    from txns
    group by order_id
)

select * from aggregated
