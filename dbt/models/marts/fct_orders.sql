-- Grain: one row per order. Primary key: order_key (the platform order id).
--
-- Built from int_order_transactions, which nets each order's reversal cycle. Orders
-- that reversed and re-booked are flagged rather than smoothed away, because a
-- reversal rate is itself an operational signal.

with orders as (
    select * from {{ ref('int_order_transactions') }}
),

error_rollup as (
    select
        order_id,
        sum(charge_amount)                                  as error_charge_amount,
        sum(leakage_amount)                                 as unrecovered_error_amount,
        count(*)                                            as error_claim_count,
        count_if(is_recovered)                              as recovered_claim_count
    from {{ ref('int_error_recovery') }}
    group by order_id
)

select
    o.order_id                                              as order_key,
    o.channel                                               as channel_key,
    o.order_date                                            as date_key,
    o.payout_date                                           as payout_date_key,

    o.final_order_status,
    o.first_event_at,
    o.last_event_at,
    o.transaction_count,
    o.has_reversal,
    o.reversal_count,

    o.subtotal,
    o.staff_tip,
    o.commission,
    o.payment_processing_fee,
    o.marketing_fees,
    o.discounts_merchant_funded,
    o.discounts_platform_funded,
    o.marketing_credit,

    coalesce(e.error_charge_amount, 0)                      as error_charge_amount,
    coalesce(e.unrecovered_error_amount, 0)                 as unrecovered_error_amount,
    coalesce(e.error_claim_count, 0)                        as error_claim_count,
    coalesce(e.recovered_claim_count, 0)                    as recovered_claim_count,

    -- merchant's net take on the order, before error charges
    coalesce(o.subtotal, 0) + coalesce(o.commission, 0)
      + coalesce(o.payment_processing_fee, 0)
      + coalesce(o.marketing_fees, 0)
      + coalesce(o.discounts_merchant_funded, 0)            as net_before_errors,

    case when o.subtotal > 0
         then round(-o.commission * 100.0 / o.subtotal, 2)
    end                                                     as commission_rate_pct
from orders o
left join error_rollup e on e.order_id = o.order_id
