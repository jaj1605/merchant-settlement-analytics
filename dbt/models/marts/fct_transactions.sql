-- Grain: one row per financial transaction event. Primary key: transaction_id.
--
-- This is the atomic fact — the lowest grain available, and the table every other
-- financial fact is derived from. order_id is a DEGENERATE DIMENSION here: it is
-- carried on the fact for drill-through but is NOT unique, because the source
-- records reversal cycles under a single order id.

select
    t.transaction_id                            as transaction_key,
    t.order_id,
    t.delivery_uuid,
    t.channel                                   as channel_key,
    t.event_date                                as date_key,
    t.payout_date                               as payout_date_key,

    t.transaction_type,
    t.final_order_status,
    t.event_at,

    t.subtotal,
    t.staff_tip,
    t.commission,
    t.payment_processing_fee,
    t.marketing_fees,
    t.discounts_merchant_funded,
    t.discounts_platform_funded,
    t.marketing_credit,
    t.thirdparty_contribution,
    t.error_charges,
    t.adjustments,

    -- net economic value of the event to the merchant
    coalesce(t.subtotal, 0)
      + coalesce(t.staff_tip, 0)
      + coalesce(t.commission, 0)
      + coalesce(t.payment_processing_fee, 0)
      + coalesce(t.marketing_fees, 0)
      + coalesce(t.discounts_merchant_funded, 0)
      + coalesce(t.discounts_platform_funded, 0)
      + coalesce(t.marketing_credit, 0)
      + coalesce(t.thirdparty_contribution, 0)
      + coalesce(t.error_charges, 0)
      + coalesce(t.adjustments, 0)              as net_amount
from {{ ref('stg_transactions') }} t
