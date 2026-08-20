-- Grain: one row per payout id per channel. Primary key: payout_key.
--
-- The settlement fact. Carries the reconciliation outcome for its period so that a
-- control report can be built directly off the mart without recomputing the match.

with payouts as (
    select * from {{ ref('stg_payout_summary') }}
),

recon as (
    select * from {{ ref('int_settlement_reconciliation') }}
)

select
    md5(p.payout_id || '|' || coalesce(p.channel, 'NULL'))  as payout_key,
    p.payout_id,
    p.payout_date                                           as date_key,
    coalesce(p.channel, 'UNKNOWN')                          as channel_key,
    p.payout_status,

    p.subtotal,
    p.staff_tip,
    p.commission,
    p.payment_processing_fee,
    p.marketing_fees,
    p.discounts_merchant_funded,
    p.discounts_platform_funded,
    p.marketing_credit,
    p.thirdparty_contribution,
    p.error_charges,
    p.adjustments,
    p.net_total,

    -- internal consistency: do the stated components reproduce the stated net total?
    round(
        coalesce(p.subtotal, 0) + coalesce(p.subtotal_tax_passed, 0)
      + coalesce(p.staff_tip, 0) + coalesce(p.commission, 0)
      + coalesce(p.payment_processing_fee, 0) + coalesce(p.marketing_fees, 0)
      + coalesce(p.discounts_merchant_funded, 0) + coalesce(p.discounts_platform_funded, 0)
      + coalesce(p.discounts_thirdparty_funded, 0) + coalesce(p.marketing_credit, 0)
      + coalesce(p.thirdparty_contribution, 0) + coalesce(p.error_charges, 0)
      + coalesce(p.adjustments, 0) - coalesce(p.net_total, 0)
    , 2)                                                    as net_total_variance,

    case when p.subtotal > 0
         then round((p.subtotal - p.net_total) * 100.0 / p.subtotal, 2)
    end                                                     as effective_take_rate_pct,

    r.is_break,
    r.break_reason,
    r.is_auto_resolved,
    r.delta_subtotal                                        as recon_delta_subtotal
from payouts p
left join recon r
  on  r.payout_date = p.payout_date
  and r.channel     = coalesce(p.channel, 'UNKNOWN')
