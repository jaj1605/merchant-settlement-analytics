-- Two-sided reconciliation: transaction feed rolled up, against reported payouts.
--
-- These are independent records of the same money, which is what makes this a
-- reconciliation. Differences are classified by cause rather than merely counted —
-- an unclassified break is an open item, and the count of unclassified breaks is the
-- number a control report actually reports on.
--
-- The dominant benign cause is period cutoff: payouts settle on a lag, so at the
-- start of the window a payout settles orders that predate the export, and at the
-- end orders exist whose payout falls after it. The direction of the difference must
-- agree with the edge, otherwise the break is NOT explained by timing.

{% set tolerance = 0.005 %}

with txn_side as (
    select
        payout_date,
        coalesce(channel, 'UNKNOWN')    as channel,
        sum(subtotal)                   as subtotal,
        sum(commission)                 as commission,
        sum(error_charges)              as error_charges,
        sum(adjustments)                as adjustments
    from {{ ref('stg_transactions') }}
    group by 1, 2
),

payout_side as (
    select
        payout_date,
        coalesce(channel, 'UNKNOWN')    as channel,
        sum(subtotal)                   as subtotal,
        sum(commission)                 as commission,
        sum(error_charges)              as error_charges,
        sum(adjustments)                as adjustments
    from {{ ref('stg_payout_summary') }}
    group by 1, 2
),

bounds as (
    select
        min(payout_date) as window_start,
        max(payout_date) as window_end
    from payout_side
),

joined as (
    select
        coalesce(t.payout_date, p.payout_date)                              as payout_date,
        coalesce(t.channel, p.channel)                                      as channel,
        coalesce(t.subtotal, 0)                                             as txn_subtotal,
        coalesce(p.subtotal, 0)                                             as payout_subtotal,
        coalesce(t.commission, 0)                                           as txn_commission,
        coalesce(p.commission, 0)                                           as payout_commission,
        coalesce(t.error_charges, 0)                                        as txn_error_charges,
        coalesce(p.error_charges, 0)                                        as payout_error_charges,
        coalesce(t.adjustments, 0)                                          as txn_adjustments,
        coalesce(p.adjustments, 0)                                          as payout_adjustments
    from txn_side t
    full outer join payout_side p
      on t.payout_date = p.payout_date and t.channel = p.channel
),

deltas as (
    select
        j.*,
        b.window_start,
        b.window_end,
        round(txn_subtotal      - payout_subtotal,      2)  as delta_subtotal,
        round(txn_commission    - payout_commission,    2)  as delta_commission,
        round(txn_error_charges - payout_error_charges, 2)  as delta_error_charges,
        round(txn_adjustments   - payout_adjustments,   2)  as delta_adjustments
    from joined j
    cross join bounds b
)

select
    *,
    (abs(delta_subtotal)      > {{ tolerance }}
     or abs(delta_commission) > {{ tolerance }}
     or abs(delta_error_charges) > {{ tolerance }}
     or abs(delta_adjustments)   > {{ tolerance }})                     as is_break,

    case
        when abs(delta_subtotal) <= {{ tolerance }}
             and abs(delta_commission) <= {{ tolerance }}
             and abs(delta_error_charges) <= {{ tolerance }}
             and abs(delta_adjustments) <= {{ tolerance }}
            then 'matched'
        -- opening edge: payout settles orders that predate the order export
        when payout_date <= window_start + interval 14 day and delta_subtotal < 0
            then 'cutoff/timing - settles pre-window orders'
        -- closing edge: orders booked whose payout falls after the export
        when payout_date >= window_end - interval 14 day and delta_subtotal > 0
            then 'cutoff/timing - settles post-window'
        else 'unexplained - investigate'
    end                                                                 as break_reason,

    case
        when payout_date <= window_start + interval 14 day and delta_subtotal < 0 then true
        when payout_date >= window_end   - interval 14 day and delta_subtotal > 0 then true
        else false
    end                                                                 as is_auto_resolved
from deltas
