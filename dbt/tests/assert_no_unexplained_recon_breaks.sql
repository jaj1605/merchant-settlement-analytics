-- Two-sided reconciliation must leave no unclassified break.
--
-- Breaks caused by period cutoff are auto-resolved with a reason code. Anything the
-- classifier cannot explain is a genuine open item and fails the build — which is
-- the point: an unexplained difference between two independent records of the same
-- money is exactly what a control is meant to surface.
-- Severity: error.

select
    payout_date,
    channel,
    delta_subtotal,
    delta_commission,
    break_reason
from {{ ref('int_settlement_reconciliation') }}
where break_reason = 'unexplained - investigate'
