-- Orders should reach a payout within the settlement lag.
--
-- Orders near the end of the export window legitimately have no payout yet, so the
-- window edge is excluded. Anything older that is still unsettled is an aged item
-- worth chasing.
-- Severity: warn — this is an operational signal, not a pipeline defect.

with bounds as (
    select max(date_key) as window_end from {{ ref('fct_orders') }}
)

select
    o.order_key,
    o.date_key,
    o.subtotal
from {{ ref('fct_orders') }} o
cross join bounds b
where o.payout_date_key is null
  and o.date_key < b.window_end - interval 21 day
