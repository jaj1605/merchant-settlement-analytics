-- Every payout's stated components must reproduce its stated net total.
--
-- Tolerance is half a cent: below that is float representation noise, above it is a
-- real difference. This is the first control any settlement process needs, because
-- if the source cannot add up its own numbers, nothing downstream can be trusted.
-- Severity: error. A failure here should stop the pipeline.

select
    payout_key,
    payout_id,
    date_key,
    channel_key,
    net_total,
    net_total_variance
from {{ ref('fct_payouts') }}
where abs(net_total_variance) > 0.005
