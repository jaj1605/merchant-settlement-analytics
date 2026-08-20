-- Each adjustment credit may offset at most one error charge.
--
-- Without this, a delivery with two identical charges and one credit would match the
-- credit to both and understate leakage. This test guards the matching logic itself,
-- not the source data.
-- Severity: error.

select
    credit_transaction_id,
    count(*) as charges_matched
from {{ ref('fct_error_events') }}
where credit_transaction_id is not null
group by credit_transaction_id
having count(*) > 1
