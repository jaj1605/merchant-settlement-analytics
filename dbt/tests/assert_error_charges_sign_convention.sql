-- Error charges are deductions and must never be positive; adjustments are credits
-- and must never be negative. A sign flip here would silently invert leakage
-- calculations, so it is asserted rather than assumed.
-- Severity: error.

select transaction_key, transaction_type, error_charges, adjustments
from {{ ref('fct_transactions') }}
where error_charges > 0
   or adjustments < 0
