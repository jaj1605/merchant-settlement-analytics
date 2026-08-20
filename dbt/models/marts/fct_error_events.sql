-- Grain: one row per error charge claim. Primary key: charge_transaction_id.
--
-- The dispute lifecycle fact: what was charged, whether it was recovered, how long
-- recovery took, and what leaked permanently.

select
    r.charge_transaction_id                     as error_event_key,
    r.order_id,
    r.delivery_uuid,
    r.channel                                   as channel_key,
    cast(r.charged_at as date)                  as date_key,
    r.charge_payout_date                        as payout_date_key,

    r.claim_type,
    r.description,
    r.charged_at,
    r.credited_at,
    r.credit_transaction_id,

    r.charge_amount,
    r.is_recovered,
    r.recovery_lag_days,
    r.leakage_amount,

    case
        when not r.is_recovered            then 'not recovered'
        when r.recovery_lag_days <= 3      then '0-3 days'
        when r.recovery_lag_days <= 7      then '4-7 days'
        when r.recovery_lag_days <= 14     then '8-14 days'
        else '15+ days'
    end                                         as recovery_aging_bucket
from {{ ref('int_error_recovery') }} r
