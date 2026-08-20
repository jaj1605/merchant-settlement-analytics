-- Matches each error charge to the adjustment credit that later reverses it.
--
-- Business context: the platform deducts an error charge when an order has a problem
-- (missing item, wrong order, quality complaint). The merchant can dispute it, and a
-- successful dispute appears later as an adjustment credit on the same delivery.
--
-- Matching rule: same delivery, credit amount equals charge amount, credit occurs on
-- or after the charge. Each credit is consumed by at most one charge, so a delivery
-- with two identical charges and one credit correctly shows one recovered and one
-- outstanding.
--
-- An unmatched charge is permanent leakage — money deducted and never returned.

with charges as (
    select
        transaction_id                  as charge_transaction_id,
        delivery_uuid,
        order_id,
        channel,
        description,
        event_at                        as charged_at,
        payout_date                     as charge_payout_date,
        abs(error_charge_amount)        as charge_amount
    from {{ ref('stg_error_charges') }}
    where transaction_type = 'Error Charge'
      and delivery_uuid is not null
),

credits as (
    select
        transaction_id                  as credit_transaction_id,
        delivery_uuid,
        event_at                        as credited_at,
        adjustment_amount               as credit_amount
    from {{ ref('stg_error_charges') }}
    where transaction_type = 'Adjustment'
      and delivery_uuid is not null
),

-- rank candidate credits per charge, and charges per credit, so each side is used once
candidates as (
    select
        ch.charge_transaction_id,
        cr.credit_transaction_id,
        cr.credited_at,
        row_number() over (
            partition by ch.charge_transaction_id
            order by cr.credited_at, cr.credit_transaction_id
        ) as credit_rank,
        row_number() over (
            partition by cr.credit_transaction_id
            order by ch.charged_at, ch.charge_transaction_id
        ) as charge_rank
    from charges ch
    join credits cr
      on  ch.delivery_uuid = cr.delivery_uuid
      and cr.credited_at   >= ch.charged_at
      and abs(cr.credit_amount - ch.charge_amount) < 0.005
),

matched as (
    select charge_transaction_id, credit_transaction_id, credited_at
    from candidates
    where credit_rank = 1 and charge_rank = 1
)

select
    ch.charge_transaction_id,
    ch.delivery_uuid,
    ch.order_id,
    ch.channel,
    ch.description,
    ch.charged_at,
    ch.charge_payout_date,
    ch.charge_amount,
    m.credit_transaction_id,
    m.credited_at,
    m.credit_transaction_id is not null                                 as is_recovered,
    date_diff('day', ch.charged_at, m.credited_at)                      as recovery_lag_days,
    case when m.credit_transaction_id is null then ch.charge_amount else 0 end as leakage_amount,

    -- claim taxonomy: what kind of failure caused the charge
    case
        when lower(ch.description) like '%missing%'                                     then 'item missing'
        when lower(ch.description) like '%quality%'                                     then 'food quality'
        when lower(ch.description) like '%incorrect%' or lower(ch.description) like '%wrong%' then 'incorrect order'
        else 'other'
    end                                                                 as claim_type
from charges ch
left join matched m on m.charge_transaction_id = ch.charge_transaction_id
