# Requirements — merchant settlement analytics

Written before any code. Consulting work starts with what the client needs, not with a stack.

## Client and context

A single-location restaurant selling through a third-party delivery platform. The platform deducts commission, marketing fees, and error charges from gross sales, then pays the remainder weekly. The owner sees a net deposit and a portal, but has no independent view of whether the deductions are correct or where money is going.

## The problem in the owner's words

> "Money comes in every week and I don't really know what got taken out or whether it's right."

## Decisions this must support

1. **Is the platform paying me correctly?** — an independent check, not just reading the platform's own summary back.
2. **What is this channel actually costing me?** — effective take rate over time, not the headline commission rate.
3. **Am I losing money to order problems, and can I get it back?** — error charges, how many are recovered on dispute, how much never comes back.
4. **Which menu items cause the most problems?** — where operational fixes would pay off.

## Questions the output must answer

| # | Question | Where it is answered |
|---|---|---|
| Q1 | Does each weekly payout add up to what the platform says it does? | `fct_payouts.net_total_variance` |
| Q2 | Does the order-level detail agree with the settlement summary? | `int_settlement_reconciliation` |
| Q3 | When they disagree, why? | `break_reason`, `is_auto_resolved` |
| Q4 | What share of error charges is recovered, and how fast? | `fct_error_events`, `recovery_aging_bucket` |
| Q5 | How much never comes back, and from which claim type? | `leakage_amount` by `claim_type` — reported for every type present, not only the largest |
| Q6 | What is the effective take rate, and is it moving? | `fct_payouts.effective_take_rate_pct` |
| Q7 | Which items drive error exposure? | `dim_item.error_rate_pct`, `error_charge_per_unit` |

## What "correct" means for this data

Defined up front, because a control with an undefined threshold is not a control.

- **Materiality: $0.005.** Below half a cent is float representation noise. Above it is a real difference and is investigated. Chosen because all source amounts are stated to the cent, so any genuine discrepancy is at least $0.01.
- **A payout reconciles** when its stated components sum to its stated net total within materiality.
- **A period reconciles** when the order-level roll-up equals the settlement summary within materiality, per payout date and channel.
- **A break is explained** only when its cause is named. Period-cutoff breaks are auto-resolved when the direction of the difference agrees with the window edge; anything else is an open item and fails the build.
- **An error charge is recovered** when a later adjustment credit on the same delivery matches its amount. Each credit offsets at most one charge.

## Scope boundaries

**In scope:** settlement reconciliation, dispute recovery, unit economics, item error exposure.

**Out of scope:** demand forecasting, menu pricing recommendations, labour and food cost (not in this data), and any causal claim about marketing. The platform reports a ROAS figure; it is observational and is carried through as *the platform's* number, never restated as measured incremental return.

The same restraint applies to dispute recovery. Recovery rates differ sharply by claim type, and that difference is measured and reported. *Why* they differ is not in this data — the dispute outcome is recorded, the platform's reasoning is not. Any explanation is a hypothesis for the client to test against their own dispute history, never a result of this pipeline.

## Constraints discovered during build

Worth recording, because they shaped the model.

1. **The order feed has no order identifier.** It cannot be joined row-by-row to the transaction feed, only compared in aggregate. That limitation is what makes the comparison a genuine reconciliation of two independent sources rather than a self-consistency check.
2. **Order IDs are not unique.** Orders can post, reverse, and re-book under the same id within minutes. The transaction id is the only true key. Net order value is the sum across the reversal cycle — taking the first or last row misstates revenue in both directions.
3. **Payouts settle on a lag,** so the first and last weeks of any export window will always show cutoff differences. This is expected and must be classified, not treated as an error.
4. **Claim type is three-way, not two-way, and the third breaks the obvious story.** The data carries `item missing` (45 claims, 41.2% recovered), `incorrect order` (16, 80.7%) and `food quality` (8, 36.7%). The first two invite a scope-based reading — whole-order failures get resolved, single-item ones do not — but `food quality` is a whole-order complaint recovering worst of the three, which rules that reading out. At $77.08 charged over 8 claims it does not move the totals and is too thin to support a positive claim of its own; it is recorded because it constrains what may be concluded from the other two. `accepted_values` on `claim_type` also admits `other`, which this export happens not to contain — a fourth type appearing later should be treated as a new case, not folded into an existing one.

## Non-functional requirements

- Re-runnable without duplication (idempotent ingestion).
- Fails closed: if a control fails, nothing downstream is treated as trustworthy.
- No client data in the repository or in CI.
- A new engineer can run the whole thing from a clean checkout with two commands.
