# Merchant Settlement Analytics Pipeline

An end-to-end ELT pipeline over a restaurant's third-party delivery settlement data: raw exports land in a warehouse, dbt models them into a star schema, and data quality controls decide whether the results are trustworthy enough to publish.

## The finding

A full year of settlement data, reconciled two ways:

- **Every weekly payout reconciles to $0.00** — all 72 payouts, stated components against stated net total.
- **91.9% first-pass match** on the independent order-level roll-up (68 of 74 payout-date × channel groups). All 6 breaks classify to period-boundary cutoff timing. **Zero unexplained.**
- **Only 57.3% of error charges are recovered.** Of $1,027.46 deducted across 69 claims, $438.60 never came back.
- **Recovery depends on claim type — but not on how much of the order was wrong.**

  | Claim type | Claims | Charged | Recovered | Leakage | Share of leakage |
  |---|---:|---:|---:|---:|---:|
  | `item missing` | 45 | $522.14 | 41.2% | $307.03 | 70.0% |
  | `incorrect order` | 16 | $428.24 | 80.7% | $82.78 | 18.9% |
  | `food quality` | 8 | $77.08 | 36.7% | $48.79 | 11.1% |

  Missing-item alone is **70% of all unrecovered value**, so that is where the money is. The tempting reading is that disputes succeed on whole-order failures and fail on single-item ones — but `food quality` is a whole-order complaint and recovers worst of the three, at 36.7%. It is small enough ($77.08 charged) not to move the headline, and 8 claims is a thin base to argue from, so it is reported as a qualifier rather than a finding. What it rules out is the clean scope-based story: the split is not simply whole-order versus single-item.

  Why the three differ is not in this data. A plausible reading is that it tracks what the platform's own record can settle without the customer's account of events — but nothing here tests that, and it is not claimed as a result.
- **Unit economics:** $108,141.73 gross, 28.49% commission rate, **29.47% effective take rate**, $76,273.19 net.

## Architecture

```
source CSVs ──► anonymize ──► ingest (raw) ──► dbt staging ──► intermediate ──► marts ──► dashboard
                                  │                                                │
                             _load_log                                      data quality tests
                                                                                   │
                                                                    fail ──► pipeline stops
```

Orchestrated in Airflow. Every pull request runs the full build against synthetic fixtures in GitHub Actions.

### Star schema

**Facts**

| Model | Grain | Key |
|---|---|---|
| `fct_transactions` | one row per transaction event | `transaction_key` |
| `fct_orders` | one row per order, reversal cycles netted | `order_key` |
| `fct_payouts` | one row per payout per channel | `payout_key` |
| `fct_error_events` | one row per error charge claim | `error_event_key` |

**Dimensions:** `dim_date` (conformed), `dim_channel`, `dim_item`, `dim_campaign`

## Three problems in the data, and how the model handles them

**Order IDs are not unique.** An order can post, reverse, and re-book under one id within minutes — `BB4DD059` posts $39.65, reverses −$39.65, then re-books $39.65. The transaction id is the only true key. `int_order_transactions` sums across the cycle rather than picking a row, and flags `has_reversal` so the behaviour stays visible.

**The order feed has no order identifier at all.** It cannot be joined row-by-row to the transaction feed. That constraint is a feature: it makes the two sources genuinely independent, which is what makes the comparison a reconciliation rather than a self-consistency check.

**Payouts settle on a lag.** The edges of any export window will always disagree. The classifier resolves a break as cutoff timing only when the *direction* of the difference agrees with the window edge — a difference at an edge with the wrong sign stays unexplained, so the rule cannot quietly absorb a real break.

## Data quality

33 tests. Generic tests cover key uniqueness, not-null, accepted values, and referential integrity between facts and dimensions. Six custom tests cover business rules:

| Test | Severity | Why |
|---|---|---|
| `assert_payout_net_total_reconciles` | error | If the source can't add up its own numbers, nothing downstream is trustworthy |
| `assert_no_unexplained_recon_breaks` | error | An unclassified difference between independent records is the whole point of the control |
| `assert_error_charges_sign_convention` | error | A sign flip would silently invert leakage |
| `assert_recovery_credit_used_once` | error | Guards the matching logic, not the source |
| `assert_ingestion_had_no_failures` | error | Prevents transformations running on a partial load |
| `assert_orders_settle_within_lag` | warn | Operational signal, not a pipeline defect |

Severity is a deliberate choice per test: a broken pipeline stops the build, an operational condition warns and continues.

## Running it

```bash
pip install -r requirements.txt

# with real source exports
python ingestion/anonymize.py --src /path/to/exports --dest data/anonymized
python ingestion/ingest.py --src data/anonymized --db warehouse.duckdb
cd dbt && dbt build

# or with synthetic fixtures — no client data needed
python ingestion/make_fixtures.py --dest data/anonymized
python ingestion/ingest.py --src data/anonymized --db warehouse.duckdb
cd dbt && dbt build
```

`dbt docs generate && dbt docs serve` renders the model lineage and column documentation.

## Design decisions

**ELT, not ETL.** The raw layer is a faithful VARCHAR copy of the source. All cleaning, casting, and business logic happens in dbt where it is version-controlled, tested, and reviewable. A source type change can never fail ingestion.

**Idempotent ingestion.** Every row carries a content hash; loads insert only hashes not already present. Re-running after a partial failure is safe, which is what makes automatic retries in Airflow safe.

**Fail closed.** `dbt test` is a separate Airflow task and publishing depends on it. Controls are not advisory.

**No client data anywhere.** Identifying attributes are replaced with surrogates before ingestion. CI builds against synthetic fixtures that reproduce the *shape* of the source — including a reversal cycle, an unrecovered error charge, and a cutoff break — so the controls are genuinely exercised without client data in the repo.

## Warehouse portability

Developed on DuckDB for fast, credential-free iteration. `profiles.yml` carries a Snowflake target for migration; models are standard SQL with minimal dialect-specific syntax.

## Repository layout

```
ingestion/     anonymize.py, ingest.py, make_fixtures.py
dbt/
  models/
    staging/       stg_* — 1:1 with sources, rename and cast only
    intermediate/  int_* — joins, reversal netting, recovery matching, reconciliation
    marts/         fct_* and dim_* — the star schema
  tests/           custom business-rule tests
  macros/          shared casting helpers
dags/          Airflow DAG
docs/          requirements.md — written before the code
.github/       CI workflow
```
