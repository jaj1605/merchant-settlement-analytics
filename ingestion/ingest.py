"""
Ingest anonymized source CSVs into the warehouse raw layer.

This is the EL of ELT: land the data faithfully, transform later in dbt. Nothing
here cleans, renames, or reshapes anything — raw stays raw so that every
transformation is version-controlled and testable downstream.

Two properties this layer guarantees:

  IDEMPOTENT  Re-running never duplicates rows. Each row carries a _row_hash of its
              own content plus its source file; loads insert only hashes not already
              present. Re-running a partially failed load is therefore safe, which is
              the single most important property of an ingestion job that runs on a
              schedule.

  OBSERVABLE  Every run writes a row to raw._load_log with counts and status. A
              pipeline that runs silently is a pipeline nobody trusts.

Usage:
    python ingestion/ingest.py --src data/anonymized --db warehouse.duckdb
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

# source file (without .csv) -> raw table name
DATASETS = {
    "sales_by_order": "orders",
    "sales_by_time": "sales_by_time",
    "financial_detailed_transactions": "transactions_detailed",
    "financial_simplified_transactions": "transactions_simplified",
    "financial_payout_summary": "payout_summary",
    "financial_error_charges_and_adjustments": "error_charges",
    "product_mix": "product_mix",
    "marketing_promotion": "marketing_promotion",
}


def ensure_load_log(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw._load_log (
            run_id          VARCHAR,
            loaded_at       TIMESTAMP,
            source_file     VARCHAR,
            target_table    VARCHAR,
            rows_in_file    BIGINT,
            rows_inserted   BIGINT,
            rows_skipped    BIGINT,
            status          VARCHAR,
            message         VARCHAR
        )
        """
    )


def load_one(
    con: duckdb.DuckDBPyConnection,
    csv_path: Path,
    table: str,
    run_id: str,
    loaded_at: datetime,
) -> tuple[int, int, int]:
    """Load one CSV into raw.<table>. Returns (rows_in_file, inserted, skipped)."""
    fq = f"raw.{table}"

    # Read every column as VARCHAR. Type casting is a transformation and belongs in
    # dbt staging, not here — this keeps the raw layer a faithful copy of the source
    # and means a source type change can never fail the ingest.
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _incoming AS
        SELECT
            *,
            '{csv_path.name}'                                   AS _source_file,
            TIMESTAMP '{loaded_at.isoformat(sep=" ", timespec="seconds")}' AS _loaded_at,
            '{run_id}'                                          AS _run_id
        FROM read_csv_auto('{csv_path.as_posix()}', all_varchar=true, header=true)
        """
    )

    # Content hash over the source columns only (excluding our own metadata), so the
    # same source row always produces the same hash regardless of when it was loaded.
    src_cols = [
        r[0] for r in con.execute("DESCRIBE _incoming").fetchall()
        if not r[0].startswith("_")
    ]
    hash_expr = "md5(" + " || '|' || ".join(f"COALESCE(\"{c}\", '')" for c in src_cols) + " || '|' || _source_file)"
    con.execute(f"CREATE OR REPLACE TEMP TABLE _hashed AS SELECT *, {hash_expr} AS _row_hash FROM _incoming")

    rows_in_file = con.execute("SELECT count(*) FROM _hashed").fetchone()[0]

    exists = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='raw' AND table_name=?",
        [table],
    ).fetchone()[0]

    if not exists:
        con.execute(f"CREATE TABLE {fq} AS SELECT * FROM _hashed")
        inserted, skipped = rows_in_file, 0
    else:
        before = con.execute(f"SELECT count(*) FROM {fq}").fetchone()[0]
        con.execute(
            f"""
            INSERT INTO {fq}
            SELECT h.* FROM _hashed h
            WHERE NOT EXISTS (SELECT 1 FROM {fq} t WHERE t._row_hash = h._row_hash)
            """
        )
        after = con.execute(f"SELECT count(*) FROM {fq}").fetchone()[0]
        inserted = after - before
        skipped = rows_in_file - inserted

    return rows_in_file, inserted, skipped


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--src", default="data/anonymized")
    ap.add_argument("--db", default="warehouse.duckdb")
    args = ap.parse_args()

    src = Path(args.src)
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    run_id = hashlib.md5(loaded_at.isoformat().encode()).hexdigest()[:12]

    con = duckdb.connect(args.db)
    ensure_load_log(con)

    print(f"run_id={run_id}  target={args.db}\n")
    print(f"  {'source':<45} {'rows':>7} {'inserted':>9} {'skipped':>8}  status")
    print(f"  {'-' * 45} {'-' * 7} {'-' * 9} {'-' * 8}  ------")

    failures = 0
    for stem, table in DATASETS.items():
        csv_path = src / f"{stem}.csv"
        if not csv_path.exists():
            con.execute(
                "INSERT INTO raw._load_log VALUES (?,?,?,?,?,?,?,?,?)",
                [run_id, loaded_at, csv_path.name, table, 0, 0, 0, "MISSING", "source file not found"],
            )
            print(f"  {csv_path.name:<45} {'-':>7} {'-':>9} {'-':>8}  MISSING")
            failures += 1
            continue
        try:
            n, ins, skip = load_one(con, csv_path, table, run_id, loaded_at)
            con.execute(
                "INSERT INTO raw._load_log VALUES (?,?,?,?,?,?,?,?,?)",
                [run_id, loaded_at, csv_path.name, table, n, ins, skip, "OK", None],
            )
            print(f"  {csv_path.name:<45} {n:>7,} {ins:>9,} {skip:>8,}  OK")
        except Exception as exc:  # noqa: BLE001 - we want the message in the log
            con.execute(
                "INSERT INTO raw._load_log VALUES (?,?,?,?,?,?,?,?,?)",
                [run_id, loaded_at, csv_path.name, table, 0, 0, 0, "FAILED", str(exc)[:500]],
            )
            print(f"  {csv_path.name:<45} {'-':>7} {'-':>9} {'-':>8}  FAILED: {exc}")
            failures += 1

    con.close()

    if failures:
        print(f"\n{failures} dataset(s) failed to load — exiting non-zero so the orchestrator halts.")
        return 1
    print("\nall datasets loaded.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
