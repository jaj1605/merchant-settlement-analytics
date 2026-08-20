"""
Anonymize client source data before it enters the pipeline.

Runs once, ahead of ingestion. Replaces identifying merchant attributes with stable
surrogate values so that nothing in the repo, the warehouse, or any published output
can be traced back to a real business.

Design choice: surrogates are deterministic (same input always maps to the same
output) so that joins across files still work. They are NOT reversible without this
script's mapping, and the mapping itself is written to data/anonymized/ which is
gitignored.

Usage:
    python ingestion/anonymize.py --src /path/to/raw/csvs --dest data/anonymized
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path

# Columns whose values identify the client. Matched case-insensitively against
# the CSV header, so a column only needs listing once even if files spell it
# slightly differently.
IDENTIFYING_COLUMNS = {
    "business id": "business_id",
    "business name": "business_name",
    "store id": "store_id",
    "store name": "store_name",
    "merchant store id": "merchant_store_id",
    "merchant supplied store id": "store_id",
    "store address": "address",
}

# Fixed replacements. A single-merchant dataset does not need a lookup table for
# names — one surrogate identity covers it — but IDs are mapped through a
# registry so the approach still holds if more stores are added later.
SURROGATE = {
    "business_name": "Merchant A",
    "store_name": "Merchant A - Store 01",
    "address": "REDACTED",
    "merchant_store_id": "NULL",
}

# Free-text columns that can embed the merchant name or id, e.g.
#   "<Promo type> - <Merchant Name> (bus id: <id>)"
# These are scrubbed against names discovered in the data rather than against a
# hardcoded value — hardcoding the client's name into the anonymizer would leak the
# very thing the script exists to remove.
CAMPAIGN_NAME_COLUMNS = {"campaign name"}

# Columns whose values ARE the merchant's real names, collected in a first pass so
# they can be stripped out of free text in the second.
NAME_SOURCE_COLUMNS = {"business name", "store name"}


class SurrogateRegistry:
    """Deterministic id -> surrogate id mapping, stable across files and re-runs."""

    def __init__(self) -> None:
        self._maps: dict[str, dict[str, str]] = {}

    def get(self, kind: str, value: str) -> str:
        if value is None or value == "" or value.upper() == "NULL":
            return value
        table = self._maps.setdefault(kind, {})
        if value not in table:
            prefix = {"business_id": "9", "store_id": "8"}.get(kind, "7")
            table[value] = f"{prefix}{len(table) + 1:06d}"
        return table[value]

    def as_dict(self) -> dict[str, dict[str, str]]:
        return self._maps


def collect_real_names(files: list[Path]) -> set[str]:
    """First pass: gather the merchant's actual names so free text can be scrubbed."""
    names: set[str] = set()
    for f in files:
        with f.open(newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            if not reader.fieldnames:
                continue
            cols = [c for c in reader.fieldnames if c.strip().lower() in NAME_SOURCE_COLUMNS]
            if not cols:
                continue
            for row in reader:
                for c in cols:
                    v = (row.get(c) or "").strip()
                    if v and v.upper() != "NULL":
                        names.add(v)
    # longest first, so "Acme Diner - Downtown" is removed before "Acme Diner"
    return names


def scrub_free_text(value: str, registry: SurrogateRegistry, real_names: set[str]) -> str:
    """Remove embedded merchant names and map embedded business ids to surrogates.

    Example shape: "<Promo> - <Merchant> (bus id: <real id>)" -> "<Promo> (bus id: 9000001)"
    """
    if not value or value.upper() == "NULL":
        return value

    def _sub(m: "re.Match[str]") -> str:
        return f"bus id: {registry.get('business_id', m.group(1))}"

    value = re.sub(r"bus id:\s*(\d+)", _sub, value)

    for real in sorted(real_names, key=len, reverse=True):
        value = re.sub(r"\s*[-–—]?\s*" + re.escape(real), "", value, flags=re.IGNORECASE)

    return re.sub(r"\s{2,}", " ", value).strip(" -–—")


def anonymize_file(
    src: Path, dest: Path, registry: SurrogateRegistry, real_names: set[str]
) -> tuple[int, int]:
    """Returns (rows_written, columns_scrubbed)."""
    with src.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            return 0, 0
        fieldnames = reader.fieldnames

        # map header -> what kind of identifier it is
        targets: dict[str, str] = {}
        for col in fieldnames:
            kind = IDENTIFYING_COLUMNS.get(col.strip().lower())
            if kind:
                targets[col] = kind

        rows_out = 0
        dest.parent.mkdir(parents=True, exist_ok=True)
        with dest.open("w", newline="", encoding="utf-8") as out:
            writer = csv.DictWriter(out, fieldnames=fieldnames)
            writer.writeheader()
            for row in reader:
                for col, kind in targets.items():
                    value = row.get(col)
                    if kind in ("business_id", "store_id"):
                        row[col] = registry.get(kind, value)
                    elif kind in SURROGATE:
                        row[col] = SURROGATE[kind] if value not in (None, "") else value
                for col in fieldnames:
                    if col.strip().lower() in CAMPAIGN_NAME_COLUMNS:
                        row[col] = scrub_free_text(row.get(col, ""), registry, real_names)
                writer.writerow(row)
                rows_out += 1
    return rows_out, len(targets)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--src", required=True, help="folder of raw client CSVs")
    ap.add_argument("--dest", default="data/anonymized", help="output folder")
    args = ap.parse_args()

    src_dir, dest_dir = Path(args.src), Path(args.dest)
    files = sorted(src_dir.glob("*.csv"))
    if not files:
        print(f"no CSVs found in {src_dir}", file=sys.stderr)
        return 1

    registry = SurrogateRegistry()
    real_names = collect_real_names(files)
    total_rows = 0
    print(f"anonymizing {len(files)} file(s) -> {dest_dir}")
    print(f"  {len(real_names)} merchant name(s) discovered in the data and scrubbed from free text\n")
    for f in files:
        # normalize the noisy export filenames into stable dataset names
        stem = re.sub(r"^[0-9a-f]{8}-", "", f.stem)          # drop upload hash prefix
        # drop date range + export id; exports use YYYYMMDD or YYYY-MM-DD
        stem = re.sub(r"_\d{4}-?\d{2}-?\d{2}_\d{4}-?\d{2}-?\d{2}_.*$", "", stem)
        out_name = f"{stem.lower()}.csv"
        rows, cols = anonymize_file(f, dest_dir / out_name, registry, real_names)
        total_rows += rows
        print(f"  {out_name:<45} {rows:>6,} rows  ({cols} identifying columns scrubbed)")

    mapping_path = dest_dir / "_surrogate_mapping.json"
    mapping_path.write_text(json.dumps(registry.as_dict(), indent=2))
    print(f"\n  {total_rows:,} rows total")
    print(f"  surrogate mapping written to {mapping_path} (gitignored — do not commit)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
