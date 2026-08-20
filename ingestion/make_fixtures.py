"""
Generate synthetic source files for CI.

CI must never see client data, but a build that runs against trivially clean fixtures
proves nothing. These fixtures deliberately reproduce the SHAPE of the real export,
including the awkward parts the controls exist to catch:

  * a reversal cycle   — one order id posting, reversing, and re-booking, so the
                         order-grain collapse in int_order_transactions is exercised
  * a recovered charge — error charge followed by a matching adjustment credit
  * an unrecovered one — error charge with no credit, so leakage is non-zero
  * a cutoff break     — a payout settling orders outside the window, so the
                         reconciliation classifier has something to classify
  * 'NULL' as text     — the export writes the literal string, not an empty field

If a schema change breaks the pipeline, CI fails here rather than in production.

Usage:
    python ingestion/make_fixtures.py --dest data/anonymized
"""

from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timedelta
from pathlib import Path

BUSINESS_ID = "9000001"
BUSINESS_NAME = "Merchant A"
STORE_ID = "8000001"
STORE_NAME = "Merchant A - Store 01"

START = date(2025, 1, 6)      # a Monday
WEEKS = 6
ORDERS_PER_DAY = 4

# Full transaction-export column list, replicated exactly from the real source header
# so the fixture exercises the same schema the pipeline sees in production.
TXN_COLUMNS = [
    "Timestamp UTC time", "Timestamp UTC date", "Timestamp local time", "Timestamp local date",
    "Order received local time", "Order pickup local time", "Payout time", "Payout date",
    "Business ID", "Business name", "Store ID", "Store name", "Merchant store ID",
    "Transaction type", "Delivery UUID", "DoorDash transaction ID", "DoorDash order ID",
    "Merchant delivery ID", "POS order ID", "Channel", "Description", "Final order status",
    "Currency", "Subtotal", "Subtotal tax passed to merchant", "Staff tip", "Commission",
    "Payment processing fee", "Marketing fees | (including any applicable taxes)",
    "Customer discounts from marketing | (funded by you)",
    "Customer discounts from marketing | (funded by DoorDash)",
    "Customer discounts from marketing | (funded by a third-party)",
    "DoorDash marketing credit", "Third-party contribution",
    "Marketing fees (for historical reference only) | (all discounts and fees)",
    "Marketing fee tax (for historical reference only) | (taxes on any applicable marketing fees)",
    "Ad fee (for historical reference only)", "Ad fee tax (for historical reference only)",
    "Error charges", "Adjustments", "Net total", "Pre-adjusted subtotal",
    "Pre-adjusted tax subtotal", "Subtotal for tax",
    "Subtotal tax remitted by DoorDash to tax authorities",
    "DoorDash funded subtotal discount amount | (for historical reference only)",
    "Merchant funded subtotal discount amount | (for historical reference only)",
    "Payout ID",
]


def _payout_date(d: date) -> date:
    """Payouts settle the Thursday following the order week."""
    return d + timedelta(days=(3 - d.weekday()) % 7 + 7)


def write_dicts(path: Path, columns: list[str], rows: list[dict]) -> None:
    """Write dict rows with an explicit column list — width mismatches become impossible."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=columns)
        w.writeheader()
        w.writerows(rows)
    print(f"  {path.name:<45} {len(rows):>5} rows")


def write(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    print(f"  {path.name:<45} {len(rows):>5} rows")


def build(dest: Path) -> None:
    orders, txns, errors = [], [], []
    payout_acc: dict[tuple[date, str], dict[str, float]] = {}

    txn_seq = 10_000_000_000
    order_seq = 0

    def add_txn(order_id, ts, channel, subtotal, commission, ttype="Order", err=0.0, adj=0.0, delivery=None, desc="NULL"):
        """Append one transaction row.

        Rows are built as dicts keyed by column name and written with DictWriter, so a
        schema change can never silently shift values into the wrong column — the
        failure mode that a positional row list invites.
        """
        nonlocal txn_seq
        txn_seq += 1
        pd_ = _payout_date(ts.date())
        net = subtotal + commission + err + adj
        row = {c: "0.00" for c in TXN_COLUMNS}
        row.update({
            "Timestamp UTC time": ts.isoformat(sep=" "),
            "Timestamp UTC date": ts.date().isoformat(),
            "Timestamp local time": ts.isoformat(sep=" "),
            "Timestamp local date": ts.date().isoformat(),
            "Order received local time": ts.isoformat(sep=" "),
            "Order pickup local time": ts.isoformat(sep=" "),
            "Payout time": ts.isoformat(sep=" "),
            "Payout date": pd_.isoformat(),
            "Business ID": BUSINESS_ID,
            "Business name": BUSINESS_NAME,
            "Store ID": STORE_ID,
            "Store name": STORE_NAME,
            "Merchant store ID": "NULL",
            "Transaction type": ttype,
            "Delivery UUID": delivery or "NULL",
            "DoorDash transaction ID": str(txn_seq),
            "DoorDash order ID": order_id,
            "Merchant delivery ID": "NULL",
            "POS order ID": "NULL",
            "Channel": channel,
            "Description": desc,
            "Final order status": "Delivered",
            "Currency": "USD",
            "Subtotal": f"{subtotal:.2f}",
            "Commission": f"{commission:.2f}",
            "Error charges": f"{err:.2f}",
            "Adjustments": f"{adj:.2f}",
            "Net total": f"{net:.2f}",
            "Subtotal for tax": f"{subtotal:.2f}",
            "Payout ID": f"{500000}",
        })
        txns.append(row)
        acc = payout_acc.setdefault((pd_, channel), dict(sub=0.0, com=0.0, err=0.0, adj=0.0))
        acc["sub"] += subtotal
        acc["com"] += commission
        acc["err"] += err
        acc["adj"] += adj
        return str(txn_seq)

    for day_offset in range(WEEKS * 7):
        d = START + timedelta(days=day_offset)
        for i in range(ORDERS_PER_DAY):
            order_seq += 1
            order_id = f"F{order_seq:07X}"
            ts = datetime(d.year, d.month, d.day, 12 + (i % 8), 15 * (i % 4))
            subtotal = round(18.0 + (order_seq % 23) * 1.35, 2)
            commission = -round(subtotal * 0.285, 2)
            channel = "Storefront" if order_seq % 37 == 0 else "Marketplace"

            add_txn(order_id, ts, channel, subtotal, commission)
            orders.append([
                BUSINESS_ID, BUSINESS_NAME, STORE_ID, STORE_NAME, "REDACTED", "US/Eastern",
                d.isoformat(), ts.time().isoformat(), ts.isoformat(sep=" "),
                d.isoformat(), ts.time().isoformat(), channel, "false", "0.00",
                "NULL", "NULL", "USD", f"{subtotal:.2f}",
            ])

            # --- reversal cycle: post, reverse, re-book under the same order id
            if order_seq % 60 == 0:
                add_txn(order_id, ts + timedelta(minutes=11), channel, -subtotal, -commission)
                add_txn(order_id, ts + timedelta(minutes=19), channel, subtotal, commission)

            # --- error charge, recovered by a later credit
            if order_seq % 45 == 0:
                delivery = f"d{order_seq:08d}-uuid"
                amt = round(subtotal * 0.35, 2)
                add_txn(order_id, ts + timedelta(days=1), channel, 0.0, 0.0,
                        ttype="Error Charge", err=-amt, delivery=delivery,
                        desc="1 Item missing")
                errors.append([
                    (ts + timedelta(days=1)).isoformat(sep=" "), _payout_date(d).isoformat(),
                    BUSINESS_ID, BUSINESS_NAME, STORE_ID, STORE_NAME, "NULL",
                    "Error Charge", delivery, str(txn_seq), order_id, "NULL", "NULL",
                    channel, "1 Item missing", f"{-amt:.2f}", "0.00",
                ])
                add_txn(order_id, ts + timedelta(days=3), channel, 0.0, 0.0,
                        ttype="Adjustment", adj=amt, delivery=delivery)
                errors.append([
                    (ts + timedelta(days=3)).isoformat(sep=" "), _payout_date(d).isoformat(),
                    BUSINESS_ID, BUSINESS_NAME, STORE_ID, STORE_NAME, "NULL",
                    "Adjustment", delivery, str(txn_seq), order_id, "NULL", "NULL",
                    channel, "NULL", "0.00", f"{amt:.2f}",
                ])

            # --- error charge that is never recovered -> non-zero leakage
            if order_seq % 70 == 0:
                delivery = f"d{order_seq:08d}-lost"
                amt = round(subtotal * 0.5, 2)
                add_txn(order_id, ts + timedelta(days=1), channel, 0.0, 0.0,
                        ttype="Error Charge", err=-amt, delivery=delivery,
                        desc="Entirely Wrong Order")
                errors.append([
                    (ts + timedelta(days=1)).isoformat(sep=" "), _payout_date(d).isoformat(),
                    BUSINESS_ID, BUSINESS_NAME, STORE_ID, STORE_NAME, "NULL",
                    "Error Charge", delivery, str(txn_seq), order_id, "NULL", "NULL",
                    channel, "Entirely Wrong Order", f"{-amt:.2f}", "0.00",
                ])

    # ---- payout summary, derived so the net-total control reconciles exactly
    payout_rows = []
    for idx, ((pd_, channel), acc) in enumerate(sorted(payout_acc.items()), start=1):
        net = acc["sub"] + acc["com"] + acc["err"] + acc["adj"]
        payout_rows.append([
            BUSINESS_ID, BUSINESS_NAME, STORE_ID, STORE_NAME, "NULL",
            pd_.isoformat(), "USD", channel,
            f"{acc['sub']:.2f}", "0.00", "0.00", f"{acc['com']:.2f}", "0.00",
            "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00",
            f"{acc['err']:.2f}", f"{acc['adj']:.2f}", f"{net:.2f}",
            f"{acc['sub']:.2f}", "0.00", "0.00", "0.00",
            f"{500000 + idx}", "PAID",
        ])

    # ---- cutoff break: a payout at the opening edge settling pre-window orders.
    # Present on the payout side only, so the reconciliation classifier has a real
    # break to explain. Its own net total still reconciles internally.
    first_payout = min(pd_ for pd_, _ in payout_acc)
    payout_rows.insert(0, [
        BUSINESS_ID, BUSINESS_NAME, STORE_ID, STORE_NAME, "NULL",
        (first_payout - timedelta(days=7)).isoformat(), "USD", "Marketplace",
        "1450.00", "0.00", "0.00", "-413.25", "0.00",
        "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00",
        "0.00", "0.00", "1036.75",
        "1450.00", "0.00", "0.00", "0.00",
        "499999", "PAID",
    ])

    write_dicts(dest / "financial_detailed_transactions.csv", TXN_COLUMNS, txns)
    write_dicts(dest / "financial_simplified_transactions.csv", TXN_COLUMNS, txns)

    write(dest / "sales_by_order.csv", [
        "Business ID", "Business name", "Store ID", "Store name", "Store address", "Timezone",
        "Order placed date", "Order placed time", "Pickup timestamp", "Delivery date",
        "Delivery time", "Channel", "Is cancelled", "Error charge", "Customer rating",
        "Customer emoji rating", "Currency", "Subtotal",
    ], orders)

    write(dest / "financial_payout_summary.csv", [
        "Business ID", "Business name", "Store ID", "Store name", "Merchant store ID",
        "Payout date", "Currency", "Channel", "Subtotal", "Subtotal tax passed to merchant",
        "Staff tip", "Commission", "Payment processing fee",
        "Marketing fees | (including any applicable taxes)",
        "Customer discounts from marketing | (funded by you)",
        "Customer discounts from marketing | (funded by DoorDash)",
        "Customer discounts from marketing | (funded by a third-party)",
        "DoorDash marketing credit", "Third-party contribution",
        "Marketing fees (for historical reference only) | (all discounts and fees)",
        "Marketing fee tax (for historical reference only) | (taxes on any applicable marketing fees)",
        "Ad fee (for historical reference only)", "Ad fee tax (for historical reference only)",
        "Error charges", "Adjustments", "Net total", "Subtotal for tax",
        "Subtotal tax remitted by DoorDash to tax authorities",
        "DoorDash funded subtotal discount amount | (for historical reference only)",
        "Merchant funded subtotal discount amount | (for historical reference only)",
        "Payout ID", "Payout status",
    ], payout_rows)

    write(dest / "financial_error_charges_and_adjustments.csv", [
        "Timestamp local time", "Payout date", "Business ID", "Business name", "Store ID",
        "Store name", "Merchant store ID", "Transaction type", "Delivery UUID",
        "DoorDash transaction ID", "DoorDash order ID", "Merchant delivery ID", "POS order ID",
        "Channel", "Description", "Error charges", "Adjustments",
    ], errors)

    write(dest / "product_mix.csv", [
        "Start date", "End date", "Business ID", "Business name", "Store ID", "Store name",
        "Channel", "Item name", "Popular item", "Currency", "Gross sales", "Discounts",
        "Total sold", "Total item errors", "Total error charges",
    ], [
        [START.isoformat(), (START + timedelta(days=WEEKS * 7)).isoformat(), BUSINESS_ID,
         BUSINESS_NAME, STORE_ID, STORE_NAME, "Marketplace", name, pop, "USD",
         f"{gs:.2f}", "0.00", str(sold), str(errs), f"{ec:.2f}"]
        for name, pop, gs, sold, errs, ec in [
            ("Item Alpha", "1", 9820.50, 812, 9, 84.20),
            ("Item Bravo", "1", 4210.00, 355, 4, 38.75),
            ("Item Charlie", "0", 1980.25, 141, 2, 19.40),
            ("Item Delta", "0", 610.75, 48, 3, 31.05),
        ]
    ])

    write(dest / "marketing_promotion.csv", [
        "Date", "Is self serve campaign", "Campaign ID", "Campaign name", "Type of promotion",
        "Campaign start date", "Campaign end date", "Store ID", "Store name", "Currency",
        "Orders", "Sales", "Customer discounts from marketing | (Funded by you)",
        "Customer discounts from marketing | (Funded by DoorDash)",
        "Customer discounts from marketing | (Funded by a third-party)",
        "Marketing fees | (including any applicable taxes)", "DoorDash marketing credit",
        "Third-party contribution", "Average order value", "ROAS", "New customers acquired",
        "Existing customers acquired", "Total customers acquired", "New DP customers acquired",
        "Existing DP customers acquired", "Total DP customers acquired",
    ], [
        [(START + timedelta(days=i * 7)).isoformat(), "false", "cmp-0001",
         f"Loyalty Promo (bus id: {BUSINESS_ID})", "NULL",
         START.isoformat(), (START + timedelta(days=365)).isoformat(),
         STORE_ID, STORE_NAME, "USD", "2", "58.40", "19.00", "0.0", "0.0",
         "0.0", "0.0", "0.0", "29.20", "1.94", "0", "2", "2", "0", "1", "1"]
        for i in range(WEEKS)
    ])

    write(dest / "sales_by_time.csv", [
        "Start date", "End date", "Business ID", "Business name", "Merchant supplied store ID",
        "Currency", "Total orders/deliveries (including cancelled)",
        "Total cancelled orders/deliveries", "Gross sales", "AOV",
    ], [
        [(START + timedelta(days=i)).isoformat(), (START + timedelta(days=i)).isoformat(),
         BUSINESS_ID, BUSINESS_NAME, STORE_ID, "USD", str(ORDERS_PER_DAY), "0", "120.00", "30.00"]
        for i in range(WEEKS * 7)
    ])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dest", default="data/anonymized")
    args = ap.parse_args()
    dest = Path(args.dest)
    print(f"generating CI fixtures -> {dest}\n")
    build(dest)
    print("\nfixtures ready (synthetic — contains no client data)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
