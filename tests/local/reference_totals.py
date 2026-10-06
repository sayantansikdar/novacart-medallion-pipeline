"""Independent reference calculation of the gold numbers, in plain Python (no Spark).

Reads the eight original input files and applies the business rules from the lab brief
and the design-note assumptions, using Decimal with the same rounding as the pipeline
(net_local and net_usd rounded half-up to 4 decimals per line). The gold tests compare
the pipeline's totals with these numbers, so a bug in either shows up as a mismatch.

Usage: python3 tests/local/reference_totals.py <folder with the input files>
"""
import csv, json, re, sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP

SRC = sys.argv[1] if len(sys.argv) > 1 else "."
Q4 = Decimal("0.0001")
REVENUE = {"paid", "shipped", "delivered"}
COUNTRY_CCY = {"IN": "INR", "US": "USD", "GB": "GBP", "UK": "GBP", "DE": "EUR", "SG": "SGD"}


def ts(s):
    s = (s or "").strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})$", s):
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    if re.match(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$", s):
        return datetime.strptime(s, "%d/%m/%Y %H:%M").replace(tzinfo=timezone.utc)
    return None


def rows(name):
    with open(f"{SRC}/{name}", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def jsonl(name):
    return [json.loads(l) for l in open(f"{SRC}/{name}", encoding="utf-8") if l.strip()]


# ---- orders: latest updated_at wins across both batches
orders = {}
for b, f in ((1, "orders_batch_1.csv"), (2, "orders_batch_2.csv")):
    for r in rows(f):
        u = ts(r["updated_at"])
        cur = orders.get(r["order_id"])
        if cur is None or u > cur["u"] or (u == cur["u"] and b > cur["b"]):
            ccy = r["currency"].strip().upper() or COUNTRY_CCY[r["shipping_country"].strip().upper()]
            orders[r["order_id"]] = {"u": u, "b": b, "status": r["status"].strip().lower(), "ccy": ccy,
                                     "cust": r["customer_id"], "ots": ts(r["order_ts"])}

# ---- items: later batch replaces a line; lines without an order are excluded
items = {}
for b, f in ((1, "order_items_batch_1_json.txt"), (2, "order_items_batch_2_jsonl.txt")):
    for r in jsonl(f):
        if r["order_id"] in orders:
            items[(r["order_id"], int(r["line_no"]))] = r
products = {r["product_id"]: r["category"] for r in rows("products.csv")}

# ---- FX: as-of rate on the UTC order date
fx = defaultdict(dict)
for r in rows("fx_rates.csv"):
    fx[r["currency"]][r["rate_date"]] = Decimal(r["rate_to_usd"])


def rate(ccy, day):
    if ccy == "USD":
        return Decimal(1)
    for back in range(0, 40):
        d = (day - timedelta(days=back)).isoformat()
        if d in fx[ccy]:
            return fx[ccy][d]
    return None


# ---- CRM: customers known to the CRM (rows with updated_at); others map to -1
crm = {r["customer_id"] for r in rows("customers_changes.csv") if r["updated_at"].strip()}

daily, by_cat, by_cust, order_usd = defaultdict(Decimal), defaultdict(Decimal), defaultdict(Decimal), defaultdict(Decimal)
fact_lines = 0
for (oid, _), r in items.items():
    o = orders[oid]
    qty = int(r["qty"])
    if r["line_type"] == "return":
        qty = -abs(qty)
    disc = Decimal(str(r["discount_pct"])) if r["discount_pct"] is not None else Decimal(0)
    net_local = (qty * Decimal(str(r["unit_price"])) * (1 - disc / 100)).quantize(Q4, ROUND_HALF_UP)
    rt = rate(o["ccy"], o["ots"].date())
    net_usd = (net_local * rt).quantize(Q4, ROUND_HALF_UP)
    fact_lines += 1
    order_usd[oid] += net_usd
    if o["status"] in REVENUE:
        daily[o["ots"].date().isoformat()] += net_usd
        by_cat[products.get(r["product_id"], "UNKNOWN")] += net_usd
        if o["cust"] in crm:
            by_cust[o["cust"]] += net_usd

# ---- payment mismatches: expected = order net total if revenue status, else 0
paid = defaultdict(Decimal)
orphan_payments = 0
for p in json.load(open(f"{SRC}/payments_json.txt", encoding="utf-8")):
    if p["order_id"] not in orders:
        orphan_payments += 1
        continue
    o = orders[p["order_id"]]
    sign = {"success": 1, "refunded": -1}.get(p["status"], 0)
    paid[p["order_id"]] += sign * Decimal(str(p["amount"])) * rate(p["currency"], o["ots"].date())
mismatches = 0
for oid, o in orders.items():
    expected = order_usd[oid] if o["status"] in REVENUE else Decimal(0)
    if abs(paid[oid] - expected) > Decimal("0.50"):
        mismatches += 1

top = sorted(by_cust.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
print(json.dumps({
    "fact_lines": fact_lines,
    "revenue_usd_total": str(sum(daily.values())),
    "revenue_days": len(daily),
    "revenue_by_category": {k: str(v) for k, v in sorted(by_cat.items())},
    "top_customers": [[c, str(v)] for c, v in top],
    "payment_mismatches": mismatches,
    "orphan_payments": orphan_payments,
}, indent=2))
