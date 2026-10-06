# %%
# Module 6 tests: gold.
# Part A (generated DataFrames, no tables): G1, G2, G3, G4, G7, G8, G9, C5, C6, I5.
# Part B (real data): runs 05_gold, then compares every total with the independent
#   plain-Python reference (tests/local/reference_totals.py) and checks G5, G6, re-run, export.
# Results: silver Files/_test_results/module6.json.

# %%
%run 00_config

# %%
%run lib_gold

# %%
import json, traceback
from datetime import datetime, date
from decimal import Decimal

results = []


def check(test_id, passed, detail=""):
    results.append({"test": test_id, "passed": bool(passed), "detail": str(detail)})
    print(f"{'PASS' if passed else 'FAIL'}  {test_id}  {detail}")


def section(name, fn):
    try:
        fn()
    except Exception as e:
        check(f"{name}: unexpected error", False, str(e)[:1500] + "\n...\n" + traceback.format_exc()[-800:])


D = lambda s: Decimal(s)
TS = lambda s: datetime.fromisoformat(s)


# ---------------------------------------------------------------- Part A: generated data
def generated_checks():
    orders = spark.createDataFrame([
        ("SAT", "K1", TS("2026-09-05 10:00:00"), date(2026, 9, 5), "paid", "INR"),       # G1 Saturday
        ("USD", "K1", TS("2026-09-02 10:00:00"), date(2026, 9, 2), "paid", "USD"),       # G2
        ("EARLY", "K1", TS("2026-08-30 10:00:00"), date(2026, 8, 30), "paid", "INR"),    # G3 before first rate
        ("PLACED", "K1", TS("2026-09-02 10:00:00"), date(2026, 9, 2), "placed", "USD"),  # G4
        ("CANC", "K1", TS("2026-09-02 10:00:00"), date(2026, 9, 2), "cancelled", "USD"), # G4
        ("BEFORE", "K2", TS("2026-09-09 10:00:00"), date(2026, 9, 9), "paid", "USD"),    # C6 before tier change
        ("AFTER", "K2", TS("2026-09-11 10:00:00"), date(2026, 9, 11), "paid", "USD"),    # C6 after tier change
        ("NOCRM", "C0901", TS("2026-09-02 10:00:00"), date(2026, 9, 2), "paid", "USD"),  # C5
        ("P050", "K1", TS("2026-09-02 10:00:00"), date(2026, 9, 2), "paid", "USD"),      # G7 diff exactly 0.50
        ("P051", "K1", TS("2026-09-02 10:00:00"), date(2026, 9, 2), "paid", "USD"),      # G8 diff 0.51
        ("G9", "K1", TS("2026-09-02 10:00:00"), date(2026, 9, 2), "paid", "USD"),        # G9
    ], "order_id string, customer_id string, order_ts timestamp, order_date date, status string, currency string")
    line = lambda oid, n, net, typ="sale", qty=1: (oid, n, "P1", True, qty, net, D("0"), typ, net)
    items = spark.createDataFrame([
        line("SAT", 1, D("1000")), line("USD", 1, D("100")), line("EARLY", 1, D("100")),
        line("PLACED", 1, D("100")), line("CANC", 1, D("100")), line("BEFORE", 1, D("10")),
        line("AFTER", 1, D("10")), line("NOCRM", 1, D("10")),
        line("SAT", 2, D("-200"), "return", -1),                                            # I5
        line("P050", 1, D("100")), line("P051", 1, D("100")), line("G9", 1, D("80")),
    ], "order_id string, line_no int, product_id string, product_known boolean, qty int, "
       "unit_price decimal(18,4), discount_pct decimal(5,2), line_type string, net_local decimal(18,4)")
    products = spark.createDataFrame([("P1", "Books")], "product_id string, category string")
    fx = spark.createDataFrame([(date(2026, 9, 1), "INR", D("0.0100")), (date(2026, 9, 4), "INR", D("0.0120"))],
                               "rate_date date, currency string, rate_to_usd decimal(18,8)")
    dim = spark.createDataFrame([
        (-1, "UNKNOWN", TS("1900-01-01 00:00:00"), TS("9999-12-31 23:59:59"), True),
        (11, "K1", TS("1900-01-01 00:00:00"), TS("9999-12-31 23:59:59"), True),
        (21, "K2", TS("1900-01-01 00:00:00"), TS("2026-09-10 00:00:00"), False),
        (22, "K2", TS("2026-09-10 00:00:00"), TS("9999-12-31 23:59:59"), True),
    ], "customer_sk bigint, customer_id string, valid_from timestamp, valid_to timestamp, is_current boolean") \
        .withColumn("full_name", F.lit("x")).withColumn("tier", F.lit("Gold")).withColumn("country", F.lit("IN"))

    fact = {(r["order_id"], r["line_no"]): r for r in build_fact_order_line(orders, items, products, fx, dim).collect()}
    sat = fact[("SAT", 1)]
    check("G1 Saturday INR order uses Friday's rate", (sat["fx_rate_date"], sat["fx_rate"]) == (date(2026, 9, 4), D("0.01200000")),
          (sat["fx_rate_date"], sat["fx_rate"]))
    check("G1 net_usd = 1000 x 0.012 = 12", sat["net_usd"] == D("12.0000"), sat["net_usd"])
    check("G2 USD order has rate 1", fact[("USD", 1)]["fx_rate"] == 1, fact[("USD", 1)]["fx_rate"])
    early = fact[("EARLY", 1)]
    check("G3 order before the first rate -> fx_missing, not revenue",
          early["fx_missing"] and early["net_usd"] is None and not early["is_revenue"], early.asDict())
    check("G4 placed and cancelled orders are not revenue",
          not fact[("PLACED", 1)]["is_revenue"] and not fact[("CANC", 1)]["is_revenue"])
    check("C6 order before the tier change joins the old version", fact[("BEFORE", 1)]["customer_sk"] == 21)
    check("C6 order after the tier change joins the new version", fact[("AFTER", 1)]["customer_sk"] == 22)
    check("C5 customer not in the CRM -> customer_sk -1", fact[("NOCRM", 1)]["customer_sk"] == -1)

    fact_df = build_fact_order_line(orders, items, products, fx, dim)
    daily = {r["order_date"]: r["revenue_usd"] for r in build_daily_revenue(fact_df).collect()}
    check("I5 return reduces revenue on the original order's date (Sat: 12 - 2.4 = 9.6)",
          daily.get(date(2026, 9, 5)) == D("9.6000"), daily.get(date(2026, 9, 5)))
    top = build_top_customers(fact_df, dim).collect()
    check("G6 top customers exclude the unknown customer", all(r["customer_id"] != "C0901" for r in top),
          [r["customer_id"] for r in top])

    pays = spark.createDataFrame([
        ("p1", "P050", "card", D("100.50"), "USD", "success"),
        ("p2", "P051", "card", D("100.51"), "USD", "success"),
        ("p3", "G9", "card", D("100"), "USD", "success"),
        ("p4", "G9", "card", D("20"), "USD", "refunded"),
        ("p5", "G9", "card", D("50"), "USD", "failed"),
        ("p6", "GHOST", "card", D("10"), "USD", "success"),
    ], "payment_id string, order_id string, method string, amount decimal(18,4), currency string, status string")
    mism, orphans = build_payment_mismatches(fact_df, orders, pays, fx)
    flagged = {r["order_id"] for r in mism.collect()}
    check("G7 difference exactly 0.50 -> not flagged", "P050" not in flagged, flagged)
    check("G8 difference 0.51 -> flagged", "P051" in flagged, flagged)
    check("G9 100 success - 20 refunded (50 failed ignored) = 80 -> not flagged", "G9" not in flagged, flagged)
    check("payment for an unknown order counted, not compared", orphans == 1, orphans)


# ---------------------------------------------------------------- Part B: real data
# From tests/local/reference_totals.py run on the original files (plain Python, no Spark).
REFERENCE = {
    "fact_lines": 1933,
    "revenue_usd_total": "295585.5703",
    "revenue_days": 30,
    "revenue_by_category": {"Beauty": "9639.4490", "Books": "3834.9797", "Electronics": "223943.5064",
                            "Fashion": "20869.6472", "Home": "15924.4683", "Sports": "18413.6678",
                            "UNKNOWN": "2959.8519"},
    "top_customers": [["C0035", "20337.7709"], ["C0091", "18499.4867"], ["C0027", "17524.8262"],
                      ["C0066", "17071.9142"], ["C0058", "16855.5545"], ["C0003", "16578.5552"],
                      ["C0079", "14070.2138"], ["C0044", "13056.1478"], ["C0020", "12491.2514"],
                      ["C0011", "11686.1405"]],
    "payment_mismatches": 13,
    "orphan_payments": 5,
}


def snapshot():
    g = lambda n: spark.table(tbl("gold", n))
    return {
        "fact_lines": g("fact_order_line").count(),
        "revenue_usd_total": str(g("daily_revenue").agg(F.sum("revenue_usd")).first()[0]),
        "revenue_days": g("daily_revenue").count(),
        "revenue_by_category": {r["category"]: str(r["revenue_usd"]) for r in g("revenue_by_category").collect()},
        "top_customers": [[r["customer_id"], str(r["revenue_usd"])] for r in g("top_customers").orderBy("rank").collect()],
        "payment_mismatches": g("payment_mismatches").count(),
    }


def real_data_checks():
    notebookutils.notebook.run("05_gold", 1200, {"batch_id": "2"})
    s = snapshot()
    for key in ["fact_lines", "revenue_usd_total", "revenue_days", "revenue_by_category", "top_customers", "payment_mismatches"]:
        check(f"gold {key} = independent Python reference", s[key] == REFERENCE[key],
              {"gold": s[key], "reference": REFERENCE[key]} if s[key] != REFERENCE[key] else s[key])
    recon = {r["check_name"]: r for r in spark.table(tbl("gold", "reconciliation")).collect()}
    for name, r in recon.items():
        if r["passed"] is not None:
            check(f"G5 {name}", r["passed"], f"{r['value']} vs {r['expected']}")
    orphan = recon["payments whose order is unknown (excluded from mismatches)"]["value"]
    check("5 payments for unknown orders counted", orphan == str(REFERENCE["orphan_payments"]), orphan)
    top = spark.table(tbl("gold", "top_customers")).orderBy("rank").collect()
    revs = [r["revenue_usd"] for r in top]
    check("G6 at most 10, descending, no unknown customer",
          len(top) <= 10 and revs == sorted(revs, reverse=True)
          and all(r["customer_id"] and not r["customer_id"].startswith("C090") for r in top), len(top))
    n_unknown = spark.table(tbl("gold", "fact_order_line")).where("customer_sk = -1") \
        .select("customer_id").distinct().collect()
    check("C5 real data: only C0901-C0906 map to -1",
          sorted(r[0] for r in n_unknown) == [f"C090{i}" for i in range(1, 7)], [r[0] for r in n_unknown])
    n_unknown_cat = spark.table(tbl("gold", "fact_order_line")).where("category = 'UNKNOWN'").count()
    check("I2 14 unknown-product lines in category UNKNOWN", n_unknown_cat == 14, n_unknown_cat)

    notebookutils.notebook.run("05_gold", 1200, {"batch_id": "2"})
    check("re-run gold -> identical results", snapshot() == s)
    exported = json.loads(spark.read.text(f"{DQ_EXPORT_DIR}/dq_batch_2.json", wholetext=True).first()[0])
    check("DQ report exported as JSON for batch 2",
          exported["batch_id"] == 2 and len(exported["dq_report"]) > 0 and len(exported["reconciliation"]) > 0,
          f"{len(exported['dq_report'])} dq rows, {len(exported['reconciliation'])} reconciliation rows")


section("generated data", generated_checks)
section("real data", real_data_checks)

# %%
failed = [r["test"] for r in results if not r["passed"]]
notebookutils.fs.put(files_path("silver", "_test_results", "module6.json"),
                     json.dumps({"passed": len(results) - len(failed), "failed": failed, "results": results}, indent=2),
                     True)
print(f"\n{len(results) - len(failed)} passed, {len(failed)} failed")
assert not failed, f"Failed tests: {failed}"
