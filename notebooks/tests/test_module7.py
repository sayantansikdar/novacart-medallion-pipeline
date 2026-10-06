# %%
# Module 7 verification, run once after the official pipeline runs (batch 1, batch 2, batch 2 again).
# Uses Delta time travel to read every table as it was right after batch 1, and compares both that
# state and the final state with the independent plain-Python references
# (tests/local/reference_totals.py run with batches "1" and "1,2"; values embedded below).
# Results: silver Files/_test_results/module7.json.

# %%
%run 00_config

# %%
import json, traceback

REF_AFTER_BATCH_1 = json.loads(r"""{"orders": 495, "status_mix": {"cancelled": 59, "delivered": 274, "paid": 45, "placed": 51, "shipped": 66}, "fact_lines": 971, "revenue_usd_total": "150062.6435", "revenue_days": 15, "revenue_by_category": {"Beauty": "4201.6341", "Books": "1778.3158", "Electronics": "117109.1863", "Fashion": "10681.3423", "Home": "6933.2281", "Sports": "8598.0831", "UNKNOWN": "760.8538"}, "top_customers": [["C0066", "11434.8848"], ["C0035", "9976.2711"], ["C0044", "8567.5652"], ["C0003", "8411.0652"], ["C0027", "8027.1254"], ["C0091", "7170.4045"], ["C0079", "6940.9393"], ["C0058", "6933.2252"], ["C0011", "6926.1459"], ["C0020", "5810.8639"]], "payment_mismatches": 64}""")
REF_AFTER_BATCH_2 = json.loads(r"""{"orders": 945, "status_mix": {"cancelled": 107, "delivered": 666, "paid": 39, "placed": 44, "shipped": 89}, "fact_lines": 1933, "revenue_usd_total": "295585.5703", "revenue_days": 30, "revenue_by_category": {"Beauty": "9639.4490", "Books": "3834.9797", "Electronics": "223943.5064", "Fashion": "20869.6472", "Home": "15924.4683", "Sports": "18413.6678", "UNKNOWN": "2959.8519"}, "top_customers": [["C0035", "20337.7709"], ["C0091", "18499.4867"], ["C0027", "17524.8262"], ["C0066", "17071.9142"], ["C0058", "16855.5545"], ["C0003", "16578.5552"], ["C0079", "14070.2138"], ["C0044", "13056.1478"], ["C0020", "12491.2514"], ["C0011", "11686.1405"]], "payment_mismatches": 13}""")

results = []


def check(test_id, passed, detail=""):
    results.append({"test": test_id, "passed": bool(passed), "detail": str(detail)})
    print(f"{'PASS' if passed else 'FAIL'}  {test_id}  {detail}")


def section(name, fn):
    try:
        fn()
    except Exception as e:
        check(f"{name}: unexpected error", False, str(e)[:1500] + "\n...\n" + traceback.format_exc()[-800:])


def history(layer, name):
    return spark.sql(f"DESCRIBE HISTORY {tbl(layer, name)}").select("version", "operation", "userMetadata").collect()


def version_tagged(layer, name, tag):
    """Latest version of a table whose commit carries this tag (e.g. 'batch_id=1 step=gold')."""
    versions = [h["version"] for h in history(layer, name) if (h["userMetadata"] or "") == tag]
    return max(versions) if versions else None


def at(layer, name, version):
    return spark.read.option("versionAsOf", version).table(tbl(layer, name))


def gold_state(version_of):
    """Gold totals, reading each table at the version chosen by version_of(table_name)."""
    g = lambda n: at("gold", n, version_of(n))
    return {
        "fact_lines": g("fact_order_line").count(),
        "revenue_usd_total": str(g("daily_revenue").agg(F.sum("revenue_usd")).first()[0]),
        "revenue_days": g("daily_revenue").count(),
        "revenue_by_category": {r["category"]: str(r["revenue_usd"]) for r in g("revenue_by_category").collect()},
        "top_customers": [[r["customer_id"], str(r["revenue_usd"])] for r in g("top_customers").orderBy("rank").collect()],
        "payment_mismatches": g("payment_mismatches").count(),
    }


def compare(label, state, ref):
    for key, value in state.items():
        check(f"{label}: gold {key} = Python reference", value == ref[key],
              value if value == ref[key] else {"gold": value, "reference": ref[key]})


def orders_checks():
    h = history("silver", "orders")
    merges = sorted((x["version"], x["userMetadata"]) for x in h if x["operation"] == "MERGE")
    check("R1 history: one MERGE for batch 1, one for batch 2", [m[1] for m in merges] ==
          ["batch_id=1 step=silver.orders", "batch_id=2 step=silver.orders"], merges)
    latest = max(x["version"] for x in h)
    check("R1/S10 batch 2 re-run added no version (latest version = batch 2 MERGE)",
          merges and latest == merges[-1][0], f"latest={latest}, merges={merges}")
    v1 = version_tagged("silver", "orders", "batch_id=1 step=silver.orders")
    after_1 = at("silver", "orders", v1)
    mix1 = {r["status"]: r["count"] for r in after_1.groupBy("status").count().collect()}
    check(f"R2 VERSION AS OF {v1} (right after batch 1): {REF_AFTER_BATCH_1['orders']} orders",
          after_1.count() == REF_AFTER_BATCH_1["orders"], after_1.count())
    check("R2 status mix right after batch 1 = Python reference", mix1 == REF_AFTER_BATCH_1["status_mix"], mix1)
    now = spark.table(tbl("silver", "orders"))
    mix = {r["status"]: r["count"] for r in now.groupBy("status").count().collect()}
    check("final orders and status mix = Python reference",
          now.count() == REF_AFTER_BATCH_2["orders"] and mix == REF_AFTER_BATCH_2["status_mix"], mix)


def gold_checks():
    compare("after batch 1", gold_state(lambda n: version_tagged("gold", n, "batch_id=1 step=gold")), REF_AFTER_BATCH_1)
    latest = lambda n: max(x["version"] for x in history("gold", n))
    compare("after batch 2 (final)", gold_state(latest), REF_AFTER_BATCH_2)
    recon = spark.table(tbl("gold", "reconciliation")).collect()
    check("G5 reconciliation all pass (final)", all(r["passed"] in (True, None) for r in recon),
          [(r["check_name"], r["value"]) for r in recon])


def dq_checks():
    dq = spark.table(tbl("silver", "dq_report")).where("rule = '_TOTAL'")
    for b in (1, 2):
        rows = dq.where(f"batch_id = {b}").collect()
        entities = sorted({(r["layer"], r["entity"]) for r in rows})
        check(f"D1 dq_report has every step for batch {b}", len(entities) == 13, entities)
        bad = [(r["layer"], r["entity"]) for r in rows
               if r["rows_in"] != r["rows_out"] + r["rows_quarantined"] + r["rows_deduped"]]
        check(f"D1 every count balances for batch {b}", not bad, bad or "all balanced")
    for b in (1, 2):
        ok = notebookutils.fs.exists(f"{DQ_EXPORT_DIR}/dq_batch_{b}.json")
        check(f"DQ JSON export exists for batch {b}", ok)
    alerts = spark.table(tbl("silver", "pipeline_alerts")).where("batch_id = 'abc'").count()
    check("on-failure alert from the batch_id=abc run is still recorded", alerts >= 1, alerts)


section("silver.orders history and time travel", orders_checks)
section("gold vs Python reference", gold_checks)
section("data-quality report", dq_checks)

# %%
failed = [r["test"] for r in results if not r["passed"]]
notebookutils.fs.put(files_path("silver", "_test_results", "module7.json"),
                     json.dumps({"passed": len(results) - len(failed), "failed": failed, "results": results}, indent=2),
                     True)
print(f"\n{len(results) - len(failed)} passed, {len(failed)} failed")
assert not failed, f"Failed tests: {failed}"
