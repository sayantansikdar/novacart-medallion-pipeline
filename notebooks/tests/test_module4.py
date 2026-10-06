# %%
# Module 4 tests: silver.orders.
# Part A (generated data, throwaway tables): S3, S5, S6, S7, S8, S9, S10, S11, O4 and the tie rule.
# Part B (real data): runs 02_silver_orders for batch 1, batch 2 and batch 2 again, then checks
#   S12 (one row per order), the final status mix, late data (S9), S10 and the DQ balance.
# Also re-checks Module 3's corrected B2 test. Results: silver Files/_test_results/module4.json.

# %%
%run 00_config

# %%
%run lib_silver

# %%
import json, traceback

results = []


def check(test_id, passed, detail=""):
    results.append({"test": test_id, "passed": bool(passed), "detail": str(detail)})
    print(f"{'PASS' if passed else 'FAIL'}  {test_id}  {detail}")


def section(name, fn):
    try:
        fn()
    except Exception as e:
        check(f"{name}: unexpected error", False, str(e)[:1500] + "\n...\n" + traceback.format_exc()[-800:])


# %%
def module3_b2_recheck():
    ids = [r[0] for r in spark.table(tbl("bronze", "orders"))
           .where(F.col("_source_file") == source_file("orders", 1)).select("_batch_id").distinct().collect()]
    check("B2 (Module 3, corrected) orders batch 1 slice has _batch_id = 1", ids == [1], ids)


# ---------------------------------------------------------------- Part A: generated data
FIXTURE_SCHEMA = (", ".join(f"{c} string" for c in ORDERS_SOURCE_COLS)
                  + f", {CORRUPT_COL} string, _source_file string, _batch_id int")


def fixture_batch(batch, rows):
    """Bronze-shaped DataFrame: rows are tuples in ORDERS_SOURCE_COLS order, all text."""
    data = [r + (None, f"fixture_orders_{batch}.csv", batch) for r in rows]
    return spark.createDataFrame(data, FIXTURE_SCHEMA).withColumn("_ingested_at", F.current_timestamp())


T = "2026-09-08T09:00:00Z"
BATCH_1 = [
    ("A-1", "C1", T, "paid", "INR", "IN", "2026-09-10T10:00:00Z", None),       # S6: exact duplicate pair
    ("A-1", "C1", T, "paid", "INR", "IN", "2026-09-10T10:00:00Z", None),
    ("B-1", "C1", T, "placed", "INR", "IN", "2026-09-10T10:00:00Z", None),     # S7: two versions,
    ("B-1", "C1", T, "paid", "INR", "IN", "2026-09-10T11:00:00Z", None),       #     latest wins
    ("C-1", "C2", T, "paid", "usd", "US", "2026-09-10T12:00:00Z", None),       # S8 base
    ("D-1", "C3", T, "cancelled", "GBP", "GB", "2026-09-10T12:00:00Z", None),  # S9 base
    ("E-1", "C4", T, "placed", "", "IN", "2026-09-09T12:00:00Z", None),        # tie base, S3 currency
    ("F-1", "C5", T, "paid", "INR", "IN", "", None),                           # S11 UPDATED_AT_INVALID
    ("G-1", "C5", "garbage", "paid", "INR", "IN", "2026-09-09T12:00:00Z", None),  # S11 ORDER_TS_INVALID
    ("H-1", "C5", T, "refunded", "INR", "IN", "2026-09-09T12:00:00Z", None),   # S11 STATUS_INVALID
    ("I-1", "C5", T, "paid", "", "XX", "2026-09-09T12:00:00Z", None),          # S5 CURRENCY_UNRESOLVED
    ("J-1", "C5", T, "paid", "JPY", "IN", "2026-09-09T12:00:00Z", None),       # CURRENCY_INVALID
]
BATCH_2 = [
    ("C-1", "C2", T, "shipped", "USD", "US", "2026-09-20T12:00:00Z", None),    # S8 newer -> update
    ("D-1", "C3", T, "paid", "GBP", "GB", "2026-09-05T12:00:00Z", None),       # S9 older -> ignored
    ("E-1", "C4", T, "paid", "", "IN", "2026-09-09T12:00:00Z", None),          # same updated_at, differs
    ("K-1", "C6", T, "paid", "SGD", "SG", "2026-09-21T10:00:00Z", None),       # new order
]


def apply(batch, rows, target):
    valid, bad = prepare_orders(fixture_batch(batch, rows))
    latest = latest_per_key(valid, ["order_id"], "updated_at", ORDERS_CONTENT_COLS)
    ins, upd = merge_orders(latest, target)
    reasons = {r["order_id"]: r["_reason"] for r in bad.select("order_id", "_reason").collect()}
    return ins, upd, reasons


def state(target):
    return {r["order_id"]: (r["status"], r["currency"], r["currency_derived"], str(r["updated_at"]))
            for r in spark.table(target).collect()}


def generated_data_checks():
    a, b = tbl("silver", "test_orders_a"), tbl("silver", "test_orders_b")
    for t in (a, b):
        spark.sql(f"CREATE OR REPLACE TABLE {t} USING DELTA AS SELECT * FROM {tbl('silver', 'orders')} WHERE 1 = 0")

    ins, upd, reasons = apply(1, BATCH_1, a)
    check("S11/S5 quarantine reasons", reasons == {
        "F-1": "UPDATED_AT_INVALID", "G-1": "ORDER_TS_INVALID", "H-1": "STATUS_INVALID",
        "I-1": "CURRENCY_UNRESOLVED", "J-1": "CURRENCY_INVALID"}, reasons)
    check("batch 1: 5 inserted, 0 updated", (ins, upd) == (5, 0), (ins, upd))
    s = state(a)
    check("S6 exact duplicate -> one row", spark.table(a).where("order_id = 'A-1'").count() == 1)
    check("S7 two versions in a batch -> latest wins", s["B-1"][0] == "paid", s["B-1"])
    check("S3 blank currency + IN -> INR derived", s["E-1"][1:3] == ("INR", True), s["E-1"])
    check("S4 'usd' -> USD", s["C-1"][1] == "USD", s["C-1"])

    ins, upd, _ = apply(2, BATCH_2, a)
    s = state(a)
    check("batch 2: 1 inserted (K-1), 2 updated (C-1, E-1)", (ins, upd) == (1, 2), (ins, upd))
    check("S8 newer version in batch 2 -> updated", s["C-1"][0] == "shipped", s["C-1"])
    check("S9 older (late) version in batch 2 -> ignored", s["D-1"][0] == "cancelled", s["D-1"])
    check("tie: same updated_at, later batch, different content -> later batch wins", s["E-1"][0] == "paid", s["E-1"])

    ins, upd, _ = apply(2, BATCH_2, a)
    check("S10 re-run batch 2 -> 0 inserted, 0 updated", (ins, upd) == (0, 0), (ins, upd))
    check("S12 one row per order_id", spark.table(a).count() == spark.table(a).select("order_id").distinct().count())

    apply(2, BATCH_2, b)
    apply(1, BATCH_1, b)
    check("O4 batch 2 then batch 1 -> same state as 1 then 2", state(a) == state(b),
          {k: (state(a).get(k), state(b).get(k)) for k in set(state(a)) | set(state(b)) if state(a).get(k) != state(b).get(k)} or "identical")

    for t in (a, b):
        spark.sql(f"DROP TABLE IF EXISTS {t}")


# ---------------------------------------------------------------- Part B: real data
def run_silver_orders(batch):
    return notebookutils.notebook.run("02_silver_orders", 1200, {"batch_id": str(batch)})


def dq_total(batch):
    return (spark.table(tbl("silver", "dq_report"))
            .where(f"layer = 'silver' AND entity = 'orders' AND rule = '_TOTAL' AND batch_id = {batch}").first())


def real_data_checks():
    orders = tbl("silver", "orders")
    fresh = spark.table(orders).count() == 0

    run_silver_orders(1)
    n, n_ids = spark.table(orders).count(), spark.table(orders).select("order_id").distinct().count()
    check("batch 1: 495 orders, one row each", (n, n_ids) == (495, 495), (n, n_ids))
    d = dq_total(1)
    check("D1 batch 1 balance in = out + quarantined + deduped",
          d["rows_in"] == d["rows_out"] + d["rows_quarantined"] + d["rows_deduped"],
          d.asDict())
    if fresh:
        check("batch 1 on empty table: 495 inserted, 0 updated",
              (d["rows_inserted"], d["rows_updated"]) == (495, 0), (d["rows_inserted"], d["rows_updated"]))

    run_silver_orders(2)
    n, n_ids = spark.table(orders).count(), spark.table(orders).select("order_id").distinct().count()
    check("S12 after batch 2: 945 orders, one row each", (n, n_ids) == (945, 945), (n, n_ids))
    mix = {r["status"]: r["count"] for r in spark.table(orders).groupBy("status").count().collect()}
    want = {"delivered": 666, "cancelled": 107, "shipped": 89, "placed": 44, "paid": 39}
    check("final status mix matches local profile (late copies ignored)", mix == want, mix)
    n_derived = spark.table(orders).where("currency_derived").count()
    check("48 final orders have a derived currency", n_derived == 48, n_derived)
    d = dq_total(2)
    check("D1 batch 2 balance", d["rows_in"] == d["rows_out"] + d["rows_quarantined"] + d["rows_deduped"], d.asDict())
    if fresh:
        check("batch 2: 450 new orders inserted", d["rows_inserted"] == 450, d["rows_inserted"])

    run_silver_orders(2)
    d = dq_total(2)
    check("S10 re-run batch 2 (real data): 0 inserted, 0 updated",
          (d["rows_inserted"], d["rows_updated"]) == (0, 0), (d["rows_inserted"], d["rows_updated"]))
    check("S10 still 945 orders", spark.table(orders).count() == 945)

    n_q = spark.table(tbl("silver", "quarantine")).where("entity = 'orders'").count()
    check("real orders data has no quarantined rows", n_q == 0, n_q)

    # A MERGE that changes nothing writes no Delta commit, so the batch 2 re-run adds no
    # version: history shows one MERGE per batch that changed data, and the re-run is
    # visible in dq_report (0 inserted, 0 updated) instead.
    merges = (spark.sql(f"DESCRIBE HISTORY {orders}")
              .where("operation = 'MERGE' AND userMetadata LIKE '%step=silver.orders%'")
              .select("version", "userMetadata").collect())
    tags = sorted(m["userMetadata"] for m in merges)
    check("R1 preview: one tagged MERGE commit per batch; the no-op re-run adds none",
          tags == ["batch_id=1 step=silver.orders", "batch_id=2 step=silver.orders"],
          [(m["version"], m["userMetadata"]) for m in merges])


section("Module 3 B2 re-check", module3_b2_recheck)
section("generated data", generated_data_checks)
section("real data", real_data_checks)

# %%
failed = [r["test"] for r in results if not r["passed"]]
notebookutils.fs.put(files_path("silver", "_test_results", "module4.json"),
                     json.dumps({"passed": len(results) - len(failed), "failed": failed, "results": results}, indent=2),
                     True)
print(f"\n{len(results) - len(failed)} passed, {len(failed)} failed")
assert not failed, f"Failed tests: {failed}"
