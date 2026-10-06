# %%
# Module 5 tests: order items, dim_customer (SCD2) and the reference tables.
# Part A (generated data, throwaway tables): I1-I7, C1-C3, snapshot sync re-run.
# Part B (real data): runs 03_silver_reference and 04_silver_items for batch 1, batch 2 and
#   batch 2 again, then checks counts against the local profile, C4 integrity and re-run safety.
# Also re-checks Module 4's corrected R1 test. Results: silver Files/_test_results/module5.json.

# %%
%run 00_config

# %%
%run lib_silver

# %%
import json, traceback
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


def module4_r1_recheck():
    tags = sorted(r["userMetadata"] for r in spark.sql(f"DESCRIBE HISTORY {tbl('silver', 'orders')}")
                  .where("operation = 'MERGE' AND userMetadata LIKE '%step=silver.orders%'").collect())
    check("R1 (Module 4, corrected) one tagged MERGE per batch, none for the no-op re-run",
          tags == ["batch_id=1 step=silver.orders", "batch_id=2 step=silver.orders"], tags)


def bronze_like(rows, cols, batch):
    schema = ", ".join(f"{c} string" for c in cols) + f", {CORRUPT_COL} string, _source_file string, _batch_id int"
    return (spark.createDataFrame([r + (None, f"fixture_{batch}", batch) for r in rows], schema)
            .withColumn("_ingested_at", F.current_timestamp()))


# ---------------------------------------------------------------- Part A: items
def item_checks():
    orders = spark.createDataFrame([("O-1",), ("O-2",)], "order_id string")
    products = spark.createDataFrame([("P001",)], "product_id string")
    target = tbl("silver", "test_items")
    spark.sql(f"CREATE OR REPLACE TABLE {target} USING DELTA AS SELECT * FROM {tbl('silver', 'order_items')} WHERE 1 = 0")

    b1 = [
        ("O-1", "1", "P001", "2", "100", "10", "sale", '{"color": "red", "warranty_months": 12}'),   # I3, I6
        ("O-1", "2", "P999", "1", "50", None, "sale", None),                                         # I2, I4
        ("O-1", "3", "P001", "2", "100", "0", "return", '{"reason": "damaged"}'),                    # I5 (+2 -> -2)
        ("O-1", "4", "P001", "1", "80", "0", "sale", None),                                          # I7 base
        ("O-9", "1", "P001", "1", "10", "0", "sale", None),                                          # I1 orphan
        ("O-2", "1", "P001", "abc", "10", "0", "sale", None),                                        # QTY_INVALID
        ("O-2", "2", "P001", "1", "10", "150", "sale", None),                                        # DISCOUNT_INVALID
    ]
    valid, bad = prepare_items(bronze_like(b1, ITEMS_SOURCE_COLS, 1), orders, products)
    reasons = {(r["order_id"], r["line_no"]): r["_reason"] for r in bad.collect()}
    check("I1 line without order -> ORDER_NOT_FOUND", reasons.get(("O-9", "1")) == "ORDER_NOT_FOUND", reasons)
    check("qty 'abc' -> QTY_INVALID; discount 150 -> DISCOUNT_INVALID",
          reasons.get(("O-2", "1")) == "QTY_INVALID" and reasons.get(("O-2", "2")) == "DISCOUNT_INVALID", reasons)
    ins, upd = merge_items(latest_per_key(valid, ["order_id", "line_no"], "_ingested_at", ITEMS_CONTENT_COLS), target)
    rows = {r["line_no"]: r for r in spark.table(target).where("order_id = 'O-1'").collect()}
    check("I3 qty 2 x price 100 x (1 - 10%) = 180", rows[1]["net_local"] == Decimal("180.0000"), rows[1]["net_local"])
    check("I4 NULL discount -> 0", rows[2]["discount_pct"] == 0 and rows[2]["net_local"] == Decimal("50.0000"),
          (rows[2]["discount_pct"], rows[2]["net_local"]))
    check("I2 unknown product loads, product_known = false", rows[2]["product_known"] is False, rows[2]["product_id"])
    check("I5 return qty forced negative", rows[3]["qty"] == -2 and rows[3]["net_local"] == Decimal("-200.0000"),
          (rows[3]["qty"], rows[3]["net_local"]))
    check("I6 attributes parsed to a map, raw JSON kept",
          rows[1]["attributes"] == {"color": "red", "warranty_months": "12"} and rows[1]["attributes_json"].startswith("{"),
          (rows[1]["attributes"], rows[1]["attributes_json"]))

    b2 = [("O-1", "4", "P001", "1", "75", "0", "sale", None)]                                        # I7 corrected price
    v2, _ = prepare_items(bronze_like(b2, ITEMS_SOURCE_COLS, 2), orders, products)
    ins, upd = merge_items(v2, target)
    price = spark.table(target).where("order_id = 'O-1' AND line_no = 4").first()["unit_price"]
    check("I7 line corrected in batch 2 replaces batch 1", (upd, price) == (1, Decimal("75.0000")), (upd, price))
    ins, upd = merge_items(v2, target)
    check("I7 re-run batch 2 -> 0 inserted, 0 updated", (ins, upd) == (0, 0), (ins, upd))
    v1_again, _ = prepare_items(bronze_like(b1, ITEMS_SOURCE_COLS, 1), orders, products)
    merge_items(latest_per_key(v1_again, ["order_id", "line_no"], "_ingested_at", ITEMS_CONTENT_COLS), target)
    price = spark.table(target).where("order_id = 'O-1' AND line_no = 4").first()["unit_price"]
    check("O4 batch 1 run after batch 2 does not undo the correction", price == Decimal("75.0000"), price)
    spark.sql(f"DROP TABLE IF EXISTS {target}")


# ---------------------------------------------------------------- Part A: customers
def customer_checks():
    rows = [
        ("K1", "Ann", "ann@a.com", "Gold", "IN", "2026-03-01T00:00:00Z"),
        ("K1", "Ann", "ann@new.com", "Gold", "IN", "2026-05-01T00:00:00Z"),    # C2 e-mail only
        ("K1", "Ann", "ann@new.com", "Platinum", "IN", "2026-09-10T00:00:00Z"),  # C3 tier change
        ("K1", "Ann", "ann@new.com", "Platinum", "IN", "2026-09-10T00:00:00Z"),  # exact duplicate
        ("K2", "Bob", "bob@b.com", "Silver", "UK", "2026-02-01T00:00:00Z"),    # UK -> GB
        ("K3", "Cat", "cat@c.com", "Gold", "US", ""),                          # C1 no updated_at
    ]
    valid, bad = prepare_customers(bronze_like(rows, CUSTOMERS_SOURCE_COLS, 1))
    reasons = {r["customer_id"]: r["_reason"] for r in bad.collect()}
    check("C1 CRM row without updated_at -> quarantined", reasons == {"K3": "UPDATED_AT_MISSING"}, reasons)
    dim = {(r["customer_id"], r["tier"]): r for r in build_dim_customer(valid).collect()}
    k1 = sorted([r for k, r in dim.items() if k[0] == "K1"], key=lambda r: r["valid_from"])
    check("C2 e-mail-only change -> no new version (2 versions for K1, not 3)", len(k1) == 2, [r["tier"] for r in k1])
    check("C2 e-mail is Type 1: every version has the latest e-mail",
          all(r["email"] == "ann@new.com" for r in k1), [r["email"] for r in k1])
    check("C3 tier change -> new version, dates chain, is_current flips",
          len(k1) == 2 and k1[0]["valid_to"] == k1[1]["valid_from"]
          and (k1[0]["is_current"], k1[1]["is_current"]) == (False, True)
          and str(k1[0]["valid_from"]).startswith("1900-01-01"),
          [(str(r["valid_from"]), str(r["valid_to"]), r["is_current"]) for r in k1])
    check("UK country stored as GB", ("K2", "Silver") in dim and dim[("K2", "Silver")]["country"] == "GB")
    again = {r["customer_sk"] for r in build_dim_customer(valid).collect()}
    check("customer_sk is deterministic across rebuilds", again == {r["customer_sk"] for r in dim.values()})


# ---------------------------------------------------------------- Part B: real data
def run(nb, batch):
    return notebookutils.notebook.run(nb, 1200, {"batch_id": str(batch)})


def versions(names):
    return {n: spark.sql(f"DESCRIBE HISTORY {tbl('silver', n)} LIMIT 1").first()["version"] for n in names}


def dq_total(entity, batch):
    return (spark.table(tbl("silver", "dq_report"))
            .where(f"layer = 'silver' AND entity = '{entity}' AND rule = '_TOTAL' AND batch_id = {batch}").first())


def real_data_checks():
    ref_tables = ["products", "fx_rates", "payments", "dim_customer"]
    for b in (1, 2):
        run("03_silver_reference", b)
        run("04_silver_items", b)

    counts = {n: spark.table(tbl("silver", n)).count() for n in ref_tables}
    check("reference tables: 40 products, 84 fx rates, 1164 payments, 166 + 1 customer versions",
          counts == {"products": 40, "fx_rates": 84, "payments": 1164, "dim_customer": 167}, counts)

    dim = spark.table(tbl("silver", "dim_customer")).where("customer_sk != -1")
    n_cust = dim.select("customer_id").distinct().count()
    n_current = dim.where("is_current").count()
    check("C4 one current version per customer (130 customers)", (n_cust, n_current) == (130, 130), (n_cust, n_current))
    overlaps = spark.sql(f"""
        SELECT a.customer_id FROM {tbl('silver', 'dim_customer')} a JOIN {tbl('silver', 'dim_customer')} b
          ON a.customer_id = b.customer_id AND a.customer_sk < b.customer_sk
         AND a.valid_from < b.valid_to AND b.valid_from < a.valid_to""").count()
    check("C4 no overlapping validity ranges", overlaps == 0, overlaps)
    gaps = spark.sql(f"""
        SELECT customer_id FROM (
          SELECT customer_id, valid_to, lead(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from) AS next_from
          FROM {tbl('silver', 'dim_customer')}) WHERE next_from IS NOT NULL AND next_from != valid_to""").count()
    check("C3 versions chain with no gaps", gaps == 0, gaps)
    q_crm = spark.table(tbl("silver", "quarantine")).where("entity = 'dim_customer' AND batch_id = 2").count()
    check("C1 real data: 5 CRM rows without updated_at quarantined", q_crm == 5, q_crm)

    items = spark.table(tbl("silver", "order_items"))
    n_items = items.count()
    check("items: 971 (batch 1) + 974 (batch 2) - 12 re-delivered = 1933 lines", n_items == 1933, n_items)
    q = {r["batch_id"]: r["count"] for r in spark.table(tbl("silver", "quarantine"))
         .where("entity = 'order_items' AND reason = 'ORDER_NOT_FOUND'").groupBy("batch_id").count().collect()}
    check("I1 real data: 10 + 8 orphan lines quarantined (NC-990xxx)", q == {1: 10, 2: 8}, q)
    n_unknown = items.where("NOT product_known").count()
    check("I2 real data: 14 lines with unknown products loaded and flagged", n_unknown == 14, n_unknown)
    n_bad_returns = items.where("line_type = 'return' AND qty >= 0").count()
    check("I5 every return line has negative qty", n_bad_returns == 0, n_bad_returns)
    for e in ["order_items", "products", "fx_rates", "payments", "dim_customer"]:
        for b in (1, 2):
            d = dq_total(e, b)
            check(f"D1 {e} batch {b} balance",
                  d is not None and d["rows_in"] == d["rows_out"] + d["rows_quarantined"] + d["rows_deduped"],
                  d.asDict() if d else "missing")

    before = versions(ref_tables + ["order_items"])
    run("03_silver_reference", 2)
    run("04_silver_items", 2)
    after = versions(ref_tables + ["order_items"])
    check("re-run batch 2: no new commit on any table", before == after, {"before": before, "after": after})
    d = dq_total("order_items", 2)
    check("re-run batch 2: items 0 inserted, 0 updated", (d["rows_inserted"], d["rows_updated"]) == (0, 0),
          (d["rows_inserted"], d["rows_updated"]))


section("Module 4 R1 re-check", module4_r1_recheck)
section("items (generated)", item_checks)
section("customers (generated)", customer_checks)
section("real data", real_data_checks)

# %%
failed = [r["test"] for r in results if not r["passed"]]
notebookutils.fs.put(files_path("silver", "_test_results", "module5.json"),
                     json.dumps({"passed": len(results) - len(failed), "failed": failed, "results": results}, indent=2),
                     True)
print(f"\n{len(results) - len(failed)} passed, {len(failed)} failed")
assert not failed, f"Failed tests: {failed}"
