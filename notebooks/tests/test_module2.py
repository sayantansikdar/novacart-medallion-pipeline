# %%
# Module 2 tests: config helpers and setup idempotency.
# Covers O1 (batch_id), S1/S2 (timestamps, UTC date), S3-S5 (currency), D1 (DQ balance),
# R4 + NOT NULL (constraints reject bad rows) and setup idempotency (second run = no commits).
# Results are printed and saved to LH_NovaCart_Silver/Files/_test_results/module2.json.
# The run fails if any test fails.

# %%
%run 00_setup

# %%
import json

results = []


def check(test_id, passed, detail=""):
    results.append({"test": test_id, "passed": bool(passed), "detail": str(detail)})
    print(f"{'PASS' if passed else 'FAIL'}  {test_id}  {detail}")


# ---- O1: batch_id validation
for bad in ["abc", "3", "", None, "1.5"]:
    try:
        validate_batch_id(bad)
        check(f"O1 batch_id={bad!r} rejected", False, "accepted")
    except ValueError:
        check(f"O1 batch_id={bad!r} rejected", True)
check("O1 batch_id='2' accepted", validate_batch_id("2") == 2)

# ---- S1 / S2: three timestamp formats, UTC conversion and the UTC date boundary
ts_cases = spark.createDataFrame(
    [("2026-09-01T13:48:48+05:30",), ("2026-09-01T08:18:48Z",), ("01/09/2026 08:18",),
     ("2026-09-02T02:00:00+05:30",), ("2026-13-45T00:00:00Z",), ("not a date",), ("",), (None,)],
    "raw string")
parsed = {r["raw"]: r for r in ts_cases.select(
    "raw",
    F.date_format(parse_ts(F.col("raw")), "yyyy-MM-dd HH:mm").alias("utc_minute"),
    F.date_format(F.to_date(parse_ts(F.col("raw"))), "yyyy-MM-dd").alias("utc_date"),
).collect()}
for raw in ["2026-09-01T13:48:48+05:30", "2026-09-01T08:18:48Z", "01/09/2026 08:18"]:
    got = parsed[raw]["utc_minute"]
    check(f"S1 {raw} -> 2026-09-01 08:18 UTC", got == "2026-09-01 08:18", got)
got = parsed["2026-09-02T02:00:00+05:30"]["utc_date"]
check("S2 2026-09-02T02:00+05:30 -> order_date 2026-09-01", got == "2026-09-01", got)
for raw in ["2026-13-45T00:00:00Z", "not a date", "", None]:
    check(f"S11 unparseable {raw!r} -> NULL", parsed[raw]["utc_minute"] is None, parsed[raw]["utc_minute"])

# ---- S3 / S4 / S5: currency normalisation and derivation
cur_cases = spark.createDataFrame(
    [("", "IN"), ("usd", "US"), ("", "XX"), (" gbp ", "GB"), ("", "UK"), (None, "SG")],
    "currency string, country string")
cur, derived = normalise_currency(F.col("currency"), F.col("country"))
got = {(r["currency"], r["country"]): (r["cur"], r["derived"])
       for r in cur_cases.select("currency", "country", cur.alias("cur"), derived.alias("derived")).collect()}
check("S3 blank + IN -> INR derived", got[("", "IN")] == ("INR", True), got[("", "IN")])
check("S4 'usd' -> USD not derived", got[("usd", "US")] == ("USD", False), got[("usd", "US")])
check("S5 blank + XX -> NULL (quarantine)", got[("", "XX")][0] is None, got[("", "XX")])
check("S4 ' gbp ' -> GBP", got[(" gbp ", "GB")] == ("GBP", False), got[(" gbp ", "GB")])
check("UK treated as GB -> GBP derived", got[("", "UK")] == ("GBP", True), got[("", "UK")])
check("S3 NULL + SG -> SGD derived", got[(None, "SG")] == ("SGD", True), got[(None, "SG")])

# ---- D1: DQ balance is enforced before anything is written
try:
    DQReport("silver", "test_entity", 1).write(rows_in=10, rows_out=8)   # 2 rows unaccounted for
    check("D1 unbalanced DQ counts rejected", False, "write accepted")
except AssertionError as e:
    check("D1 unbalanced DQ counts rejected", True)

# ---- Setup: tables, properties, unknown member
expected_tables = list(DDL.keys())
for name in expected_tables:
    check(f"setup table silver.{name} exists", spark.catalog.tableExists(tbl("silver", name)))
cdf = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {tbl('silver', 'orders')}").collect()}
check("setup Change Data Feed on silver.orders", cdf.get("delta.enableChangeDataFeed") == "true",
      cdf.get("delta.enableChangeDataFeed"))
for name, checks in CHECKS.items():
    props = {r["key"] for r in spark.sql(f"SHOW TBLPROPERTIES {tbl('silver', name)}").collect()}
    for c in checks:
        check(f"setup constraint {c}", f"delta.constraints.{c.lower()}" in props)
n_unknown = spark.table(tbl("silver", "dim_customer")).where("customer_sk = -1").count()
check("C5 unknown customer row exists exactly once", n_unknown == 1, n_unknown)


# ---- R4 + NOT NULL: constraints reject bad rows and leave the table unchanged
def version(table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]


def expect_rejected(test_id, table, insert_sql):
    before = version(table)
    try:
        spark.sql(insert_sql)
        check(test_id, False, "write was accepted")
    except Exception as e:
        message = str(e).splitlines()[0][:160]
        check(test_id, "constraint" in str(e).lower() and version(table) == before, message)


orders = tbl("silver", "orders")
expect_rejected("R4 status 'refunded' violates CHECK", orders, f"""
    INSERT INTO {orders} VALUES ('T-1', 'C1', TIMESTAMP'2026-09-01 00:00:00', DATE'2026-09-01',
        'refunded', 'INR', false, 'IN', NULL, TIMESTAMP'2026-09-01 00:00:00', 'test', 1, current_timestamp())""")
expect_rejected("NOT NULL customer_id enforced", orders, f"""
    INSERT INTO {orders} VALUES ('T-2', NULL, TIMESTAMP'2026-09-01 00:00:00', DATE'2026-09-01',
        'paid', 'INR', false, 'IN', NULL, TIMESTAMP'2026-09-01 00:00:00', 'test', 1, current_timestamp())""")
items = tbl("silver", "order_items")
expect_rejected("return line with positive qty violates CHECK", items, f"""
    INSERT INTO {items} VALUES ('T-3', 1, 'P001', true, 2, 100, 0, 'return', 200, NULL, NULL,
        'test', 1, current_timestamp())""")

# Remember table versions, then run setup a second time in the next cell.
versions_before = {name: version(tbl("silver", name)) for name in expected_tables}

# %%
%run 00_setup

# %%
versions_after = {name: version(tbl("silver", name)) for name in expected_tables}
changed = {n: (versions_before[n], versions_after[n]) for n in expected_tables if versions_before[n] != versions_after[n]}
check("setup second run makes no commits (idempotent)", not changed, changed or "all versions unchanged")

# ---- Save results and fail the run if anything failed
failed = [r["test"] for r in results if not r["passed"]]
notebookutils.fs.put(files_path("silver", "_test_results", "module2.json"),
                     json.dumps({"passed": len(results) - len(failed), "failed": failed, "results": results}, indent=2),
                     True)
print(f"\n{len(results) - len(failed)} passed, {len(failed)} failed")
assert not failed, f"Failed tests: {failed}"
