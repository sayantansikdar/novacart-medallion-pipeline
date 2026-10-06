# %% [parameters]
# Set by the pipeline; this default is only used when the notebook is run by hand.
batch_id = "1"

# %%
%run 00_config

# %%
%run lib_gold

# %%
# 05_gold: rebuild every gold table from the current silver state.
#   fact_order_line, daily_revenue, revenue_by_category, top_customers, payment_mismatches
#   reconciliation: totals that must agree (test G5) and counts worth reporting
#   exports/dq/dq_batch_<N>.json: this batch's dq_report rows + reconciliation
#
# Re-run safety: gold is a deterministic full rebuild from silver (overwrite), so running it
# again gives identical tables. (Incremental gold via Change Data Feed is a Module 8 extra.)

import json
from decimal import Decimal

BATCH = validate_batch_id(batch_id)
silver = {n: spark.table(tbl("silver", n)) for n in
          ["orders", "order_items", "products", "fx_rates", "dim_customer", "payments"]}

fact = build_fact_order_line(silver["orders"], silver["order_items"], silver["products"],
                             silver["fx_rates"], silver["dim_customer"]).cache()
mismatches, orphan_payments = build_payment_mismatches(fact, silver["orders"], silver["payments"], silver["fx_rates"])
outputs = {
    "fact_order_line": fact,
    "daily_revenue": build_daily_revenue(fact),
    "revenue_by_category": build_revenue_by_category(fact),
    "top_customers": build_top_customers(fact, silver["dim_customer"]),
    "payment_mismatches": mismatches,
}

tag_commits(f"batch_id={BATCH} step=gold")
for name, df in outputs.items():
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl("gold", name))
    print(f"gold.{name}: {spark.table(tbl('gold', name)).count()} rows")

# %%
# Reconciliation: the same revenue measured three ways must agree to the cent (test G5).
gold = {n: spark.table(tbl("gold", n)) for n in outputs}
fact_rev = gold["fact_order_line"].where("is_revenue").agg(F.sum("net_usd")).first()[0] or Decimal(0)
daily_rev = gold["daily_revenue"].agg(F.sum("revenue_usd")).first()[0] or Decimal(0)
cat_rev = gold["revenue_by_category"].agg(F.sum("revenue_usd")).first()[0] or Decimal(0)
n_lines = gold["fact_order_line"].count()
n_items = silver["order_items"].count()

checks = [
    ("fact lines = silver order_items lines", n_lines, n_items, n_lines == n_items),
    ("daily_revenue total = fact revenue total", daily_rev, fact_rev, daily_rev == fact_rev),
    ("revenue_by_category total = fact revenue total", cat_rev, fact_rev, cat_rev == fact_rev),
    ("lines without an FX rate (fx_missing, excluded)", gold["fact_order_line"].where("fx_missing").count(), 0, None),
    ("lines for customers not in the CRM (customer_sk = -1)",
     gold["fact_order_line"].where(f"customer_sk = {UNKNOWN_CUSTOMER_SK}").count(), None, None),
    ("payments whose order is unknown (excluded from mismatches)", orphan_payments, None, None),
    ("orders with a payment mismatch > 0.50 USD", gold["payment_mismatches"].count(), None, None),
]
recon = spark.createDataFrame(
    [(BATCH, name, str(value), None if expected is None else str(expected), passed) for name, value, expected, passed in checks],
    "batch_id int, check_name string, value string, expected string, passed boolean")
recon.write.format("delta").mode("overwrite").saveAsTable(tbl("gold", "reconciliation"))
tag_commits(None)

DQReport("gold", "fact_order_line", BATCH).write(rows_in=n_items, rows_out=n_lines)
for name, value, expected, passed in checks:
    print(f"{'OK  ' if passed in (True, None) else 'FAIL'} {name}: {value}" + (f" (expected {expected})" if expected is not None else ""))
failed = [name for name, _, _, passed in checks if passed is False]

# %%
# Export this batch's data-quality report as JSON (gold Files/exports/dq/).
dq_rows = [r.asDict() for r in spark.table(tbl("silver", "dq_report")).where(f"batch_id = {BATCH}")
           .orderBy("layer", "entity", "rule").collect()]
export = {"batch_id": BATCH, "dq_report": dq_rows, "reconciliation": [r.asDict() for r in recon.collect()]}
notebookutils.fs.put(f"{DQ_EXPORT_DIR}/dq_batch_{BATCH}.json", json.dumps(export, indent=2, default=str), True)
print(f"DQ report exported to {DQ_EXPORT_DIR}/dq_batch_{BATCH}.json")
fact.unpersist()

# A broken reconciliation fails the step, so the pipeline's failure path fires.
assert not failed, f"Reconciliation failed: {failed}"
