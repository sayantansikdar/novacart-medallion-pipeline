# %%
# 06_evidence: Part 5 (table reliability) and Part 6 (extras) evidence on the official tables.
# Run once after the official pipeline runs. Every section prints/displays its evidence (screenshot
# the run snapshot) and records a pass/fail check; results go to silver Files/_test_results/module8.json.
#
# Safe to run: it only reads the real tables. The two deliberate bad writes (extra column, bad status)
# are rejected by Delta and change nothing; OPTIMIZE rewrites files without changing data; the VACUUM
# demonstration and the incremental-gold demonstration use throwaway tables that are dropped at the end.

# %%
%run 00_config

# %%
%run lib_gold

# %%
import json, traceback

ORDERS = tbl("silver", "orders")
FACT = tbl("gold", "fact_order_line")
evidence, results = {}, []


def check(test_id, passed, detail=""):
    results.append({"test": test_id, "passed": bool(passed), "detail": str(detail)})
    print(f"{'PASS' if passed else 'FAIL'}  {test_id}  {detail}")


def section(name, fn):
    print(f"\n==================== {name}")
    try:
        fn()
    except Exception as e:
        check(f"{name}: unexpected error", False, str(e)[:1500] + "\n...\n" + traceback.format_exc()[-800:])


def latest_version(table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]


def version_tagged(table, tag):
    rows = spark.sql(f"DESCRIBE HISTORY {table}").where(F.col("userMetadata") == tag).collect()
    return max(r["version"] for r in rows) if rows else None


def rows_of(df, n=50):
    return [{k: (str(v) if v is not None else None) for k, v in r.asDict().items()} for r in df.limit(n).collect()]


# %%
def change_history():
    """Part 5.1: history of silver.orders after batch 1, batch 2 and batch 2 again."""
    hist = (spark.sql(f"DESCRIBE HISTORY {ORDERS}")
            .select("version", "timestamp", "operation", "userMetadata",
                    F.col("operationMetrics")["numTargetRowsInserted"].alias("rows_inserted"),
                    F.col("operationMetrics")["numTargetRowsUpdated"].alias("rows_updated"))
            .orderBy("version"))
    display(hist)
    evidence["history"] = rows_of(hist)
    merges = [r for r in hist.collect() if r["operation"] == "MERGE"]
    check("R1 one MERGE commit per batch, tagged in userMetadata",
          [m["userMetadata"] for m in merges] == ["batch_id=1 step=silver.orders", "batch_id=2 step=silver.orders"],
          [(m["version"], m["userMetadata"], m["rows_inserted"], m["rows_updated"]) for m in merges])
    check("R1 the batch 2 re-run made no commit (no rows changed, so no new version)",
          latest_version(ORDERS) == merges[-1]["version"], f"latest version = {latest_version(ORDERS)}")


def point_in_time():
    """Part 5.2: the table exactly as it was right after batch 1."""
    v1 = version_tagged(ORDERS, "batch_id=1 step=silver.orders")
    ts1 = spark.sql(f"DESCRIBE HISTORY {ORDERS}").where(f"version = {v1}").first()["timestamp"]
    by_version = spark.sql(f"SELECT * FROM {ORDERS} VERSION AS OF {v1}")
    by_time = spark.read.option("timestampAsOf", str(ts1)).table(ORDERS)
    summary = by_version.groupBy("status").count().orderBy("status")
    display(summary)
    evidence["point_in_time"] = {"query": f"SELECT * FROM {ORDERS} VERSION AS OF {v1}", "timestamp": str(ts1),
                                 "rows_then": by_version.count(), "rows_now": spark.table(ORDERS).count(),
                                 "status_then": rows_of(summary)}
    check(f"R2 VERSION AS OF {v1} returns the post-batch-1 table (495 orders)", by_version.count() == 495,
          by_version.count())
    check("R2 TIMESTAMP AS OF the batch-1 commit returns the same rows",
          by_time.exceptAll(by_version).count() == 0 and by_version.exceptAll(by_time).count() == 0, str(ts1))


def schema_enforcement():
    """Part 5.3: appending data with an unexpected column is rejected (plus a CHECK violation)."""
    before = latest_version(ORDERS)
    bad = spark.table(ORDERS).limit(1).withColumn("loyalty_points", F.lit(10))
    try:
        bad.write.format("delta").mode("append").saveAsTable(ORDERS)
        message = "write was accepted"
    except Exception as e:
        message = str(e).split("\n")[0][:600]
    after = latest_version(ORDERS)
    print("Extra column ->", message)
    evidence["schema_enforcement"] = {"attempt": "append 1 row of silver.orders with an extra column loyalty_points",
                                      "error": message, "version_before": before, "version_after": after}
    check("R3 append with an extra column is rejected; table version unchanged",
          message != "write was accepted" and before == after, f"v{before} -> v{after}: {message[:160]}")

    try:
        spark.sql(f"""INSERT INTO {ORDERS} VALUES ('T-R4', 'C0001', TIMESTAMP'2026-09-01 00:00:00', DATE'2026-09-01',
                      'refunded', 'INR', false, 'IN', NULL, TIMESTAMP'2026-09-01 00:00:00', 'evidence', 1, current_timestamp())""")
        check_msg = "insert was accepted"
    except Exception as e:
        lines = [l for l in str(e).split("\n") if "constraint" in l.lower()]
        check_msg = (lines[0] if lines else str(e).split("\n")[0])[:600]
    print("Bad status ->", check_msg)
    evidence["check_constraint"] = {"attempt": "insert an order with status 'refunded'", "error": check_msg,
                                    "version_after": latest_version(ORDERS)}
    check("R4 status 'refunded' violates the CHECK constraint; version unchanged",
          "constraint" in check_msg.lower() and latest_version(ORDERS) == before, check_msg[:160])


def storage_optimisation():
    """Part 5.4: compact the fact table's small files (and co-locate by order_date)."""
    def detail():
        d = spark.sql(f"DESCRIBE DETAIL {FACT}").first()
        return {"numFiles": d["numFiles"], "sizeInBytes": d["sizeInBytes"]}

    def fingerprint():
        return spark.table(FACT).agg(F.count("*").alias("rows"), F.sum("net_usd").alias("net_usd")).first().asDict()

    before, fp_before = detail(), fingerprint()
    try:
        spark.sql(f"OPTIMIZE {FACT} ZORDER BY (order_date)")
        command = f"OPTIMIZE {FACT} ZORDER BY (order_date)"
    except Exception:
        spark.sql(f"OPTIMIZE {FACT}")
        command = f"OPTIMIZE {FACT}"
    after, fp_after = detail(), fingerprint()
    # The table history keeps every OPTIMIZE with its file counts, including one done by an earlier run.
    optimizes = [{"version": r["version"], "timestamp": str(r["timestamp"]),
                  "files_removed": r["operationMetrics"].get("numRemovedFiles"),
                  "files_added": r["operationMetrics"].get("numAddedFiles")}
                 for r in spark.sql(f"DESCRIBE HISTORY {FACT}").where("operation = 'OPTIMIZE'").orderBy("version").collect()]
    print(command, before, "->", after, "| OPTIMIZE commits:", optimizes)
    evidence["optimize"] = {"command": command, "before": before, "after": after, "optimize_commits": optimizes,
                            "data_before": {k: str(v) for k, v in fp_before.items()},
                            "data_after": {k: str(v) for k, v in fp_after.items()}}
    first = optimizes[0] if optimizes else {}
    check("R5 OPTIMIZE compacted the fact table's files; data identical",
          first and int(first["files_removed"]) > int(first["files_added"]) and fp_before == fp_after,
          f"OPTIMIZE v{first.get('version')}: {first.get('files_removed')} files -> {first.get('files_added')}; "
          f"now {after['numFiles']} file(s), rows {fp_after['rows']}, net_usd {fp_after['net_usd']}")


def vacuum_warning():
    """Why VACUUM must be used carefully: it deletes the files old versions need.
    Demonstrated on a throwaway copy, never on the real tables."""
    demo = tbl("gold", "vacuum_demo")
    spark.sql(f"DROP TABLE IF EXISTS {demo}")
    spark.table(tbl("gold", "daily_revenue")).write.format("delta").saveAsTable(demo)               # version 0
    # Replace every file (an UPDATE could use deletion vectors and keep version 0's file in use)
    spark.createDataFrame([], spark.table(demo).schema).write.format("delta").mode("overwrite").saveAsTable(demo)  # v1
    readable_before = spark.read.option("versionAsOf", 0).table(demo).count()
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    spark.sql(f"VACUUM {demo} RETAIN 0 HOURS")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
    try:
        # collect() really opens the data files; count() alone can be answered from the Delta log's statistics
        spark.read.option("versionAsOf", 0).table(demo).collect()
        after = "version 0 still readable"
    except Exception as e:
        after = str(e).split("\n")[0][:300]
    print("Time travel to version 0 after VACUUM ->", after)
    evidence["vacuum"] = {"version_0_rows_before_vacuum": readable_before, "after_vacuum": after}
    check("VACUUM removes the files of old versions: time travel to them fails afterwards",
          after != "version 0 still readable", after[:160])
    spark.sql(f"DROP TABLE IF EXISTS {demo}")


def change_data_feed():
    """Part 6.1: which rows of silver.orders changed in batch 2."""
    v2 = version_tagged(ORDERS, "batch_id=2 step=silver.orders")
    changes = (spark.read.format("delta").option("readChangeFeed", "true")
               .option("startingVersion", v2).option("endingVersion", v2).table(ORDERS))
    by_type = changes.groupBy("_change_type").count().orderBy("_change_type")
    display(by_type)
    pre = changes.where("_change_type = 'update_preimage'").select("order_id", F.col("status").alias("status_before"))
    post = changes.where("_change_type = 'update_postimage'").select("order_id", F.col("status").alias("status_after"))
    transitions = (pre.join(post, "order_id").groupBy("status_before", "status_after").count()
                   .orderBy(F.col("count").desc()))
    display(transitions)
    counts = {r["_change_type"]: r["count"] for r in by_type.collect()}
    evidence["cdf"] = {"version": v2, "by_change_type": counts, "status_transitions": rows_of(transitions),
                       "sample_updates": rows_of(pre.join(post, "order_id").orderBy("order_id"), 10)}
    check("CDF: batch 2 = 450 inserted orders and 137 updated orders",
          counts.get("insert") == 450 and counts.get("update_preimage") == 137 == counts.get("update_postimage"), counts)


def incremental_gold():
    """Part 6.2: update gold incrementally instead of rebuilding it, and prove it gives the same answer.

    Start from gold.fact_order_line as it was after batch 1 (time travel), find the orders that batch 2
    touched (Change Data Feed on silver.orders + the _batch_id lineage of silver.order_items), rebuild
    only those orders' lines, and compare with the full rebuild. Valid while the reference tables did
    not change in batch 2 (checked); if they had, the safe answer is a full rebuild."""
    ref_changed = [n for n in ["products", "fx_rates", "dim_customer", "payments"]
                   if latest_version(tbl("silver", n)) != version_tagged(tbl("silver", n), f"batch_id=1 step=silver.{n}")]
    demo = tbl("gold", "fact_incremental_demo")
    spark.sql(f"DROP TABLE IF EXISTS {demo}")
    spark.read.option("versionAsOf", version_tagged(FACT, "batch_id=1 step=gold")).table(FACT) \
        .write.format("delta").saveAsTable(demo)

    v1 = version_tagged(ORDERS, "batch_id=1 step=silver.orders")
    changed_orders = (spark.read.format("delta").option("readChangeFeed", "true")
                      .option("startingVersion", v1 + 1).table(ORDERS).select("order_id"))
    changed_items = spark.table(tbl("silver", "order_items")).where("_batch_id > 1").select("order_id")
    affected = changed_orders.union(changed_items).distinct().cache()
    affected.createOrReplaceTempView("_affected_orders")

    s = {n: spark.table(tbl("silver", n)) for n in ["orders", "order_items", "products", "fx_rates", "dim_customer"]}
    new_lines = build_fact_order_line(s["orders"].join(affected, "order_id"), s["order_items"].join(affected, "order_id"),
                                      s["products"], s["fx_rates"], s["dim_customer"])
    # Delta does not allow a subquery in a DELETE condition, so the affected orders' old lines are removed with a MERGE
    spark.sql(f"MERGE INTO {demo} AS t USING _affected_orders AS s ON t.order_id = s.order_id WHEN MATCHED THEN DELETE")
    new_lines.write.format("delta").mode("append").saveAsTable(demo)

    full, incr = spark.table(FACT), spark.table(demo)
    diff = full.exceptAll(incr).count() + incr.exceptAll(full).count()
    n_affected, n_lines = affected.count(), new_lines.count()
    print(f"affected orders: {n_affected}; lines rebuilt: {n_lines} of {full.count()}; differences vs full rebuild: {diff}")
    evidence["incremental_gold"] = {"affected_orders": n_affected, "lines_rebuilt": n_lines, "lines_total": full.count(),
                                    "differences_vs_full_rebuild": diff, "reference_tables_changed_in_batch_2": ref_changed}
    check("reference tables unchanged in batch 2, so an incremental update is valid", not ref_changed, ref_changed or "none")
    check("incremental gold (only changed orders rebuilt) = full rebuild", diff == 0,
          f"{n_affected} orders / {n_lines} lines rebuilt, {diff} differences")
    affected.unpersist()
    spark.sql(f"DROP TABLE IF EXISTS {demo}")


for name, fn in [("5.1 change history", change_history), ("5.2 point-in-time read", point_in_time),
                 ("5.3 schema enforcement", schema_enforcement), ("5.4 storage optimisation", storage_optimisation),
                 ("VACUUM warning", vacuum_warning), ("6.1 change data feed", change_data_feed),
                 ("6.2 incremental gold", incremental_gold)]:
    section(name, fn)

# %%
failed = [r["test"] for r in results if not r["passed"]]
notebookutils.fs.put(files_path("silver", "_test_results", "module8.json"),
                     json.dumps({"passed": len(results) - len(failed), "failed": failed, "results": results,
                                 "evidence": evidence}, indent=2, default=str), True)
print(f"\n{len(results) - len(failed)} passed, {len(failed)} failed")
assert not failed, f"Failed checks: {failed}"
