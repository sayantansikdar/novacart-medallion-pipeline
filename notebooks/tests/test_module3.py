# %%
# Module 3 tests: bronze ingestion.
# Covers B1 (counts), B2 (lineage), B3 (re-run), B4 (values as received),
# B5 (malformed line), B6 (snapshot loaded once) and the bronze rows of the DQ report.
# Runs 01_bronze for batch 1, batch 1 again, then batch 2, so it leaves bronze holding
# both batches. Results go to LH_NovaCart_Silver/Files/_test_results/module3.json.
# O1 for the bronze step is tested in Module 7 through the pipeline: a deliberately
# failing child notebook run here may cancel the whole test session.

# %%
%run 00_config

# %%
import json, traceback

results = []


def check(test_id, passed, detail=""):
    results.append({"test": test_id, "passed": bool(passed), "detail": str(detail)})
    print(f"{'PASS' if passed else 'FAIL'}  {test_id}  {detail}")


def section(name, fn):
    """Run one group of tests; an unexpected error is recorded as a failure (with its
    traceback) instead of stopping the notebook, so the results file says where it broke."""
    try:
        fn()
    except Exception as e:
        # The first lines carry the actual message; the tail shows where it was raised.
        check(f"{name}: unexpected error", False,
              str(e)[:1500] + "\n...\n" + traceback.format_exc()[-800:])


def run_bronze(batch):
    return notebookutils.notebook.run("01_bronze", 1200, {"batch_id": str(batch)})


def independent_count(entity, batch):
    """Count records without Spark's CSV/JSON readers: the whole file is read as plain text
    and parsed in Python."""
    text = spark.read.text(f"{INPUT_DIR}/{source_file(entity, batch)}", wholetext=True).first()[0]
    fmt = SOURCES[entity]["format"]
    if fmt == "json_array":
        return len(json.loads(text))
    lines = [l for l in text.splitlines() if l.strip()]
    return len(lines) - 1 if fmt == "csv" else len(lines)


def slice_counts(entity):
    return {r["_source_file"]: r["count"]
            for r in spark.table(tbl("bronze", entity)).groupBy("_source_file").count().collect()}


# %%
def batch_1_checks():
    run_bronze(1)
    for entity in SOURCES:
        file = source_file(entity, 1)
        got = slice_counts(entity).get(file, 0)
        want = independent_count(entity, 1)
        check(f"B1 {entity} bronze rows = source records", got == want, f"bronze={got} source={want}")

    for entity in SOURCES:
        df = spark.table(tbl("bronze", entity))
        nulls = df.where(F.col("_source_file").isNull() | F.col("_batch_id").isNull()
                         | F.col("_ingested_at").isNull()).count()
        check(f"B2 {entity} lineage never null", nulls == 0, f"null rows={nulls}")
        non_string = [f.name for f in df.schema.fields
                      if f.name not in ("_batch_id", "_ingested_at") and f.dataType != T.StringType()]
        check(f"B4 {entity} every source column is STRING", not non_string, non_string or "all STRING")

    # Only batch 1's own slice is checked: the table may already hold batch 2's slice
    # from an earlier run, and keeping other batches' slices is exactly what bronze should do.
    batch_ids = [r[0] for r in spark.table(tbl("bronze", "orders"))
                 .where(F.col("_source_file") == source_file("orders", 1))
                 .select("_batch_id").distinct().collect()]
    check("B2 orders batch 1 slice has _batch_id = 1", batch_ids == [1], batch_ids)

    orders = spark.table(tbl("bronze", "orders"))
    n_dmy = orders.where(F.col("order_ts").rlike(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$")).count()
    check("B4 day/month/year order_ts stored verbatim", n_dmy > 0, f"{n_dmy} rows like 01/09/2026 08:18")
    n_lower = orders.where(F.col("currency") == "inr").count()
    check("B4 lowercase 'inr' stored verbatim", n_lower > 0, f"{n_lower} rows")
    items = spark.table(tbl("bronze", "order_items"))
    sample_attr = items.where(F.col("attributes").isNotNull()).select("attributes").first()[0]
    check("B4/I6 nested attributes kept as raw JSON text", sample_attr.startswith("{"), sample_attr)
    n_quoted_price = items.where(F.col("unit_price") == "37.46").count()
    check("B4 unit_price sent as a JSON string kept as text", n_quoted_price > 0, f"{n_quoted_price} rows")


def rerun_checks():
    # B3: re-running batch 1 changes no counts
    before = {e: slice_counts(e) for e in SOURCES}
    run_bronze(1)
    after = {e: slice_counts(e) for e in SOURCES}
    check("B3 re-run batch 1: counts per _source_file unchanged", before == after,
          "unchanged" if before == after else {e: (before[e], after[e]) for e in SOURCES if before[e] != after[e]})


def batch_2_checks():
    # B6: batch 2 adds its own incremental files; snapshots stay a single copy
    run_bronze(2)
    for entity, spec in SOURCES.items():
        counts = slice_counts(entity)
        if spec["kind"] == "incremental":
            want = {source_file(entity, 1): independent_count(entity, 1),
                    source_file(entity, 2): independent_count(entity, 2)}
            check(f"B6 {entity} holds both batch slices", counts == want, counts)
        else:
            want = {spec["file"]: independent_count(entity, 2)}
            ids = [r[0] for r in spark.table(tbl("bronze", entity)).select("_batch_id").distinct().collect()]
            check(f"B6 {entity} snapshot loaded once, from batch 2", counts == want and ids == [2],
                  f"counts={counts} batch_ids={ids}")


def dq_checks():
    dq = (spark.table(tbl("silver", "dq_report"))
          .where("layer = 'bronze' AND rule = '_TOTAL'").select("batch_id", "entity", "rows_in", "rows_out"))
    n_dq = dq.count()
    unbalanced = dq.where("rows_in != rows_out").count()
    check("D1 bronze dq_report rows for 2 batches x 6 entities", n_dq == 12, n_dq)
    check("D1 bronze rows_in = rows_out", unbalanced == 0, f"unbalanced={unbalanced}")


def malformed_line_checks():
    # B5: the real inputs have no malformed lines and may not be edited, so a fixture is generated.
    fixture = files_path("bronze", "_test_fixtures", "items_with_bad_line.jsonl")
    notebookutils.fs.put(fixture, "\n".join([
        '{"order_id": "F-1", "line_no": 1, "qty": 2, "attributes": {"color": "red"}}',
        '{"order_id": "F-2", "line_no": 1, "qty": 1, this is not json',
        '{"order_id": "F-3", "line_no": 2, "qty": 3, "attributes": null}',
    ]), True)
    fx = read_raw(fixture, "jsonl").cache()   # cache: Spark refuses filters on _corrupt_record alone otherwise
    n_all, n_bad = fx.count(), fx.where(F.col(CORRUPT_COL).isNotNull()).count()
    good_ids = sorted(r[0] for r in fx.where(F.col(CORRUPT_COL).isNull()).select("order_id").collect())
    check("B5 malformed line kept in _corrupt_record, load succeeds",
          n_all == 3 and n_bad == 1 and good_ids == ["F-1", "F-3"],
          f"rows={n_all} corrupt={n_bad} good={good_ids}")
    fx.unpersist()
    notebookutils.fs.rm(files_path("bronze", "_test_fixtures"), True)


section("B5 fixture", malformed_line_checks)     # first: needs no bronze run
section("batch 1", batch_1_checks)
section("B3 re-run", rerun_checks)
section("batch 2", batch_2_checks)
section("DQ report", dq_checks)

# %%
# Save results and fail the run if anything failed
failed = [r["test"] for r in results if not r["passed"]]
notebookutils.fs.put(files_path("silver", "_test_results", "module3.json"),
                     json.dumps({"passed": len(results) - len(failed), "failed": failed, "results": results}, indent=2),
                     True)
print(f"\n{len(results) - len(failed)} passed, {len(failed)} failed")
assert not failed, f"Failed tests: {failed}"
