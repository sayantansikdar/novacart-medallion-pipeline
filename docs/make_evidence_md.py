"""Write evidence/EVIDENCE.md from the 06_evidence notebook's results (module8.json).

Usage: python3 docs/make_evidence_md.py <module8.json>
"""
import json, os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
d = json.loads(open(sys.argv[1]).read(), strict=False)
ev = d["evidence"]
mark = {True: "PASS", False: "FAIL"}


def table(rows, cols):
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    out += ["| " + " | ".join(str(r.get(c, "")) for c in cols) + " |" for r in rows]
    return "\n".join(out)


h, pit, se, ck, opt = ev["history"], ev["point_in_time"], ev["schema_enforcement"], ev["check_constraint"], ev["optimize"]
vac, cdf, inc = ev["vacuum"], ev["cdf"], ev["incremental_gold"]
md = f"""# NovaCart pipeline: table reliability evidence (Parts 5 and 6)

Produced by the notebook `06_evidence` on the official tables, after a full reset and the pipeline runs
for batch 1, batch 2 and batch 2 again (run logs in `run_logs/`). Every claim below is checked automatically:
**{d['passed']} of {d['passed'] + len(d['failed'])} checks passed.** Screenshots: Fabric portal → NovaCart_HCL →
`06_evidence` → *Recent runs* → open the latest run to see each output cell.

## 5.1 Change history of `silver.orders` (batch 1, batch 2, batch 2 again)

{table(h, ["version", "timestamp", "operation", "userMetadata", "rows_inserted", "rows_updated"])}

- Versions 0–2 are table creation and the two CHECK constraints (setup). **v3 = batch 1** (495 orders inserted),
  **v4 = batch 2** (450 new orders, 137 newer versions applied).
- **Batch 2 run again: no new version.** The MERGE found nothing newer, changed no rows, and Delta writes no
  commit for a MERGE that changes nothing. The re-run itself is visible in the pipeline run log and in
  `silver.dq_report` (0 inserted, 0 updated). This is the idempotency guarantee shown directly.

## 5.2 Point-in-time read: the table right after batch 1

```sql
{pit['query']}
-- or: TIMESTAMP AS OF '{pit['timestamp']}'
```

Returns **{pit['rows_then']} orders** (today: {pit['rows_now']}). Status mix then:
{', '.join(f"{r['status']} {r['count']}" for r in pit['status_then'])}, which is exactly the
plain-Python reference for "after batch 1" (`tests/local/reference_totals.py ... 1`).

## 5.3 Schema enforcement failure

**Attempt:** {se['attempt']}.

**Result:** rejected. `{se['error']}`

**What happened:** Delta stores the table's schema in its transaction log and checks every write against it.
A DataFrame with a column the table does not have is refused as a whole, before any file is committed, so the
table stayed at version {se['version_before']} → {se['version_after']}. Silently adding the column would require an
explicit opt-in (`mergeSchema`), which silver never uses; only bronze opts in, so a new source column is kept
there for review instead of breaking or silently changing silver.

**Also enforced, a CHECK constraint:** {ck['attempt']} → `{ck['error']}` (version still {ck['version_after']}).

## 5.4 Storage optimisation on the fact table

```sql
{opt['command']}
```

| | files | size (bytes) | rows | sum of net_usd |
|---|---|---|---|---|
| before | {opt['before']['numFiles']} | {opt['before']['sizeInBytes']} | {opt['data_before']['rows']} | {opt['data_before']['net_usd']} |
| after | {opt['after']['numFiles']} | {opt['after']['sizeInBytes']} | {opt['data_after']['rows']} | {opt['data_after']['net_usd']} |

OPTIMIZE commits recorded in the table history: {'; '.join(f"v{c['version']}: {c['files_removed']} files → {c['files_added']}" for c in opt.get('optimize_commits', []))}
(the first one compacted the files written by the gold rebuild; a later run had nothing left to compact).

Same data, fewer files: queries open fewer files, and Z-ordering by `order_date` keeps each date's rows
together so date filters can skip files.

**When to partition, cluster or sort.** Partition only large tables, by a low-cardinality column that queries
always filter on (for the fact table at scale: `order_date` or month), aiming for roughly 1 GB per partition.
This data is a few hundred KB, so partitioning would only create many tiny files. For selective lookups on
high-cardinality columns (`customer_id`, `product_id`) use Z-order or clustering instead of partitions. In Fabric,
V-Order (on by default) also sorts and compresses Parquet for faster reads by the SQL endpoint and Power BI.
Run OPTIMIZE after many small MERGE commits, not after every write.

**Why to be careful deleting old files (VACUUM).** VACUUM permanently deletes data files that only old
versions use. Demonstrated on a throwaway copy: version 0 had {vac['version_0_rows_before_vacuum']} rows and was
readable; after `VACUUM ... RETAIN 0 HOURS`, reading version 0 failed with
`{vac['after_vacuum'][:200]}`. On the real tables that would destroy the point-in-time evidence above and the Change
Data Feed history. Keep the default 7-day retention or longer (as long as audits or re-processing need to look
back), never RETAIN 0 in production, and remember VACUUM is irreversible.

## 6.1 Row-level changes in batch 2 (Change Data Feed)

Change Data Feed is enabled on `silver.orders`, so every commit also records row-level changes.
For the batch 2 commit (version {cdf['version']}): {', '.join(f"{k} {v}" for k, v in cdf['by_change_type'].items())}.

Status changes made by batch 2:

{table(cdf['status_transitions'], ["status_before", "status_after", "count"])}

## 6.2 Incremental gold instead of a full rebuild

Gold is rebuilt in full on every run (simple, deterministic, cheap at this size). The incremental alternative was
tested: start from the fact table as it was after batch 1 (time travel), find the orders batch 2 touched
(Change Data Feed on `silver.orders` plus the `_batch_id` lineage of `silver.order_items`), rebuild only
those orders' lines and replace them.

- Orders affected: **{inc['affected_orders']}**; lines rebuilt: **{inc['lines_rebuilt']} of {inc['lines_total']}**.
- Differences from the full rebuild: **{inc['differences_vs_full_rebuild']}**.
- Valid only while the reference tables (products, FX, customers, payments) did not change in the batch
  (changed: {inc['reference_tables_changed_in_batch_2'] or 'none'}). If they change, every order can be affected and a full
  rebuild is the safe choice. For production, enabling Change Data Feed on `order_items` too would replace the lineage shortcut.

## 6.3 Ingesting new files automatically

1. **Event trigger (chosen design).** A Fabric storage-event trigger (Activator on OneLake/Azure Blob
   "file created" events) watches a landing folder such as `landing/batch=N/`. When the files land, it starts
   `novacart_medallion` with `batch_id` taken from the folder name. No idle runs, and data is processed minutes
   after it arrives. A daily schedule stays as a fallback in case an event is missed; re-running is harmless
   because every step is idempotent.
2. **Structured Streaming with a checkpoint** (Fabric's equivalent of Databricks Auto Loader):
   `spark.readStream` on the landing folder with `trigger(availableNow=True)` and a checkpoint location.
   Each run processes only files it has not seen before, exactly once, then stops. This suits a steady
   stream of many files.

## Trigger design: fixed schedule vs windowed schedule vs file-arrival event

| Option | How it works | Fit for NovaCart |
|---|---|---|
| Fixed schedule | Runs at set times (e.g. daily 02:00) | Simple, but batch 2's arrival time is unknown: runs either too early (nothing to load) or late |
| Windowed schedule | Runs per time window, with catch-up/backfill of missed windows | Good for regular time-sliced data; still polls, and NovaCart batches are not regular windows |
| **File-arrival event** | Starts when a file lands | **Chosen**: runs exactly when a batch arrives, never idle; daily schedule kept as a safety net |

## All checks

{chr(10).join(f"- **{mark[r['passed']]}** {r['test']} — {r['detail'][:220]}" for r in d['results'])}
"""
os.makedirs(os.path.join(ROOT, "evidence"), exist_ok=True)
open(os.path.join(ROOT, "evidence", "EVIDENCE.md"), "w").write(md)
print("written evidence/EVIDENCE.md")
