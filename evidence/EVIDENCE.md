# NovaCart pipeline: table reliability evidence (Parts 5 and 6)

Produced by the notebook `06_evidence` on the official tables, after a full reset and the pipeline runs
for batch 1, batch 2 and batch 2 again (run logs in `run_logs/`). Every claim below is checked automatically:
**11 of 11 checks passed.** Screenshots: Fabric portal → NovaCart_HCL →
`06_evidence` → *Recent runs* → open the latest run to see each output cell.

## 5.1 Change history of `silver.orders` (batch 1, batch 2, batch 2 again)

| version | timestamp | operation | userMetadata | rows_inserted | rows_updated |
|---|---|---|---|---|---|
| 0 | 2026-10-06 11:42:01.606000 | CREATE TABLE | step=setup | None | None |
| 1 | 2026-10-06 11:42:11.711000 | ADD CONSTRAINT | step=setup | None | None |
| 2 | 2026-10-06 11:42:16.450000 | ADD CONSTRAINT | step=setup | None | None |
| 3 | 2026-10-06 11:50:07.206000 | MERGE | batch_id=1 step=silver.orders | 495 | 0 |
| 4 | 2026-10-06 12:04:57.919000 | MERGE | batch_id=2 step=silver.orders | 450 | 137 |

- Versions 0–2 are table creation and the two CHECK constraints (setup). **v3 = batch 1** (495 orders inserted),
  **v4 = batch 2** (450 new orders, 137 newer versions applied).
- **Batch 2 run again: no new version.** The MERGE found nothing newer, changed no rows, and Delta writes no
  commit for a MERGE that changes nothing. The re-run itself is visible in the pipeline run log and in
  `silver.dq_report` (0 inserted, 0 updated). This is the idempotency guarantee shown directly.

## 5.2 Point-in-time read: the table right after batch 1

```sql
SELECT * FROM LH_NovaCart_Silver.dbo.orders VERSION AS OF 3
-- or: TIMESTAMP AS OF '2026-10-06 11:50:07.206000'
```

Returns **495 orders** (today: 945). Status mix then:
cancelled 59, delivered 274, paid 45, placed 51, shipped 66, which is exactly the
plain-Python reference for "after batch 1" (`tests/local/reference_totals.py ... 1`).

## 5.3 Schema enforcement failure

**Attempt:** append 1 row of silver.orders with an extra column loyalty_points.

**Result:** rejected. `[_LEGACY_ERROR_TEMP_DELTA_0007] A schema mismatch detected when writing to the Delta table (Table ID: 68dbd529-1650-4581-9274-b45dc507c19f).`

**What happened:** Delta stores the table's schema in its transaction log and checks every write against it.
A DataFrame with a column the table does not have is refused as a whole, before any file is committed, so the
table stayed at version 4 → 4. Silently adding the column would require an
explicit opt-in (`mergeSchema`), which silver never uses; only bronze opts in, so a new source column is kept
there for review instead of breaking or silently changing silver.

**Also enforced, a CHECK constraint:** insert an order with status 'refunded' → `: org.apache.spark.sql.delta.schema.DeltaInvariantViolationException: [DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint orders_status_valid (status IN ('placed', 'paid', 'shipped', 'delivered', 'cancelled')) violated by row with values:` (version still 4).

## 5.4 Storage optimisation on the fact table

```sql
OPTIMIZE LH_NovaCart_Gold.dbo.fact_order_line ZORDER BY (order_date)
```

| | files | size (bytes) | rows | sum of net_usd |
|---|---|---|---|---|
| before (this run) | 1 | 83705 | 1933 | 351746.0626 |
| after (this run) | 1 | 83705 | 1933 | 351746.0626 |

OPTIMIZE commits recorded in the table history: v3: 13 files → 1; v4: 1 files → 1; v5: 1 files → 1; v6: 1 files → 1
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
versions use. Demonstrated on a throwaway copy: version 0 had 30 rows and was
readable; after `VACUUM ... RETAIN 0 HOURS`, reading version 0 failed with
`An error occurred while calling o9766.collectToPython.`. On the real tables that would destroy the point-in-time evidence above and the Change
Data Feed history. Keep the default 7-day retention or longer (as long as audits or re-processing need to look
back), never RETAIN 0 in production, and remember VACUUM is irreversible.

## 6.1 Row-level changes in batch 2 (Change Data Feed)

Change Data Feed is enabled on `silver.orders`, so every commit also records row-level changes.
For the batch 2 commit (version 4): insert 450, update_postimage 137, update_preimage 137.

Status changes made by batch 2:

| status_before | status_after | count |
|---|---|---|
| shipped | delivered | 66 |
| paid | delivered | 43 |
| placed | delivered | 26 |
| paid | cancelled | 2 |

## 6.2 Incremental gold instead of a full rebuild

Gold is rebuilt in full on every run (simple, deterministic, cheap at this size). The incremental alternative was
tested: start from the fact table as it was after batch 1 (time travel), find the orders batch 2 touched
(Change Data Feed on `silver.orders` plus the `_batch_id` lineage of `silver.order_items`), rebuild only
those orders' lines and replace them.

- Orders affected: **608**; lines rebuilt: **1266 of 1933**.
- Differences from the full rebuild: **0**.
- Valid only while the reference tables (products, FX, customers, payments) did not change in the batch
  (changed: none). If they change, every order can be affected and a full
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

- **PASS** R1 one MERGE commit per batch, tagged in userMetadata — [(3, 'batch_id=1 step=silver.orders', '495', '0'), (4, 'batch_id=2 step=silver.orders', '450', '137')]
- **PASS** R1 the batch 2 re-run made no commit (no rows changed, so no new version) — latest version = 4
- **PASS** R2 VERSION AS OF 3 returns the post-batch-1 table (495 orders) — 495
- **PASS** R2 TIMESTAMP AS OF the batch-1 commit returns the same rows — 2026-10-06 11:50:07.206000
- **PASS** R3 append with an extra column is rejected; table version unchanged — v4 -> v4: [_LEGACY_ERROR_TEMP_DELTA_0007] A schema mismatch detected when writing to the Delta table (Table ID: 68dbd529-1650-4581-9274-b45dc507c19f).
- **PASS** R4 status 'refunded' violates the CHECK constraint; version unchanged — : org.apache.spark.sql.delta.schema.DeltaInvariantViolationException: [DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint orders_status_valid (status IN ('p
- **PASS** R5 OPTIMIZE compacted the fact table's files; data identical — OPTIMIZE v3: 13 files -> 1; now 1 file(s), rows 1933, net_usd 351746.0626
- **PASS** VACUUM removes the files of old versions: time travel to them fails afterwards — An error occurred while calling o9766.collectToPython.
- **PASS** CDF: batch 2 = 450 inserted orders and 137 updated orders — {'insert': 450, 'update_postimage': 137, 'update_preimage': 137}
- **PASS** reference tables unchanged in batch 2, so an incremental update is valid — none
- **PASS** incremental gold (only changed orders rebuilt) = full rebuild — 608 orders / 1266 lines rebuilt, 0 differences
