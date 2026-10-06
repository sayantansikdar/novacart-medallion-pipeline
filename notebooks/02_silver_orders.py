# %% [parameters]
# Set by the pipeline; this default is only used when the notebook is run by hand.
batch_id = "1"

# %%
%run 00_config

# %%
%run lib_silver

# %%
# 02_silver_orders: bronze orders of one batch -> silver.orders (one current row per order).
#
#  1. parse order_ts / updated_at to UTC, order_date = UTC date of order_ts
#  2. normalise status and currency (derive currency from shipping_country when blank)
#  3. quarantine rows that break a rule, with the reason and the original record
#  4. keep the latest updated_at per order within the batch
#  5. MERGE: insert new orders; replace a row only with a newer version (late data is ignored)
#  6. record counts in silver.dq_report
#
# Re-run safety: the MERGE guard makes a second run of the same batch change nothing;
# quarantine and dq_report replace their own (entity, batch) rows.

BATCH = validate_batch_id(batch_id)
TARGET = tbl("silver", "orders")

bronze = (spark.table(tbl("bronze", "orders"))
          .where(F.col("_source_file") == source_file("orders", BATCH)))
rows_in = bronze.count()

valid, bad = prepare_orders(bronze)
valid = valid.cache()

dq = DQReport("silver", "orders", BATCH)
dq.add_quarantined(quarantine(bad, "orders", BATCH, ORDERS_SOURCE_COLS + [CORRUPT_COL]))

latest = latest_per_key(valid, ["order_id"], "updated_at", ORDERS_CONTENT_COLS).cache()
rows_valid, rows_out = valid.count(), latest.count()

tag_commits(f"batch_id={BATCH} step=silver.orders")
inserted, updated = merge_orders(latest, TARGET)
tag_commits(None)

dq.write(rows_in=rows_in, rows_out=rows_out, rows_deduped=rows_valid - rows_out,
         rows_inserted=inserted, rows_updated=updated)
print(f"silver.orders batch {BATCH}: in={rows_in} quarantined={sum(dq.rule_counts.values())} "
      f"deduped={rows_valid - rows_out} out={rows_out} inserted={inserted} updated={updated}")
valid.unpersist(); latest.unpersist()
