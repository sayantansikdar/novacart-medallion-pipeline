# %% [parameters]
# Set by the pipeline; this default is only used when the notebook is run by hand.
batch_id = "1"

# %%
%run 00_config

# %%
%run lib_silver

# %%
# 04_silver_items: bronze order lines of one batch -> silver.order_items.
# Runs after 03_silver_reference (needs products) and 02_silver_orders (needs this batch's orders),
# so ORDER_NOT_FOUND means "not in any batch loaded so far".
#
# Re-run safety: merge_items only replaces a line with a different version from the same or a
# later batch; quarantine and dq_report replace their own (entity, batch) rows.

BATCH = validate_batch_id(batch_id)
TARGET = tbl("silver", "order_items")

bronze = (spark.table(tbl("bronze", "order_items"))
          .where(F.col("_source_file") == source_file("order_items", BATCH)))
rows_in = bronze.count()

valid, bad = prepare_items(bronze, spark.table(tbl("silver", "orders")), spark.table(tbl("silver", "products")))
valid = valid.cache()

dq = DQReport("silver", "order_items", BATCH)
dq.add_quarantined(quarantine(bad, "order_items", BATCH, ITEMS_SOURCE_COLS + [CORRUPT_COL]))

# Lines have no updated_at: within a batch, exact duplicates collapse and differing copies of
# the same line are decided by content, so the result does not depend on row order.
latest = latest_per_key(valid, ["order_id", "line_no"], "_ingested_at", ITEMS_CONTENT_COLS).cache()
rows_valid, rows_out = valid.count(), latest.count()

tag_commits(f"batch_id={BATCH} step=silver.order_items")
inserted, updated = merge_items(latest, TARGET)
tag_commits(None)

dq.write(rows_in=rows_in, rows_out=rows_out, rows_deduped=rows_valid - rows_out,
         rows_inserted=inserted, rows_updated=updated)
print(f"silver.order_items batch {BATCH}: in={rows_in} quarantined={sum(dq.rule_counts.values())} "
      f"deduped={rows_valid - rows_out} out={rows_out} inserted={inserted} updated={updated}")
valid.unpersist(); latest.unpersist()
