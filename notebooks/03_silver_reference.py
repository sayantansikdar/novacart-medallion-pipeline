# %% [parameters]
# Set by the pipeline; this default is only used when the notebook is run by hand.
batch_id = "1"

# %%
%run 00_config

# %%
%run lib_silver

# %%
# 03_silver_reference: the snapshot entities of one batch -> silver.
#   products, fx_rates, payments: typed, quarantined, deduplicated, then synced to the snapshot
#   dim_customer: SCD Type 2 rebuilt from the full CRM history, then synced
# Runs before 02_silver_orders and 04_silver_items (items need products).
#
# Re-run safety: every table is synced with a MERGE that only touches rows that differ,
# so loading the same snapshot again changes nothing and writes no commit.

BATCH = validate_batch_id(batch_id)


def load_snapshot(entity, prepare, source_cols, key_cols, content_cols, build=None, protect=None, target_name=None):
    """bronze snapshot -> prepare -> quarantine -> dedup (or build) -> sync -> dq_report"""
    target = tbl("silver", target_name or entity)
    bronze = spark.table(tbl("bronze", entity)).where(F.col("_source_file") == source_file(entity, BATCH))
    rows_in = bronze.count()

    valid, bad = prepare(bronze)
    valid = valid.cache()
    dq = DQReport("silver", target_name or entity, BATCH)
    dq.add_quarantined(quarantine(bad, target_name or entity, BATCH, source_cols + [CORRUPT_COL]))

    final = build(valid) if build else latest_per_key(valid, key_cols, "_ingested_at", content_cols)
    final = final.cache()
    rows_valid, rows_out = valid.count(), final.count()

    tag_commits(f"batch_id={BATCH} step=silver.{target_name or entity}")
    inserted, updated, deleted = sync_snapshot(final, target, key_cols, content_cols, protect)
    tag_commits(None)

    dq.write(rows_in=rows_in, rows_out=rows_out, rows_deduped=rows_valid - rows_out,
             rows_inserted=inserted, rows_updated=updated)
    print(f"silver.{target_name or entity} batch {BATCH}: in={rows_in} quarantined={sum(dq.rule_counts.values())} "
          f"out={rows_out} inserted={inserted} updated={updated} deleted={deleted}")
    valid.unpersist(); final.unpersist()


# %%
load_snapshot("products", prepare_products, PRODUCTS_SOURCE_COLS, ["product_id"], PRODUCTS_CONTENT_COLS)
load_snapshot("fx_rates", prepare_fx, FX_SOURCE_COLS, ["rate_date", "currency"], FX_CONTENT_COLS)
load_snapshot("payments", prepare_payments, PAYMENTS_SOURCE_COLS, ["payment_id"], PAYMENTS_CONTENT_COLS)

# dim_customer: rows_out counts SCD2 versions; CRM rows that start no new version
# (exact duplicates, e-mail/name-only changes) are counted as deduped.
load_snapshot("customers_changes", prepare_customers, CUSTOMERS_SOURCE_COLS, ["customer_sk"],
              DIM_CUSTOMER_CONTENT_COLS, build=build_dim_customer,
              protect=f"t.customer_sk = {UNKNOWN_CUSTOMER_SK}", target_name="dim_customer")
