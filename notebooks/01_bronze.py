# %% [parameters]
# Set by the pipeline; this default is only used when the notebook is run by hand.
batch_id = "1"

# %%
%run 00_config

# %%
# 01_bronze: load every raw file for this batch into a bronze table, exactly as received.
#
#  - every source column is STRING; nothing is cast, trimmed or fixed here
#  - lineage columns: _source_file (file name), _batch_id, _ingested_at
#  - malformed records are kept in _corrupt_record (silver quarantines them)
#
# Re-run safety (tests B3, B6): each write replaces only the rows with the same
# _source_file. Re-running a batch replaces that batch's slice; a snapshot file loaded
# again in batch 2 replaces the copy from batch 1, so there is always exactly one copy.
#
# A new column in a later file is added to the bronze table (mergeSchema) rather than
# failing the load; silver decides what to do with it.

BATCH = validate_batch_id(batch_id)        # fails fast on anything but 1 or 2 (test O1)
print(f"bronze load for batch {BATCH}")

# %%
for entity, spec in SOURCES.items():
    file = source_file(entity, BATCH)
    path = f"{INPUT_DIR}/{file}"
    table = tbl("bronze", entity)

    df = read_raw(path, spec["format"]).select(
        "*",
        F.lit(file).alias("_source_file"),
        F.lit(BATCH).cast("int").alias("_batch_id"),
        F.current_timestamp().alias("_ingested_at"),
    )

    tag_commits(f"batch_id={BATCH} step=bronze.{entity}")
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", f"_source_file = '{file}'")
       .option("mergeSchema", "true")
       .saveAsTable(table))

    # rows_in is counted independently from the file; rows_out is what is now in the table.
    rows_in = count_source_records(path, spec["format"])
    rows_out = spark.table(table).where(F.col("_source_file") == file).count()
    DQReport("bronze", entity, BATCH).write(rows_in=rows_in, rows_out=rows_out)
    print(f"  {entity:18} {file:32} in={rows_in:5} out={rows_out:5}")

tag_commits(None)
print("bronze complete")
