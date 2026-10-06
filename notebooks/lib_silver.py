# %%
# lib_silver: silver transformations as plain functions.
# The silver notebooks call them on real bronze data; the tests call them on small
# generated data and throwaway tables, so the logic is tested without touching real tables.
# Requires 00_config to have been run first. Defines functions only; writes nothing by itself.

from pyspark.sql import Window

# ---------------------------------------------------------------- Shared
def content_key(*cols):
    """Text form of a row's business columns, used to tell whether two versions differ
    and to break ties deterministically. to_json keeps NULLs apart from values, unlike a hash."""
    return F.to_json(F.struct(*cols))


def latest_per_key(df, key_cols, version_col, content_cols):
    """Keep one row per key: the latest version_col. Exact duplicates collapse to one row,
    and two different rows with the same version_col are decided by content_key, so the
    result never depends on the order rows arrive in (tests S6, S7)."""
    w = Window.partitionBy(*key_cols).orderBy(F.col(version_col).desc(), content_key(*content_cols).desc())
    return df.withColumn("_rank", F.row_number().over(w)).where("_rank = 1").drop("_rank")


def merge_metrics(merge_result_rows, table):
    """Inserted/updated counts of the MERGE just run. Delta returns them as the MERGE result;
    if this runtime does not, read them from the latest commit in the table history."""
    if merge_result_rows and "num_inserted_rows" in merge_result_rows[0].asDict():
        r = merge_result_rows[0]
        return int(r["num_inserted_rows"]), int(r["num_updated_rows"])
    m = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["operationMetrics"]
    return int(m.get("numTargetRowsInserted", 0)), int(m.get("numTargetRowsUpdated", 0))


# ---------------------------------------------------------------- Orders
ORDERS_SOURCE_COLS = ["order_id", "customer_id", "order_ts", "status", "currency",
                      "shipping_country", "updated_at", "promo_code"]
ORDERS_TABLE_COLS = ["order_id", "customer_id", "order_ts", "order_date", "status", "currency",
                     "currency_derived", "shipping_country", "promo_code", "updated_at",
                     "_source_file", "_batch_id", "_ingested_at"]
# Columns that make two versions of an order different (lineage excluded)
ORDERS_CONTENT_COLS = ["customer_id", "order_ts", "status", "currency", "currency_derived",
                       "shipping_country", "promo_code"]


def prepare_orders(bronze_df):
    """One batch of bronze orders -> (valid_df, bad_df).

    valid_df has the silver.orders columns, typed and cleaned.
    bad_df keeps the bronze columns as received plus `_reason`; the first rule a row
    breaks is its reason."""
    currency, currency_derived = normalise_currency(F.col("currency"), F.col("shipping_country"))
    df = bronze_df.select(
        "*",
        blank_to_null(F.col("order_id")).alias("_order_id"),
        blank_to_null(F.col("customer_id")).alias("_customer_id"),
        parse_ts(F.col("order_ts")).alias("_order_ts"),
        parse_ts(F.col("updated_at")).alias("_updated_at"),
        F.lower(blank_to_null(F.col("status"))).alias("_status"),
        currency.alias("_currency"),
        currency_derived.alias("_currency_derived"),
    )
    reason = (F.when(F.col(CORRUPT_COL).isNotNull(), "MALFORMED_RECORD")
               .when(F.col("_order_id").isNull(), "ORDER_ID_MISSING")
               .when(F.col("_customer_id").isNull(), "CUSTOMER_ID_MISSING")
               .when(F.col("_order_ts").isNull(), "ORDER_TS_INVALID")
               .when(F.col("_updated_at").isNull(), "UPDATED_AT_INVALID")
               .when(~F.col("_status").isin(ORDER_STATUSES) | F.col("_status").isNull(), "STATUS_INVALID")
               .when(F.col("_currency").isNull(), "CURRENCY_UNRESOLVED")
               .when(~F.col("_currency").isin(CURRENCIES), "CURRENCY_INVALID"))
    df = df.withColumn("_reason", reason)

    valid = df.where(F.col("_reason").isNull()).select(
        F.col("_order_id").alias("order_id"),
        F.col("_customer_id").alias("customer_id"),
        F.col("_order_ts").alias("order_ts"),
        F.to_date("_order_ts").alias("order_date"),          # UTC date (session time zone is UTC)
        F.col("_status").alias("status"),
        F.col("_currency").alias("currency"),
        F.col("_currency_derived").alias("currency_derived"),
        F.upper(blank_to_null(F.col("shipping_country"))).alias("shipping_country"),
        blank_to_null(F.col("promo_code")).alias("promo_code"),
        F.col("_updated_at").alias("updated_at"),
        "_source_file", "_batch_id", "_ingested_at",
    )
    bad = df.where(F.col("_reason").isNotNull())
    return valid, bad


def merge_orders(batch_df, target):
    """MERGE one deduplicated batch into the orders table (test S8, S9, S10, O4).

    A matched row is replaced only when the incoming version is newer, or has the same
    updated_at, comes from a later batch and actually differs. An older (late) copy can
    therefore never overwrite a newer one, and re-running a batch changes nothing.
    Returns (rows_inserted, rows_updated)."""
    batch_df.select(*ORDERS_TABLE_COLS).createOrReplaceTempView("_orders_batch")
    s_content = ", ".join(f"s.{c}" for c in ORDERS_CONTENT_COLS)
    t_content = ", ".join(f"t.{c}" for c in ORDERS_CONTENT_COLS)
    result = spark.sql(f"""
        MERGE INTO {target} AS t
        USING _orders_batch AS s
        ON t.order_id = s.order_id
        WHEN MATCHED AND (
                s.updated_at > t.updated_at
             OR (s.updated_at = t.updated_at
                 AND s._batch_id > t._batch_id
                 AND to_json(struct({s_content})) <> to_json(struct({t_content})))
        ) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).collect()
    return merge_metrics(result, target)
