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


# ---------------------------------------------------------------- Snapshot tables
def sync_snapshot(df, target, key_cols, content_cols, protect=None):
    """Make `target` equal to the snapshot `df`: insert new keys, update rows whose content
    changed, delete keys that are no longer in the snapshot. Unchanged rows are not touched,
    so loading the same snapshot again writes no commit. `protect` is a SQL condition on the
    target for rows that must never be deleted (the -1 unknown customer).
    Returns (rows_inserted, rows_updated, rows_deleted)."""
    df.createOrReplaceTempView("_snapshot")
    on = " AND ".join(f"t.{k} = s.{k}" for k in key_cols)
    s_content = ", ".join(f"s.{c}" for c in content_cols)
    t_content = ", ".join(f"t.{c}" for c in content_cols)
    keep = f"AND NOT ({protect})" if protect else ""
    rows = spark.sql(f"""
        MERGE INTO {target} AS t
        USING _snapshot AS s
        ON {on}
        WHEN MATCHED AND to_json(struct({s_content})) <> to_json(struct({t_content})) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE {keep} THEN DELETE
    """).collect()
    if rows and "num_inserted_rows" in rows[0].asDict():
        r = rows[0]
        return int(r["num_inserted_rows"]), int(r["num_updated_rows"]), int(r["num_deleted_rows"])
    m = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").first()["operationMetrics"]
    return (int(m.get("numTargetRowsInserted", 0)), int(m.get("numTargetRowsUpdated", 0)),
            int(m.get("numTargetRowsDeleted", 0)))


def try_cast(col_name, sql_type):
    """Cast text to sql_type; NULL instead of an error when the text is not a valid value."""
    return F.expr(f"try_cast(trim({col_name}) AS {sql_type})")


LINEAGE_COLS = ["_source_file", "_batch_id", "_ingested_at"]

# products --------------------------------------------------------
PRODUCTS_SOURCE_COLS = ["product_id", "sku", "product_name", "category", "list_price_usd"]
PRODUCTS_CONTENT_COLS = ["sku", "product_name", "category", "list_price_usd"]


def prepare_products(bronze_df):
    df = bronze_df.select(
        "*",
        blank_to_null(F.col("product_id")).alias("_product_id"),
        try_cast("list_price_usd", "DECIMAL(18,4)").alias("_price"),
    )
    reason = (F.when(F.col(CORRUPT_COL).isNotNull(), "MALFORMED_RECORD")
               .when(F.col("_product_id").isNull(), "PRODUCT_ID_MISSING")
               .when(blank_to_null(F.col("list_price_usd")).isNotNull()
                     & (F.col("_price").isNull() | (F.col("_price") < 0)), "PRICE_INVALID"))
    df = df.withColumn("_reason", reason)
    valid = df.where(F.col("_reason").isNull()).select(
        F.col("_product_id").alias("product_id"),
        blank_to_null(F.col("sku")).alias("sku"),
        blank_to_null(F.col("product_name")).alias("product_name"),
        F.coalesce(blank_to_null(F.col("category")), F.lit(UNKNOWN_CATEGORY)).alias("category"),
        F.col("_price").alias("list_price_usd"),
        *LINEAGE_COLS)
    return valid, df.where(F.col("_reason").isNotNull())


# fx_rates --------------------------------------------------------
FX_SOURCE_COLS = ["rate_date", "currency", "rate_to_usd"]
FX_CONTENT_COLS = ["rate_to_usd"]


def prepare_fx(bronze_df):
    df = bronze_df.select(
        "*",
        F.expr("try_to_date(trim(rate_date), 'yyyy-MM-dd')").alias("_date"),
        F.upper(blank_to_null(F.col("currency"))).alias("_currency"),
        try_cast("rate_to_usd", "DECIMAL(18,8)").alias("_rate"),
    )
    reason = (F.when(F.col(CORRUPT_COL).isNotNull(), "MALFORMED_RECORD")
               .when(F.col("_date").isNull(), "RATE_DATE_INVALID")
               .when(~F.col("_currency").isin(CURRENCIES) | F.col("_currency").isNull(), "CURRENCY_INVALID")
               .when(F.col("_rate").isNull() | (F.col("_rate") <= 0), "RATE_INVALID"))
    df = df.withColumn("_reason", reason)
    valid = df.where(F.col("_reason").isNull()).select(
        F.col("_date").alias("rate_date"), F.col("_currency").alias("currency"),
        F.col("_rate").alias("rate_to_usd"), *LINEAGE_COLS)
    return valid, df.where(F.col("_reason").isNotNull())


# payments --------------------------------------------------------
# All structurally valid payments are kept, including the few whose order is unknown;
# gold leaves those out of the mismatch check and counts them (design-note assumption).
PAYMENTS_SOURCE_COLS = ["payment_id", "order_id", "method", "amount", "currency", "status", "paid_at", "gateway_ref"]
PAYMENTS_CONTENT_COLS = ["order_id", "method", "amount", "currency", "status", "paid_at", "gateway_ref"]


def prepare_payments(bronze_df):
    df = bronze_df.select(
        "*",
        blank_to_null(F.col("payment_id")).alias("_payment_id"),
        blank_to_null(F.col("order_id")).alias("_order_id"),
        try_cast("amount", "DECIMAL(18,4)").alias("_amount"),
        F.upper(blank_to_null(F.col("currency"))).alias("_currency"),
        F.lower(blank_to_null(F.col("status"))).alias("_status"),
        parse_ts(F.col("paid_at")).alias("_paid_at"),
    )
    reason = (F.when(F.col(CORRUPT_COL).isNotNull(), "MALFORMED_RECORD")
               .when(F.col("_payment_id").isNull(), "PAYMENT_ID_MISSING")
               .when(F.col("_order_id").isNull(), "ORDER_ID_MISSING")
               .when(F.col("_amount").isNull() | (F.col("_amount") < 0), "AMOUNT_INVALID")
               .when(~F.col("_currency").isin(CURRENCIES) | F.col("_currency").isNull(), "CURRENCY_INVALID")
               .when(~F.col("_status").isin(PAYMENT_STATUSES) | F.col("_status").isNull(), "STATUS_INVALID")
               .when(blank_to_null(F.col("paid_at")).isNotNull() & F.col("_paid_at").isNull(), "PAID_AT_INVALID"))
    df = df.withColumn("_reason", reason)
    valid = df.where(F.col("_reason").isNull()).select(
        F.col("_payment_id").alias("payment_id"), F.col("_order_id").alias("order_id"),
        F.lower(blank_to_null(F.col("method"))).alias("method"),
        F.col("_amount").alias("amount"), F.col("_currency").alias("currency"),
        F.col("_status").alias("status"), F.col("_paid_at").alias("paid_at"),
        blank_to_null(F.col("gateway_ref")).alias("gateway_ref"), *LINEAGE_COLS)
    return valid, df.where(F.col("_reason").isNotNull())


# ---------------------------------------------------------------- Customers (SCD Type 2)
CUSTOMERS_SOURCE_COLS = ["customer_id", "full_name", "email", "tier", "country", "updated_at"]
CUSTOMER_TYPE2_COLS = ["tier", "country"]          # a change here creates a new version
DIM_CUSTOMER_COLS = ["customer_sk", "customer_id", "full_name", "email", "tier", "country",
                     "valid_from", "valid_to", "is_current", "source_updated_at", "_batch_id"]
DIM_CUSTOMER_CONTENT_COLS = ["customer_id", "full_name", "email", "tier", "country",
                             "valid_from", "valid_to", "is_current", "source_updated_at"]


def prepare_customers(bronze_df):
    """CRM rows -> (valid, bad). Business rule 8: a row without updated_at is quarantined."""
    country = F.upper(blank_to_null(F.col("country")))
    df = bronze_df.select(
        "*",
        blank_to_null(F.col("customer_id")).alias("_customer_id"),
        parse_ts(F.col("updated_at")).alias("_updated_at"),
        F.when(country == "UK", "GB").otherwise(country).alias("_country"),   # UK = GB (assumption)
    )
    reason = (F.when(F.col(CORRUPT_COL).isNotNull(), "MALFORMED_RECORD")
               .when(F.col("_customer_id").isNull(), "CUSTOMER_ID_MISSING")
               .when(blank_to_null(F.col("updated_at")).isNull(), "UPDATED_AT_MISSING")
               .when(F.col("_updated_at").isNull(), "UPDATED_AT_INVALID")
               .when(F.col("_country").isNull(), "COUNTRY_MISSING"))
    df = df.withColumn("_reason", reason)
    valid = df.where(F.col("_reason").isNull()).select(
        F.col("_customer_id").alias("customer_id"),
        blank_to_null(F.col("full_name")).alias("full_name"),
        blank_to_null(F.col("email")).alias("email"),
        blank_to_null(F.col("tier")).alias("tier"),
        F.col("_country").alias("country"),
        F.col("_updated_at").alias("updated_at"),
        "_batch_id")
    return valid, df.where(F.col("_reason").isNotNull())


def build_dim_customer(valid):
    """All CRM versions -> SCD Type 2 rows (tests C2, C3, C4).

    - one row per customer and updated_at (exact duplicates and same-time ties collapse)
    - a version starts only when tier or country differs from the previous version;
      name and e-mail are Type 1: every row of a customer carries the latest values
    - the first version is valid from 1900-01-01, later ones from their updated_at;
      valid_to is the next version's valid_from, or 9999-12-31 for the current one
    - customer_sk = hash(customer_id, valid_from): the same version always gets the same key"""
    versions = latest_per_key(valid, ["customer_id", "updated_at"], "updated_at",
                              ["full_name", "email", "tier", "country"])
    by_time = Window.partitionBy("customer_id").orderBy("updated_at")
    changed = F.lit(False)
    for c in CUSTOMER_TYPE2_COLS:
        changed = changed | ~F.col(c).eqNullSafe(F.lag(c).over(by_time))
    first = F.row_number().over(by_time) == 1
    starts = versions.withColumn("_starts", first | changed).where("_starts")

    latest_type1 = (latest_per_key(versions, ["customer_id"], "updated_at", ["full_name", "email"])
                    .select("customer_id", F.col("full_name").alias("_name"), F.col("email").alias("_email")))

    valid_from = F.when(F.row_number().over(by_time) == 1, F.to_timestamp(F.lit(SCD2_START))).otherwise(F.col("updated_at"))
    dim = (starts.withColumn("valid_from", valid_from)
                 .withColumn("valid_to", F.coalesce(F.lead("valid_from").over(by_time),
                                                    F.to_timestamp(F.lit(SCD2_END))))
                 .join(latest_type1, "customer_id"))
    return dim.select(
        F.xxhash64("customer_id", "valid_from").alias("customer_sk"),
        "customer_id",
        F.col("_name").alias("full_name"),
        F.col("_email").alias("email"),
        "tier", "country", "valid_from", "valid_to",
        (F.col("valid_to") == F.to_timestamp(F.lit(SCD2_END))).alias("is_current"),
        F.col("updated_at").alias("source_updated_at"),
        "_batch_id")


# ---------------------------------------------------------------- Order items
ITEMS_SOURCE_COLS = ["order_id", "line_no", "product_id", "qty", "unit_price", "discount_pct", "line_type", "attributes"]
ITEMS_TABLE_COLS = ["order_id", "line_no", "product_id", "product_known", "qty", "unit_price", "discount_pct",
                    "line_type", "net_local", "attributes", "attributes_json", *LINEAGE_COLS]
ITEMS_CONTENT_COLS = ["product_id", "product_known", "qty", "unit_price", "discount_pct",
                      "line_type", "net_local", "attributes_json"]


def prepare_items(bronze_df, orders_df, products_df):
    """One batch of bronze order lines -> (valid, bad).

    - discount NULL -> 0 (business rule 3); outside 0-100 or not a number -> quarantine
    - return lines: qty forced to -|qty|; a sale line must have qty > 0
    - a line whose order is not in silver.orders -> ORDER_NOT_FOUND (business rule 7)
    - a line whose product is unknown still loads, with product_known = false (rule 7)
    - net_local = qty x unit_price x (1 - discount_pct / 100) (business rule 3), DECIMAL"""
    known_orders = orders_df.select(F.col("order_id").alias("_o"), F.lit(True).alias("_order_exists")).distinct()
    known_products = products_df.select(F.col("product_id").alias("_p"), F.lit(True).alias("_product_known")).distinct()
    line_type = F.lower(blank_to_null(F.col("line_type")))
    qty = try_cast("qty", "INT")
    df = (bronze_df
          .select("*",
                  blank_to_null(F.col("order_id")).alias("_order_id"),
                  try_cast("line_no", "INT").alias("_line_no"),
                  blank_to_null(F.col("product_id")).alias("_product_id"),
                  F.when(line_type == "return", -F.abs(qty)).otherwise(qty).alias("_qty"),
                  try_cast("unit_price", "DECIMAL(18,4)").alias("_price"),
                  F.when(blank_to_null(F.col("discount_pct")).isNull(), F.lit(0).cast("DECIMAL(5,2)"))
                   .otherwise(try_cast("discount_pct", "DECIMAL(5,2)")).alias("_discount"),
                  line_type.alias("_line_type"))
          .join(known_orders, F.col("_order_id") == F.col("_o"), "left")
          .join(known_products, F.col("_product_id") == F.col("_p"), "left"))
    reason = (F.when(F.col(CORRUPT_COL).isNotNull(), "MALFORMED_RECORD")
               .when(F.col("_order_id").isNull(), "ORDER_ID_MISSING")
               .when(F.col("_line_no").isNull(), "LINE_NO_INVALID")
               .when(~F.col("_line_type").isin(LINE_TYPES) | F.col("_line_type").isNull(), "LINE_TYPE_INVALID")
               .when(F.col("_qty").isNull() | (F.col("_qty") == 0)
                     | ((F.col("_line_type") == "sale") & (F.col("_qty") < 0)), "QTY_INVALID")
               .when(F.col("_price").isNull() | (F.col("_price") < 0), "UNIT_PRICE_INVALID")
               .when(F.col("_discount").isNull() | (F.col("_discount") < 0) | (F.col("_discount") > 100),
                     "DISCOUNT_INVALID")
               .when(F.col("_order_exists").isNull(), "ORDER_NOT_FOUND"))
    df = df.withColumn("_reason", reason)
    net = (F.col("_qty") * F.col("_price") * (F.lit(1) - F.col("_discount") / F.lit(100))).cast("DECIMAL(18,4)")
    valid = df.where(F.col("_reason").isNull()).select(
        F.col("_order_id").alias("order_id"),
        F.col("_line_no").alias("line_no"),
        F.col("_product_id").alias("product_id"),
        F.coalesce(F.col("_product_known"), F.lit(False)).alias("product_known"),
        F.col("_qty").alias("qty"),
        F.col("_price").alias("unit_price"),
        F.col("_discount").alias("discount_pct"),
        F.col("_line_type").alias("line_type"),
        net.alias("net_local"),
        F.from_json(F.col("attributes"), "map<string,string>").alias("attributes"),
        F.col("attributes").alias("attributes_json"),
        *LINEAGE_COLS)
    return valid, df.where(F.col("_reason").isNotNull())


def merge_items(batch_df, target):
    """MERGE on (order_id, line_no). A line is replaced only by a version from the same or a
    later batch whose content differs (test I7): re-runs change nothing, and an earlier
    batch run late cannot overwrite a correction. Returns (rows_inserted, rows_updated)."""
    batch_df.select(*ITEMS_TABLE_COLS).createOrReplaceTempView("_items_batch")
    s_content = ", ".join(f"s.{c}" for c in ITEMS_CONTENT_COLS)
    t_content = ", ".join(f"t.{c}" for c in ITEMS_CONTENT_COLS)
    result = spark.sql(f"""
        MERGE INTO {target} AS t
        USING _items_batch AS s
        ON t.order_id = s.order_id AND t.line_no = s.line_no
        WHEN MATCHED AND s._batch_id >= t._batch_id
                     AND to_json(struct({s_content})) <> to_json(struct({t_content}))
            THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).collect()
    return merge_metrics(result, target)
