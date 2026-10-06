# %%
# lib_gold: gold transformations as plain functions over DataFrames.
# 05_gold calls them on the silver tables; the tests call them on small generated
# DataFrames. Requires 00_config. Defines functions only; writes nothing by itself.

from pyspark.sql import Window

MONEY = "DECIMAL(18,4)"
RATE = "DECIMAL(18,8)"


def fx_asof(keys, fx):
    """keys: distinct (currency, order_date). Business rule 4: use the rate of the order date;
    if that date has none (weekends, holidays), the most recent earlier rate. USD = 1.
    A currency/date with no earlier rate at all gets fx_rate NULL (fx_missing, test G3)."""
    k, f = keys.alias("k"), fx.alias("f")
    candidates = (k.where(F.col("k.currency") != "USD")
                  .join(f, (F.col("k.currency") == F.col("f.currency"))
                        & (F.col("f.rate_date") <= F.col("k.order_date")), "left")
                  .select(F.col("k.currency").alias("currency"), F.col("k.order_date").alias("order_date"),
                          F.col("f.rate_to_usd").alias("fx_rate"), F.col("f.rate_date").alias("fx_rate_date")))
    latest_first = Window.partitionBy("currency", "order_date").orderBy(F.col("fx_rate_date").desc_nulls_last())
    non_usd = (candidates.withColumn("_rank", F.row_number().over(latest_first))
               .where("_rank = 1").drop("_rank"))
    usd = keys.where(F.col("currency") == "USD").select(
        "currency", "order_date", F.lit(1).cast(RATE).alias("fx_rate"), F.col("order_date").alias("fx_rate_date"))
    return non_usd.unionByName(usd)


def build_fact_order_line(orders, items, products, fx, dim_customer):
    """One row per order line with local and USD amounts (gold.fact_order_line).

    - FX: as-of rate of the UTC order date (fx_asof)
    - customer_sk: the dim_customer version valid at order_ts (point in time, test C6);
      -1 when the customer is not in the CRM (business rule 6)
    - category: UNKNOWN for products not in the catalogue
    - is_revenue: order status paid/shipped/delivered and an FX rate exists (rule 2).
      Return lines are negative and carry the parent order's date, so they reduce
      revenue on the original order's date."""
    lines = items.join(
        orders.select("order_id", "customer_id", "order_ts", "order_date", "status", "currency"), "order_id")
    rates = fx_asof(lines.select("currency", "order_date").distinct(), fx)
    lines = (lines.join(rates, ["currency", "order_date"], "left")
                  .join(products.select("product_id", "category"), "product_id", "left"))

    versions = dim_customer.where(F.col("customer_sk") != UNKNOWN_CUSTOMER_SK).select(
        F.col("customer_id").alias("_cid"), F.col("customer_sk").alias("_sk"), "valid_from", "valid_to")
    lines = lines.join(versions, (F.col("customer_id") == F.col("_cid"))
                       & (F.col("order_ts") >= F.col("valid_from")) & (F.col("order_ts") < F.col("valid_to")), "left")

    fx_missing = F.col("fx_rate").isNull()
    return lines.select(
        "order_id", "line_no", "order_date", "order_ts", "status", "customer_id",
        F.coalesce(F.col("_sk"), F.lit(UNKNOWN_CUSTOMER_SK).cast("bigint")).alias("customer_sk"),
        "product_id", F.coalesce(F.col("category"), F.lit(UNKNOWN_CATEGORY)).alias("category"),
        "product_known", "line_type", "qty", "unit_price", "discount_pct", "currency",
        F.col("net_local").cast(MONEY).alias("net_local"),
        F.col("fx_rate").cast(RATE).alias("fx_rate"), "fx_rate_date",
        (F.col("net_local") * F.col("fx_rate")).cast(MONEY).alias("net_usd"),
        fx_missing.alias("fx_missing"),
        (F.col("status").isin(REVENUE_STATUSES) & ~fx_missing).alias("is_revenue"),
    )


def build_daily_revenue(fact):
    """USD revenue per UTC order date (revenue lines only, returns included as negatives)."""
    return (fact.where("is_revenue").groupBy("order_date")
            .agg(F.sum("net_usd").cast(MONEY).alias("revenue_usd"),
                 F.countDistinct("order_id").alias("orders"),
                 F.count("*").alias("lines"))
            .orderBy("order_date"))


def build_revenue_by_category(fact):
    """USD revenue per product category; unknown products are in UNKNOWN."""
    return (fact.where("is_revenue").groupBy("category")
            .agg(F.sum("net_usd").cast(MONEY).alias("revenue_usd"), F.count("*").alias("lines"))
            .orderBy(F.col("revenue_usd").desc()))


def build_top_customers(fact, dim_customer, n=10):
    """Top n customers by net USD revenue, excluding the unknown customer (-1).
    Ties are broken by customer_id so the list is deterministic. Name, tier and
    country come from the customer's current version."""
    totals = (fact.where(F.col("is_revenue") & (F.col("customer_sk") != UNKNOWN_CUSTOMER_SK))
              .groupBy("customer_id").agg(F.sum("net_usd").cast(MONEY).alias("revenue_usd")))
    ranked = totals.withColumn("rank", F.row_number().over(
        Window.orderBy(F.col("revenue_usd").desc(), F.col("customer_id"))))
    current = dim_customer.where("is_current").select("customer_id", "full_name", "tier", "country")
    return (ranked.where(F.col("rank") <= n).join(current, "customer_id", "left")
            .select("rank", "customer_id", "full_name", "tier", "country", "revenue_usd").orderBy("rank"))


def build_payment_mismatches(fact, orders, payments, fx):
    """Orders whose net payments (success minus refunded; failed ignored) differ from the
    expected amount by strictly more than PAYMENT_MISMATCH_THRESHOLD USD (tests G7-G9).

    Expected amount (design-note assumption): the order's net USD total when its status is
    paid/shipped/delivered, otherwise 0, so a cancelled order must be fully refunded.
    Payments are converted at the as-of rate of the order date. Payments whose order is
    unknown are left out here and counted separately.
    Returns (mismatches_df, orphan_payment_count)."""
    order_net = fact.groupBy("order_id").agg(F.sum("net_usd").alias("order_net_usd"))
    expected = (orders.select("order_id", "order_date", "status", "currency")
                .join(order_net, "order_id", "left")
                .withColumn("expected_usd", F.when(F.col("status").isin(REVENUE_STATUSES),
                                                   F.coalesce("order_net_usd", F.lit(0))).otherwise(F.lit(0)).cast(MONEY)))

    known = payments.join(orders.select("order_id", "order_date"), "order_id", "inner")
    orphan_count = payments.join(orders.select("order_id"), "order_id", "left_anti").count()
    rates = fx_asof(known.select("currency", "order_date").distinct(), fx)
    signed = F.when(F.col("status") == "success", F.col("amount")) \
              .when(F.col("status") == "refunded", -F.col("amount")).otherwise(F.lit(0))
    paid = (known.join(rates, ["currency", "order_date"], "left")
            .withColumn("usd", (signed * F.col("fx_rate")).cast(MONEY))
            .groupBy("order_id")
            .agg(F.sum(F.when(F.col("status") == "success", F.col("usd")).otherwise(0)).cast(MONEY).alias("paid_success_usd"),
                 F.sum(F.when(F.col("status") == "refunded", -F.col("usd")).otherwise(0)).cast(MONEY).alias("refunded_usd"),
                 F.sum("usd").cast(MONEY).alias("net_paid_usd")))

    result = (expected.join(paid, "order_id", "left")
              .fillna(0, subset=["paid_success_usd", "refunded_usd", "net_paid_usd"])
              .withColumn("difference_usd", (F.col("net_paid_usd") - F.col("expected_usd")).cast(MONEY))
              .where(F.abs("difference_usd") > F.lit(PAYMENT_MISMATCH_THRESHOLD))
              .select("order_id", "order_date", "status", "currency", "expected_usd",
                      "paid_success_usd", "refunded_usd", "net_paid_usd", "difference_usd")
              .orderBy(F.abs("difference_usd").desc(), "order_id"))
    return result, orphan_count
