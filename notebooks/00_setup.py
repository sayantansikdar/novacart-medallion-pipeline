# %% [parameters]
# Set by the pipeline; this default is only used when the notebook is run by hand.
batch_id = "1"

# %%
# 00_setup: creates every silver table with its constraints. First task of every pipeline run.
# Idempotent: CREATE TABLE IF NOT EXISTS, constraints added only when missing, and the
# unknown-customer row inserted only when missing. A second run makes no Delta commit at all.

# %%
%run 00_config

# %%
# Fail the whole run at its first task when batch_id is invalid (test O1), before anything is written.
validate_batch_id(batch_id)

# %%
# ---------------------------------------------------------------- Table definitions
# Money is DECIMAL (never float), timestamps are UTC, and every table keeps the
# bronze lineage columns so any row can be traced back to its file and batch.
LINEAGE = "_source_file STRING NOT NULL, _batch_id INT NOT NULL, _ingested_at TIMESTAMP NOT NULL"

DDL = {
    # One current row per order (merge key order_id). Change Data Feed records the
    # row-level changes each batch makes (Part 6 extra).
    "orders": f"""
        order_id STRING NOT NULL,
        customer_id STRING NOT NULL,
        order_ts TIMESTAMP NOT NULL,
        order_date DATE NOT NULL,
        status STRING NOT NULL,
        currency STRING NOT NULL,
        currency_derived BOOLEAN NOT NULL,
        shipping_country STRING,
        promo_code STRING,
        updated_at TIMESTAMP NOT NULL,
        {LINEAGE}""",
    # One row per order line (merge key order_id + line_no). Returns have negative qty.
    "order_items": f"""
        order_id STRING NOT NULL,
        line_no INT NOT NULL,
        product_id STRING,
        product_known BOOLEAN NOT NULL,
        qty INT NOT NULL,
        unit_price DECIMAL(18,4) NOT NULL,
        discount_pct DECIMAL(5,2) NOT NULL,
        line_type STRING NOT NULL,
        net_local DECIMAL(18,4) NOT NULL,
        attributes MAP<STRING, STRING>,
        attributes_json STRING,
        {LINEAGE}""",
    # SCD Type 2: one row per customer version; valid_to of the current row is 9999-12-31.
    "dim_customer": """
        customer_sk BIGINT NOT NULL,
        customer_id STRING NOT NULL,
        full_name STRING,
        email STRING,
        tier STRING,
        country STRING NOT NULL,
        valid_from TIMESTAMP NOT NULL,
        valid_to TIMESTAMP NOT NULL,
        is_current BOOLEAN NOT NULL,
        source_updated_at TIMESTAMP,
        _batch_id INT""",
    "products": f"""
        product_id STRING NOT NULL,
        sku STRING,
        product_name STRING,
        category STRING NOT NULL,
        list_price_usd DECIMAL(18,4),
        {LINEAGE}""",
    "fx_rates": f"""
        rate_date DATE NOT NULL,
        currency STRING NOT NULL,
        rate_to_usd DECIMAL(18,8) NOT NULL,
        {LINEAGE}""",
    "payments": f"""
        payment_id STRING NOT NULL,
        order_id STRING NOT NULL,
        method STRING,
        amount DECIMAL(18,4) NOT NULL,
        currency STRING NOT NULL,
        status STRING NOT NULL,
        paid_at TIMESTAMP,
        gateway_ref STRING,
        {LINEAGE}""",
    # Rejected rows: entity, reason and the record exactly as received.
    "quarantine": """
        entity STRING NOT NULL,
        reason STRING NOT NULL,
        batch_id INT NOT NULL,
        source_file STRING,
        original_record STRING NOT NULL,
        quarantined_at TIMESTAMP NOT NULL""",
    # Row counts per entity and rule for every batch (see DQReport in 00_config).
    "dq_report": """
        batch_id INT NOT NULL,
        layer STRING NOT NULL,
        entity STRING NOT NULL,
        rule STRING NOT NULL,
        rows_in BIGINT,
        rows_out BIGINT,
        rows_quarantined BIGINT,
        rows_deduped BIGINT,
        rows_inserted BIGINT,
        rows_updated BIGINT,
        recorded_at TIMESTAMP NOT NULL""",
}

TABLE_PROPERTIES = {"orders": "TBLPROPERTIES (delta.enableChangeDataFeed = true)"}


def in_list(values):
    return ", ".join(f"'{v}'" for v in values)


# CHECK constraints: Delta rejects the whole write if any row breaks one (test R4).
CHECKS = {
    "orders": {
        "orders_status_valid": f"status IN ({in_list(ORDER_STATUSES)})",
        "orders_currency_valid": f"currency IN ({in_list(CURRENCIES)})",
    },
    "order_items": {
        "items_line_type_valid": f"line_type IN ({in_list(LINE_TYPES)})",
        "items_qty_sign": "(line_type = 'sale' AND qty > 0) OR (line_type = 'return' AND qty < 0)",
        "items_discount_range": "discount_pct BETWEEN 0 AND 100",
        "items_price_not_negative": "unit_price >= 0",
    },
    "dim_customer": {
        "customer_valid_range": "valid_from < valid_to",
    },
    "fx_rates": {
        "fx_rate_positive": "rate_to_usd > 0",
        "fx_currency_valid": f"currency IN ({in_list(CURRENCIES)})",
    },
    "payments": {
        "payments_status_valid": f"status IN ({in_list(PAYMENT_STATUSES)})",
    },
}


def ensure_constraint(table, name, expression):
    """ADD CONSTRAINT fails if it already exists, so check the table properties first."""
    props = {r["key"] for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    if f"delta.constraints.{name.lower()}" not in props:
        spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({expression})")
        print(f"  added constraint {name}")


# %%
tag_commits("step=setup")
for name, columns in DDL.items():
    table = tbl("silver", name)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({columns}) USING DELTA {TABLE_PROPERTIES.get(name, '')}")
    for constraint, expression in CHECKS.get(name, {}).items():
        ensure_constraint(table, constraint, expression)
    print(f"ready  {table}")

# %%
# Business rule 6: orders whose customer is not in the CRM point at this member.
dim = tbl("silver", "dim_customer")
if spark.table(dim).where(F.col("customer_sk") == UNKNOWN_CUSTOMER_SK).isEmpty():
    spark.sql(f"""
        INSERT INTO {dim} (customer_sk, customer_id, full_name, email, tier, country,
                           valid_from, valid_to, is_current, source_updated_at, _batch_id)
        VALUES ({UNKNOWN_CUSTOMER_SK}, 'UNKNOWN', 'Unknown customer', NULL, NULL, 'UNKNOWN',
                TIMESTAMP'{SCD2_START}', TIMESTAMP'{SCD2_END}', true, NULL, NULL)""")
    print("inserted unknown customer row (customer_sk = -1)")

# Folder for the per-batch DQ report export (gold Files area).
notebookutils.fs.mkdirs(DQ_EXPORT_DIR)
tag_commits(None)
print("00_setup complete")
