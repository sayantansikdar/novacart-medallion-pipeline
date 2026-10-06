# %%
# 00_config: the single place for names, paths, business constants and shared helpers.
# Every other notebook starts with a cell containing only:  %run 00_config
# This notebook writes nothing by itself; the helpers write only when another notebook calls them.

from pyspark.sql import DataFrame, functions as F, types as T

# ---------------------------------------------------------------- Spark session
# Business rule 4: dates are taken in UTC, so the whole session works in UTC.
spark.conf.set("spark.sql.session.timeZone", "UTC")
# A string that does not match one timestamp pattern must give NULL (not an error),
# so parse_ts() can fall through to the next pattern.
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# ---------------------------------------------------------------- Fabric names
WORKSPACE = "NovaCart_HCL"
LAKEHOUSE = {"bronze": "LH_NovaCart_Bronze", "silver": "LH_NovaCart_Silver", "gold": "LH_NovaCart_Gold"}
SCHEMA = "dbo"


def tbl(layer, name):
    """Fully qualified table name: tbl('silver', 'orders') -> LH_NovaCart_Silver.dbo.orders"""
    return f"{LAKEHOUSE[layer]}.{SCHEMA}.{name}"


def files_path(layer, *parts):
    """OneLake path inside a lakehouse's Files area."""
    root = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE[layer]}.Lakehouse/Files"
    return "/".join([root, *parts])


# ---------------------------------------------------------------- Raw input files
# Uploaded once and never edited (lab ground rule). Three files have different names
# from the lab brief; they are mapped here rather than renamed.
INPUT_DIR = files_path("bronze", "NovaCart_SourceData", "NovaCart_SourceData")
DQ_EXPORT_DIR = files_path("gold", "exports", "dq")

# Incremental entities have one file per batch. Snapshot entities are full extracts:
# the same file is (re)loaded in every batch and replaces the previous copy.
SOURCES = {
    "orders":            {"format": "csv",        "kind": "incremental",
                          "files": {1: "orders_batch_1.csv", 2: "orders_batch_2.csv"}},
    "order_items":       {"format": "jsonl",      "kind": "incremental",
                          "files": {1: "order_items_batch_1_json.txt", 2: "order_items_batch_2_jsonl.txt"}},
    "customers_changes": {"format": "csv",        "kind": "snapshot", "file": "customers_changes.csv"},
    "products":          {"format": "csv",        "kind": "snapshot", "file": "products.csv"},
    "fx_rates":          {"format": "csv",        "kind": "snapshot", "file": "fx_rates.csv"},
    "payments":          {"format": "json_array", "kind": "snapshot", "file": "payments_json.txt"},
}

VALID_BATCHES = (1, 2)


def source_file(entity, batch_id):
    """File name an entity reads in a given batch."""
    s = SOURCES[entity]
    return s["files"][batch_id] if s["kind"] == "incremental" else s["file"]


CORRUPT_COL = "_corrupt_record"


def read_raw(path, fmt):
    """Read one raw file exactly as received: every column is STRING and a malformed
    record is kept in _corrupt_record instead of failing the load (test B5).

    Two passes: the first only discovers the column names (so a new column in a later
    file is picked up, not dropped), the second reads with an all-STRING schema.
    A nested JSON object (order item `attributes`) read as STRING keeps its raw JSON text."""
    if fmt == "csv":
        reader = spark.read.format("csv").option("header", True)
    else:
        reader = spark.read.format("json").option("primitivesAsString", True)
        if fmt == "json_array":
            reader = reader.option("multiLine", True)      # one JSON array spread over many lines
    columns = reader.load(path).columns                     # pass 1: column names only
    schema = T.StructType([T.StructField(c, T.StringType()) for c in columns if c != CORRUPT_COL]
                          + [T.StructField(CORRUPT_COL, T.StringType())])
    return (reader.schema(schema)                           # pass 2: everything as STRING
                  .option("mode", "PERMISSIVE")
                  .option("columnNameOfCorruptRecord", CORRUPT_COL)
                  .load(path))


def count_source_records(path, fmt):
    """Independent record count for a raw file, used to prove bronze loaded every record (B1).
    CSV: non-blank lines minus the header. JSON Lines: non-blank lines.
    JSON array: number of elements, counted by Python's json module, not Spark's JSON reader.
    (notebookutils.fs.head is not used: it returns at most ~100 KB, so a larger file is cut off.)"""
    if fmt == "json_array":
        import json
        whole_file = spark.read.text(path, wholetext=True).first()[0]
        return len(json.loads(whole_file))
    lines = spark.read.text(path).where(F.trim("value") != "").count()
    return lines - 1 if fmt == "csv" else lines


def validate_batch_id(value):
    """The pipeline passes batch_id as text. Anything other than 1 or 2 stops the run
    at the first step (test O1) instead of loading a wrong or empty batch."""
    try:
        batch = int(str(value).strip())
    except ValueError:
        raise ValueError(f"batch_id must be one of {VALID_BATCHES}, got {value!r}")
    if batch not in VALID_BATCHES:
        raise ValueError(f"batch_id must be one of {VALID_BATCHES}, got {value!r}")
    return batch


# ---------------------------------------------------------------- Business constants
ORDER_STATUSES = ["placed", "paid", "shipped", "delivered", "cancelled"]
REVENUE_STATUSES = ["paid", "shipped", "delivered"]          # business rule 2
LINE_TYPES = ["sale", "return"]
PAYMENT_STATUSES = ["success", "failed", "refunded"]

# Business rule 5. "UK" is not an ISO code, but is accepted as GB (assumption in the design note).
COUNTRY_CURRENCY = {"IN": "INR", "US": "USD", "GB": "GBP", "UK": "GBP", "DE": "EUR", "SG": "SGD"}
CURRENCIES = sorted(set(COUNTRY_CURRENCY.values()))

UNKNOWN_CUSTOMER_SK = -1                                     # business rule 6
UNKNOWN_CATEGORY = "UNKNOWN"
PAYMENT_MISMATCH_THRESHOLD = 0.50                            # flagged only when strictly greater
SCD2_START = "1900-01-01 00:00:00"                           # first version of every customer
SCD2_END = "9999-12-31 23:59:59"                             # valid_to of the current version

# ---------------------------------------------------------------- Parsing helpers
# order_ts arrives in three formats (lab data dictionary):
#   2026-09-01T13:48:48+05:30   ISO with offset  -> converted to UTC
#   2026-09-01T08:18:48Z        ISO in UTC       ("XXX" accepts both "Z" and "+05:30")
#   01/09/2026 08:18            day/month/year, no offset, stated to be UTC
# The day-first pattern is explicit: 435 rows have a day <= 12 and would silently get
# the wrong date if read month-first.
TS_PATTERNS = ["yyyy-MM-dd'T'HH:mm:ssXXX", "dd/MM/yyyy HH:mm"]


def parse_ts(col):
    """String column -> UTC timestamp, or NULL when no pattern matches (caller quarantines)."""
    text = F.trim(col)
    return F.coalesce(*[F.try_to_timestamp(text, F.lit(p)) for p in TS_PATTERNS])


def blank_to_null(col):
    """'' and whitespace-only strings become NULL."""
    return F.when(F.trim(col) == "", None).otherwise(F.trim(col))


def normalise_currency(currency_col, country_col):
    """Business rule 5. Returns (currency, currency_derived):
    - a given code is upper-cased ('usd' -> 'USD'),
    - a blank code is derived from shipping_country,
    - NULL currency means it could not be derived; the caller quarantines the row.
    A given code that is not in CURRENCIES is also the caller's job to quarantine."""
    given = F.upper(blank_to_null(currency_col))
    country_map = F.create_map(*[F.lit(x) for pair in COUNTRY_CURRENCY.items() for x in pair])
    derived = country_map[F.upper(blank_to_null(country_col))]
    currency = F.coalesce(given, derived)
    currency_derived = given.isNull() & derived.isNotNull()
    return currency, currency_derived


COMMIT_TAG_KEY = "spark.databricks.delta.commitInfo.userMetadata"


def tag_commits(text):
    """Every Delta commit made after this call carries `text` as userMetadata,
    so it is easy to find in DESCRIBE HISTORY (e.g. 'batch_id=2 step=silver.orders').
    tag_commits(None) removes the tag."""
    if text:
        spark.conf.set(COMMIT_TAG_KEY, text)
    else:
        spark.conf.unset(COMMIT_TAG_KEY)


# ---------------------------------------------------------------- Quarantine
def quarantine(bad_df, entity, batch_id, original_cols):
    """Store rejected rows in silver.quarantine, each with its reason and the original record.

    bad_df        must have `_reason` and `_source_file` columns
    original_cols the columns exactly as received, kept as JSON in original_record

    Replaces this (entity, batch) slice, so a re-run never duplicates quarantine rows.
    Call it once per entity and batch with all bad rows unioned together.
    Returns {reason: row_count} for the DQ report."""
    out = bad_df.select(
        F.lit(entity).alias("entity"),
        F.col("_reason").alias("reason"),
        F.lit(batch_id).cast("int").alias("batch_id"),
        F.col("_source_file").alias("source_file"),
        F.to_json(F.struct(*original_cols)).alias("original_record"),
        F.current_timestamp().alias("quarantined_at"),
    ).cache()
    (out.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"entity = '{entity}' AND batch_id = {batch_id}")
        .saveAsTable(tbl("silver", "quarantine")))
    counts = {r["reason"]: r["count"] for r in out.groupBy("reason").count().collect()}
    out.unpersist()
    return counts


# ---------------------------------------------------------------- Data-quality report
DQ_SCHEMA = T.StructType([
    T.StructField("batch_id", T.IntegerType(), False),
    T.StructField("layer", T.StringType(), False),
    T.StructField("entity", T.StringType(), False),
    T.StructField("rule", T.StringType(), False),
    T.StructField("rows_in", T.LongType(), True),
    T.StructField("rows_out", T.LongType(), True),
    T.StructField("rows_quarantined", T.LongType(), True),
    T.StructField("rows_deduped", T.LongType(), True),
    T.StructField("rows_inserted", T.LongType(), True),
    T.StructField("rows_updated", T.LongType(), True),
])


class DQReport:
    """Collects counts for one entity in one batch and writes them to silver.dq_report.

    One '_TOTAL' row holds rows in / out / quarantined / deduped (+ merge inserts/updates);
    one row per quarantine rule holds that rule's count. Before writing, the balance
    rows_in = rows_out + rows_quarantined + rows_deduped is checked (test D1).
    Re-running a batch replaces its own rows."""

    def __init__(self, layer, entity, batch_id):
        self.layer, self.entity, self.batch_id = layer, entity, batch_id
        self.rule_counts = {}

    def add_quarantined(self, counts):
        for rule, n in counts.items():
            self.rule_counts[rule] = self.rule_counts.get(rule, 0) + n

    def write(self, rows_in, rows_out, rows_deduped=0, rows_inserted=None, rows_updated=None):
        quarantined = sum(self.rule_counts.values())
        if rows_in != rows_out + quarantined + rows_deduped:
            raise AssertionError(
                f"DQ balance broken for {self.layer}.{self.entity} batch {self.batch_id}: "
                f"in={rows_in} out={rows_out} quarantined={quarantined} deduped={rows_deduped}")
        rows = [(self.batch_id, self.layer, self.entity, "_TOTAL",
                 rows_in, rows_out, quarantined, rows_deduped, rows_inserted, rows_updated)]
        rows += [(self.batch_id, self.layer, self.entity, rule, None, None, n, None, None, None)
                 for rule, n in sorted(self.rule_counts.items())]
        df = spark.createDataFrame(rows, DQ_SCHEMA).withColumn("recorded_at", F.current_timestamp())
        (df.write.format("delta").mode("overwrite")
           .option("replaceWhere",
                   f"batch_id = {self.batch_id} AND layer = '{self.layer}' AND entity = '{self.entity}'")
           .saveAsTable(tbl("silver", "dq_report")))


print(f"00_config loaded: workspace={WORKSPACE}, session timezone="
      f"{spark.conf.get('spark.sql.session.timeZone')}")
