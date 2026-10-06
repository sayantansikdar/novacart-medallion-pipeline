# Module 1 verification notebook (runs inside Fabric Spark).
# Proves Spark can read all eight raw input files from OneLake with no key or secret,
# and that each file has the row count measured locally before upload.
# Read-only: it writes nothing.

INPUT_DIR = ("abfss://NovaCart_HCL@onelake.dfs.fabric.microsoft.com/"
             "LH_NovaCart_Bronze.Lakehouse/Files/NovaCart_SourceData/NovaCart_SourceData")

# file name -> (reader, expected rows). Counts come from profiling the local copies.
EXPECTED = {
    "orders_batch_1.csv":            ("csv",   1418),
    "orders_batch_2.csv":            ("csv",   1545),
    "order_items_batch_1_json.txt":  ("jsonl", 1006),
    "order_items_batch_2_jsonl.txt": ("jsonl", 1007),
    "customers_changes.csv":         ("csv",    194),
    "products.csv":                  ("csv",     40),
    "fx_rates.csv":                  ("csv",     84),
    "payments_json.txt":             ("array", 1164),
}

def read(path, kind):
    if kind == "csv":
        return spark.read.option("header", True).csv(path)
    if kind == "jsonl":                      # one JSON object per line
        return spark.read.json(path)
    return spark.read.option("multiLine", True).json(path)   # one JSON array over many lines

failures = []
for name, (kind, expected) in EXPECTED.items():
    actual = read(f"{INPUT_DIR}/{name}", kind).count()
    status = "OK  " if actual == expected else "FAIL"
    print(f"{status} {name:32} expected={expected:5} actual={actual:5}")
    if actual != expected:
        failures.append(name)

# Raising makes the notebook run show as Failed, which the runner script reports.
assert not failures, f"Row-count mismatch: {failures}"
print("All eight input files readable with expected row counts.")
