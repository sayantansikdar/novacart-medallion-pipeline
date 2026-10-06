# %% [parameters]
# Safety switch: nothing is deleted unless the caller passes confirm = "RESET".
confirm = "no"

# %%
%run 00_config

# %%
# 98_reset_lab: start the lab from an empty state (used once before the official runs).
#
# Drops every table in the bronze, silver and gold lakehouses, except silver.pipeline_alerts
# (evidence that the failure path fired), and clears the test-result and DQ-export folders.
# NEVER touches the raw input files (bronze Files/NovaCart_SourceData) or any notebook:
# everything dropped here can be rebuilt from the raw files by running the pipeline.

KEEP = {("silver", "pipeline_alerts")}
FOLDERS_TO_CLEAR = [files_path("silver", "_test_results"), DQ_EXPORT_DIR]

if confirm != "RESET":
    raise ValueError("98_reset_lab: pass confirm='RESET' to drop the lab tables")

for layer in ("bronze", "silver", "gold"):
    for row in spark.sql(f"SHOW TABLES IN {LAKEHOUSE[layer]}.{SCHEMA}").collect():
        name = row["tableName"]
        if (layer, name) in KEEP:
            print(f"kept     {tbl(layer, name)}")
            continue
        spark.sql(f"DROP TABLE IF EXISTS {tbl(layer, name)}")
        print(f"dropped  {tbl(layer, name)}")

for folder in FOLDERS_TO_CLEAR:
    if notebookutils.fs.exists(folder):
        notebookutils.fs.rm(folder, True)
        print(f"cleared  {folder}")

remaining_inputs = [f.name for f in notebookutils.fs.ls(INPUT_DIR)]
assert len(remaining_inputs) == 8, f"raw inputs changed: {remaining_inputs}"
print(f"raw inputs untouched: {len(remaining_inputs)} files")
