# %% [parameters]
# Set by the pipeline's on-failure path.
batch_id = ""
pipeline_run_id = ""
pipeline_name = ""

# %%
%run 00_config

# %%
# 99_notify_failure: the pipeline's on-failure step.
# Records the failed run in silver.pipeline_alerts and writes an alert file to
# gold Files/exports/alerts/. This is "a step that would send an alert": an Office 365
# Outlook activity after it turns the alert into an e-mail once its connection is authorised.
# batch_id is NOT validated here: an invalid batch_id may be the reason the run failed.

import json
from datetime import datetime, timezone

alert = {
    "alerted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "pipeline_name": pipeline_name,
    "pipeline_run_id": pipeline_run_id,
    "batch_id": str(batch_id),
    "message": f"NovaCart pipeline run {pipeline_run_id} failed for batch_id={batch_id!r}. "
               f"See Monitor > {pipeline_name} for the failed step and its error.",
}

(spark.createDataFrame([alert], "alerted_at string, pipeline_name string, pipeline_run_id string, "
                                "batch_id string, message string")
      .withColumn("alerted_at", F.to_timestamp("alerted_at"))
      .write.format("delta").mode("append").saveAsTable(tbl("silver", "pipeline_alerts")))
notebookutils.fs.put(files_path("gold", "exports", "alerts", f"alert_{pipeline_run_id or 'manual'}.json"),
                     json.dumps(alert, indent=2), True)
print("ALERT:", alert["message"])
