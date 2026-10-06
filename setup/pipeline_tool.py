"""Deploy, run and monitor the NovaCart Fabric Data Pipeline.

  python3 -u setup/pipeline_tool.py deploy
  python3 -u setup/pipeline_tool.py run <batch_id>      # runs, waits, saves the run log to evidence/run_logs/

Builds the Fabric pipeline JSON from pipeline/novacart_pipeline.json (notebook names -> ids).
Auth and HTTP helpers come from run_fabric_notebook.py (short-lived Entra tokens from `az`).
"""
import base64, json, os, sys, time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))
from run_fabric_notebook import call, find, wait_lro, WORKSPACE_NAME  # noqa: E402

PIPELINE_NAME = "novacart_medallion"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def workspace_id():
    return find(call("GET", "/workspaces")[2]["value"], WORKSPACE_NAME)["id"]


def notebook_activity(name, notebook_id, ws, after, cfg, params):
    return {
        "name": name,
        "type": "TridentNotebook",
        "dependsOn": [{"activity": a, "dependencyConditions": c} for a, c in after],
        "policy": {"timeout": cfg["timeout"], "retry": cfg["retry"],
                   "retryIntervalInSeconds": cfg["retry_interval_seconds"],
                   "secureOutput": False, "secureInput": False},
        "typeProperties": {
            "notebookId": notebook_id,
            "workspaceId": ws,
            "parameters": {k: {"value": {"value": v, "type": "Expression"}, "type": "string"}
                           for k, v in params.items()},
        },
    }


def build_definition(ws):
    cfg = json.load(open(os.path.join(ROOT, "pipeline", "novacart_pipeline.json")))
    notebooks = {n["displayName"]: n["id"] for n in call("GET", f"/workspaces/{ws}/notebooks")[2]["value"]}
    batch = "@pipeline().parameters.batch_id"
    activities = [notebook_activity(s["name"], notebooks[s["notebook"]], ws,
                                    [(s["after"], ["Succeeded"])] if s["after"] else [], cfg, {"batch_id": batch})
                  for s in cfg["steps"]]
    f = cfg["on_failure"]
    # Runs when the last step failed, or was skipped because an earlier step failed.
    notify = notebook_activity("notify_failure", notebooks[f["notify_notebook"]], ws,
                               [(f["watch"], ["Failed", "Skipped"])], {**cfg, "retry": 1},
                               {"batch_id": batch, "pipeline_run_id": "@pipeline().RunId",
                                "pipeline_name": "@pipeline().Pipeline"})
    # Keeps the run marked Failed in Monitor after the alert step succeeded.
    fail = {"name": "fail_run", "type": "Fail",
            "dependsOn": [{"activity": "notify_failure", "dependencyConditions": ["Succeeded"]}],
            "typeProperties": {"message": f["fail_message"], "errorCode": "NOVACART_PIPELINE_FAILED"}}
    content = {"properties": {"activities": activities + [notify, fail], "parameters": cfg["parameters"]}}
    payload = base64.b64encode(json.dumps(content).encode()).decode()
    return {"parts": [{"path": "pipeline-content.json", "payload": payload, "payloadType": "InlineBase64"}]}


def deploy():
    ws = workspace_id()
    definition = build_definition(ws)
    existing = find(call("GET", f"/workspaces/{ws}/dataPipelines")[2]["value"], PIPELINE_NAME)
    if existing is None:
        _, headers, _ = call("POST", f"/workspaces/{ws}/dataPipelines",
                             {"displayName": PIPELINE_NAME, "definition": definition})
        wait_lro(headers)
        print(f"created pipeline {PIPELINE_NAME}")
    else:
        _, headers, _ = call("POST", f"/workspaces/{ws}/dataPipelines/{existing['id']}/updateDefinition",
                             {"definition": definition})
        wait_lro(headers)
        print(f"updated pipeline {PIPELINE_NAME} ({existing['id']})")


def run(batch_id):
    ws = workspace_id()
    pipe = find(call("GET", f"/workspaces/{ws}/dataPipelines")[2]["value"], PIPELINE_NAME)
    started = datetime.now(timezone.utc)
    _, headers, _ = call("POST", f"/workspaces/{ws}/items/{pipe['id']}/jobs/instances?jobType=Pipeline",
                         {"executionData": {"parameters": {"batch_id": str(batch_id)}}})
    job_url = headers["Location"]
    run_id = job_url.rstrip("/").split("/")[-1]
    print(f"pipeline run {run_id} started for batch_id={batch_id!r}")
    while True:
        time.sleep(20)
        job = call("GET", job_url)[2]
        print(f"  {time.strftime('%H:%M:%S')} {job['status']}")
        if job["status"] in ("Completed", "Failed", "Cancelled", "Deduped"):
            break

    # Activity-level log (which steps ran, retried, failed or were skipped).
    window = {"lastUpdatedAfter": (started - timedelta(minutes=5)).isoformat(),
              "lastUpdatedBefore": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()}
    try:
        _, _, acts = call("POST", f"/workspaces/{ws}/datapipelines/pipelineruns/{run_id}/queryactivityruns", window)
    except SystemExit as e:          # call() exits on HTTP errors; keep the job-level log anyway
        print(f"  activity runs not available: {e}")
        acts = {}
    activity_runs = sorted((acts or {}).get("value", []), key=lambda a: a.get("activityRunStart") or "")
    for a in activity_runs:
        err = (a.get("error") or {}).get("message", "")
        print(f"  {a.get('activityName', ''):17} {a.get('status', ''):10} "
              f"{a.get('activityRunStart', '')[:19]}  {a.get('durationInMs', 0) / 1000:6.0f}s  {err[:120]}")
    log_dir = os.path.join(ROOT, "evidence", "run_logs")
    os.makedirs(log_dir, exist_ok=True)
    path = os.path.join(log_dir, f"run_batch_{batch_id}_{started.strftime('%Y%m%dT%H%M%SZ')}.json")
    json.dump({"pipeline": PIPELINE_NAME, "run_id": run_id, "batch_id": str(batch_id), "job": job,
               "activity_runs": activity_runs}, open(path, "w"), indent=2, default=str)
    print(f"run {job['status']}; log saved to {os.path.relpath(path, ROOT)}")
    return job["status"]


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "deploy":
        deploy()
    elif len(sys.argv) == 3 and sys.argv[1] == "run":
        run(sys.argv[2])
    else:
        sys.exit(__doc__)
