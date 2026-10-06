"""Upload a local .py file as a Fabric notebook and (optionally) run it once, waiting for the result.

Usage:
  python3 -u setup/run_fabric_notebook.py <local_file.py> <notebook_name> [--lakehouse NAME] [--no-run]

Cells: the file is split into notebook cells at lines starting with "# %%".
       "# %% [parameters]" marks the parameters cell a pipeline can override.
--lakehouse: attach that lakehouse as the notebook's default (needed for Spark SQL table names).
--no-run:    upload only (for notebooks that are only ever called with %run, like 00_config).

Auth: borrows a short-lived Entra token from `az` for each call; nothing is saved.
Idempotent: creates the notebook if missing, otherwise replaces its content.
"""
import argparse, base64, json, subprocess, sys, time, urllib.error, urllib.request

WORKSPACE_NAME = "NovaCart_HCL"
API = "https://api.fabric.microsoft.com/v1"


def token():
    return subprocess.check_output(
        ["az", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com",
         "--query", "accessToken", "-o", "tsv"], text=True).strip()


def call(method, url, body=None, attempts=60):
    """Returns (status, headers, parsed json or None).
    Network errors (e.g. DNS outages, seen for minutes at a time on the dev machine) are
    retried for up to 10 minutes, so a running notebook is never abandoned; HTTP errors stop the script."""
    for attempt in range(1, attempts + 1):
        req = urllib.request.Request(url if url.startswith("http") else API + url, method=method,
                                     data=json.dumps(body).encode() if body is not None else None,
                                     headers={"Authorization": f"Bearer {token()}", "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                raw = r.read()
                return r.status, r.headers, json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            sys.exit(f"{method} {url} -> HTTP {e.code}: {e.read().decode()[:500]}")
        except urllib.error.URLError as e:
            if attempt == attempts:
                raise
            print(f"  network error ({e.reason}); retrying in 10 s")
            time.sleep(10)


def wait_lro(headers):
    """Wait for a Fabric long-running operation (202 + Location) to finish."""
    loc = headers.get("Location")
    while loc:
        time.sleep(int(headers.get("Retry-After", 3)))
        status, headers, body = call("GET", loc)
        if body and body.get("status") in ("Succeeded", "Failed"):
            if body["status"] == "Failed":
                sys.exit(f"Operation failed: {body}")
            return


def split_cells(source):
    """Split on '# %%' marker lines. Returns a list of (source_text, is_parameters_cell)."""
    cells, current, is_params = [], [], False
    for line in source.splitlines(keepends=True):
        if line.startswith("# %%"):
            if "".join(current).strip():
                cells.append(("".join(current).strip("\n") + "\n", is_params))
            current, is_params = [], "[parameters]" in line
        else:
            current.append(line)
    if "".join(current).strip():
        cells.append(("".join(current).strip("\n") + "\n", is_params))
    return cells


def notebook_definition(source, ws_id, lakehouse):
    cells = [{"cell_type": "code", "execution_count": None, "outputs": [],
              "metadata": {"tags": ["parameters"]} if is_params else {},
              "source": text.splitlines(keepends=True)}
             for text, is_params in split_cells(source)]
    metadata = {"kernel_info": {"name": "synapse_pyspark"},
                "kernelspec": {"name": "synapse_pyspark", "display_name": "Synapse PySpark"},
                "language_info": {"name": "python"}}
    if lakehouse:
        metadata["dependencies"] = {"lakehouse": {"default_lakehouse": lakehouse["id"],
                                                  "default_lakehouse_name": lakehouse["displayName"],
                                                  "default_lakehouse_workspace_id": ws_id}}
    nb = {"nbformat": 4, "nbformat_minor": 5, "metadata": metadata, "cells": cells}
    payload = base64.b64encode(json.dumps(nb).encode()).decode()
    return {"format": "ipynb",
            "parts": [{"path": "notebook-content.ipynb", "payload": payload, "payloadType": "InlineBase64"}]}


def find(items, name):
    return next((i for i in items if i["displayName"] == name), None)


def run(ws, nb_id):
    status, headers, _ = call("POST", f"/workspaces/{ws}/items/{nb_id}/jobs/instances?jobType=RunNotebook")
    job_url = headers["Location"]
    print("run started; Spark session start-up can take a few minutes")
    while True:
        time.sleep(15)
        job = call("GET", job_url)[2]
        print(f"  {time.strftime('%H:%M:%S')} {job['status']}")
        if job["status"] in ("Completed", "Failed", "Cancelled", "Deduped"):
            break
    if job["status"] != "Completed":
        sys.exit(f"Notebook run {job['status']}: {json.dumps(job.get('failureReason'), indent=2)}")
    print("Notebook run Completed.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("local_file"); ap.add_argument("name")
    ap.add_argument("--lakehouse"); ap.add_argument("--no-run", action="store_true")
    args = ap.parse_args()

    source = open(args.local_file, encoding="utf-8").read()
    ws = find(call("GET", "/workspaces")[2]["value"], WORKSPACE_NAME)["id"]
    lakehouse = None
    if args.lakehouse:
        lakehouse = find(call("GET", f"/workspaces/{ws}/lakehouses")[2]["value"], args.lakehouse)
        if lakehouse is None:
            sys.exit(f"Lakehouse {args.lakehouse} not found")
    definition = notebook_definition(source, ws, lakehouse)

    nb = find(call("GET", f"/workspaces/{ws}/notebooks")[2]["value"], args.name)
    if nb is None:
        _, headers, _ = call("POST", f"/workspaces/{ws}/notebooks", {"displayName": args.name, "definition": definition})
        wait_lro(headers)
        nb = find(call("GET", f"/workspaces/{ws}/notebooks")[2]["value"], args.name)
        print(f"created notebook {args.name} ({nb['id']})")
    else:
        _, headers, _ = call("POST", f"/workspaces/{ws}/notebooks/{nb['id']}/updateDefinition", {"definition": definition})
        wait_lro(headers)
        print(f"updated notebook {args.name} ({nb['id']})")

    if not args.no_run:
        run(ws, nb["id"])


if __name__ == "__main__":
    main()
