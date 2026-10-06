"""Upload a local .py file as a Fabric notebook and run it once, waiting for the result.

Usage: python3 setup/run_fabric_notebook.py <local_file.py> <notebook_name>

Auth: borrows a short-lived Entra token from `az` for each call; nothing is saved.
Idempotent: creates the notebook if missing, otherwise replaces its content.
"""
import base64, json, subprocess, sys, time, urllib.error, urllib.request

WORKSPACE_NAME = "NovaCart_HCL"
API = "https://api.fabric.microsoft.com/v1"


def token():
    return subprocess.check_output(
        ["az", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com",
         "--query", "accessToken", "-o", "tsv"], text=True).strip()


def call(method, url, body=None):
    """Returns (status, headers, parsed json or None)."""
    req = urllib.request.Request(url if url.startswith("http") else API + url, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": f"Bearer {token()}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            raw = r.read()
            return r.status, r.headers, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {url} -> HTTP {e.code}: {e.read().decode()[:500]}")


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


def notebook_definition(source):
    nb = {"nbformat": 4, "nbformat_minor": 5,
          "metadata": {"language_info": {"name": "python"}},
          "cells": [{"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [],
                     "source": source.splitlines(keepends=True)}]}
    payload = base64.b64encode(json.dumps(nb).encode()).decode()
    return {"format": "ipynb",
            "parts": [{"path": "notebook-content.ipynb", "payload": payload, "payloadType": "InlineBase64"}]}


def main(local_file, name):
    source = open(local_file, encoding="utf-8").read()
    ws = next(w["id"] for w in call("GET", "/workspaces")[2]["value"] if w["displayName"] == WORKSPACE_NAME)
    nb_id = next((i["id"] for i in call("GET", f"/workspaces/{ws}/notebooks")[2]["value"]
                  if i["displayName"] == name), None)

    if nb_id is None:
        status, headers, body = call("POST", f"/workspaces/{ws}/notebooks",
                                     {"displayName": name, "definition": notebook_definition(source)})
        wait_lro(headers)
        nb_id = next(i["id"] for i in call("GET", f"/workspaces/{ws}/notebooks")[2]["value"]
                     if i["displayName"] == name)
        print(f"created notebook {name} ({nb_id})")
    else:
        status, headers, _ = call("POST", f"/workspaces/{ws}/notebooks/{nb_id}/updateDefinition",
                                  {"definition": notebook_definition(source)})
        wait_lro(headers)
        print(f"updated notebook {name} ({nb_id})")

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
    print(f"Notebook run Completed. Output: Fabric portal > {WORKSPACE_NAME} > {name} > Recent runs")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
