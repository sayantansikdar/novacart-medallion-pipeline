"""Export the submission deliverables from Fabric into deliverables/raw/ (capacity must be resumed).

  python3 -u setup/export_deliverables.py

- pipeline_definition.json   the Data Pipeline exactly as stored in Fabric (getDefinition)
- pipeline_runs.json         every run of the pipeline (status, start, end) from the job scheduler
- dq_batch_1.json / _2.json  the DQ report each gold run exported to gold Files/exports/dq/
- alerts/*.json              alerts recorded by the on-failure path
Auth: short-lived Entra tokens from `az`; nothing is stored.
"""
import base64, json, os, subprocess, sys, time, urllib.request

sys.path.insert(0, os.path.dirname(__file__))
from run_fabric_notebook import call, find, WORKSPACE_NAME  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "deliverables", "raw")
ONELAKE = "https://onelake.dfs.fabric.microsoft.com"


def storage_get(path, attempts=30):
    token = subprocess.check_output(["az", "account", "get-access-token", "--resource", "https://storage.azure.com",
                                     "--query", "accessToken", "-o", "tsv"], text=True).strip()
    for i in range(attempts):
        try:
            req = urllib.request.Request(f"{ONELAKE}/{path}", headers={"Authorization": f"Bearer {token}",
                                                                        "x-ms-version": "2023-11-03"})
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.read()
        except urllib.error.HTTPError:
            raise
        except urllib.error.URLError:
            time.sleep(10)
    raise RuntimeError(f"could not download {path}")


def storage_list(path):
    token = subprocess.check_output(["az", "account", "get-access-token", "--resource", "https://storage.azure.com",
                                     "--query", "accessToken", "-o", "tsv"], text=True).strip()
    fs, directory = path.split("/", 1)
    req = urllib.request.Request(f"{ONELAKE}/{fs}?resource=filesystem&recursive=false&directory={directory}",
                                 headers={"Authorization": f"Bearer {token}", "x-ms-version": "2023-11-03"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return [p["name"] for p in json.loads(r.read()).get("paths", [])]


def save(name, data):
    path = os.path.join(OUT, name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open(path, "wb").write(data if isinstance(data, bytes) else json.dumps(data, indent=2).encode())
    print(f"saved deliverables/raw/{name}")


def main():
    ws = find(call("GET", "/workspaces")[2]["value"], WORKSPACE_NAME)["id"]
    pipe = find(call("GET", f"/workspaces/{ws}/dataPipelines")[2]["value"], "novacart_medallion")

    status, headers, body = call("POST", f"/workspaces/{ws}/items/{pipe['id']}/getDefinition")
    if status == 202:                                   # long-running: wait, then read the result
        loc = headers["Location"]
        while True:
            time.sleep(int(headers.get("Retry-After", 3)))
            _, headers, op = call("GET", loc)
            if op and op.get("status") in ("Succeeded", "Failed"):
                break
        body = call("GET", loc.rstrip("/") + "/result")[2]
    part = next(p for p in body["definition"]["parts"] if p["path"] == "pipeline-content.json")
    save("pipeline_definition.json", json.loads(base64.b64decode(part["payload"])))

    runs = call("GET", f"/workspaces/{ws}/items/{pipe['id']}/jobs/instances")[2]["value"]
    save("pipeline_runs.json", runs)

    lakehouses = {l["displayName"]: l["id"] for l in call("GET", f"/workspaces/{ws}/lakehouses")[2]["value"]}
    gold = lakehouses["LH_NovaCart_Gold"]
    for b in (1, 2):
        save(f"dq_batch_{b}.json", storage_get(f"{ws}/{gold}/Files/exports/dq/dq_batch_{b}.json"))
    for name in storage_list(f"{ws}/{gold}/Files/exports/alerts"):
        save(f"alerts/{name.rsplit('/', 1)[-1]}", storage_get(f"{ws}/{name}"))


if __name__ == "__main__":
    main()
