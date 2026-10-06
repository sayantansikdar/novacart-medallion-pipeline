#!/usr/bin/env bash
# Module 9 teardown. Prints the plan and does nothing unless a step is named explicitly.
#
#   setup/teardown.sh pause          pause the F2 capacity (stops all compute billing; everything is kept)
#   setup/teardown.sh delete-tests   delete the test/verification notebooks from the workspace (T02..T07, 00_verify_inputs, 98_reset_lab)
#
# Deliberately NOT automated (do it by hand in the portal, after grading):
#   - deleting the lakehouses or the NovaCart_HCL workspace: removes all tables, history and the raw inputs in OneLake
#   - deleting the hackafabric capacity: it belongs to the team's Azure subscription (resource group fabricnew)
# Both are irreversible; the code and evidence in this repo are enough to rebuild everything from the raw files.
set -euo pipefail
cd "$(dirname "$0")/.."

case "${1:-plan}" in
  pause)
    cap=$(az resource show -g fabricnew -n hackafabric --resource-type Microsoft.Fabric/capacities --query id -o tsv)
    az resource invoke-action --action suspend --ids "$cap" -o none 2>/dev/null || true
    echo "F2 capacity state: $(az resource show --ids "$cap" --query properties.state -o tsv)"
    ;;
  delete-tests)
    python3 - <<'EOF'
import sys
sys.path.insert(0, "setup")
from run_fabric_notebook import call, find, WORKSPACE_NAME
TEST_NOTEBOOKS = {"T02_module2_tests", "T03_module3_tests", "T04_module4_tests", "T05_module5_tests",
                  "T06_module6_tests", "T07_module7_verify", "00_verify_inputs", "98_reset_lab"}
ws = find(call("GET", "/workspaces")[2]["value"], WORKSPACE_NAME)["id"]
for nb in call("GET", f"/workspaces/{ws}/notebooks")[2]["value"]:
    if nb["displayName"] in TEST_NOTEBOOKS:
        call("DELETE", f"/workspaces/{ws}/items/{nb['id']}")
        print("deleted", nb["displayName"])
EOF
    ;;
  *)
    sed -n '2,12p' "$0"
    ;;
esac
