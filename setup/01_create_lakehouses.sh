#!/usr/bin/env bash
# Module 1: create the silver and gold lakehouses in the NovaCart_HCL Fabric workspace.
# Bronze (LH_NovaCart_Bronze) already exists and holds the raw input files.
#
# Auth: your Entra sign-in through `az login`. No key, token or secret is stored anywhere.
# Idempotent: a lakehouse is created only if no item with that name exists yet.
set -euo pipefail

WORKSPACE_NAME="NovaCart_HCL"
LAKEHOUSES=("LH_NovaCart_Bronze" "LH_NovaCart_Silver" "LH_NovaCart_Gold")
FABRIC_API="https://api.fabric.microsoft.com/v1"

fabric() {  # fabric <METHOD> <path> [json-body]
  az rest --resource https://api.fabric.microsoft.com --method "$1" --url "$FABRIC_API$2" ${3:+--body "$3"}
}

ws_id=$(fabric get /workspaces | jq -r --arg n "$WORKSPACE_NAME" '.value[] | select(.displayName == $n) | .id')
[[ -n "$ws_id" ]] || { echo "Workspace $WORKSPACE_NAME not found or no access"; exit 1; }
echo "Workspace $WORKSPACE_NAME = $ws_id"

for lh in "${LAKEHOUSES[@]}"; do
  existing=$(fabric get "/workspaces/$ws_id/lakehouses" | jq -r --arg n "$lh" '.value[] | select(.displayName == $n) | .id')
  if [[ -n "$existing" ]]; then
    echo "exists   $lh ($existing)"
  else
    # enableSchemas matches the bronze lakehouse (tables live under the dbo schema)
    fabric post "/workspaces/$ws_id/lakehouses" \
      "{\"displayName\": \"$lh\", \"creationPayload\": {\"enableSchemas\": true}}" >/dev/null
    echo "created  $lh"
  fi
done

echo
echo "Lakehouses now in $WORKSPACE_NAME:"
fabric get "/workspaces/$ws_id/lakehouses" | jq -r '.value[] | "  \(.displayName)\t\(.id)\tdefault schema: \(.properties.defaultSchema // "none")"'
