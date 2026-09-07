#!/usr/bin/env bash
# Creates the 'lakehouse' catalog in local Polaris with FILE storage and grants
# the root principal full content access. Idempotent-ish (409s are fine on rerun).
set -uo pipefail

BASE=http://localhost:8181
# Where Polaris writes the catalog. Defaults to this script's own data directory, which is what
# docker-compose.yml mounts; override for a warehouse somewhere else. It was one machine's home
# directory until 2026-09-04, so nobody but its author could run this.
WAREHOUSE="${KAHSHE_WAREHOUSE:-file://$(cd "$(dirname "$0")" && pwd)/data/lakehouse}"
TOKEN=$(curl -s -X POST "$BASE/api/catalog/v1/oauth/tokens" \
  -d 'grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')

auth=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")

echo "-- create catalog"
curl -s -o /dev/stderr -w "%{http_code}\n" "$BASE/api/management/v1/catalogs" "${auth[@]}" -d '{
  "catalog": {
    "name": "lakehouse",
    "type": "INTERNAL",
    "properties": { "default-base-location": "'"$WAREHOUSE"'" },
    "storageConfigInfo": {
      "storageType": "FILE",
      "allowedLocations": ["'"$WAREHOUSE"'"]
    }
  }
}'

echo "-- grant catalog_admin manage content"
curl -s -o /dev/stderr -w "%{http_code}\n" -X PUT \
  "$BASE/api/management/v1/catalogs/lakehouse/catalog-roles/catalog_admin/grants" \
  "${auth[@]}" -d '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}'

echo "-- assign catalog_admin to service_admin"
curl -s -o /dev/stderr -w "%{http_code}\n" -X PUT \
  "$BASE/api/management/v1/principal-roles/service_admin/catalog-roles/lakehouse" \
  "${auth[@]}" -d '{"catalogRole":{"name":"catalog_admin"}}'
