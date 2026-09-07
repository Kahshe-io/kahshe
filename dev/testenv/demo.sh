#!/usr/bin/env bash
# End-to-end demo: assumes kahshe-polaris container is up, catalog+table seeded
# (setup-polaris.sh + seed.py), index built (kahshe index lakehouse logs.events msg),
# and kahshe running on :8282.
set -uo pipefail

TOKEN=$(curl -s -X POST http://localhost:8282/v1/oauth/tokens \
  -d 'grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')

plan() {
  echo "== $1"
  curl -s -X POST "http://localhost:8282/v1/lakehouse/namespaces/logs/tables/events/plan" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d "$2" \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); ts=d.get("file-scan-tasks"); print("   tasks:", len(ts) if ts is not None else d)'
}

plan "baseline (no filter) — expect 4"                          '{}'
plan "eq long msg (stats keep 2, bloom -> 1)"                   '{"filter":{"type":"eq","term":"msg","value":"connection timeout to broker seq=3000"}}'
plan "contains 'timeout' (stats keep 4, bloom -> 1)"            '{"filter":{"type":"contains","term":"msg","value":"timeout"}}'
plan "contains 'zzz-not-present' (bloom -> 0, nothing scanned)" '{"filter":{"type":"contains","term":"msg","value":"zzz-not-present"}}'
plan "id>=2000 AND contains 'disk pressure' (composed) -> 1"    '{"filter":{"type":"and","left":{"type":"gt-eq","term":"id","value":2000},"right":{"type":"contains","term":"msg","value":"disk pressure"}}}'

echo "== contains under OR is rejected (correctness guard):"
curl -s -X POST "http://localhost:8282/v1/lakehouse/namespaces/logs/tables/events/plan" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"filter":{"type":"or","left":{"type":"contains","term":"msg","value":"timeout"},"right":{"type":"eq","term":"id","value":1}}}' \
  | python3 -c 'import json,sys; print("  ", json.load(sys.stdin)["error"]["message"])'

echo "== stock Java client (zero config, flips to server planning):"
java -cp "$(dirname "$0")/../build/install/kahshe/lib/*" io.kahshe.ClientDemo 2>/dev/null | sed 's/^/   /'
