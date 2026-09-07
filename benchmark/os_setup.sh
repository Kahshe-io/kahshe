#!/usr/bin/env bash
# Single-node OpenSearch for the benchmark: security off, 2g heap, data in a
# named Docker volume (not a bind mount) so its I/O path is the Docker VM's
# native filesystem, same as it would be for any local dev install.
set -euo pipefail

docker rm -f kahshe-bench-os 2>/dev/null || true
docker volume rm -f kahshe-bench-os-data 2>/dev/null || true
docker run -d --name kahshe-bench-os \
  -p 9200:9200 \
  -e discovery.type=single-node \
  -e DISABLE_SECURITY_PLUGIN=true \
  -e DISABLE_INSTALL_DEMO_CONFIG=true \
  -e "OPENSEARCH_JAVA_OPTS=-Xms2g -Xmx2g" \
  -v kahshe-bench-os-data:/usr/share/opensearch/data \
  "${OS_IMAGE:-opensearchproject/opensearch:3.8.0}"

echo -n "waiting for OpenSearch"
for i in $(seq 1 60); do
  if curl -sf http://localhost:9200 >/dev/null 2>&1; then echo " UP"; break; fi
  echo -n "."; sleep 2
done
curl -s http://localhost:9200 | python3 -c 'import json,sys; d=json.load(sys.stdin); print("OpenSearch", d["version"]["number"])'
