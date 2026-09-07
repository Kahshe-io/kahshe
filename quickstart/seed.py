"""Quickstart seeder: run via `docker compose run --rm seed`.

1. Waits for Polaris, creates the `lakehouse` catalog (FILE storage) and grants.
2. Through kahshe, creates `logs.events` with the `kahshe.index=msg` property
   and appends four data files with disjoint id ranges and distinct message
   vocabularies.
3. Polls kahshe's plan endpoint until the automatic indexer's work is visible:
   a substring filter that min/max stats cannot prune returns 1 file, not 4.

Only stdlib is used for the REST calls; pyiceberg writes the table.
"""

import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

POLARIS = "http://polaris:8181"
KAHSHE = "http://kahshe:8282"
WAREHOUSE_ROOT = "file:///warehouse/lakehouse"
CLIENT_ID, CLIENT_SECRET = "root", "s3cr3t"


def request(method, url, token=None, body=None, form=None):
    data = None
    headers = {}
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    elif body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=10) as resp:
        payload = resp.read()
        return resp.status, json.loads(payload) if payload else None


def wait_for_polaris(timeout_s=180):
    print("waiting for polaris ...", flush=True)
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            return get_token()
        except Exception as exc:  # noqa: BLE001 - retry anything until deadline
            if time.monotonic() > deadline:
                raise SystemExit(f"polaris did not come up: {exc}")
            time.sleep(2)


def get_token():
    _, body = request(
        "POST",
        f"{POLARIS}/api/catalog/v1/oauth/tokens",
        form={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "PRINCIPAL_ROLE:ALL",
        },
    )
    return body["access_token"]


def ensure(description, method, url, token, body):
    try:
        request(method, url, token=token, body=body)
        print(f"  {description}: ok")
    except urllib.error.HTTPError as err:
        if err.code == 409:
            print(f"  {description}: already exists")
        else:
            raise


def setup_catalog(token):
    print("configuring polaris catalog 'lakehouse' ...")
    ensure(
        "create catalog",
        "POST",
        f"{POLARIS}/api/management/v1/catalogs",
        token,
        {
            "catalog": {
                "name": "lakehouse",
                "type": "INTERNAL",
                "properties": {"default-base-location": WAREHOUSE_ROOT},
                "storageConfigInfo": {
                    "storageType": "FILE",
                    "allowedLocations": [WAREHOUSE_ROOT],
                },
            }
        },
    )
    ensure(
        "grant catalog_admin manage content",
        "PUT",
        f"{POLARIS}/api/management/v1/catalogs/lakehouse/catalog-roles/catalog_admin/grants",
        token,
        {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}},
    )
    ensure(
        "assign catalog_admin to service_admin",
        "PUT",
        f"{POLARIS}/api/management/v1/principal-roles/service_admin/catalog-roles/lakehouse",
        token,
        {"catalogRole": {"name": "catalog_admin"}},
    )


def seed_table():
    import pyarrow as pa
    from pyiceberg.catalog import load_catalog

    print("seeding logs.events through kahshe ...")
    catalog = load_catalog(
        "kahshe",
        type="rest",
        uri=KAHSHE,
        credential=f"{CLIENT_ID}:{CLIENT_SECRET}",
        warehouse="lakehouse",
        scope="PRINCIPAL_ROLE:ALL",
    )
    catalog.create_namespace_if_not_exists("logs")

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("level", pa.string()),
            pa.field("msg", pa.string()),
        ]
    )

    try:
        table = catalog.load_table("logs.events")
        print("  table exists; appending is skipped (run `docker compose down -v` to reset)")
        return
    except Exception:  # noqa: BLE001 - NoSuchTableError across pyiceberg versions
        table = catalog.create_table(
            "logs.events", schema=schema, properties={"kahshe.index": "msg"}
        )
        print("  created logs.events with kahshe.index=msg")

    batches = [
        (0, "INFO", ["user login ok", "session started", "heartbeat received"]),
        (1000, "INFO", ["cache warm complete", "snapshot committed", "compaction finished"]),
        (2000, "WARN", ["disk pressure rising", "retry scheduled", "slow response upstream"]),
        (3000, "ERROR", ["connection timeout to broker", "timeout waiting for quorum", "request timeout exceeded"]),
    ]
    for base, level, msgs in batches:
        rows = {
            "id": [base + i for i in range(1000)],
            "level": [level] * 1000,
            "msg": [msgs[i % len(msgs)] + f" seq={base + i}" for i in range(1000)],
        }
        table.append(pa.table(rows, schema=schema))
        print(f"  appended 1000 rows (ids {base}..{base + 999}, level {level})")

    # The indexer is triggered by loadTable traffic passing through the proxy
    # (in production, engines do this constantly). One refresh is that trigger.
    table.refresh()


def plan_task_count(token, filter_json):
    _, body = request(
        "POST",
        f"{KAHSHE}/v1/lakehouse/namespaces/logs/tables/events/plan",
        token=token,
        body={"filter": filter_json} if filter_json else {},
    )
    tasks = body.get("file-scan-tasks") or []
    return len(tasks)


def wait_for_index(token, timeout_s=90):
    print("waiting for the automatic indexer (kahshe.index=msg) ...")
    contains_timeout = {"type": "contains", "term": "msg", "value": "timeout"}
    deadline = time.monotonic() + timeout_s
    while True:
        n = plan_task_count(token, contains_timeout)
        if n == 1:
            return
        if time.monotonic() > deadline:
            raise SystemExit(
                f"index never became visible in plans (still {n} tasks for a "
                "1-file substring); check `docker compose logs kahshe`"
            )
        time.sleep(2)


def main():
    token = wait_for_polaris()
    setup_catalog(token)
    seed_table()
    wait_for_index(token)

    print()
    print("verified plan results (files returned out of 4):")
    for label, filt in [
        ("no filter", None),
        ("id >= 3000 (min/max stats)", {"type": "gt-eq", "term": "id", "value": 3000}),
        ("contains 'timeout' (3-gram bloom)", {"type": "contains", "term": "msg", "value": "timeout"}),
        ("contains 'zzz-absent' (3-gram bloom)", {"type": "contains", "term": "msg", "value": "zzz-absent"}),
    ]:
        print(f"  {label:38s} -> {plan_task_count(token, filt)}")
    print()
    print("quickstart ready: kahshe on http://localhost:8282 (admin :8283)")


if __name__ == "__main__":
    sys.exit(main())
