# kahshe Helm chart

One image, roles by configuration. The default install is a single proxy replica that serves
plans, builds the indexes of tables carrying the `kahshe.index` property, and runs no watcher.

```
kubectl create secret generic kahshe-backend-credential --from-literal=credential='client:secret'
helm install kahshe ./helm/kahshe --set backend.url=http://nessie:19120/iceberg
```

Point engines' Iceberg REST catalog URI at the Service the chart creates (NOTES prints it).

## The three shapes

| Shape | Values | What runs |
| --- | --- | --- |
| Single process (default) | `proxy.replicas: 1`, `proxy.indexer: true`, `watch.enabled: false` | One pod: data plane, admin, indexer |
| Fleet | `indexer.enabled: true`, `indexer.replicas: N`, plus a watch pod with `watch.indexer: false` | N builders sharing the columns of the rule-named tables by build lease (they neither deliver alerts nor scan), and one watch pod that scans, alerts, and delivers from their build reports |
| Split | `examples/values-split.yaml` plus `watch.rules` | N serving replicas with the indexer off and the table cache TTL at 0; one watch pod that discovers and row-scans the tables its rules name and delivers alerts; with `watch.indexer: true` it also builds those tables' indexes, with `watch.indexer: false` it builds nothing and also delivers the alerts of builds run elsewhere from their build reports |

```
helm install kahshe ./helm/kahshe -f helm/kahshe/examples/values-split.yaml \
  --set-file watch.rules=helm/kahshe/examples/watch-rules.yaml
```

## Values that are not tuning

- `proxy.tableCacheTtlMs` must be `0` before `proxy.replicas` goes above one, or before
  `proxy.autoscaling.enabled`. The chart refuses to render otherwise — see "Scaling out" below.
- `proxy.build` and `watch.build` are the build's memory arithmetic against the container's
  memory limit; the pod refuses a build at startup if they contradict it.
- `*.build.scratchSizeLimit` stays larger than `*.build.termBuildMaxSpillBytes`.
- `tls.enabled`: off, port 8282 is plaintext HTTP and every request on it carries a bearer token.
  On, the pods serve TLS themselves from `tls.secretName` — see the cert-manager block below.
- `networkPolicy.enabled`: worth setting either way, and required in spirit while `tls.enabled` is
  false, so that only an ingress or the engine namespaces can reach the data plane.

Secrets are read by name and, with one exception, never created by the chart:
`backend.credentialSecret`; `watch.webhookSecret` with the watcher on the `webhook` sink; and with
`tls.enabled`, `tls.secretName` plus `tls.clientCaSecret` when `tls.clientAuth` is not `none`. The
exception is `tls.certManager.enabled`, where the chart's `Certificate` has cert-manager write
`tls.secretName`. The rules file comes from `watch.rules` (a ConfigMap the chart creates) or
`watch.existingRulesConfigMap`.

`helm template` renders the plain manifests for clusters without Helm.

## Kubernetes objects the chart can create

| Object | Value | Why |
| --- | --- | --- |
| `ServiceAccount` | `serviceAccount.create` (default **on**) | kahshe asks the API server for nothing, so this exists to be **annotated**: IRSA on EKS and Workload Identity on GKE both bind a cloud role to a service account. Without one, an S3 index root needs a static access key in a Secret. `automountServiceAccountToken` is off, since nothing here talks to the API |
| `Certificate` (cert-manager) | `tls.certManager.enabled` | cert-manager issues and renews into `tls.secretName`, which the pods mount. Because kahshe re-reads its certificate while running, a renewal needs no restart and no rollout. Defaults to PKCS#8 and the in-cluster Service names |
| `ServiceMonitor` (Prometheus Operator) | `metrics.serviceMonitor.enabled` | Scrapes `/metrics` on the admin port. Its `scheme` follows `tls.admin`, because scraping the wrong protocol fails every scrape |
| `Ingress` | `ingress.enabled` | For engines OUTSIDE the cluster; in-cluster ones reach the Service directly |
| `HorizontalPodAutoscaler` | `proxy.autoscaling.enabled` | Scales the serving role — see the constraint below, which the chart enforces |
| `NetworkPolicy` | `networkPolicy.enabled` | Restricts who may reach the data plane |
| `PodDisruptionBudget` | `proxy.podDisruptionBudget.enabled` | For more than one proxy replica |

The `Certificate` and `ServiceMonitor` are off by default because their CRDs may not be
installed, and a chart that renders an unknown kind fails to apply.

### cert-manager in one block

```yaml
tls:
  enabled: true
  certManager:
    enabled: true
    issuerRef:
      name: my-ca-issuer      # required
      kind: ClusterIssuer
```

That is the whole setup: cert-manager writes the Secret, the pods mount it, and renewals are
picked up on the next handshake.

## Scaling out, and the one thing the chart will not let you do

**More than one proxy replica requires `proxy.tableCacheTtlMs: 0`, and the chart refuses to
render without it** — whether the replicas come from `proxy.replicas` or from the autoscaler.

The reason is a correctness one rather than a tuning one. Every cache is per process, and so is
the record of which snapshot a replica last forwarded for a table. With two replicas at a
non-zero TTL, a client can `loadTable` through one and plan through the other, and that plan is
answered from a view up to the TTL old — it silently **lacks the files committed since**. Pruning
stays correct, because an unindexed file is never pruned; it is the file list that goes stale,
which is the one direction this system does not accept.

A replica that caches nothing cannot hold a stale view, so at TTL 0 the serving role scales
freely. It costs one metadata read per plan.

The autoscaler additionally refuses when `proxy.indexer` is true and no indexer fleet is
configured, because every new pod would then drain the same build queue: a lease stops them
corrupting an index, but it does not make the second builder useful.

## Ingress and TLS: three arrangements, not interchangeable

| | `tls.enabled` | Controller annotation | What the engine sees |
| --- | --- | --- | --- |
| Terminate at the ingress | false | — | The ingress's certificate; the hop to the pod is plaintext |
| Re-encrypt | true | `backend-protocol: HTTPS` | The ingress's certificate; the hop to the pod is encrypted |
| Passthrough | true | `ssl-passthrough: "true"` | **kahshe's own** certificate — the only one that can carry mutual TLS end to end |

The chart cannot choose: it depends on your controller and on what your security review asked
for. Annotations go in `ingress.annotations`.

## Upgrading

Two changes alter an existing release's rendered output. Neither affects a fresh install.

**Pods move off the `default` ServiceAccount.** `serviceAccount.create` defaults to true, so an
upgrade binds the pods to a new account named after the release. If you had annotated the
namespace's `default` account — with an IRSA role, say — those credentials no longer reach the
pod. Either move the annotation to `serviceAccount.annotations`, or set
`serviceAccount.create: false` and `serviceAccount.name: default` to keep the previous behaviour.

**More than one proxy replica now requires `proxy.tableCacheTtlMs: 0`,** and the chart refuses to
render otherwise. A release already running that way is already serving plans that can omit
recently committed files; the refusal is loud where the behaviour was silent. The fix is the one
line the error names.
