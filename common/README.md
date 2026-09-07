# kahshe-common

The primitives every kahshe role shares: one metrics registry, two bounded caches, per-key
single-flight loading, the one digest, and the PEM reader for a private CA.

This is the bottom of the [dependency stack](../docs/ARCHITECTURE.md) and it depends on nothing
of kahshe's — no Iceberg types, no catalog, no storage client, no configuration. Six classes,
roughly 700 lines of source, two third-party libraries (Caffeine and the slf4j API). Everything
above it — `format`, `indexer`, `proxy`, `watch`, `app` — compiles against these types, which is
exactly why they stay this small: a change here is a change everywhere.

| Type | What it is |
|---|---|
| `Metrics` | the process-wide registry, and the Prometheus text a `/metrics` scrape returns |
| `BoundedCache<K,V>` | a synchronized access-ordered LRU with an entry bound and an eviction callback |
| `WeighedCache<K,V>` | a Caffeine cache bounded in **bytes**, for entries whose sizes differ by orders of magnitude |
| `SingleFlight<K>` | per-key mutual exclusion, so one cold load serves every thread that raced it |
| `Hashing` | SHA-256 base64, for cache keys derived from credentials |
| `Pem` | certificates out of a PEM file, and trust managers over a CA bundle and nothing else — for a backing catalog behind a private authority |

## Metrics

One `Metrics` is constructed per process — in `Kahshe`, the app's `main` — and handed to every
component that counts anything. `AdminHandler` serves `metrics.scrape()` at `/metrics` as
Prometheus text format 0.0.4. There is no client library.

The registry has three kinds of member, and the differences matter:

**Counters are public `LongAdder` fields.** A component increments the field it was handed
(`metrics.planCacheHits.increment()`). There is no name lookup, no registration call and no way to
typo a metric into existence — a counter that does not exist does not compile. The cost of that
trade is that a new counter is two edits, the field and its line in `scrape()`, and they are next
to each other in one file. The one exception is the pair of per-tier pruning counters,
`pruneFilesIn` and `pruneFilesKept`: a `ConcurrentHashMap` keyed by the index type's own `key()`
and filled through `prunePass(tier, in, kept)`, because the types are a `ServiceLoader` seam. That
path is string-keyed, so it is the one place a metric can be typed into existence — which is why
the label is always the type's key and never spelled by hand.

**Gauges are `volatile LongSupplier` fields the measured component installs.** The registry never
holds the cache, the index reader or the freshness tracker it reports on; each owner assigns a
method reference at construction:

```java
metrics.indexCacheWeightBytes = cache::estimatedWeightBytes;   // IndexStore
metrics.indexTablesBehind = this::tablesBehind;                // IndexFreshness
metrics.watchRulesLoaded = () -> rules.size();                 // WatchRules
```

Two properties fall out of that. First, `common` can name index tiers, watch windows and proxy
rewrites without depending on any of them — the coupling is a `LongSupplier`, installed by the
owner. Second, every gauge defaults to `() -> 0`, so a scrape is valid before wiring and stays
truthful in a process that does not run the subsystem at all: an indexer-only deployment reports
zero watch rules because it has none, not because the plumbing is missing. `responseRewriteFailures`
is a supplier for the same reason even though it counts events — the adder lives beside the rewrite
code in `proxy`, and the handler installs its sum.

Naming follows the Prometheus convention the scrape emits: counters end in `_total`, gauges do
not. Plan latency is a microsecond total (`kahshe_plan_duration_us_total`) rather than a
histogram; divided by `kahshe_plan_requests_total` it gives a mean, and there are no percentiles.

A counter earns a doc comment when its **absence** or its **presence** means something an operator
would not otherwise learn. `kahshe_indexer_jobs_dropped_total` is the clearest case: a dropped
build is not a failure, so no failure counter moves, and the only other symptom is index staleness
rising with nothing to attribute it to. `kahshe_index_dead_ordinal_percent` and
`kahshe_index_bloom_leaves` are the other shape — structures that only grow during normal
operation, keep answering correctly, and cost more on every cold load while moving no other signal.

## The caches

Both caches exist because **every cache in kahshe is bounded**. They divide on what the bound
counts.

### `BoundedCache` — bounded by entries

A `LinkedHashMap` in access order behind one monitor. Use it when entries are small and roughly
alike: auth verdicts, observed snapshot ids, per-table freshness records, alert dedup keys. `get`
returns null for an absent key and counts as a use.

Its optional constructor takes a `Consumer<V> onEvict`, which exists for one reason: some values
own things the garbage collector will not reclaim. A cached catalog client holds a token-refresh
executor and a pooled HTTP client, and dropping the reference leaks both. The callback runs
**while the cache's monitor is held**, so it must not block — close quietly, or hand the work off.

Every method takes that single monitor, including `computeIfAbsent`. That is fine for a map lookup
and wrong for a load that talks to the network, which is where `SingleFlight` comes in.

### `WeighedCache` — bounded by bytes

Index tiers are not alike. One column's bloom tier can be a thousand times another's, so a bound
on entry count is not a memory bound at all — it is an assumption about the data that any table
can break. `WeighedCache` takes a byte budget and a `ToLongFunction<V>` that reads each value's
precomputed weight, and Caffeine evicts against the total. Its users are the three index tiers —
`IndexStore` (bloom), `TermIndex` (terms, and a second cache for resolved lookups) and
`GramIndex` — plus the proxy's plan cache in `PlanService`, each with its own budget and its own
weight gauge.

Three consequences are worth knowing before you use it:

- **Weights are held in KiB.** Caffeine weighs in `int`, which caps a per-entry weight at 2 GiB
  and would silently break any budget above that. Weighing in KiB keeps large budgets expressible;
  `estimatedWeightBytes()` multiplies back, so gauges report KiB-rounded bytes rather than exact
  ones. Weights are clamped to at least 1 KiB, so a zero or negative weight cannot make an entry
  free.
- **An entry heavier than the whole budget is clamped to the budget and retained.** It evicts
  everything else, which is the lesser evil against re-loading it on every single request — but it
  is a real cost, and a caller that would rather not pay it must refuse the entry *before* putting
  it in. `IndexStore` and `GramIndex` do exactly that, and count the refusal
  (`kahshe_index_too_large_total`, `kahshe_gram_too_large_total`) so it is visible rather than
  merely slow.
- **Nothing expires.** No TTL, no refresh-after-write, no `recordStats`. Freshness belongs to the
  callers, who key on the snapshot and metadata they read; a second, independent expiry policy
  inside the cache could only agree with them by accident.

Maintenance runs on the calling thread (`.executor(Runnable::run)`), so an eviction and its
counter increment are visible as soon as the write that triggered them returns. That keeps the
eviction counters honest against a scrape taken immediately after, and makes the tests
deterministic rather than timing-dependent.

## `SingleFlight`

Cold loads in kahshe are expensive and duplicated: eight threads planning the same table at the
same snapshot all miss the same cache entry and all decide to read the same index leaves out of
object storage. `SingleFlight` collapses that into one load.

```java
V cached = cache.get(key);              // lock-free fast path, the caller's own
if (cached != null) return cached;
return flight.load(key, () -> cache.get(key), () -> loadFromStorage(key));
```

The contract is the check-then-load pair: `check` runs under the key's lock and must return null
**exactly** when a load is still needed. A caller that returns a stale value from `check` gets a
stale value; a caller that returns null when the entry is fine pays for a redundant load.

Why not `BoundedCache.computeIfAbsent`? Because that holds one monitor for the whole map, so a
slow load of one key blocks every caller of every other key. `SingleFlight` locks per key: the
same-key racers wait, different keys proceed in parallel. `BackendCatalogs` uses it for exactly
this — two different bearer tokens must be able to build their catalog clients at the same time.

The invariant that makes it safe to keep the map small: a key's flight is refcounted and pinned
while any thread is inside `load`. A lock in use is never discarded — discarding one would let two
loads of the same key run concurrently, which is the entire thing this class prevents — and the
entry is removed when its last user leaves, so the map holds only keys with a load actually in
flight and never grows without bound. A load that throws propagates to its caller and is not
remembered: the lock is released and the next caller tries again.

## `Hashing`

`Hashing.sha256Base64` exists so that cache keys derived from a bearer token do not contain the
token. `AuthGate` keys its verdict cache on
`sha256Base64(token) + "|" + prefix + "|" + namespace + "|" + table`; `BackendCatalogs` keys
per-caller catalog clients on the digest plus the prefix. The point is what a heap dump, a
debugger or a stray log line can see — the digest is a map key and never a credential, and nothing
in kahshe reverses it or compares it against anything but another digest.

## Extending it

There are no interfaces here, no `ServiceLoader` SPI and no registry to plug into — those seams
live in the modules that need them (alert delivery, for instance, is `AlertSinkProvider` in
`watch`). What `common` offers instead is four functional parameters, and they are the whole
extension surface:

| Seam | Supplied by | What it buys |
|---|---|---|
| `LongSupplier` gauge fields on `Metrics` | the component being measured | a value in `/metrics` without `common` depending on your type |
| `ToLongFunction<V>` weigher | the `WeighedCache` owner | a byte bound over values only you can size |
| `Consumer<V> onEvict` | the `BoundedCache` owner | deterministic release of non-heap resources |
| `Supplier` check/load pair | the `SingleFlight` caller | de-duplicated cold loads over a cache this module never sees |

Adding a counter or gauge is the one change here that touches other people: declare the field with
a comment saying what a non-zero value *means*, add its line to `scrape()` under the `kahshe_`
prefix, and — for a gauge — leave the `() -> 0` default in place so processes that never wire it
still scrape correctly.

The module also publishes one test fixture, `Records.with`, which rebuilds a Java record with
named components replaced at any depth of a record tree. The `format`, `indexer`, `proxy` and `watch` suites use it to override
one setting of a deeply nested config record without naming the path to it; a component name no
record has is an error rather than a silent no-op. Depend on it with
`testImplementation(testFixtures(project(":common")))`.

## What this module deliberately does not do

- **It knows nothing about Iceberg**, or about tables, snapshots, files or indexes. Names like
  `indexBloomLeaves` are metric identifiers, not types — the module compiles with no Iceberg
  artifact on the classpath, and would compile in front of a different lakehouse.
- **It reads no configuration.** All environment reading happens once, in `app`, and arrives as
  constructor arguments. Nothing here consults a system property or an environment variable.
- **It is not a metrics facade.** No histograms, no summaries, no exporters, no registry lookup by
  name. One process, one registry, one text format, one endpoint. The one concession is a label:
  `kahshe_prune_files_in_total{tier}` and `kahshe_prune_files_kept_total{tier}` carry the index
  type's own key, because the types are a `ServiceLoader` seam and a fixed field per tier would be
  wrong the day someone adds one.
- **The caches are not a cache library.** `get`, `put`, `remove` and a size or weight — no loading
  caches, no listeners beyond the two above, no statistics, no asynchronous population. Anything
  richer belongs to the caller that needs it.

## Tests

```sh
./gradlew :common:check
```

`check` includes `javadoc`, which is a real gate rather than decoration: a `{@link}` left pointing
at a member a rename moved fails the build. The suites pin the contracts this README describes —
that eviction is by weight and not by count, that budgets above `Integer.MAX_VALUE` bytes still
weigh entries, that an oversized entry is retained rather than dropped, that an explicit `remove`
is not counted as an eviction, that every evicted `BoundedCache` value reaches the callback, and
that concurrent cold loads of one key run the loader exactly once while distinct keys stay
independent.
