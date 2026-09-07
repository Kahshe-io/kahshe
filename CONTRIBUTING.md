# Contributing to kahshe

kahshe is an index-aware planning proxy for Apache Iceberg REST catalogs. It prunes data files
during scan planning using Parquet sidecar indexes. It never returns rows and never opens a data
file on a serving path.

Before you change anything, read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — in particular
**the invariant**. Everything below serves it:

> kahshe may cause a data file to be SKIPPED only when that file certainly cannot match. On any
> error, doubt, partial state, or staleness, it KEEPS the file.

Keeping a file that cannot match costs a scan and is fine. Pruning a file that *does* match
silently deletes rows from someone's query result, and produces no exception, no log line and no
metric. That is the one error class this system may never have.

---

## Toolchain

| Need | Version | Why |
|---|---|---|
| JDK | **17** | Every kahshe module compiles and runs on 17. |
| JDK | **25** | The overlaid Trino classes in `dev/trino-patch/` compile against Trino 483, whose class files target a much newer JDK. `./gradlew check` type-checks and tests them. |
| Python | 3.10+ | The pySigma backend in `sigma/`. Only needed if you touch it. |
| Docker | any recent | The quickstart stack, and the image build. |

**Two JDKs on purpose.** kahshe itself stays on 17; the overlay needs its own compiler and its own
test launcher, both requested through Gradle toolchains. If only 17 is installed, `check` fails
while *resolving a toolchain* — which reads like a broken build rather than a missing JDK, so
install both up front. Gradle finds any JDK your OS or SDK manager has registered; on CI it is
Temurin 17 and 25.

You do not need to install Gradle. Use the wrapper (`./gradlew`).

---

## The gate

```bash
./gradlew check
```

Run it **before** you change anything, so you know the baseline is green, and again before every
commit. It needs nothing installed and nothing running — no catalog, no object store, no Docker.

`check` is more than tests:

| Part | What it is |
|---|---|
| every module's `test` | the JUnit 5 suites in `common`, `analysis`, `format`, `indexer`, `proxy`, `watch`, `app` |
| every module's `javadoc` | package-level members, `-Xdoclint:all,-missing`, errors only |
| `compileTrinoPatch` | type-checks the overlaid Trino classes against the real Trino 483 artifacts |
| `trinoPatchTestTask` | runs the overlay's own tests, on JDK 25, with the overlay classes ahead of the stock jar on the classpath |

**Do not substitute `./gradlew test`.** The javadoc step is not decoration: a `{@link}` left
pointing at a method a rename just moved passes `test` and fails `check`. The overlay steps are
not decoration either — the overlay is where a false negative once lived with no harness at all,
and its tests run against the resolved Trino jars, needing no Docker and no cluster.

Faster loops while you work:

```bash
./gradlew test                        # whole Java suite, seconds warm
./gradlew :format:test                # one module
./gradlew :watch:test --tests '*WatchRulesTest*'
./gradlew :proxy:test --info          # when you need the logs
```

Test reports land in `<module>/build/reports/tests/test/index.html`.

---

## The pySigma suite

`sigma/` is `pysigma-backend-kahshe`: it compiles Sigma detection rules into kahshe watch rules
(not into a query — see [sigma/README.md](sigma/README.md)). Its suite is **not** part of
`./gradlew check`; run it yourself:

```bash
pip install -e ./sigma pytest
python -m pytest sigma/tests -q
```

### The cross-language round trip

This is the part worth understanding before you edit the backend.

1. The Python suite converts a corpus of Sigma rules and **writes every converted rule** to
   `sigma/tests/generated/`.
2. A Java test — `watch/src/test/java/io/kahshe/watch/rules/SigmaGeneratedRulesTest.java` — loads
   all of those files through kahshe's own rule loader and asserts **nothing is skipped**.

A Python suite alone can only check that the converter agrees with its author. The round trip
checks that the converter agrees with the *consumer*, across a language boundary, and it has
caught defects the Python assertions structurally could not: a numeric comparison emitted as a
quoted string (kahshe refuses it, because a comparison's value must *be* a number), and a regular
expression emitted unquoted, whose brackets closed the YAML flow sequence early so the whole file
parsed to zero rules and zero skips — which reads as an empty rules file rather than an error.

So: **if you change the backend, regenerate the fixtures and run the Java test.**

```bash
python -m pytest sigma/tests -q          # rewrites sigma/tests/generated/
./gradlew :watch:test                    # SigmaGeneratedRulesTest reads them
git status sigma/tests/generated         # commit whatever moved
```

A committed fixture that agrees with a stale backend is exactly the failure this arrangement
exists to prevent, so the fixtures are generated output and belong in the same commit as the
backend change.

---

## Testing discipline

The suite needs nothing external. Index tests build a real Iceberg table through `HadoopTables`
over a JUnit `@TempDir` (`indexer/src/testFixtures/java/io/kahshe/indexer/LocalTableFixture.java`),
so they exercise the genuine build path against `file://` rather than a mock of it. Mockito is
used in a handful of places and is not the default reach.

**Adding a test — in this order:**

1. **Write the paragraph first.** If you cannot say which failure the test catches and what that
   failure costs, you are writing coverage, not a guard.
2. **Delete the guard. Watch the test go red. Put the guard back.** This is not optional here. A
   test that stays green with the code it protects removed is worse than no test, because it
   reports safety that does not exist. Say in the commit message which deletion you made red.
3. **Prefer an oracle to a bare assertion** when two implementations must agree. Self-consistency
   proves nothing when the danger is drift. The suite already checks `GramAccumulator` against
   `Grams.Contract.gramsOf` — the build's view of the gram cut against the probe's, which a rule
   must keep in step (`GramRule.forEachWindowUnits` against `forEachWindow`) — and term counts
   against a plain `HashMap<String,Long>`.
4. **Pin properties, not tuning knobs.** Assert `spills >= corpus.length`, not an exact spill
   count that a buffer-size change would break for no reason.
5. **Test the invariant by degrading a layer and asserting a matching file survives** — not by
   asserting that the invariant holds. Turn a tier off, corrupt a leaf, narrow the coverage: the
   file that matches must still come back.

**What the suite cannot do, stated so you do not assume it away.** Every test runs on a handful of
files. Nothing in it exercises scale, so "the suite is green" and "this works at size" are
different claims — if your change is only tested small, say so in the commit message and in the
docs you touch.

**One concern per commit**, and run the gate before each one.

---

## Rules that are not negotiable

Each of these has been proposed, and some have been merged and reverted. They are here so the
next proposal starts from the reason.

| Never | Because |
|---|---|
| Translate `LIKE '%x%'` into a token `MATCH` | Tokens are maximal `[a-z0-9]` runs (plus whole compound identifiers — IPs, UUIDs, dashed hex). A row holding `trace=abc123def` *contains* `abc123`, but its token set does not. A token match would prune that file and lose the row. `LIKE '%x%'` maps to `CONTAINS` or to nothing; only the anchored `LIKE 'x%'` reaches the term dictionary. |
| Make a tier lenient so a build can finish | A term aggregate that is merely *missing terms* prunes the files that contain them. The term tier fails builds loudly instead of shipping something partial. (The gram layer may be dropped wholesale when over budget — the table degrades to blooms, which is slower and still correct. That is a different thing.) |
| Treat an unreadable index artifact as "term absent" | Absent prunes. Unreadable must keep every file. The branch that enforces it is the catch in `format/src/main/java/io/kahshe/format/IndexPruner.java`, and every tier reader applies the same rule to a leaf it refuses. |
| Key anything on a value that moves — a position, a column name, a shared sentinel | Every false negative found in this project so far came from this. Identify by identity: field ids, allocated ordinals, digests. |
| Change `Analyzer` behaviour without a new analyzer id | The build tokenizes data with it and the query tokenizes predicates with it. Change it in place and every existing index silently disagrees with every new query, and no self-consistency test notices. See below. |

### The analyzer is a pinned contract

A tokens column is built under an analyzer id that carries the family *and* the resolved
token-length cap (`kahshe-ascii-v3-max256` at the default); a whole-value column under
`kahshe-value-v1-max<n>`. Older families are read under their own contracts. The cap an index was
built under is part of its identity, and the term reader refuses any id outside the family it can
read.

The rule that follows: **anything that changes what an index CONTAINS must be recorded in the
index metadata**, so build and read cannot drift. If you change tokenization, canonical value
forms, or the gram rule, mint a new id rather than editing the old one's meaning. The existing
indexes then keep being read correctly under their own contract while maintenance rebuilds them.
`AnalyzerTest` holds golden outputs for exactly this reason, and
`format/src/test/resources/conformance/v1/` is a frozen artifact plus its corpus and manifest,
recut only on request:

```bash
./gradlew :format:test -Dkahshe.conformance.regenerate=true   # only when you mean it
```

[docs/FORMAT.md](docs/FORMAT.md) is the normative description of the bytes. If you change the
format, change that document in the same commit.

---

## Documentation is part of the change

**Every commit that changes behaviour updates the affected documentation in the same commit.** Not
afterwards, not in a cleanup pass. In order:

1. **Delete what your change made false.** This is the step people skip. A superseded number, a
   mechanism that no longer works the way the prose says — take it out. Wrong information is worse
   than missing information, because a reader trusts it.
2. **Add what your change made true**, including the parts you are not proud of: a new sharp edge,
   a threshold someone will want to tune, a guard that only works because of something non-obvious
   elsewhere.
3. **Re-verify anything nearby.** If you edited a file the docs cite, grep that the symbol still
   exists and still means what the doc says.
4. **State the caveat.** If a change is only tested at small scale, write that down.

Two standing checks:

- `docs/CONFIGURATION.md` carries every variable and its default, and must have **zero drift** from
  the code. The check is worth running rather than trusting: diff every `KAHSHE_*` literal in every
  module's `src/main/java` against that table. There are two readers — `app/Kahshe.java` and
  `app/ServerTls.java` — so a grep that stops at the first one misses the whole TLS block. A
  variable added and left undocumented is the normal failure mode. README's "Deploying it" table is
  a deliberate subset, the settings that are decisions rather than tuning, and does not grow with
  the code.
- **When you change a number in code, grep for the old number.** Documented constants have
  contradicted the code repeatedly.

Where a claim is assertable, **assert it** rather than writing it down.

Cite a file plus a greppable symbol, never a line number — line numbers rot into pointing at
unrelated code within days.

---

## Comments and javadoc

A comment earns its place by saying what the signature cannot:

- the contract and its edge cases — "null when the column is absent, never empty"
- an invariant the code depends on that a reader would otherwise break
- a constraint that is surprising — "callers must hold the lease; this does not check"
- a failure mode and what happens on it — "keeps every file rather than throwing"
- *why* a non-obvious shape was chosen, in one or two sentences

A comment does **not**:

- narrate history — "this used to be X", "before the refactor this was Y"
- recite measurements — a runtime, a corpus size, a date
- record test provenance — "verified red by", "found by TestX"
- argue at length against alternatives, or re-explain the project's philosophy

Most javadoc is one to three sentences. A class-level block may run longer when the type is a seam
a third party implements — `Scanner`, `AlertSink`, `IndexType`, `TableSource` — but even then aim
under ten lines. If you are writing a fourth paragraph, that content belongs in the module README.

The audience is a junior engineer who must *use* the class and an expert who must *change* it.
Both are served by short and precise.

---

## Module layout

Seven Gradle subprojects, one per role or layer. The root builds nothing of its own: it holds the
rules every module shares plus the Trino overlay tasks. Each module has its own README with the
detail.

| Module | Package | What it owns | Depends on |
|---|---|---|---|
| [`common/`](common/README.md) | `io.kahshe.common` | metrics registry, the bounded and byte-weighed caches, single-flight | nothing of ours |
| [`analysis/`](analysis/README.md) | `io.kahshe.analysis` | what a **value** means — canonical forms and tokenization | nothing of ours |
| [`format/`](format/README.md) | `io.kahshe.format` | the artifact: each tier's reader and writer, coverage, the lease, the pruner | `common`, `analysis`, Iceberg |
| [`indexer/`](indexer/README.md) | `io.kahshe.indexer` | the read pass over data files, build orchestration, maintenance | `format`, `analysis`, `common` |
| [`proxy/`](proxy/README.md) | `io.kahshe.proxy` | the REST passthrough, the planning and count routes, catalog clients | `indexer`, `format`, `analysis`, `common` |
| [`watch/`](watch/README.md) | `io.kahshe.watch` | detection rules, discovery, the row scan, alert delivery | `indexer`, `format`, `analysis`, `common` |
| [`app/`](app/README.md) | `io.kahshe` | `main`, the roles, environment read once into config records, external index IO | everything |

Also in the tree:

- [`sigma/`](sigma/README.md) — the pySigma backend (Python, its own suite)
- [`dev/trino-patch/`](dev/trino-patch/README.md) — two overlaid Trino classes and their tests
- [`helm/kahshe/`](helm/kahshe/README.md) — the chart, with the deployment shapes
- [`docs/FORMAT.md`](docs/FORMAT.md) — the index format specification

**The dependency direction is enforced by the build files, and it is one-way.** `format` never
depends on the indexer, the proxy, the watch, or a storage client — the app registers one. `proxy`
never depends on `watch`. `analysis` depends on nothing of ours at all — not `common`, not Iceberg
— so a second implementation in another language can read it as a specification without the
artifact, and a row-scan-only watcher gets the rules without the index. The one permitted backward
edge is **test-only**: `format`'s round-trip tests build an index through `indexer`, and borrow its
test fixtures.

If you find yourself needing a new edge, that is a design conversation, not a build-file edit.

---

## Running it locally

### The quickstart stack

All you need is Docker. This brings up Apache Polaris as the backing catalog with kahshe in front
of it, then seeds a four-file demo table whose `msg` column is indexed:

```bash
docker compose up -d --build
docker compose run --rm seed
```

The seeder waits for the automatic indexer and prints verified plan results — including the two
substring cases min/max statistics can never prune. Plan against it yourself:

```bash
TOKEN=$(curl -s -X POST http://localhost:8282/v1/oauth/tokens \
  -d 'grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])') \
&& curl -s -X POST \
  http://localhost:8282/v1/lakehouse/namespaces/logs/tables/events/plan \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"filter":{"type":"contains","term":"msg","value":"disk pressure"}}' \
  | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["file-scan-tasks"]), "of 4 files to scan")'
```

Ports: `8282` is the data plane (Iceberg REST), `8283` is admin (`/healthz`, `/readyz`,
`/metrics`). The stack is ephemeral by design — `docker compose down -v` resets everything.

The image build is worth running when you touch module structure, because it is the one path the
Java suite does not exercise:

```bash
docker build -t kahshe:dev .
```

### The distribution, without Docker

```bash
./gradlew installDist
KAHSHE_BACKEND=http://your-catalog:8181/api/catalog \
KAHSHE_CREDENTIAL=client:secret \
app/build/install/kahshe/bin/kahshe
```

The same distribution carries the CLI for manual index backfills:

```bash
app/build/install/kahshe/bin/kahshe index <prefix> <ns.table> <column>
```

### The zero-config client flip

A completely stock Iceberg 1.11+ Java client pointed at kahshe loads `RESTTable` and plans
server-side, with no client configuration. That is the claim the whole proxy rests on, so there is
a demo that proves it against the running quickstart stack:

```bash
./gradlew installDist -q && java -cp "app/build/install/kahshe/lib/*" io.kahshe.ClientDemo
```

### Indexing is a table property

There is no index DDL and no index service call:

```sql
ALTER TABLE logs.events SET PROPERTIES ('kahshe.index' = 'msg');
```

kahshe observes the property in passing `loadTable` traffic and keeps the column's indexes current
in the background. A stale index costs performance and never correctness, which is why this is
safe to leave running while you develop.

---

## Sending a change

- Branch from `main`. One concern per commit.
- `./gradlew check` green, plus `pytest sigma/tests` if you touched `sigma/`.
- For every guard you added, say which deletion you watched go red.
- Documentation updated in the same commit — including the deletions.
- If the change is only tested at small scale, say so.

CI runs the Gradle build on JDK 17 and 25, the pySigma suite on Python 3.11, and the container
image build. It is the same gate you ran locally, so a green local run should stay green.
