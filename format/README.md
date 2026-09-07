# kahshe-format

The index artifact: the bytes kahshe writes beside an Iceberg table, the readers that answer from
them, and the pruner that turns a scan plan into a shorter one.

Every indexed column gets up to three tiers of evidence — an n-gram bloom filter per data file, an
exact gram-to-file bitmap, and a term dictionary — stored as Parquet leaves and JSON metadata under
an index root. Nothing here plans a build or serves a request; this module knows only how to write
the artifact, how to read it back, and how to use it to prove that a data file cannot satisfy a
predicate. It depends on `common`, on `analysis`, and on Iceberg's own reading and writing
libraries, and on nothing else in the repository.

**The normative reference is [`docs/FORMAT.md`](../docs/FORMAT.md)** — layout, metadata fields, leaf
schemas, versioning rules, and what a second implementation must do. This README does not repeat it.
It describes the shape of the code: what the types are, how data moves between them, where a third
party can plug in, and why the design is what it is.

---

## The one invariant

**Advisory keep.** An index may only remove files that cannot hold a match. Everything else in this
module follows from that, including most of its error handling.

The direction matters and is not symmetric. Keeping a file that holds no match costs a scan. Dropping
a file that does hold one is a wrong answer with nothing to indicate it happened. So every failure
mode resolves toward keeping:

- A file outside a tier's coverage is kept by that tier.
- A metadata document that cannot be read, or whose version is newer than this reader, means the
  tier is **absent** — not empty. Absence keeps every file, loudly.
- An index type that throws while answering is treated as absent (`IndexPruner.pruneWith`), and the
  rest of the plan proceeds.
- A leaf that will not fit the cache budget is refused **before** it is opened, since loading it and
  then refusing would already have paid the cost being avoided.
- A term the artifact's own analyzer contract would not have admitted is never probed for. Absence
  prunes, so probing for a token the build never wrote would prune the files that match.

The consequence worth internalizing: a stale, partial, corrupt or unreadable index makes planning
slower, never wrong. That is what makes the format safe to evolve and safe to run half-built.

---

## Architecture

### The tiers

Three index types, consulted cheapest proof first (`IndexTypes.COST_ORDER`):

| tier | key | proves | reader | writer |
|---|---|---|---|---|
| bloom | `bloom` | "no value in this file contains these grams" — probabilistic, false positives only | `IndexStore`, `NgramBloom` | `BloomLeaf` |
| gram | `grams` | the same question, **exactly**, over the files it covers | `GramIndex` | `GramIndexWriter` |
| term | `aggregate` | which files hold an analyzed term, a term prefix, or a range of the dictionary | `TermIndex` | `TermIndexWriter` |

The gram tier is authoritative over the bloom for a file it covers: a bloom could only re-approve
what an exact answer already kept. `IndexType.GramProbes` resolves each candidate's gram answer once
per plan and both tiers read it, so the two answer one loop between them.

The bloom and term tiers each have their own directory and metadata document; the GRAM tier has
neither. Its leaf lives in the term tier's directory and its coverage sits in the term tier's
metadata, so `TermIndexType.write` publishes both in one metadata write. This is the main place the type abstraction is thinner than it
looks, and `IndexType.PublishContext` is one shared record rather than one per type because of it.

### The plan path

```
IndexPruner.prune(table, filter, hints, tasks)
  │
  ├─ collect()          conjunctive positions of the expression tree only —
  │                     a predicate under OR or NOT cannot prune on its own
  │                     → Candidate (EQ / STARTS_WITH, canonical literals)
  │                     → RangeCandidate (>, >=, <, <= on STRING, tightened pairwise)
  ├─ resolveColumn()    field-id references resolve to a full column path
  │
  └─ for each IndexType in IndexTypes.inCostOrder():
         type.load(ReadContext)  →  null means absent, keep everything
         type.prune(loaded, Probe, tasks)
```

Literals are converted to the canonical form the build wrote (`Canonical.form`, keyed by
`IcebergKinds.of` — a bigint's decimal text, a UUID's undashed hex, a binary's hex). A literal in a
form this index does not write makes the predicate not ours, and every file is kept.

### The build path

The module does not orchestrate a build; the `indexer` module does. What lives here is everything
the build writes through:

```
per data file        GramAccumulator      distinct grams, packed as primitive longs
                     TermCounts           one file's term → count, for watch rules
                     RunBuffer            terms appended raw into a fixed byte arena
                          │  arena full → sort in place, coalesce, write a sorted run
                          ▼
local scratch        TermRun.Writer       (term, ordinal, count), front-coded varints,
                     TermRunStore         range byte offsets recorded per run
                          │
                          ▼
merge                RunMerger            k-way min-heap, one merge per term range,
                                          cascading through intermediate runs past 256 sources
                          │
                          ▼
publish              TermIndexWriter.finish   leaves first, then the metadata naming them
                     BloomLeaf.write          appends, or compacts past 8 leaves
                     BuildReport.write        the record of the build, for anyone outside it
```

Guarding all of it: `BuildLease`, one builder per column at a time; `IndexScope`, which narrows what
a build reads; and `Coverage`, which decides what ordinal each data file is known by.

---

## The seams

Three things a third party can implement or register.

### `IndexType` — a whole new tier

Discovered by `ServiceLoader` from `META-INF/services/io.kahshe.format.type.IndexType`. The
built-ins are declared the same way and hold no privilege beyond a fixed place in the cost order;
anything discovered beyond them runs after them, in discovery order.

Five methods: a `key()`, a `Collector` for the build's read pass, a `write()` at publish, a `load()`
at serve, and a `prune()`. Implementing one buys a new kind of evidence in every plan and every build
without touching the pruner or the builder — they iterate the registry rather than naming tiers.

Two rules make this survivable across implementations. A reader must **ignore a `leaves` key it does
not own**: an index carrying an extra key alongside `aggregate` and `grams` is a valid index to a
reader that knows only those two. And a key a reader does own but cannot open is *absent*, never
empty. `Collector` calls for one file are ordered and contiguous, but several files are read
concurrently, so a collector holding state must be safe for that.

### `GramRule` — a different way to cut a value into grams

Discovered from `META-INF/services/io.kahshe.format.type.gram.GramRule`. A rule owns a prefix of the
gram-id space and the whole of how a value becomes grams. The built-ins are `kahshe-grams-v1`
(windows of three UTF-16 units — read, never written, because it can split a surrogate pair) and
`kahshe-grams-v2-n<size>` (windows of whole code points, size 2–8), and they are registered first and
by identity, so nothing on the classpath can shadow an id kahshe already writes.

Implementing one buys a gram alphabet the built-ins do not express. The hard requirement is
determinism per id: a rule that cuts differently for one id in two processes desynchronises every
index it wrote. The cut has one definition, in the rule: probing goes through
`GramRule.forEachWindow` and the build through `GramRule.forEachWindowUnits`, whose default
decomposes the first into primitive units — so implementing `forEachWindow` alone gives you an
index your own probe can read. Overriding the units form buys the build its allocation-free path
(`GramAccumulator` walks every gram position of every value and cannot afford a String each) and is
the only place the two views can be made to disagree; an override that does disagree builds under
one cut and probes under the other, which loses files silently. An id no loaded rule owns is
refused rather than guessed at.

### `IndexPaths.externalIo` — how an external index root is reached

Not a `ServiceLoader` seam but a registration: the format constructs no storage client of its own.
An application registers a `Function<FormatConfig, FileIO>` at startup; kahshe's own resolves the IO
the way Iceberg does, from a class name plus dotted properties, and an embedder registers whatever it
likes instead.

Analyzer contracts — what a token is, what a canonical value is — are the `analysis` module's seam,
not this one's.

---

## Why the code has this shape

### Ordinals are allocated, never positional

The ordinal is the join key between the coverage list and every bitmap in the gram and term tiers.
It is the single identity the whole format rests on, so `Coverage` is deliberately conservative
about it.

A departed file keeps its slot, marked dead; new files are numbered from one past the highest
ordinal ever issued. Nothing an existing bitmap says has to change when the table changes.

- **A dead ordinal is harmless** because the pruner resolves a data file to an ordinal *by path*, and
  only live entries are in that map. A dead ordinal is one no live path resolves to. The dangerous
  direction — a live path resolving to an ordinal describing a different file — is exactly what
  allocation prevents.
- **A returning path is revived, not renumbered.** Iceberg data files are immutable, so a path that
  comes back has the bytes it had when it was indexed and the bitmaps naming its old ordinal still
  describe it. A second ordinal would put two entries in the map and force a needless re-read.
- **Tombstones are cheap but not free forever**: a table on rolling retention eventually carries
  coverage that is almost entirely dead. `Coverage.renumberLive` reclaims it, and is safe only when
  every bitmap is rewritten through its `remap` in the same build.

### Field id, never column name

Directories are keyed by Iceberg field id. A name is not a stable identity: rename a column and the
name moves; drop and re-add it and a different field wears the old label. Blooms are keyed inside by
data-file path, which survives both — so a name-keyed directory would hand a new column the old
column's blooms and prune files that genuinely match. Field ids are assigned once and never reused.

A data file that carries no ids of its own — anything `add_files` or a Hive migrate adopted — is
resolved by name through the table's `schema.name-mapping.default`, which both read sites supply
(`DataFileIds`). Iceberg's remaining fallback assigns ids by column *position*, and a position
equals a field id only for a schema that has never dropped, reordered, or added a column out of
order; where they diverge the build publishes one column's values under another column's id and
the index prunes exactly the files that match. A file with neither ids nor a mapping should be
refused, and today is not — it still takes that fallback.

### Immutable leaves, mutable pointer

A metadata document is the one mutable object in its directory, rewritten whole on every publish.
Everything it names is immutable and carries a per-write nonce, which is load-bearing twice:

- **Staleness.** Readers revalidate on a fingerprint derived from the document — snapshot id plus
  leaf names — and the index uuid is stable across rebuilds. Without a nonce, a rebuild at the same
  snapshot would fingerprint identically and a reader holding the old generation would never reload.
- **Clobbering.** Two builds racing on one snapshot would otherwise write the same object. Distinct
  names leave the loser's leaf an orphan rather than a half-overwritten file a reader is midway
  through.

The writer never deletes a superseded leaf; collection is an operator's task.

### Freshness is a token, and the token comes from the payload

All three readers cache with a 30-second TTL and revalidate by re-reading only the small metadata
JSON. The fingerprint is derived from the *already-parsed* document — reading it once to fingerprint
and again to load is a race, and a bitmap's ordinals are positions in one generation's file list, so
a payload stamped with the wrong token prunes files that match.

`TermIndex` also caches resolved `(fingerprint, token)` lookups, including absences. A cached absence
prunes, so a stale one is a file silently dropped from someone's answer; that is safe only because
the key is the fingerprint, which moves whenever any leaf's bytes move.

### The dictionary is never loaded

On identifier-heavy text the term dictionary approaches one entry per source row, so materializing it
is not an option at any scale worth indexing. Instead it is partitioned into 36 ranges by first
character (`TermRanges`), written in sorted term order with 1 MiB row groups. A query touches at most
one leaf per distinct first character and, within a leaf, only the row groups whose statistics could
hold the token. What a reader actually loads per column is the metadata and the coverage list —
because every pruning decision needs the file list to interpret any bitmap at all.

The ranges tile the **entire** string space, not the analyzer's alphabet: range 0 runs from negative
infinity, the last to positive infinity. A term that fell into a gap would be a term the index does
not know about, and that is a file wrongly pruned. The partition is also monotonic in unsigned-byte
term order, which is what lets each range merge independently and be carried forward untouched.

### The build's memory is a number you can write down first

A reader appends terms into a fixed `RunBuffer` — a `byte[]` arena plus an `int[]` offset index —
and flushes a sorted run when either fills. Peak cost per reader is
`arenaBytes + 4 * (arenaBytes / 8)`, known before the first file is opened, and nothing else in the
term path grows with the corpus. Duplicates collapse at the flush, so a file of prose and a file of
unique trace ids cost the same.

The merge is bounded the same way. `k` is the total run count — thousands on a large corpus, more
descriptors than a host will give — so `RunMerger` cascades through intermediate runs past 256
sources, and runs one merge per range on a bounded pool. Each range is a byte slice of every run, so
the ranges are genuinely independent.

### Contracts are read from the artifact, never from configuration

The analyzer id and the gram rule id are recorded in the metadata, and readers take their admission
rules and gram cuts from there. A proxy configured differently from the build must not probe for
tokens the build never wrote, because absence prunes. An id no loaded family owns is refused, which
keeps every file and says so — which is what makes changing a contract survivable at all.

### Version refusal points both ways

A document newer than the reader, on the Iceberg spec's `format-version` or on kahshe's own tier
version, is refused. On the read path that keeps every file. On the build path the refusal is
deliberately allowed to propagate, so an older kahshe never builds over a newer index and downgrades
it in place.

### The lease is a guard rail, not a mutex

A build publishes the bloom tier's metadata and then the term tier's, and the two publishes are not
atomic with each other, so two builders of one column could interleave them. `BuildLease` refuses
loudly when another owner's unexpired lease exists.

It is not mutual exclusion, and does not claim to be. `FileIO` has no atomic create-if-absent on
object stores, so two builders in one read-then-write window can both believe they acquired the
lease. `assertStillHeld`, called immediately before the first publish, narrows the exposure from a
whole build to a single round trip: whichever write landed last owns it, the other aborts before
writing any metadata. A lease past its 12-hour TTL, or one that cannot be read at all, is overridden
with a WARN — blocking every build forever on a corrupt leftover is the worse failure.

---

## What this module deliberately does not do

- **It does not run a build.** Reading data files, deciding what to index, scheduling and retrying —
  all of that is the `indexer` module. This module is what a build writes *through*.
- **It does not serve requests.** No HTTP, no catalog protocol, no query engine integration.
- **It does not construct storage clients.** An application registers a `FileIO` factory
  (`IndexPaths.externalIo`); the module has no S3, GCS or ADLS code and no credentials of its own.
- **It reads no environment variable**, and everything an operator normally tunes arrives in
  `FormatConfig`. The one exception is the distinct-term ceiling in `TermCounts`, taken from the JVM
  system property `kahshe.watch.max.distinct.terms`, because it is a safety valve rather than a
  knob. Setting names appear only inside messages, so an operator reading one knows which knob to
  turn.
- **It does not delete anything it superseded.** Every leaf a document names is referenced;
  reclaiming the rest is an operator's task, with its own grace period.
- **It does not index below file granularity.** Bitmaps name data files, not rows or row groups.
  This is a planning index: it makes a scan shorter, and the engine still reads what survives.
- **It does not rank or score.** There is no relevance model. A term entry carries a file bitmap and
  an occurrence count, and the count is not even guaranteed exact once files have departed.
- **It does not index more than one column per index.** `key-column-ids` is a list because the draft
  index spec this format once aligned to spelled it that way; that vocabulary has since left the
  draft (`docs/FORMAT.md` §9), and kahshe always writes exactly one element.
- **It does not make the two tier documents atomic with each other**, and does not assert that a
  single `createOrOverwrite` is atomic either — that is the `FileIO`'s property, not the format's.
  A reader must tolerate any pairing of the two tiers' generations.
- **It does not guarantee one builder per column.** See the lease, above.

---

## Layout

```
io.kahshe.format
  IndexPruner        the plan path: candidates out of the filter tree, tiers in cost order
  Coverage           file → ordinal, tombstones, renumbering
  IndexPaths         where the index lives and which FileIO reaches it
  IndexScope         the table property that narrows what a build reads
  FormatConfig       everything a reader or writer needs; no environment access
  IcebergKinds       the only place Iceberg's type system meets the canonical forms
  DataFileIds        the table's schema.name-mapping.default, so a data file with no field ids
                     resolves by name and not by column position
  BuildLease         one builder per column at a time
  BuildReport        the record of one build, in Iceberg REST metrics vocabulary

io.kahshe.format.type
  IndexType          the seam; IndexTypes the ServiceLoader registry and cost order

io.kahshe.format.type.bloom
  BloomIndexType  NgramBloom  BloomLeaf  IndexStore  IndexMeta

io.kahshe.format.type.gram
  GramIndexType  GramAccumulator  GramIndex  GramIndexWriter
  Grams  GramRule  GramRules  CodePointGramRule  Utf16GramRule

io.kahshe.format.type.term
  TermIndexType  TermIndex  TermIndexWriter  TermRanges
  RunBuffer  RunMerger  TermRun  TermRunStore  TermCounts
```

---

## Tests

```sh
./gradlew :format:check
```

`check` includes `javadoc`, which is a real gate rather than decoration: a `{@link}` left pointing at
a member a rename moved fails the build. The suite pins the properties this README claims, on the
side where they can actually go wrong — that a document one version too new, a type that throws
while answering, and a file outside the index's scope each keep every file rather than prune one;
that a
rebuild at the same snapshot moves the revalidation token through the leaf names, so neither a
cached index nor a cached absence outlives the bytes it was read from; that a departed file's
ordinal is never reissued and a returning path is revived at the one it had; that the accumulator's
inline cut reproduces the reference cut at every gram size, including v1's split surrogate pairs;
that a spilled build writes the aggregate rows an in-memory one would, and a merge answers what a
map over everything that went in would; and that a tier too heavy for the cache budget is refused
rather than loaded and pinned there, with the remaining tiers still pruning.

A conformance fixture under `src/test/resources/conformance` pins two whole indexes, a scalar
`string` under the tokens contract and a `list<string>` under the whole-value contract — every
metadata document and every leaf — and the tests beside it check this module's writer against those
bytes and its reader against those semantics. A second implementation can be held to the fixture
and to [`docs/FORMAT.md`](../docs/FORMAT.md) rather than to the Java.
