# kahshe index format

This is the reference for what kahshe writes beside an Iceberg table and how a reader
must interpret it. It describes the artifact as the code at the commit that ships this
file writes and reads it; where a rule is enforced by a symbol, the symbol is named so
the claim can be checked. It is not a plan and carries no history.

Stability: the layout, the metadata documents, the leaf schemas and the analyzer
contracts below are pinned. Anything that changes what an existing reader would
conclude from an existing artifact takes a new version number (§8) or a new analyzer
id (§6.3); nothing else does.

Vocabulary: where the Apache Iceberg draft index specification (apache/iceberg#16961,
`format/index-spec.md`) pins a name, that name is used (`format-version`, `uuid`,
`table-uuid`, `location`, `type`, `properties`, `snapshots`). `transform-function` and
`key-column-ids` are kept from an earlier revision of the draft that no longer carries them.
§9 lists where this artifact diverges from that draft.

### Resolving a data file's columns

The artifact is keyed by field id, so a builder reading a data file must resolve that file's
columns to field ids the same way every other Iceberg reader does, in this order:

1. **Field ids written into the Parquet**, when the file carries them.
2. **The table's `schema.name-mapping.default`**, which maps column names to ids. A table created
   by `add_files` or a Hive `migrate` keeps its original Parquet — which has no ids — and Iceberg
   writes this property so readers resolve by name.
3. Iceberg's fallback, which assigns ids **by column position**.

**Step 3 must not be reached.** A field id equals a column's position only for a schema that has
never dropped, reordered, or added a column out of order; where they diverge, a builder taking
that branch reads one column's values and publishes them under another column's id. The artifact
then declares coverage of every file while holding the wrong tokens, and a probe prunes exactly
the files that match — a wrong answer, with nothing to observe. An implementation that cannot
resolve a file by id or by name must refuse to index it rather than fall through to position.

**kahshe does not yet enforce that refusal.** Stated here because this document otherwise describes
the artifact as the shipping code writes and reads it, and this is a rule on implementations that
the shipping code does not yet meet. Both read sites supply the mapping when the table declares one
(`DataFileIds`, `IndexBuilder.readFile`, `ScanPass.scanFile`), and a mapping that exists but will
not parse is refused loudly; a file carrying neither field ids nor a mapping still falls through to
position. Refusing it needs the Parquet footer read before the projection
(`ParquetSchemaUtil.hasIds`), which neither read site does today.

## 1. Layout

Vocabulary: the bloom filter, the gram leaf and the term dictionary are **index types**, the
pluggable unit an implementation exposes behind one interface (§7). This document also calls
them **tiers** where their order matters: a reader consults them cheapest first, and a query
that one answers never reaches the next.

Every indexed column of a table has up to two directories under one **index root**
(`IndexPaths.root`):

| root | rule |
|---|---|
| default | `<table location, one trailing "/" stripped>/_index` |
| configured (`KAHSHE_INDEX_ROOT`) | `<root, one trailing "/" stripped>/<table uuid>-<hash>` where `hash` is the first 4 bytes of SHA-256 over the table location's UTF-8 bytes as 8 lowercase hex characters (`IndexPaths.locationHash`) |

Directories are keyed by the column's Iceberg **field id**, decimal, unpadded — never by
name, so a rename changes nothing on disk:

| tier | directory | metadata document |
|---|---|---|
| bloom | `ngram-bloom-f<fieldId>/` (`IndexMeta.dir`) | `index-metadata.json` (`IndexMeta.metaPath`) |
| term and gram | `term-v1-f<fieldId>/` (`TermIndexWriter.dir`) | `index-metadata.json` |

A metadata document is the one **mutable** object in its directory: it is rewritten
whole (`createOrOverwrite`) on every publish. Everything it points at is an
**immutable leaf**, named with the source table snapshot id and a **nonce** — 32 random
bits as 1–8 unpadded lowercase hex characters
(`Long.toHexString(ThreadLocalRandom.current().nextLong() >>> 32)`) — so a rebuild of
the same snapshot writes a new object rather than rewriting one a reader may hold open:

| leaf | name |
|---|---|
| bloom | `ngram-bloom-f<id>/leaf-<snapshotId>-<nonce>.parquet` (`IndexMeta.leafPath`) |
| gram | `term-v1-f<id>/grams-<snapshotId>-<nonce>.parquet` (`GramIndexWriter.leafPath`) |
| term aggregate | `term-v1-f<id>/aggregate-<snapshotId>-r<NN>-<nonce>.parquet`, `NN` the term range, two digits zero-padded (`TermRanges.leafName`) |

Rules a writer keeps and a reader may rely on:

1. **Leaves before pointers.** Every leaf is fully written before the document that
   names it is rewritten (`TermIndexWriter.finish`, `BloomLeaf.write`).
2. **Generations coexist by pointer swap.** A reader that has loaded a document keeps
   reading the objects it named; the next document may name a different set. A leaf
   is never rewritten in place under a name a document has published; a restamp
   (§8.4) rewrites only the document.
3. **The two tiers' documents are not atomic with each other.** The bloom document is
   published first, then the term document. A reader must tolerate any pairing.
4. **Whether one `createOrOverwrite` is atomic is the `FileIO`'s property**, not this
   format's; the code does not assert it.
5. **Superseded leaves are not deleted by the writer.** Collection is an operator's
   task (§8.5).

## 2. The metadata envelope

Both documents are JSON objects with these top-level members (order as written by
`TermIndexWriter.finish` and `IndexMeta.toJson`; Jackson pretty-printed, UTF-8):

| key | type | term document | bloom document |
|---|---|---|---|
| `format-version` | number | `1` (`TermIndexWriter.FORMAT_VERSION`) | `1` (`IndexMeta.FORMAT_VERSION`) |
| `uuid` | string | stable across rebuilds: the prior document's value, else a fresh random UUID | same rule |
| `table-uuid` | string | the table's uuid | same |
| `location` | string | the tier directory | same |
| `type` | string | `"term-dictionary"` | `"ngram-bloom"` |
| `transform-function` | string | `"IDENTITY"` | `"HASH"` |
| `key-column-ids` | array of number | `[fieldId]`, exactly one | same |
| `properties` | object of string → string | §5.1 | §3.1 |
| `snapshots` | array | exactly one object, §5.2 | exactly one object, §3.2 |

`snapshots[0].snapshot-id` is always `1`; the table snapshot the build ran at is
`snapshots[0].source-table-snapshot-id`. A reader takes `snapshots[0]`
(`TermIndexWriter.snapshotNode`, `IndexMeta.parse`).

Legacy shapes a reader still accepts are listed in §8.3; a build never composes them, but a restamp (§8.4) republishes a legacy document in the shape it found it, adding only the spec fields it lacks.

## 3. The bloom tier

One n-gram Bloom filter per data file, keyed by data-file path. It is a
**substring** tier: it answers "might this file hold a value containing this text"
and never "which term".

### 3.1 `properties`

| key | value |
|---|---|
| `column` | column name at build time (informational; the field id is authoritative) |
| `field-id` | decimal field id |
| `ngram` | the gram size the leaves were cut at, as decimal text; default `"3"` (`Grams.DEFAULT_SIZE`, `KAHSHE_NGRAM`), overridable per column (`ngram`, README) |
| `grams` | the gram rule id (§6.6): `kahshe-grams-v2-n<size>`; absent in a document written before the rule had an id, which means `kahshe-grams-v1` |
| `fpp` | the target false-positive rate the leaf was built at, as `String.valueOf(double)`; default `0.01` (`IndexBuilder.FPP`), overridable per column (`bloom-fpp`, README) |

`fpp` is informational: each serialized bloom carries its own geometry (§3.4). `ngram`
is carried in the blob too, but the RULE is not: a reader takes it from `grams` and cuts
its probe literals under it (`IndexStore`, `BloomLeaf.read`).

### 3.2 `snapshots[0]`

| key | type | meaning |
|---|---|---|
| `snapshot-id` | number | `1` |
| `source-table-snapshot-id` | number | table snapshot the build ran at |
| `timestamp-ms` | number | write time, epoch millis |
| `leaf-files` | array of string | full leaf paths in write order; **later wins**: a reader opens every leaf in list order and a later row for the same `data_file_path` replaces an earlier one (`IndexStore.load`) |
| `files-covered` | number | running sum of rows across leaves; reset to the merged row count on compaction |
| `data-bytes` | number | reserved; the writer emits `0` |
| `index-bytes` | number | sum of serialized bloom sizes (`NgramBloom.sizeBytes`), uncompressed, not Parquet bytes |

An incremental build appends one leaf. When the list would exceed
`BloomLeaf.BLOOM_MAX_LEAVES` (8), prior leaves are read back and merged into one
(this build's blooms win on a shared path) and the list becomes that single path; if a
prior leaf cannot be read, the build falls back to appending.

### 3.3 Leaf schema

A Parquet file written through Iceberg (`write.parquet.compression-codec=zstd`),
schema `BloomLeaf.LEAF_SCHEMA`, one row per data file:

| id | name | type | required |
|---|---|---|---|
| 1 | `data_file_path` | string | yes |
| 2 | `column_name` | string | yes (informational; readers ignore it) |
| 3 | `bloom` | binary | yes — the blob of §3.4 |

### 3.4 Bloom blob

All integers big-endian (`NgramBloom.serialize`):

| offset | size | field |
|---|---|---|
| 0 | 4 | magic `0x504F4E42` (`"PONB"`, `NgramBloom.MAGIC`) |
| 4 | 4 | int32 `ngram` |
| 8 | 4 | int32 `numHashes` (k) |
| 12 | 8 | int64 `numBits`, a multiple of 64 |
| 20 | `numBits / 8` | the bit array as int64 words, big-endian; bit `b` is word `b >>> 6`, mask `1L << (b & 63)` |

Geometry (`NgramBloom.build`), for `n = max(distinct grams in the file, 1)` and the
resolved `fpp`: `m = ceil(-n · ln(fpp) / ln(2)²)` rounded up to a multiple of 64;
`k = clamp(round(m / n · ln 2), 1, 8)`.

Hashing (`NgramBloom.insert`, `NgramBloom.mightContainGram`): `h1` = 64-bit FNV-1a over
the gram's UTF-8 bytes (basis `0xcbf29ce484222325`, prime `0x100000001b3`, wrapping);
`h2` = the splitmix64 finalizer of `h1` (`z ^= z >>> 30; z *= 0xbf58476d1ce4e5b9;
z ^= z >>> 27; z *= 0x94d049bb133111eb; z ^= z >>> 31`), then `| 1`; bit `i` for
`i in [0, k)` is `floorMod(h1 + i · h2, numBits)` in wrapping 64-bit arithmetic.

### 3.5 Grams

The gram set of a value is derived the same way on both sides because the cut has one
definition, the rule's `GramRule.forEachWindow` — `GramRule.forEachWindowUnits` is the
allocation-free view the build takes, and an override of it must agree with the first.
`GramAccumulator.addAll` on build and `NgramBloom.mightContainForm` on probe both go
through the rule, each taking the short whole value of step 3 from
`Grams.Contract.shorterThanWindow`; `Grams.Contract.gramsOf` is the reference
implementation, which `NgramBloom.gramsOf` delegates to:

1. The value's canonical string form (§6.4); a null contributes nothing.
2. Lowercased once, `Locale.ROOT`.
3. Cut under the index's gram rule (§6.6). Under `kahshe-grams-v2-n<size>`, the
   current rule: if the lowercased value holds fewer than `size` code points, the whole
   value is one gram (the empty string is the gram `""`); otherwise every sliding
   window of `size` whole code points. Under `kahshe-grams-v1`, the legacy rule: the
   same with UTF-16 code units and `size` fixed at 3, which can split a surrogate pair
   into a gram no Rust or Go string can hold — the reason v2 exists. v1 is read, never
   written.

Grams are deduplicated per file before insertion. There is no per-mode variant at build
time; one gram set serves every predicate. A probe literal shorter than one window is
unprobeable except for equality: it keeps every file (§3.6), which is the trade a larger
size makes.

### 3.6 Probe semantics

A probe (`NgramBloom.mightContain`) lowercases the literal and also tests it with a
sentinel character prepended, appended and both, stripped by index
(`NgramBloom.variants`), so a literal at a value edge and one in the middle both find
their grams. For each form: a form shorter than `ngram` is probed whole under `EQ` and
never prunes under `CONTAINS` or `STARTS_WITH`; a form of length `ngram` or more
requires every sliding gram present (a form exactly `ngram` long is one gram). A file may be pruned only when every form says absent. A file with no
row in any leaf is kept.

## 4. The gram tier

An exact inverted index from gram to the set of files holding it, stored in the term
tier's directory. It is also a substring tier, but exact where the bloom is
probabilistic: an absent gram means no covered file holds it.

### 4.1 Leaf schema

`GramIndexWriter.GRAMS_SCHEMA`, zstd, row-group size 1 MiB, page size 64 KiB, one row
per distinct gram, rows in unsigned lexicographic byte order of `gram`:

| id | name | type |
|---|---|---|
| 1 | `gram` | required binary — the gram's UTF-8 bytes (`GramIndexWriter.ByteKey.of`) |
| 2 | `file_ordinals` | required binary — a RoaringBitmap of file ordinals (§4.3) |

Grams are derived exactly as in §3.5. An empty map writes no leaf.

### 4.2 Metadata

In the term document: `properties.grams` (§5.1) is the gram rule id the leaf was cut
under (§6.6; absent means `kahshe-grams-v1`), and under `snapshots[0]` (§5.2)
`leaves.grams` is the leaf path and `gram-coverage` is
`{"from-ordinal": n, "grams": rows, "bytes": leaf length}`; the last two are present only
when a leaf was written. **`from-ordinal` is a promise boundary:** the gram answer is exact
for every live file whose ordinal is `>= from-ordinal`; files below it were not carried into this leaf -- the prior generation had no gram
leaf, or its leaf was unreadable or too large to reload (3 x its recorded `bytes`
over `KAHSHE_GRAM_BUILD_MAX_BYTES`), so coverage restarted at this build's first
ordinal -- and a reader must fall back
to the bloom tier for them (`BloomIndexType.mightMatch`).

### 4.3 Bitmaps

`file_ordinals` and the term tier's bitmaps are `org.roaringbitmap.RoaringBitmap`
portable serialization (library 1.3.0), written after `runOptimize()` into a buffer of
exactly `serializedSizeInBytes()` (`TermIndexWriter.serialize`) and read with
`RoaringBitmap.deserialize(ByteBuffer)`. The byte layout is the Roaring format
specification's, not this document's. Each set bit is a **file ordinal** (§5.3).

### 4.4 Limits a reader must honour

A reader refuses the tier before opening the leaf when `3 × gram-coverage.bytes`
exceeds its cache budget (`GramIndex.refresh`); a build drops the tier for the rest of
the pass when its in-memory estimate exceeds `KAHSHE_GRAM_BUILD_MAX_BYTES`
(`IndexBuilder.gramValve`), in which case no leaf and no `gram-coverage` are written.
Both are loud (metrics) and both leave every file kept.

## 5. The term tier

A dictionary of analyzed terms, each with the set of files holding it and its total
occurrence count, partitioned into 36 range leaves.

### 5.1 `properties`

| key | value |
|---|---|
| `analyzer` | the analyzer id the index was built under (§6.3); a reader tokenizes and admits with **this** contract, never its own configuration |
| `grams` | the gram rule id the leaves were cut under (§6.6), written on every build; absent means `kahshe-grams-v1` (§4.2) |
| `counts-exact` | `"true"` / `"false"`: whether `total_count` is exact (§5.6) |
| `kahshe.format-version` | `"2"` (`TermIndexWriter.TIER_FORMAT_VERSION`), the tier version (§8.1) |
| `partial` | `"true"`, present only on a checkpoint pass (§5.7); absent otherwise |
| `index-scope` | present only when the table narrows the index scope; a verbatim copy of the table property `kahshe.index.scope` |
| `column` | column name at build time (informational) |
| `field-id` | decimal field id |

### 5.2 `snapshots[0]`

| key | type | meaning |
|---|---|---|
| `snapshot-id` | number | `1` |
| `source-table-snapshot-id` | number | table snapshot the build ran at |
| `timestamp-ms` | number | write time |
| `files` | array of `{"path", "ordinal", "live"}` | coverage, §5.3 |
| `totals` | object | `rows` (rows read), `tokens` (tokens seen before admission), `terms` (sum of `terms-per-range`); key order unspecified |
| `terms-per-range` | array of 36 numbers | rows in each range leaf; empty array when the tier is off |
| `gram-coverage` | object | §4.2, only with a gram leaf |
| `leaves` | object | `aggregate`: 36 strings, position = range, `""` for a range that received no rows — an empty array when the term tier is off, like `terms-per-range`; a reader treats an empty or all-empty list as "no term index". `grams`: §4.2 |

### 5.3 Coverage and ordinals

`files` lists every data file the tier has ever indexed, as `{"path": string,
"ordinal": number, "live": boolean}` (`Coverage.toJson`). Ordinals are the join key
between a bitmap bit and a file:

- **Allocation.** A new file receives `max(ordinal over all entries, dead included) + 1`
  onward (`Coverage.nextOrdinal`), in ascending file-path order — the build sorts the
  scan's files by path, so a full build's numbering is a function of the file set alone.
  Ordinals are never reused while an entry exists.
- **Tombstones.** A file that left the table keeps its entry with `live: false`
  (`Coverage.reconcile`); its bits stay in the bitmaps. A reader resolves ordinals
  through live entries only, so a dead bit maps to no file and prunes nothing. A
  path that returns is revived under its old ordinal.
- **Compaction.** When an incremental build finds at least 16 dead entries and they
  are at least 25% of the list (`IndexBuilder.TOMBSTONE_COMPACT_MIN_DEAD`,
  `TOMBSTONE_COMPACT_DEAD_PERCENT`), it drops them, renumbers the survivors `0..n-1` in
  old-ordinal order and translates every gram and term bitmap in the same build
  (`Coverage.renumberLive`). A compacting build rewrites every range (§5.5).
- **A file absent from `files` is outside coverage and is always kept.**

The bloom tier is keyed by path, not ordinal, and is unaffected by renumbering.

### 5.4 Ranges

The dictionary is partitioned into `TermRanges.COUNT` = 36 ranges over the alphabet
`0123456789abcdefghijklmnopqrstuvwxyz` (`TermRanges.ALPHABET`). A term's range is
decided by its first byte (`TermRanges.ofFirst`): an alphabet character maps to its
index; the empty term and anything below `'0'` map to 0; anything above `'z'`,
including every non-ASCII lead byte, maps to 35; a byte between two alphabet characters
(`0x3A`–`0x60`) maps to the range of the alphabet character below it, 9. Ranges are
therefore contiguous and monotonic in unsigned-byte term order and tile the whole
space, which is what lets each range merge and be carried independently.

### 5.5 Aggregate leaf

A Parquet file per range that received at least one row (`TermIndexWriter.appender`:
zstd, row-group size 1 MiB, page size 64 KiB), schema
`TermIndexWriter.AGGREGATE_SCHEMA`:

| id | name | type | value |
|---|---|---|---|
| 1 | `term` | required string | the term, UTF-8 |
| 2 | `file_count` | required int | cardinality of `file_ordinals` |
| 3 | `total_count` | required long | occurrences of the term across every file ever merged in (§5.6) |
| 4 | `file_ordinals` | required binary | RoaringBitmap of file ordinals, §4.3 |

Invariants (`TermIndexWriter.finish`, `RunMerger.merge`):

1. Rows strictly ascend by `term` as unsigned UTF-8 bytes; each term appears once, in
   the one leaf its range names.
2. No row has an empty bitmap: `file_count >= 1`.
3. `file_count` is derived from the bitmap, never summed.
4. **Copy-forward.** On an incremental, non-compacting build, a range that received no
   new row keeps the prior generation's leaf **by reference**: the new document names
   the old path (its name carries an older snapshot id and nonce) and its
   `terms-per-range` entry unchanged. A reader must not infer a leaf's generation
   from its name.
5. A prior generation that is not 36 leaves (§8.3) is merged whole and republished as
   36.
6. kahshe adds no framing of its own; Parquet column encodings are the writer's
   defaults and are not part of this contract.

Lookup (`TermIndex.entriesFor`): a reader routes each term to its range, opens that
leaf with a projection of the schema and a filter `term IN (...)`, and exact-matches
returned rows because row-group filtering is coarse. No row means no covered file holds
the term. A read failure must be reported as failure, never as "no rows".

Prefix lookup (`TermIndex.entriesForPrefix`): because rows ascend by term, every term
under a prefix is one contiguous run inside the leaf the prefix's first character names.
A reader filters `term >= prefix AND term < successor` (the prefix with its last char incremented; dropped -- leaving the range open above
-- when the last char is U+D800 or higher, a surrogate or anything that sorts after
one), exact-matches each row with
`startsWith`, and ORs the bitmaps and sums the counts. A run longer than the reader's
cap is refused (every file kept, and said so) rather than read; the cap is the reader's
(`KAHSHE_PREFIX_MAX_TERMS`), not the format's. The same read serves any string range
(`TermIndex.entriesForRange`): the leaves from the lower bound's range to the upper's,
each row compared in Iceberg's string order — the order an engine's own evaluator uses,
so a file kept for a range is a file the engine may find a match in.

### 5.6 Counts

`total_count` sums every source merged into the row, including a prior leaf's, so
after a file leaves the table it may include occurrences no live file holds. The first
build that merges after a departure (any build that reads a new file, or compacts)
writes `counts-exact` as `"false"`, and it stays false until a full rebuild; a restamp
(§8.4) republishes the tombstoned coverage with the prior value, so a departure with
nothing new to read leaves the flag as it was. A consumer that needs exact occurrence
counts must refuse an index whose `counts-exact` is `"false"`; an absent property
reads as exact (documents from before the flag existed) (`TermIndex.load`,
`CountRoutes`). Pruning is unaffected either way.

### 5.7 Checkpoint passes

A build may be split into passes (`checkpoint-files`, README). Every pass but the last
publishes a complete, readable document whose coverage is a prefix of the build's file
set and whose `properties.partial` is `"true"`. A reader prunes with it (uncovered
files are kept by §5.3); an indexer treats it as not current and resumes; an exact-count
consumer refuses it.

### 5.8 Additional leaves

A reader takes the keys it knows under `snapshots[0].leaves` (`aggregate`, `grams`) and
ignores the rest, so a tier added later is additive: its leaf lives in the column's
directory under the same naming rule (§1), its pointer is a new key under `leaves`, and a
reader that predates it is unaffected. Two obligations follow for everyone else. A
collector treats **every path under `leaves`** as referenced, whatever the key. And a
restamp (§8.4) carries unknown keys forward unchanged. The key `rows` is reserved for a
row-level tier (per term and file, a bitmap of row positions).

## 6. Analyzer contracts

The term tier's meaning depends on how a value became terms. That is fixed by the
index's `analyzer` id and nothing else: a reader that tokenizes a query with its own
rules probes for terms the build never wrote, and absence prunes.

### 6.1 Ids

`Analyzer.Contract.id()` writes exactly one of:

| id | kind | version | admission |
|---|---|---|---|
| `kahshe-ascii-v1` | tokens | 1 | any length, except a token longer than 4 characters consisting only of digits is excluded |
| `kahshe-ascii-v2-max<n>` | tokens | 2 | `length <= n` |
| `kahshe-ascii-v3-max<n>` | tokens | 3 | `length <= n`; default `n` = 256; the current family |
| `kahshe-value-v1-max<n>` | value | 1 | `length <= n` |

`Analyzer.contractOf` reads them back; any other id is refused (§8.2). A build writes
the current contract of the column's configured kind and cap (`IndexSettings.contract`):
v3 for tokens. v1 and v2 are read, never written.

An analyzer id belongs to a *family*, and the family owns a prefix of the id space: `ascii` owns
`kahshe-ascii-v1`, `kahshe-ascii-v2-max<cap>` and `kahshe-ascii-v3-max<cap>`; `value` owns
`kahshe-value-v1-max<cap>`. The family is the whole of what an id means — how a value tokenizes,
which terms a query probes for, which tokens the dictionary admits, which prefixes can start a
term, and the id it writes back — so nothing about a contract is decided by a switch on a version
number any more. Families are discovered by the Java service loader through
`META-INF/services/io.kahshe.analysis.analyzer.AnalyzerFamily`; the two built-ins are registered first, so
nothing on the classpath can shadow an id kahshe already writes, and a language-specific tokenizer
is added by putting a jar beside kahshe rather than by editing kahshe. A reader resolving an
index's analyzer id walks the families in registration order and takes the first that owns it. **An
id no loaded family owns is refused**: the reader keeps every file for that column and serves
correct but unpruned until the column is rebuilt, and it says so in a WARN — the same rule §8.2
already states, now stated once for every family rather than per version.

### 6.2 Tokens kind

`Analyzer.tokenize` (v1, v2): iterate the value's UTF-16 chars; `A`–`Z` become `a`–`z`;
a term is a maximal run of `[a-z0-9]`; any other char, including whitespace,
punctuation and every non-ASCII char, ends a term.

`Analyzer.tokensV3` (v3) emits those same pieces and, in addition, every **compound
identifier**: a maximal run of `[a-z0-9.:_-]` (after lowercasing), with leading and
trailing separators stripped, whose remainder has one of four shapes — IPv4
(`\d{1,3}(\.\d{1,3}){3}`), IPv6 (hex groups of at most four, two to seven colons, at
most one `::`), UUID (`8-4-4-4-12` hex), or dashed/underscored hex
(`[0-9a-f]+([-_][0-9a-f]+)+`) — is emitted whole (`Analyzer.isCompound`). The shapes
are syntactic: a time or a date qualifies, harmlessly. v3 is additive over v2: every v2
term of a value is a v3 term of it.

A query under v1 or v2 is tokenized as the value was. Under v3 a query probes each
compound it carries and the pieces outside any compound (`Contract.queryTerms`), since a
file holding a compound holds its pieces. A multi-term query is satisfied only by a file
holding every term (conjunctive, conservative).

The SQL statement of "this term is present" that an engine overlay may turn into a term
probe is the pattern `Analyzer.matchPattern` writes, applied to the lowercased column:
`(^|[^a-z0-9])<token>([^a-z0-9]|$)` for a plain token, and for a compound
`(^|[^a-z0-9.:_-])[.:_-]*<compound, dots escaped>[.:_-]*([^a-z0-9.:_-]|$)` — the
run-and-strip rule restated, so a row the pattern matches is a row the index holds the
term for. Only these two shapes are sound; a compound under the plain-token boundary
is not (`10.0.4.17.1` satisfies it for `10.0.4.17`).

### 6.3 Value kind

The canonical string form of the value (§6.4) is one term, **exact and
case-sensitive**; the empty string yields no term. Plain equality and `IN` on such a
column probe the term tier (`TermIndexType.pruneByValues`), union per literal.

### 6.4 Canonical string forms

`Canonical.form`, applied on the build side and to the literals of standard predicates
(`=`, `IN`, `STARTS WITH`; `IndexPruner.collect`). The table names Iceberg's types because
that is the format this version is keyed to; the implementation reaches them through a
format-independent kind vocabulary (`ValueKind`, bound by `IcebergKinds`), so a second
table format supplies that one binding rather than a second set of forms. A `match`/`contains` hint carries its
literal verbatim: a hint on a non-string column must already be in canonical form.

| Iceberg type | form |
|---|---|
| string | the value |
| int, long | decimal text |
| decimal | plain text with trailing zeros stripped (`12.50` and `12.5` are one form) |
| uuid | 32 lowercase hex digits, no dashes |
| binary, fixed | lowercase hex |
| a `list<T>` or `map<K,V>` of one of the above | each member takes `T`'s / `V`'s form, on its own (§6.7) |
| anything else | not indexable: the build refuses the column, the reader keeps every file |

A predicate literal arriving as text for an int, long, decimal or uuid column is parsed
to the column's type first (a JSON plan filter writes UUIDs dashed). A text literal on
a binary or fixed column is not parsed: the predicate is not indexable and keeps every
file.

### 6.5 Admission on the read side

A term the index's contract would not have admitted (over the cap; excluded by v1's
rule) is never probed: the query keeps every file, and an exact-count request is
refused.

### 6.6 Gram rules

`Grams.Contract.id()` writes exactly one of:

| id | rule | size |
|---|---|---|
| `kahshe-grams-v1` | windows of UTF-16 code units | 3 |
| `kahshe-grams-v2-n<size>` | windows of whole code points; the current rule | `size`, 2..8, resolved per column (`ngram`), then table, then deployment (`KAHSHE_NGRAM`, default 3) |

`Grams.Contract.of` reads them back; absent is v1, any other family is refused (§8.2).
Both tiers record the id (§3.1, §4.2) and both readers cut their probe literals under
the recorded rule, so a v1 index serves under v1 while the rebuild under v2 runs.

The gram id works the same way: a *rule* owns a prefix of the id space and is the whole of the cut
under it. `utf16-v1` owns `kahshe-grams-v1` (sliding windows of three UTF-16 code units, read and
never written); `codepoint-v2` owns `kahshe-grams-v2-n<size>` (sliding windows of `size` whole code
points). Rules are discovered through `META-INF/services/io.kahshe.format.type.gram.GramRule`, built-ins
first, and a metadata document with no gram property is still v1. An id no loaded rule owns is
refused rather than guessed at, exactly as an unknown analyzer is. What remains an enum is the unit
*classification* — UTF-16 units or code points — which is not the set of rules but the declaration
a rule makes so the gram accumulator and the bloom probe know what to count; a rule discovered on
the classpath picks one of the two and gets the same non-allocating accumulator for free.

### 6.7 Repeated columns: list and map

A column may be a `list<T>` or a `map<K,V>` whose element/value type `T`/`V` has a
canonical form (§6.4). The **container** is named, never a leaf inside it: `tags` is
indexable, `tags.element`, `props.key` and `props.value` are refused. A container of
something with no canonical form — `list<struct>`, `list<list>`, `map<K,struct>` — is
refused exactly where that type is refused on its own.

**Terms.** For each row, the term set is the union over the container's members of the
terms of each member's canonical form. A map contributes its **values**; keys are not
indexed and are not qualified onto values. Each member is canonicalised, grammed and
tokenized **on its own** — members are never joined into one string, so no gram and no
token spans a member boundary. A null container and an empty one contribute no terms,
exactly as a null scalar does.

**What a set bit asserts, and nothing more:** some row of this data file has some member
whose analysis emitted this term. Not which row, not which member, not how many.

**It follows that only ∃ over a single atomic comparison is answerable.** Predicates
needing one member to satisfy a *conjunction*, or asking about cardinality or position —
`all_match`, `cardinality`, `tags[3]` — keep every file, the treatment `OR` and `NOT`
already receive. A range is the sharp case: two halves of a conjunction tighten into one
contiguous run, and a row whose members are `['z','A']` satisfies `>= 'a'` and `< 'b'`
through two *different* members while no member is in the run. A reader must therefore
never derive a candidate from a reference below a container (`IcebergKinds.repeatedPath`).

**Counting.** Exactly one row call carrying every member's tokens. Under a whole-value
contract (§6.3) a row contributes each distinct term **once**, so `total_count` keeps
meaning *rows holding the value*; without that, `["x","x"]` would answer 2 for one row on
a count that publishes itself as exact. Under a tokens contract `total_count` counts
occurrences, as it already does for scalar text.

**The member walk must be total.** Advisory-keep does not cover a short walk. Coverage is
claimed before a record is read, and over a *covered* file the absence of a term prunes.
A walk that caps members, stops at the first, or retains the reader's reused collection
across rows publishes a covering, token-poor index — which prunes exactly the files that
match. If a cap is ever wanted it must be an admission rule recorded in the contract and
consulted at probe time (§6.5), never a build-time truncation.

**Term bytes are unchanged.** A `list<string>` writes what a `string` writes; the leaf
schema, the tier version, the analyzer id and range routing do not move, so a deployed
reader reads a list index correctly and no fence is required.

## 7. What a reader must do

The reference reader and writer is the `format` module of this repository
(`io.kahshe.format`, `format/`): each tier's reader beside its writer, what every tier
shares (`Coverage`, `IndexScope`, `IndexPaths`), the lease, and the pruner
(`IndexPruner`), with no dependency on the indexer, the proxy or a storage client.

The rules for what a VALUE means — `Canonical`, the canonical form a key is compared in,
and `Analyzer`, what a token is — are a separate module (`io.kahshe.analysis`, `analysis/`),
below the artifact and depending on none of it. That split is the one an implementer
should copy: those rules are what a reader and a writer must agree on, and they are
agreed on whether or not an index exists. §6 pins them. A second implementation is held to this section and to the conformance fixture (§11), not
to the Java.

The invariant every tier serves is **advisory keep**: an index may only remove files
that cannot hold a match. Concretely:

1. A file outside a tier's coverage (bloom: no row; gram: below `from-ordinal` or not
   in `files`; term: not in `files`) is kept by that tier.
2. A term the analyzer contract would not admit is not probed (§6.5).
3. A document the reader cannot read, or whose version is newer than it knows (§8),
   means the tier is absent: every file is kept and the condition is reported.
4. A gram or term absent from an exact tier prunes; a bloom's "absent" prunes; a
   bloom's "maybe" does not.
5. The gram tier is authoritative over the bloom for a file it covers; the bloom is
   consulted for the rest.

**One interface per index type.** An implementation exposes each index type
behind a single interface rather than naming the tiers at each site: the type
owns its key, its per-build collection, its write at publish, its reader at
serve, and its pruning, and a build or a plan iterates the registered types in
cost order instead of calling three writers and three readers by name. In
kahshe that interface is `io.kahshe.format.type.IndexType` and the registry is
`IndexTypes`, whose members are declared in
`META-INF/services/io.kahshe.format.type.IndexType`; the format itself requires only
the behaviour. A reader picks the keys it owns out of `snapshots[0].leaves` and
**ignores a key it does not own** — an index carrying `rows` alongside
`aggregate` and `grams` is a valid index to a reader that knows only the latter
two, and must be read as though the extra key were not there. A key a reader
does own but cannot open — a newer format version, a foreign analyzer or gram
id, a leaf over its cache budget, an unreadable object — is ABSENT, not empty,
and absence keeps every file.

## 8. Versioning, refusal, legacy

### 8.1 Numbers

`format-version` is `1` in both documents and means the envelope of §2 in the draft
spec's vocabulary. The term tier's own evolution is `properties["kahshe.format-version"]`,
currently `"2"`. A reader (`TermIndexWriter.newerThanThisReader`) refuses a term
document when `format-version` exceeds 1 while the tier property is present, or when the
tier version exceeds 2; a document without the property carries its tier version in
`format-version` when that is 2 or more, else tier 1 (a one-day legacy). The bloom
document is refused when `format-version` exceeds 1 (`IndexMeta.parse`). A builder
refuses to build over a document it would refuse to read.

### 8.2 Analyzer families

An `analyzer` id outside §6.1, or a `grams` id outside §6.6, is refused with a warning;
the column serves unpruned until rebuilt. Previous tokens contracts (v1, v2) and the v1
gram rule are read under their own rules, so a contract change is a rolling reindex: an
indexer treats an index under another analyzer, gram rule or gram size as not current
and rebuilds it while it serves.

### 8.3 Legacy shapes a reader accepts

- A flat document without `snapshots` (fields at the root: `snapshotId`, `leafFiles`
  or `leafFile`, `indexType`, …) for either tier.
- `files` as a plain array of strings: ordinal = position, all live.
- `leaves.aggregate` as a single string: one leaf holding every range (pre-partitioning).
- A term document whose `format-version` is 2 with no `kahshe.format-version`: tier 2.

### 8.4 The one in-place rewrite

An incremental build (prior coverage and both documents present, analyzer and gram rule
unchanged, and every prior aggregate leaf opening — `IndexBuilder.decideIncremental`
probes each before deciding, and an unreadable one takes the full rebuild instead)
that finds no new file and is not compacting tombstones ("restamp") rewrites the prior
term document in place with the new `source-table-snapshot-id`, `timestamp-ms` and
`files`, and removes the legacy `leaves.postings`/`leaves.norms` pointers, deleting
those objects. No leaf is rewritten; each is opened once by the probe. A compacting
build with no new files takes the full merge instead and rewrites every range leaf.

### 8.5 Collection

The writer never deletes a superseded leaf, so reclamation is a separate operator step.
A conforming collector treats as referenced every path named under
`snapshots[0].leaves` (whatever the key, §5.8) and by `leaf-files` in every document it
can fetch and parse, spares the whole directory of a document that fetches but does not
parse, and deletes only unreferenced objects older than a grace period (24 h by default,
chosen as the reader's metadata TTL plus the longest observed build). Two limits an
operator must know: a flat legacy document (§8.3) contributes no references, so its
leaves become candidates; and a document whose fetch fails spares nothing in its
directory. `build.lease` (§8.6) is not a format object and is fair game
once expired.

### 8.6 Build lease

`term-v1-f<id>/build.lease` is an operational object, not part of the index: a compact
JSON `{"owner", "acquired-ms", "expires-ms"}` with a 12 h TTL (`BuildLease`). A builder
refuses to start while another owner's lease is live, re-checks it immediately before
its first publish, and deletes it on exit — from its own unwind, or from the JVM shutdown
hook that releases every lease the process holds. Readers ignore it.

## 9. Relation to the Iceberg draft index spec

**Read against apache/iceberg PR #16961 at head `4365427042` (2026-09-08), the merged
`format/expressions-spec.md` at `af86b839`, and the design doc's "Iceberg Index Support Sync" tab
as of 2026-09-08.** The draft moves weekly — four things were renamed in the three days before
`551ef55`, and `identity-fields` replaced the whole identity-expression model five days later — so
this section is dated rather than written as though it tracks a moving document. Re-read it before
relying on it. Citations `index-spec:N` are line numbers at that head.

The hardest conflict is not a name, and it comes first:

- **The draft requires an index snapshot to be complete.** Its Consistency goal
  (`index-spec:34`) and its normative rule (`index-spec:199`) both say an index snapshot indexes
  "exactly the live rows" of one table snapshot. That forbids §5.7's partial publication, §4.2's
  `from-ordinal`, and the whole coverage model of §5.3 — the operating mode that lets a 48 GiB
  corpus be indexed in stages over 3h22m and be *useful* while it is incomplete, because a file
  outside coverage is KEPT. Nothing about this is a detail of ours: no index over a large table can
  be atomic with a table commit, and the draft has no answer yet. Its only concession is at
  snapshot granularity — an async index "may lag behind the table" and engines must reconcile
  (`index-spec:263-264`) — never at file granularity. It is the one thing worth raising upstream
  (§9.2).

Also divergent:

- `type` is `"term-dictionary"` / `"ngram-bloom"`. The draft's type table defines `SCALAR` and
  reserves `VECTOR` (`index-spec:88-94`); text indexes appear only in its Future Extensions
  appendix (`index-spec:466`). The design doc is *not* consistent with its own PR here: it names
  TERM as a first-class type in its definitions and twice in its metadata tables, and the sync
  agenda lists Bloom, BTree, Term and IVF-PQ. Whether to reserve names for expected future types
  was put on the 2026-07-20 agenda and never answered in the notes. §9.2 asks for the table to be
  made consistent, which is a smaller ask than it looks.
- **One snapshot per document.** Recorded here as alignment until this re-read, wrongly: the
  draft dropped the one-to-one requirement on 2026-09-03, at `551ef55` itself — the very head the
  previous revision of this section cited — and allows several index snapshots per source snapshot,
  an engine picking any match (`index-spec:210-212`). Ours is one, `snapshot-id` always
  `1`, with `source-table-snapshot-id` carrying the meaning (§2).
- **The document is a mutable fixed path.** The draft writes a new metadata file per update, keeps
  a `metadata-log` of the ones it replaced (`index-spec:214-224`), and commits by a swap that
  succeeds only if the current file is the one the writer started from, identified by name
  (`index-spec:253-255`). That is a positive feature we lack, and it would replace the 12-hour
  build lease (§8.6) outright rather than sit beside it.
- **The tracking file.** The draft interposes one tracking file per index snapshot, listing range
  files with per-file statistics (`index-spec:287-309`); we collapse that into inline leaf lists
  (§5.2). Note the names are still open upstream: as of 2026-08-31 the sync had four candidate
  vocabularies on the table, including "Index root manifest, index data file".
- **Clustering.** The draft requires a non-empty `cluster-spec` and non-overlapping clustering
  ranges, one per range file, with an exact never-rounded `group_max_value` per cluster field
  (`index-spec:151-156`, `270-273`, `328-337`). We have no cluster spec and no per-leaf bounds;
  our 36 term ranges (§5.4) achieve the same pruning by being contiguous and monotonic in unsigned
  byte order, decided by a term's first byte.
- **`transform-function` and `key-column-ids` are vestigial.** Both are gone from the draft
  entirely — zero occurrences at this head. We still write them (§2). `key-column-ids` is what the
  draft now calls `identity-fields`; `transform-function` has no successor, and our bloom
  document's `"HASH"` describes that tier's own hashing (§3.4), never a clustering transform in the
  draft's sense. Read them as history, not as claims.
- The structured extensions (`files` / `leaf-files`, `leaves`, `totals`, `terms-per-range`,
  `gram-coverage`, `files-covered`, `data-bytes`, `index-bytes`) are extra keys in `snapshots[0]`,
  not under `properties`.

Newly aligned, and worth stating because §9 previously said the opposite:

- **The draft is keyed by field id.** On 2026-09-08 it replaced the identity-expression model with
  `identity-fields`, a required non-empty `list<int>` of source table field IDs
  (`index-spec:131-136`, `174`). Expressions may reference only IDs — "Named references must not be
  used" (`index-spec:119-120`) — and readers must match range file columns by field ID, never by
  writer-generated name (`index-spec:357-358`). That is structurally our `key-column-ids`, and it
  is why §1's field-id directories and §6's rule that a builder must never resolve columns by
  position are the right shape. This is our strongest alignment with the draft.
- **Containment already has a sanctioned spelling.** See §9.2, item 3.

**We are not the discarded "one index file per data file".** The design doc discards a family of
file-level index layouts, the first of which is storing one-to-one index files alongside data
files; the stated objection is that modern file formats already embed such indexes, so the
one-to-one form buys nothing but extra blob-store access. Our object count is O(columns × tiers)
and independent of the table's file count: one bloom leaf holding one *row* per data file, merged
once eight accumulate (§3.2); one gram leaf; up to 36 term aggregate leaves (§5.5). A query opens
one artifact per column, not one per file. The nearest thing the doc discarded is aggregating
per-file indexes into Puffin files, rejected because it would require significant changes to
Iceberg's metadata structure — a cost we do not incur, because nothing here is referenced from
table metadata at all (§9.1). What the doc never evaluated is the file-ordinal indirection itself
(§5.3): its "index row identifiers" discussion weighs RowId against filename+position against
user-defined primary keys and leaves the choice to the user, and the draft's own Appendix B
(`index-spec:474-492`) restates that same menu. An index-local ordinal resolved through a coverage
table is a fifth option, and nobody upstream has proposed it.

Still aligned: the envelope's field names, `format-version` = 1, string-valued extension data
under `properties`, refusal of a newer version, and the reader obligation — the draft's rule that
an unimplemented index type must be ignored rather than failed (`index-spec:93-94`) is the one
discipline we share, and §7's advisory keep is a strict superset of it.

### 9.1 Where it lives, relative to Puffin and the catalog

Nothing here is stored in the catalog, and nothing here is a Puffin file. Iceberg's catalog holds
the pointer to a table's current metadata file; every artifact the table owns — data, manifests,
Puffin statistics and deletion-vector files — lives in storage and is reached through that
metadata. kahshe sits in the same place: Parquet leaves and JSON documents under
`<table location>/_index` (or a configured root, §1), reached by convention — the field-id
directories under the root — with the table's own metadata untouched.

**Puffin could hold these artifacts today.** That was tested, not assumed: a Puffin file carrying
`kahshe-term-v1` and `kahshe-ngram-bloom-v1` blobs beside a real theta blob writes and reads back
byte-exact under Iceberg 1.11.0, with a Parquet payload nested whole inside a blob. The container
is deliberately permissive — it holds "arbitrary pieces of information", `type` is an unconstrained
JSON string — and its stated purpose names indexes. Storage was never this format's constraint:
the ClickBench artifact is ~1.2% of table bytes (term 0.84%, bloom 0.33%).

What Puffin does not supply is a **reader obligation**. The word "reader" does not occur in its
198 lines: there is no rule that an implementation must skip a blob type it does not know. So a
private blob type is safe to write and guaranteed nothing on read — while the index draft does
supply exactly that discipline (`index-spec:93-94`). That asymmetry, not storage, is why this
format is not a Puffin file. Three further consequences are recorded in §9.2.

Note that "the draft does not use Puffin" is true of PR #16961 only — Puffin appears nowhere in it
— and is not a settled community position. The design doc considered and set aside Puffin-specific
index layouts as complicating generic handling, while keeping Puffin available as a
property-referenced side channel, and a parallel design for file-skipping bloom indexes proposes
storing them *in* Puffin and has never been retracted; its PR simply went stale.

How a table refers to its indexes is deferred by the draft to the **catalog** specification, not
the REST spec — discovery is out of scope for the index spec (`index-spec:191-192`), and index
names are not stored in index metadata at all but are the catalog's to map (`index-spec:189-190`).
The REST work is dead: PR #16963, which would have added the index endpoints, was closed for
inactivity on 2026-08-10 with nothing replacing it, and the only implementation attempt against
the spec, PR #17426, closed on 2026-09-05. Our convention-based `_index` root therefore has no
live upstream competitor. It has one real cost, and it is operational rather than formal:
`remove_orphan_files` will delete an index root that sits inside the table location, because
nothing in table metadata references it.

### 9.2 What kahshe would ask the standard for

Recorded here because the questions are the format's, not the product's, and because the answers
would change this document. Ranked by value against effort; the case for each is a scale argument
rather than a kahshe argument.

1. **Reserve a `TERM` index type** in the draft's type table, beside `SCALAR` and `VECTOR`. The
   design doc names TERM as a first-class type in its definitions and in both of its metadata
   tables, the sync agenda lists it among the proposed index types, and the PR's own Future
   Extensions appendix anticipates text indexes; the type table carries none of them. Whether to
   reserve names for expected future types was raised on the 2026-07-20 agenda and is unanswered
   in the notes. Reserving a name costs nothing and prevents a collision.
2. **Admit partial coverage** — relax the "exactly the live rows" rule (`index-spec:199`) to allow
   a declared coverage set, with files outside it always kept. No prior art exists anywhere in the
   project, which is itself the finding: the nearest the record comes is an open question about
   whether deletes may be deferred to the next full rebuild, leaving the index with stale entries.
   A coverage set is not usable without a matching reader rule. A reader must check coverage before
   trusting the index to eliminate anything; absent that rule, "absent from the index" reads as
   "prune", which drops rows with no error and no metric. The draft has exactly one reader
   obligation today, about unknown index types (`index-spec:93-94`), and no place to put a second.
   The cost is real and lands on readers; we already pay it (§7, rule 1).
3. **A portable spelling for containment — largely already answered.** Predicates in the merged
   expressions spec are a closed set with no containment operation and no extension point, but a
   boolean function compared to a literal *is* a valid predicate: the spec's own example is that
   `is_empty(str_col)` is not a predicate while comparing it to true is (`expressions-spec:114`).
   Vendor function catalogs are the documented pattern, not a workaround — `sql_functions` and
   `iceberg_functions` are reserved, and engines may name their own (`expressions-spec:87-92`).
   The community settled on 2026-08-17 that the functions usable in index expressions are not
   restricted, with engines free to ignore an expression they do not understand. So
   `eq(apply(kahshe_functions.match, ref, lit), true)` is already schema-valid, already idiomatic,
   and already consistent with the settled position; we honour the reserved `iceberg_functions`
   spelling on read for older callers and never emit it (ENDPOINTS.md §2). What remains is not a
   spec gap: iceberg-java 1.11.0's `ExpressionParser` cannot serialize `apply`, which is why the
   sentinel spelling exists. That is a code problem, and no wording upstream fixes it. The one
   thing left to ask for is a standard *name*, if one is ever minted, so `text_match` and `match`
   do not diverge across implementations.
4. **One sentence in the Puffin spec**: a reader that does not recognise a blob's `type` must
   ignore it and must not fail. It codifies what every implementation already does, and it is what
   would make a private blob type safe to write.
5. **Add the file ordinal to the source-row-pointer menu.** The draft's Appendix B lists
   `_file`+`_pos`, `_row_id`, `_file` alone, or nothing (`index-spec:474-492`), and the design doc
   is explicit that the choice is left to the user. An index-local dense ordinal per data file,
   resolved through a table the index carries, belongs on that list: it makes postings compressible
   as bitmaps rather than repeated path strings, and it lets an index renumber after files are
   removed by translating bitmaps in one pass instead of rewriting every entry (§5.3). It suits
   file-eliminating indexes and is irrelevant to row-returning ones.
## 10. Unsettled

Facts an implementer should not assume because the code does not pin them:

- Nonce uniqueness is probabilistic (32 bits); nothing checks for a collision.
- `snapshots[0].totals` key order.
- Parquet column encodings and statistics for `term` and `gram` (writer defaults).
- Under the v1 gram rule, grams are windows of UTF-16 chars, so a surrogate pair can be
  split; settled for v2 (§6.6), which cuts whole code points. The tokens analyzer never
  emits a split pair under any rule.
- Whether `createOrOverwrite` is atomic on a given `FileIO`.

## 11. Conformance

The fixture under `format/src/test/resources/conformance/v1/` is the artifact this document
describes, cut once from a known corpus and committed, with a manifest of what a reader must
answer from it. It is what a second implementation validates against, and what holds this one:
a change to a reader that moves an answer, or to a writer that moves a byte of content, fails a
test before it ships.

| file | what it is |
|---|---|
| `rows.json` | the corpus: three data files, ten rows, over a scalar `string` (`msg`) and a `list<string>` (`tags`) — plain tokens, a v3 compound (an IPv4 address), a supplementary character (U+1F600, the reason v2 grams exist), a hex id, repeated tokens, a value shorter than one gram window, and on the list a row whose members repeat, a row whose two members would form a third term if joined, an absent container and an empty one |
| `index/term-v1-f1/`, `index/ngram-bloom-f1/` | `msg` under the default tokens contract: the term tier's document, its aggregate range leaves, the gram leaf and the build report (§12), plus the bloom tier's document and leaf, as the build wrote them |
| `index/term-v1-f2/`, `index/ngram-bloom-f2/` | `tags` under the WHOLE-VALUE contract (§6.3), declared by `kahshe.index.tags.analyzer`. The contract is what makes this column pin what a scalar cannot: under it a term's `total` means ROWS holding the value, so the per-row dedup repeated columns require (§6.7) is a number in the manifest rather than an unobservable build decision |
| `expected.json` | the manifest: format and tier versions, the analyzer and gram rule ids, seventeen queries on `msg` and eleven on `tags` (`MATCH`, `CONTAINS`, `PREFIX`) with the data files each keeps, five `_count` entries on `msg` and six on `tags`, and vectors — eight inputs tokenized under `kahshe-ascii-v3-max256` and `kahshe-value-v1-max256` and cut under `kahshe-grams-v2-n3`, `kahshe-grams-v2-n4` and `kahshe-grams-v1` |

The list column's entries are chosen so a wrong build fails rather than merely differs: `alpha`
answers `total` 3 — three rows across two files, one of them holding it twice — so a build without
the per-row dedup answers 4; and `connectionrefused` and `connection refused` are queries that must
keep NOTHING, which a build that joined a row's members before analysing them would break.

**Placeholders.** The committed copy replaces what a build cannot reproduce: the index root
(`INDEXROOT`), the table location (`TABLELOCATION`) and uuid (`TABLEUUID`), the source snapshot
id (`SNAP`, a number in the documents and a fragment of every leaf name), the document uuid
(`DOCUUID`), timestamps (`TIMESTAMP`) and leaf nonces (`NONCE`). The bloom leaf names each data
file by path inside the Parquet, so it is decoded and rewritten on both export and materialise;
the aggregate and gram leaves hold ordinals and are copied whole. The manifest is ASCII with
`\u` escapes because the v1 gram vectors hold lone surrogate halves, which UTF-8 cannot encode.

**What is compared.** Never Parquet bytes. The reader test materialises the fixture for a
freshly written copy of the corpus and answers every manifest query through the pruner and the
term reader, then again through the bloom tier alone, where the answer may only be a superset
(advisory keep), and finally reads documents one `format-version` newer, which must be refused
into keeping every file. The writer test builds the corpus afresh and compares content: both
documents with the placeholders applied and every object's keys sorted (§10 leaves key order
unsettled), every aggregate range's rows as `term, file_count, total_count, ordinals`, every gram's
bitmap, every file's serialized bloom bytes (deterministic: FNV-1a over a gram set sized from the
gram count). The analyzer test checks the vectors.

**Regenerating.** `./gradlew :format:test --tests 'io.kahshe.format.ConformanceFixtureGenerator'
-Dkahshe.conformance.regenerate=true` rewrites the fixture from the current code. Do it only when
the format changes on purpose, and read the diff of `expected.json` as a specification change: a
new format or tier version gets a new fixture directory beside `v1/`, and the refusal test runs
against the version after the newest.

## 12. The build report

One document per column, `build-report.json` beside the term metadata (§1), overwritten at
every publish: a full or incremental build, or a restamp. It is the record of that build for
whoever is not in the builder's process — a watcher delivering the alerts the build raised, an
operator, a script, a second implementation writing the same document — and it is the transport
by which alerting leaves the builder: `KAHSHE_MODE=watch` with the indexer off polls it.

The numbers use the Iceberg REST metrics vocabulary, so anything that reads Iceberg scan and
commit reports reads these: `report-type` (`kahshe-build`), `table-name`, `snapshot-id`, a
`metrics` map of counters `{unit, value}` and timers `{time-unit, count, total-duration}`, and a
`metadata` map of strings. The lists and the alerts are this format's own. The shape is pinned by
`build-report.schema.json` (JSON Schema 2020-12, in the `format` module's resources) and by the
conformance fixture (§11), which carries a report.

| key | value |
|---|---|
| `format-version` | `1`; a reader refuses a newer one (`BuildReport.parse`) |
| `metrics` | `files-covered`, `files-added`, `files-departed`, the term document's `totals` copied by name, `grams` and `gram-bytes` from `gram-coverage`, `bloom-bytes`, and the timer `build` in milliseconds |
| `metadata.build-id` | a per-build nonce |
| `metadata.previous-build-id` | the id of the report this one overwrote; absent on a column's first build. A reader that last saw a different id knows a build went unreported to it |
| `metadata.prefix`, `metadata.column`, `metadata.field-id` | the catalog prefix the builder used, the column's name at build time and its field id: the report's own address, so a reader holding only the document knows which index it describes |
| `metadata.kind` | `FULL`, `INCREMENTAL` or `RESTAMP` |
| `metadata.analyzer`, `metadata.grams` | the contract ids the build wrote (§6.1, §6.6) |
| `metadata.started-ms`, `metadata.published-ms` | wall clock at the build's start and at the write of this document |
| `files.added`, `files.departed` | data file paths newly covered by this build, and paths tombstoned by it |
| `leaves` | the aggregate range leaves and the gram leaf the term document names after this build |
| `warnings` | text: gram-space saturation with a file count, a partial (checkpoint) pass |
| `alerts` | the payloads the build's listener raised, in the watch webhook's shape |

A reader that acts on reports keeps the last `build-id` it acted on per column and acts once per
new id; the document is written after the tiers' metadata, so a reader that sees the report sees
the build. Rule evaluation stays in the build — a rule's per-file threshold exists only in the
read pass — so a foreign indexer fills `alerts` from its own evaluation or leaves it empty.

