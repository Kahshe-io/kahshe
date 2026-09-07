# kahshe-analysis

**What a value means: its one canonical string form, and the terms it contributes to an index.**

Every index kahshe builds is, at bottom, a set of strings drawn from column values, and every probe
against that index is a literal turned into strings the same way. This module owns both of those
conversions and nothing else. It holds no storage format, no I/O, no query planner and no table
format — it depends on the JDK and a logging facade — so the rules it defines can be read as a
specification by a row-scanning tool that has no index at all, or by a second implementation in
another language. Build and probe agree because they call the same code here; if they ever
disagreed, a query would find a value absent from a file that holds it and prune that file, which
is the one error the system may never make.

Artifact `kahshe-analysis`, package `io.kahshe.analysis`. It is consumed by the index format, the
indexer, the planning proxy and the row-scanning watcher.

"Analysis" is used as Lucene and Elasticsearch use it: the stage that decides what a value *is* for
indexing purposes.

---

## Architecture

Four public pieces, and data moves in one direction through them.

```
column value ──► ValueKind (what the column is)
                     │
                     ▼
                 Canonical.form(kind, value) ──► the value's one string form
                     │
                     ▼
       Analyzer.Contract.tokens(form) ──► the terms an index stores
       Analyzer.Contract.queryTerms(form) ──► the terms a probe looks up
```

| Type | Role |
|---|---|
| `ValueKind` | The small type vocabulary analysis reasons in. Eleven kinds, five of them indexable. |
| `Canonical` | `form(ValueKind, Object)` — the single string form of a value, or `null` for "not indexable". |
| `Analyzer` | The pinned tokenization rules, the analyzer-id constants, and `Analyzer.Contract`. |
| `Analyzer.Contract` | What one analyzer id means: family, kind, version, token-length cap. The object build and probe both hold. |
| `AnalyzerFamily` | The SPI. One family owns a region of the id space and defines everything an index under those ids means. |
| `Analyzers` | The registry: the built-in families, then whatever `ServiceLoader` found. |
| `AsciiAnalyzerFamily`, `ValueAnalyzerFamily` | The two built-ins. |

An index build resolves a `Contract`, writes `contract.id()` into the index's metadata, and stores
`contract.tokens(...)` for each value. A reader parses the id it finds with
`Analyzer.contractOf(id)` and probes with `contract.queryTerms(...)`. The reader never consults its
own configuration to decide what an index means — only the id the index carries.

---

## The type vocabulary: `ValueKind`

Iceberg has a type system, Delta has another, and both say considerably more than this module needs.
`ValueKind` is the smaller vocabulary everything here is written against, which is what lets a table
format bind to analysis with **one function from its own types to these kinds** rather than by being
translated whole. That binding lives outside this module, one per format.

The kinds fall into two groups, and the difference matters when adding one:

- **Five have a canonical form**, and those five are exactly what is indexable: `STRING`,
  `INTEGRAL`, `DECIMAL`, `UUID`, `BINARY`.
- **The rest exist because a consumer must still tell columns apart** where no form is defined —
  a scanner reading an event time accepts `TIMESTAMP`, `TIMESTAMPTZ`, `DATE` or `INTEGRAL`, and
  renders a bound differently for each.

So a new kind is warranted by a consumer that must distinguish it, not by `Canonical` alone.
`INTEGRAL` covers every whole-number width and `BINARY` both fixed- and variable-width bytes,
because the canonical form does not distinguish them. `TIMESTAMP` and `TIMESTAMPTZ` do stay apart
despite neither having a form: both Iceberg (`timestamp` / `timestamptz`) and Delta
(`TIMESTAMP_NTZ` / `TIMESTAMP`) draw that line themselves, and a consumer that renders a bound for
the wrong one has it interpreted in the engine's session time zone — silently selecting a different
range than the one asked for.

Everything else is `OTHER`, which callers read as "not indexable".

---

## Canonical forms

The index stores strings. A non-string value is therefore indexed by its canonical string form, and
a predicate's literal must be canonicalised the same way. One method — `Canonical.form` — serves
both sides for exactly that reason.

| Kind | Form |
|---|---|
| `STRING` | the string itself |
| `INTEGRAL` | decimal text |
| `DECIMAL` | plain text with trailing zeros stripped |
| `UUID` | 32 lowercase hex digits, no dashes |
| `BINARY` | lowercase hex |
| anything else | `null` |

Two of those deserve their reason stated:

- **Decimals strip trailing zeros** so that `12.50` as stored at the column's scale and `12.5` as a
  user types it are one form. Without that, a literal's spelling decides whether a file is pruned.
- **UUIDs drop their dashes** so that a UUID tokenizes as one term rather than as five segments
  that a query would have to reassemble conjunctively.

`form` also parses: a literal arriving as text for a non-string column — a JSON plan filter writes
UUIDs as strings — is parsed to the column's type before being canonicalised. A value that will not
parse yields `null`.

**`null` is the contract for "not indexable"**, and both callers act on it conservatively: a build
refuses the column rather than writing a form it cannot reproduce, and a pruner keeps every file
rather than deciding on a literal it could not canonicalise.

---

## The analyzer contract

An analyzer id is a **pinned, versioned contract**, recorded in every index built under it. It is
not configuration a reader is free to reinterpret.

### The families and their ids

| Id | Meaning |
|---|---|
| `kahshe-ascii-v3-max<cap>` | Current tokens analyzer. v2's runs plus compound identifiers, whole. |
| `kahshe-ascii-v2-max<cap>` | Maximal ASCII `[a-z0-9]` runs after lowercasing; admitted up to `<cap>`. |
| `kahshe-ascii-v1` | The same runs, with no length cap, but pure-numeric tokens longer than four digits were never admitted. |
| `kahshe-value-v1-max<cap>` | The canonical value as one term, exact and case-sensitive — what SQL `=` means. An empty value has no term. |

### Tokens

A token is a maximal run of ASCII `[a-z0-9]` after locale-independent lowercasing; a non-ASCII
character terminates a token.

v3 adds, **additively**, the identifiers that text carries with punctuation inside them. A maximal
run of `[a-z0-9.:_-]`, stripped of leading and trailing separators, is emitted whole *in addition to*
its pieces when the remainder is shaped like an IPv4 or IPv6 address, a UUID, or dashed/underscored
hex. Nothing that matched under v2 stops matching under v3.

The shape test is deliberately **syntactic**: a clock time or an ISO date passes it. Indexing those
whole is additive and harmless, and a rule that tried to be semantic would be a second, weaker
parser to keep correct.

Because a file holding a compound necessarily holds its pieces too, a **query** for a compound
probes only the compound — probing the pieces as well would cost lookups and prune nothing extra.
That asymmetry between `tokens` and `queryTerms` exists only for v3; every other contract probes
exactly what it indexes.

### The cap is part of the identity

The token-length cap is in the id, not merely in a config file, because indexability is part of the
contract: an index built under one cap does not contain what an index under another cap would be
probed for, and **absence is what prunes**. Encoding it means a reader can *tell* rather than assume.
The default cap of 256 follows Elasticsearch's `ignore_above` convention; at that height it is a
valve against pathological tokens — a base64 blob, a stack frame run together — not a filter on
ordinary content.

More generally: **any knob that changes what an index contains must be recorded in the index's
metadata**, or build and read will drift with no symptom.

### Versioning, and why a change means a new id

The build tokenizes data with the analyzer and the query tokenizes predicates with it. If the rules
changed in place, every existing index would silently disagree with every new query, and no
self-consistency test would notice, because both sides would still agree *with each other*. So a
behaviour change requires a **new analyzer id**, and older ids stay parseable: an index built under
v1 is read under v1's rule and keeps pruning correctly until it is rebuilt, rather than being
refused.

Two failure modes are handled conservatively rather than by throwing:

- An id **no loaded family owns** parses to `null`. The reader keeps every file and serves correct
  but unpruned results rather than misreading an index it does not understand.
- A probe for a token the index's own rule **never admitted** must not prune. The index's silence
  about that token is not evidence of absence.

### Prefixes and the SQL boundary

`Contract.prefixable` answers whether a string can begin a term this contract writes: for a tokens
contract only run characters qualify, and for a whole-value contract any non-empty string does. A
prefix that cannot start a term is not probed, and therefore prunes nothing.

`Analyzer.matchPattern` produces the canonical `regexp_like` pattern meaning "this term is present" —
the one shape an engine-side recogniser can turn back into a token lookup. Its boundaries restate the
tokenizer's rule on the SQL side: a plain token needs a non-token character or a string edge on both
sides, and a compound needs a character outside the run class on both sides with any separators
between it and the needle. Because that is the same run-and-strip rule `tokensV3` applies, a row the
pattern matches is a row the index holds the term for.

---

## Seams

### `AnalyzerFamily` (ServiceLoader SPI)

A family owns a region of the analyzer-id space and defines the whole of what an index under one of
its ids means: how a value tokenizes, what a query probes for, which tokens are admitted, which
prefixes are probeable, and how an id is spelled and parsed.

Register one by putting a jar on the classpath with
`META-INF/services/io.kahshe.analysis.analyzer.AnalyzerFamily`.

**What implementing one buys you:** a tokenizer of your own — CJK segmentation, stemming, a domain
identifier grammar — becomes a first-class analyzer everywhere in kahshe. Indexes built under your
ids are parsed, probed and pruned by *your* rules, with no change to the index format and no change
to kahshe's code. Nothing about the storage layer needs to know your family exists.

**What an implementation must honour:**

- `owns(id)` is a pure test on the id text alone, false for a `null` id, and false for every id
  another family owns.
- `parse(id)` is called only for an id you own, and returns a `Contract` carrying your family.
- `id(contract)` is `parse`'s inverse: `parse(id).id().equals(id)`.
- The same id must mean the same thing in every process. Build and probe tokenize through the same
  `Contract`, so a family that answers differently in two JVMs desynchronises every index it wrote.

### `Analyzers` (the registry)

`Analyzers` holds the built-ins first and by identity, so **nothing on the classpath can shadow an id
kahshe already writes** — a provider entry naming a built-in is folded into the existing singleton
rather than added twice, which also keeps `Contract` equality stable across constructions. Families
are loaded once at class initialisation, because a family that appeared or vanished mid-process would
mean one id parsing two ways in a single JVM. A provider that fails to load is logged rather than
propagated: the built-ins are unaffected, so indexes kahshe wrote stay readable and the family that
failed is simply not there.

`Analyzer.contractOf` consults the registry in registration order and takes the first owner.

---

## What this module deliberately does not do

- **It names no table format.** There is no Iceberg or Delta type in this module, and there should
  not be. A format binds to `ValueKind` with one function of its own, outside here.
- **It does not store, read or write anything.** No index layout, no file I/O, no serialization.
  Analysis defines what the strings *are*; another module decides how they are kept.
- **It does not plan queries.** It answers "what terms does this predicate become"; deciding which
  files to keep is the pruner's job, and the conservative default when analysis answers `null` is
  the pruner's to honour.
- **It does no linguistic analysis.** No stemming, no stopwords, no synonyms, no Unicode
  normalization or folding — a non-ASCII character simply terminates a token. Text analysis of that
  kind is exactly what an `AnalyzerFamily` is the seam for.
- **It does no scoring.** There is no term weighting, frequency or relevance ranking; the only
  question asked of an index is presence.
- **Several types have no canonical form on purpose.** `date` / `time` / `timestamp` (event times
  tend to follow file order, so a table format's own min/max statistics already prune them),
  `boolean` (present in nearly every file, so a term index would prune nothing), and `float` /
  `double` (no meaningful equality case, and fragile as text). A build asked to index one of these
  fails with a named message rather than silently skipping the column.

---

## Tests

```sh
./gradlew :analysis:check
```

`check` includes `javadoc`, which is a real gate rather than decoration: a `{@link}` left pointing
at a member a rename moved fails the build. The suites pin the term space with golden assertions
rather than self-consistency ones, because build and probe tokenize through the same code — a rule
changed in place would leave the two agreeing with each other while disagreeing with every index
already written. So they fix what the tokenizer must produce at its boundaries (ASCII case folds, a
non-ASCII character separates rather than joins, a long run is one token but is not admitted past
the cap, and admission is length and nothing else); that v3 is additive over v2 and that only the
four compound shapes survive the strip-and-shape test; that a row `Analyzer.matchPattern` matches is
a row whose terms hold the needle, over random text; and that the registry keeps the built-ins first
and unshadowable, round-trips their ids, discovers a family from the classpath, and answers `null`
for an id no loaded family owns.
