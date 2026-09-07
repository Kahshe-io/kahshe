# pySigma backend for kahshe

Compiles Sigma rules into [kahshe](../README.md) watch rules, so an existing rule library runs
against an Iceberg lakehouse without being rewritten by hand.

```sh
pip install -e .
```

```python
from sigma.collection import SigmaCollection
from sigma.backends.kahshe import KahsheBackend
from sigma.pipelines.kahshe import kahshe_pipeline, kahshe_table

pipeline = kahshe_pipeline(kahshe_table(
    "logs.httplogs",                  # the kahshe table, ns.table
    category="webserver",             # the Sigma logsource it holds
    ts_column="ts",                   # only correlations need it
    field_mapping={"c-ip": "clientip", "sc-status": "status"},
))
print(KahsheBackend(pipeline).convert(SigmaCollection.load_ruleset(["rules/"])))
```

The output is a rules file for `KAHSHE_WATCH_RULES`, not a query. kahshe's rule form is already
Sigma's shape — named selections whose entries are AND'ed, and a condition over those names — so
the conversion is structural. The engine query kahshe would emit is the alert's
`confirmation_sql`, built at alert time against the file the rows are in, which only the watcher
can know.

## What converts

| Sigma | kahshe |
|---|---|
| a selection's fields | one selection, entries AND'ed |
| `field: value` | `equals_ignore_case` — Sigma's plain comparison is case-insensitive, and so is this |
| `field\|cased: value` | `equals` — kahshe's exact, case-sensitive comparison |
| `field: [a, b]` | one entry, values OR'ed — kahshe ORs an entry's values |
| `condition: sel and not filter` | the same expression over the same names |
| `1 of sel_*`, `all of them` | expanded to the `or`/`and` the quantifier means |
| `value*`, `*value`, `*value*` | `starts_with`, `ends_with`, `contains` |
| `\|contains`, `\|startswith`, `\|endswith`, `\|re` | the operator of the same name |
| `\|lt`, `\|lte`, `\|gt`, `\|gte` | the operator of the same name |
| `\|all` | separate entries in one selection, which AND |
| `correlation: event_count` with `gte`/`gt` | a **window rule**: N matching rows per key within a timeframe |

## What does not, and why it raises

Nothing is dropped silently. A converted rule missing a clause loads, reviews as correct, and
fires on the wrong rows — so every construct with no kahshe equivalent raises
`KahsheConversionError` naming itself and, where one exists, what to write instead. The one the
converter cannot see is a repeated column: `field|contains|all` on a `list` or `map` column converts
to two entries on one column, and kahshe's row scan refuses that at prepare time (each operator on a
container is satisfied by any member, so two of them do not mean one member satisfies both) — it is
logged and skipped by the watcher rather than raised here, because the converter has no column
types to raise on.

| Sigma | why |
|---|---|
| `\|cidr` | kahshe compares canonical values, not networks |
| `field: null`, `\|exists` | kahshe has no null or exists operator |
| a keyword search with no field | every kahshe field predicate names a column |
| `a*b` — a wildcard in the middle | only a leading and/or trailing `*` is expressible; use `\|re` |
| a comparison between two fields | kahshe compares a column to a literal |
| `event_count` with `lt`, `lte`, `eq`, `neq` | each asks whether something did NOT happen; a prospective counter never knows the stream has ended |
| `value_count`, `value_sum`, `value_avg`, `value_median`, `value_percentile` | kahshe's window counts ROWS per key |
| `temporal`, `temporal_ordered` | matching several rules in one window needs per-rule state per key |
| a rule whose logsource maps to no table | a library pointed at the wrong table converts cleanly and alerts on nothing |

## Known differences

**`event_count` thresholds.** `gt N` converts as `>= N+1`, which is the same set over a count.

**Case used to be one and is not any more.** Sigma's plain `field: value` is case-insensitive in
most implementations, and kahshe's `equals` is exact — so every converted rule matching a cased
string quietly matched fewer rows here than the same rule matched elsewhere. kahshe gained
`equals_ignore_case`, which is what a plain value now converts to; `|cased` asks for the exact
one. Numbers still convert to `equals`, because a number has no case.

## Tests

```sh
pytest tests          # 25 tests
```

The suite writes every converted rule to `tests/generated/`, which a **Java** test
(`watch/src/test/java/io/kahshe/watch/rules/SigmaGeneratedRulesTest.java`) loads through kahshe's
own rule loader and asserts nothing is skipped. That round trip is the gate that matters: a
Python suite can only check that the converter agrees with its author, and it found two defects
this one could not — a numeric comparison emitted as a quoted string, which kahshe refuses, and a
regular expression emitted unquoted, whose brackets closed the YAML flow sequence early and made
the whole file parse to no rules at all.
