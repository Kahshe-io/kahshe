"""The kahshe backend: Sigma rules in, kahshe watch rules out.

Unlike most pySigma backends this one does not emit a QUERY. kahshe's rule form is already
Sigma's shape — named selections whose entries are AND'ed, and a condition expression over
those names — so the conversion is structural, and what comes out is a rules file the watcher
loads and evaluates per row. The engine query kahshe would have emitted is the alert's
``confirmation_sql``, built at alert time against the file the rows are in, which is a thing
only the watcher can know.

What that buys, and it is the point of shipping a backend rather than a converter: Sigma stays
the source of truth, the field mapping lives in a pipeline where the ecosystem expects it, and
an existing rule library converts without being rewritten by hand.

Two things this backend will not do, both deliberate:

* It never drops a clause it cannot express. Every unsupported construct raises
  :class:`KahsheConversionError` naming itself. A rule that converts with a clause missing is a
  rule that loads, reviews as correct and fires on the wrong rows — the failure mode this whole
  project refuses.
* It never converts an aggregation into a match. Sigma's ``event_count`` correlation with
  ``gte``/``gt`` becomes a kahshe WINDOW rule, which counts across files; every other
  correlation type raises, because turning "five failures in five minutes" into "a failure"
  fires on the first row and reads as working.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar

from sigma.conditions import (
    ConditionAND,
    ConditionFieldEqualsValueExpression,
    ConditionNOT,
    ConditionOR,
)
from sigma.conversion.base import Backend
from sigma.conversion.deferred import DeferredQueryExpression
from sigma.conversion.state import ConversionState
from sigma.correlations import SigmaCorrelationConditionOperator as CorrOp
from sigma.correlations import SigmaCorrelationRule
from sigma.rule import SigmaRule

from .errors import KahsheConversionError

#: Sigma's severity words; kahshe's own vocabulary is the same four, lowercase.
_LEVELS = {"informational": "low", "low": "low", "medium": "medium", "high": "high",
           "critical": "critical"}


@dataclass
class Pred:
    """One kahshe field predicate: a column, an operator, and the values it ORs over."""

    column: str
    op: str
    values: list[Any]

    def entry(self) -> dict[str, Any]:
        return {"column": self.column, self.op: list(self.values)}

    def mergeable_with(self, other: "Pred") -> bool:
        """Two leaves an OR can collapse into one entry: same column, same operator.

        kahshe ORs the values inside one entry, so ``status equals [404, 405]`` is one entry
        rather than two selections joined by ``or`` — the same meaning, and what someone writing
        the rule by hand would have typed.
        """
        return isinstance(other, Pred) and other.column == self.column and other.op == self.op


@dataclass
class Node:
    """An and/or/not over other nodes."""

    kind: str
    children: list[Any] = field(default_factory=list)


class KahsheBackend(Backend):
    """Converts Sigma rules to kahshe watch rules."""

    name: ClassVar[str] = "kahshe"
    identifier: ClassVar[str] = "kahshe"
    formats: ClassVar[dict[str, str]] = {
        "default": "kahshe watch rules YAML, ready for KAHSHE_WATCH_RULES",
    }
    requires_pipeline: ClassVar[bool] = True
    #: Declaring a correlation method is what turns correlation support on; without it pySigma
    #: refuses every correlation rule with a NotImplementedError, which would read as "kahshe
    #: does not do rates" when the truth is that it does exactly one kind and refuses the rest
    #: by name.
    correlation_methods: ClassVar[dict[str, str]] = {
        "default": "kahshe window rule: N matching rows per key within a timeframe",
    }
    #: A correlation's base rule must be finalised into a whole kahshe rule, because that is what
    #: the window hangs off: the field predicates, the table and the condition all come from the
    #: base, and the correlation contributes only the threshold, the timeframe and the group-by.
    finalize_correlation_subqueries: ClassVar[bool] = True

    # kahshe needs a table, and Sigma rules name a logsource: the mapping is the pipeline's job
    # and there is no sensible default, so its absence is an error rather than a guess.
    table_attribute: ClassVar[str] = "kahshe_table"
    prefix_attribute: ClassVar[str] = "kahshe_prefix"
    ts_column_attribute: ClassVar[str] = "kahshe_ts_column"

    def __init__(self, processing_pipeline=None, collect_errors: bool = False, **kwargs):
        super().__init__(processing_pipeline, collect_errors, **kwargs)
        self._default_prefix = kwargs.get("prefix", "lakehouse")

    # ---------------------------------------------------------------- leaves

    def convert_condition_field_eq_val_str(
        self, cond: ConditionFieldEqualsValueExpression, state: ConversionState
    ) -> Any:
        value = cond.value
        if value.contains_special():
            return self._wildcard(cond, state)
        # Sigma's plain string comparison is case-insensitive in most implementations, and
        # kahshe's `equals` is exact and case-sensitive, so this used to convert into a rule that
        # matched FEWER rows on kahshe than the same rule matched everywhere else — quietly, which
        # is the worst way for a detection to be wrong. `equals_ignore_case` is the operator that
        # means what Sigma means; `|cased` below is how a rule asks for the exact one.
        return Pred(cond.field, "equals_ignore_case", [str(value)])

    def convert_condition_field_eq_val_str_case_sensitive(
        self, cond: ConditionFieldEqualsValueExpression, state: ConversionState
    ) -> Any:
        """Sigma's ``|cased`` modifier: the exact comparison, which is kahshe's plain ``equals``."""
        return Pred(cond.field, "equals", [str(cond.value)])

    def convert_condition_field_eq_val_num(
        self, cond: ConditionFieldEqualsValueExpression, state: ConversionState
    ) -> Any:
        return Pred(cond.field, "equals", [cond.value.number])

    def convert_condition_field_eq_val_bool(
        self, cond: ConditionFieldEqualsValueExpression, state: ConversionState
    ) -> Any:
        # Quoted on purpose: kahshe refuses an unquoted YAML boolean in a value, because `no`
        # parses to False and would silently match the text "false".
        return Pred(cond.field, "equals", ["true" if cond.value else "false"])

    def convert_condition_field_eq_val_re(
        self, cond: ConditionFieldEqualsValueExpression, state: ConversionState
    ) -> Any:
        return Pred(cond.field, "re", [str(cond.value.regexp)])

    def convert_condition_field_compare_op_val(
        self, cond: ConditionFieldEqualsValueExpression, state: ConversionState
    ) -> Any:
        # The operator is an enum whose .value is an ordinal, not the spelling: match on .name.
        mapping = {"LT": "lt", "LTE": "lte", "GT": "gt", "GTE": "gte"}
        op = cond.value.op.name
        if op not in mapping:
            raise KahsheConversionError(
                f"the numeric comparison '{op.lower()}' on field {cond.field!r}",
                "kahshe has lt, lte, gt and gte; a negated comparison is `not` over the "
                "selection that holds it")
        # .number is a SigmaNumber here, not a Python one: unwrap it, or the value serialises
        # as a quoted string and kahshe refuses the rule — a comparison's value must BE a number.
        return Pred(cond.field, mapping[op], [cond.value.number.number])

    def _wildcard(self, cond: ConditionFieldEqualsValueExpression, state: ConversionState) -> Any:
        """A value carrying ``*``: the three positions kahshe has operators for, and no other.

        ``val*`` is a prefix, ``*val`` a suffix, ``*val*`` a substring. A ``*`` anywhere else, or
        a single-character ``?``, has no kahshe operator: it raises rather than degrading to a
        substring match, which would return rows the rule did not ask for.
        """
        value = cond.value
        text = str(value)
        stripped = text.strip("*")
        if "*" in stripped or "?" in stripped:
            raise KahsheConversionError(
                f"the wildcard pattern {text!r} on field {cond.field!r}",
                "only a leading and/or trailing * is expressible; write it as a `re` modifier",
                state.processing_state.get("rule"))
        if not stripped:
            raise KahsheConversionError(
                f"the value {text!r} on field {cond.field!r}, which matches anything",
                "kahshe has no 'field exists' operator")
        leading, trailing = text.startswith("*"), text.endswith("*")
        if leading and trailing:
            return Pred(cond.field, "contains", [stripped])
        if trailing:
            return Pred(cond.field, "starts_with", [stripped])
        return Pred(cond.field, "ends_with", [stripped])

    # ---------------------------------------------------------------- combinators

    def convert_condition_and(self, cond: ConditionAND, state: ConversionState) -> Any:
        return Node("and", [self.convert_condition(arg, state) for arg in cond.args])

    def convert_condition_or(self, cond: ConditionOR, state: ConversionState) -> Any:
        return Node("or", [self.convert_condition(arg, state) for arg in cond.args])

    def convert_condition_not(self, cond: ConditionNOT, state: ConversionState) -> Any:
        return Node("not", [self.convert_condition(cond.args[0], state)])

    def decide_convert_condition_as_in_expression(self, cond, state) -> bool:
        # kahshe's own "in" is several values inside one entry, which the OR collapse below
        # already produces; there is no separate expression to opt into.
        return False

    # ---------------------------------------------------------------- refusals

    def _unsupported(self, what: str, remedy: str = ""):
        def raiser(cond, state):
            raise KahsheConversionError(what, remedy)
        return raiser

    def convert_condition_field_eq_val_null(self, cond, state):
        raise KahsheConversionError(
            f"a null test on field {cond.field!r}",
            "kahshe has no null or exists operator; a row whose column is null satisfies no "
            "field predicate, so `not <selection>` is the nearest thing and is not the same")

    def convert_condition_field_exists(self, cond, state):
        raise KahsheConversionError(
            f"an exists test on field {cond.field!r}", "kahshe has no exists operator")

    def convert_condition_field_not_exists(self, cond, state):
        raise KahsheConversionError(
            f"a not-exists test on field {cond.field!r}", "kahshe has no exists operator")

    def convert_condition_field_eq_val_cidr(self, cond, state):
        raise KahsheConversionError(
            f"a CIDR match on field {cond.field!r}",
            "kahshe compares canonical values, not networks; a `starts_with` on the octets a "
            "prefix fixes is exact for /8, /16 and /24 and nothing else")

    def convert_condition_field_eq_field(self, cond, state):
        raise KahsheConversionError(
            "a comparison between two fields", "kahshe compares a column to a literal")

    def convert_condition_field_eq_val_timestamp_part(self, cond, state):
        raise KahsheConversionError("a timestamp-part comparison")

    def convert_condition_field_eq_query_expr(self, cond, state):
        raise KahsheConversionError("a query expression placeholder")

    def convert_condition_query_expr(self, cond, state):
        raise KahsheConversionError("a query expression placeholder")

    def convert_condition_val_str(self, cond, state):
        raise KahsheConversionError(
            "a keyword search with no field",
            "every kahshe field predicate names a column; add one, or use `contains` on the "
            "column the text lives in")

    convert_condition_val_num = convert_condition_val_str
    convert_condition_val_re = convert_condition_val_str

    def convert_condition_as_in_expression(self, cond, state):
        raise KahsheConversionError("an 'in' expression")

    # ---------------------------------------------------------------- correlations

    def convert_correlation_event_count_rule(
        self, rule: SigmaCorrelationRule, output_format=None, method: str = "default"
    ) -> list[Any]:
        """Sigma's ``event_count`` correlation becomes a kahshe window rule.

        This is the one correlation kahshe can answer, and it can answer it EXACTLY: item 21's
        counter keeps the N most recent event times per key, so ``N within T`` is decided rather
        than estimated. The base rule supplies the field predicates, the correlation supplies the
        threshold, the timespan and the group-by.

        ``gt N`` converts as ``>= N+1``, which is the same set over integers. ``lt``, ``lte``,
        ``eq`` and ``neq`` do not convert: each asks about the ABSENCE of events, and a
        prospective counter that sees a stream of rows can never know it has seen the last one.
        """
        condition = rule.condition
        op = condition.op
        if op == CorrOp.GTE:
            count = condition.count
        elif op == CorrOp.GT:
            count = condition.count + 1
        else:
            raise KahsheConversionError(
                f"an event_count correlation with '{op.name.lower()}'",
                "kahshe counts up to a threshold, so only gte and gt convert; lt, lte, eq and "
                "neq ask whether something did NOT happen, which a prospective counter cannot "
                "answer without knowing the stream has ended",
                rule)
        if count < 2:
            raise KahsheConversionError(
                f"an event_count threshold of {count}",
                "a window of one is a rule without a window; drop the correlation", rule)

        base = self._single_base_rule(rule)
        # The base was converted when the collection was walked; asking for it again would
        # return nothing, because a rule a correlation references is not emitted on its own.
        converted = base.get_conversion_result()
        if not converted:
            raise KahsheConversionError(
                f"a correlation over {base.name or base.title!r}, which did not convert",
                "the base rule's own error is the one to fix", rule)
        if len(converted) != 1:
            raise KahsheConversionError(
                f"a base rule with {len(converted)} conditions",
                "kahshe counts the rows of ONE rule; split it into one rule per condition", rule)
        emitted = dict(converted[0])
        emitted["id"] = rule.name or str(rule.id) or emitted["id"]
        if rule.title:
            emitted["title"] = rule.title
        if rule.level is not None:
            emitted["severity"] = _LEVELS.get(str(rule.level), "medium")
        window: dict[str, Any] = {
            "ts_column": self._attribute(base, self.ts_column_attribute, required=True, what=(
                "the event-time column: a Sigma correlation says WHEN implicitly, kahshe names "
                "the column")),
            "timeframe": f"{rule.timespan.seconds}s",
            "count": count,
        }
        if rule.group_by:
            window["group_by"] = list(rule.group_by)
        emitted["window"] = window
        emitted.pop("min_count", None)
        return [emitted]

    def _single_base_rule(self, rule: SigmaCorrelationRule) -> SigmaRule:
        referenced = [r.rule for r in rule.rules]
        if len(referenced) != 1:
            raise KahsheConversionError(
                f"an event_count correlation over {len(referenced)} rules",
                "kahshe counts the rows ONE rule matches; correlate a single rule, or merge the "
                "rules' detections into one", rule)
        base = referenced[0]
        if isinstance(base, SigmaCorrelationRule):
            raise KahsheConversionError("a correlation over another correlation", rule=rule)
        return base

    def _correlation_unsupported(self, kind: str, why: str):
        def raiser(rule, output_format=None, method: str = "default"):
            raise KahsheConversionError(f"a {kind} correlation", why, rule)
        return raiser

    def convert_correlation_value_count_rule(self, rule, output_format=None, method="default"):
        raise KahsheConversionError(
            "a value_count correlation",
            "kahshe's window counts ROWS per key, not distinct values of a second field; the "
            "counter would have to hold a set per key rather than N timestamps", rule)

    def convert_correlation_temporal_rule(self, rule, output_format=None, method="default"):
        raise KahsheConversionError(
            "a temporal correlation",
            "kahshe counts one rule's rows in a window; matching SEVERAL rules within a window "
            "needs per-rule state per key, which item 21's counter does not hold", rule)

    convert_correlation_temporal_ordered_rule = convert_correlation_temporal_rule
    convert_correlation_extended_temporal_rule = convert_correlation_temporal_rule
    convert_correlation_extended_temporal_ordered_rule = convert_correlation_temporal_rule

    def convert_correlation_value_sum_rule(self, rule, output_format=None, method="default"):
        raise KahsheConversionError(
            "a value_sum correlation", "kahshe's window counts rows; it does not sum a column",
            rule)

    def convert_correlation_value_avg_rule(self, rule, output_format=None, method="default"):
        raise KahsheConversionError(
            "a value_avg correlation", "kahshe's window counts rows", rule)

    def convert_correlation_value_median_rule(self, rule, output_format=None, method="default"):
        raise KahsheConversionError(
            "a value_median correlation", "kahshe's window counts rows", rule)

    def convert_correlation_value_percentile_rule(self, rule, output_format=None, method="default"):
        raise KahsheConversionError(
            "a value_percentile correlation", "kahshe's window counts rows", rule)

    # ---------------------------------------------------------------- assembly

    def finalize_query_default(
        self, rule: SigmaRule, query: Any, index: int, state: ConversionState
    ) -> dict[str, Any]:
        if isinstance(query, dict):
            # A correlation's output arrives here already whole: the window path builds the rule
            # from its base's finalised form, and pySigma finalises the correlation's result too.
            return query
        selections: dict[str, list[dict[str, Any]]] = {}
        condition = _outermost(_emit(query, selections))
        detection: dict[str, Any] = dict(selections)
        detection["condition"] = condition
        out: dict[str, Any] = {
            "id": _rule_id(rule, index),
            "title": rule.title or _rule_id(rule, index),
            "severity": _LEVELS.get(str(rule.level).lower() if rule.level else "", "medium"),
            "prefix": self._attribute(rule, self.prefix_attribute) or self._default_prefix,
            "table": self._attribute(rule, self.table_attribute, required=True, what=(
                "the table its logsource maps to: kahshe rules name a table, Sigma rules name a "
                "logsource, and only the pipeline knows which is which")),
            "detection": detection,
        }
        return out

    def _attribute(self, rule, name: str, required: bool = False, what: str = ""):
        value = (rule.custom_attributes or {}).get(name)
        if value is None and required:
            raise KahsheConversionError(
                f"rule {rule.title or rule.id!r} without {what}",
                f"set it in the pipeline with SetCustomAttributeTransformation({name!r}, ...) — "
                "sigma.pipelines.kahshe.kahshe_table builds one", rule)
        return value

    def finalize_output_default(self, queries: list[dict[str, Any]]) -> str:
        return _yaml({"rules": queries})


# -------------------------------------------------------------------- tree to selections


def _emit(node: Any, selections: dict[str, list[dict[str, Any]]]) -> str:
    """Turns the converted tree into a kahshe condition string, filling ``selections``.

    A selection per conjunction of leaves rather than per leaf, and an OR of leaves on one column
    collapsed into one entry, so the output reads like a rule someone wrote rather than one a
    machine unrolled. Every selection is referenced exactly once, which is what kahshe's loader
    requires: a selection the condition does not use is refused as a typo.
    """
    if isinstance(node, Pred):
        return _add(selections, [node.entry()])
    if node.kind == "and":
        if all(isinstance(c, Pred) for c in node.children):
            return _add(selections, [c.entry() for c in node.children])
        return "(" + " and ".join(_emit(c, selections) for c in node.children) + ")"
    if node.kind == "or":
        collapsed = _collapse_or(node.children)
        if collapsed is not None:
            return _add(selections, [collapsed.entry()])
        return "(" + " or ".join(_emit(c, selections) for c in node.children) + ")"
    if node.kind == "not":
        inner = _emit(node.children[0], selections)
        return f"not {inner}" if inner.startswith("(") else f"not {inner}"
    raise KahsheConversionError(f"the condition node {node.kind!r}")


def _outermost(condition: str) -> str:
    """Drops one enclosing paren pair the whole condition sits inside.

    ``(a and not b)`` and ``a and not b`` parse identically; the second is what a person writes,
    and these rules are meant to be read and edited after conversion.
    """
    if not (condition.startswith("(") and condition.endswith(")")):
        return condition
    depth = 0
    for i, ch in enumerate(condition):
        depth += (ch == "(") - (ch == ")")
        if depth == 0 and i < len(condition) - 1:
            return condition          # the pair closes early: the parens are not enclosing
    return condition[1:-1]


def _collapse_or(children: list[Any]) -> Pred | None:
    """An OR over one column and one operator is one entry: kahshe ORs an entry's values."""
    if not children or not all(isinstance(c, Pred) for c in children):
        return None
    first = children[0]
    if not all(first.mergeable_with(c) for c in children[1:]):
        return None
    values: list[Any] = []
    for child in children:
        for value in child.values:
            if value not in values:
                values.append(value)
    return Pred(first.column, first.op, values)


def _add(selections: dict[str, list[dict[str, Any]]], entries: list[dict[str, Any]]) -> str:
    name = f"sel{len(selections)}"
    selections[name] = entries
    return name


def _rule_id(rule: SigmaRule, index: int) -> str:
    base = rule.name or (str(rule.id) if rule.id else None) or _slug(rule.title or "rule")
    return base if index == 0 else f"{base}-{index}"


def _slug(text: str) -> str:
    out = "".join(c.lower() if c.isalnum() else "-" for c in text).strip("-")
    while "--" in out:
        out = out.replace("--", "-")
    return out or "rule"


# -------------------------------------------------------------------- YAML, written not imported


def _yaml(data: Any, indent: int = 0) -> str:
    """A small YAML writer, so the package depends on pySigma and nothing else.

    It quotes every scalar that could be read as something other than text — the YAML boolean
    trap kahshe's loader refuses (`no` parsing to False and silently matching "false") is exactly
    what a generator must not emit.
    """
    pad = "  " * indent
    if isinstance(data, dict):
        lines = []
        for key, value in data.items():
            if isinstance(value, (dict, list)) and value:
                lines.append(f"{pad}{key}:")
                lines.append(_yaml(value, indent + 1))
            else:
                lines.append(f"{pad}{key}: {_scalar(value)}")
        return "\n".join(lines)
    if isinstance(data, list):
        lines = []
        for item in data:
            if isinstance(item, dict) and "column" in item:
                # A field entry on one line, which is how the chart's example writes them and
                # how someone editing the converted rule will expect to find them. Only field
                # entries: a rule is a block mapping, and flowing it would be unreadable.
                lines.append(f"{pad}- {_flow(item)}")
            elif isinstance(item, dict):
                body = _yaml(item, indent + 1)
                first, *rest = body.split("\n")
                lines.append(f"{pad}- {first.lstrip()}")
                lines.extend(rest)
            else:
                lines.append(f"{pad}- {_scalar(item)}")
        return "\n".join(lines)
    return f"{pad}{_scalar(data)}"


_YAML_KEYWORDS = {"y", "yes", "n", "no", "true", "false", "on", "off", "null", "~", ""}

#: The only characters left unquoted. Deny-by-default rather than a list of what to escape: a
#: regular expression is a scalar full of YAML metacharacters, and the first version of this
#: writer quoted only on the FIRST character, so `re: [^/api/v[0-9]+/]` went out unquoted, its
#: brackets closed the flow sequence early, and the whole file parsed to nothing — no rules and
#: no skips, which reads as an empty rules file rather than as an error. Found by the Java
#: round trip, not here.
_SAFE = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_./@+-()* ")

#: A scalar may CONTAIN these and still be plain; leading one of them is a YAML sigil.
_NEVER_FIRST = set("-?*&!|>%@`")


def _scalar(value: Any) -> str:
    if isinstance(value, bool):
        return '"true"' if value else '"false"'
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if (text
            and text.lower() not in _YAML_KEYWORDS
            and not _numeric(text)
            and text == text.strip()
            and text[0] not in _NEVER_FIRST
            and all(c in _SAFE for c in text)):
        return text
    escaped = text.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _flow(mapping: dict[str, Any]) -> str:
    """One field entry as a flow mapping: ``{ column: status, equals: [404] }``."""
    parts = []
    for key, value in mapping.items():
        if isinstance(value, list):
            parts.append(f"{key}: [{', '.join(_scalar(v) for v in value)}]")
        else:
            parts.append(f"{key}: {_scalar(value)}")
    return "{ " + ", ".join(parts) + " }"


def _numeric(text: str) -> bool:
    try:
        float(text)
        return True
    except ValueError:
        return False
