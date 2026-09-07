"""The pipeline: what a Sigma rule does not say and a kahshe rule must.

Three things, and none of them can be guessed from the rule:

* **The table.** A Sigma rule names a ``logsource``; a kahshe rule names ``ns.table``. Which
  logsource lives in which table is a property of the lakehouse, not of the rule.
* **The columns.** Sigma's field names are the taxonomy's (``EventID``, ``CommandLine``); a
  kahshe rule names the table's columns. This is ordinary pySigma field mapping.
* **The event-time column**, for correlations only. A Sigma ``event_count`` says "within 5
  minutes" and never says which column carries the time; kahshe's window rule must name it.

Keeping these in a pipeline rather than in the backend is the ecosystem's convention and it is
also the right seam: one rule library converts against several lakehouses by swapping the
pipeline, and the rules themselves stay untouched.
"""

from __future__ import annotations

from sigma.processing.conditions import LogsourceCondition
from sigma.processing.pipeline import ProcessingItem, ProcessingPipeline
from sigma.processing.transformations import (
    FieldMappingTransformation,
    SetCustomAttributeTransformation,
)


def kahshe_table(
    table: str,
    *,
    category: str | None = None,
    product: str | None = None,
    service: str | None = None,
    prefix: str = "lakehouse",
    ts_column: str | None = None,
    field_mapping: dict[str, str] | None = None,
    name: str | None = None,
) -> list[ProcessingItem]:
    """The processing items binding one logsource to one kahshe table.

    Args:
        table: the kahshe table, ``namespace.table`` as engines address it.
        category, product, service: the Sigma logsource this table holds. At least one is
            required — a mapping that matches every rule would send an unrelated library at one
            table, which converts cleanly and alerts on nothing.
        prefix: the catalog prefix, part of a rule's identity in alerts.
        ts_column: the column carrying event time. Only correlations need it, and a correlation
            without it is refused at convert time rather than defaulted to a guess.
        field_mapping: Sigma field name to column name, for the fields this table spells
            differently.
        name: an identifier for the processing items, for pipelines that carry several tables.

    Returns:
        The processing items, to be placed in a :class:`ProcessingPipeline`.
    """
    if not any((category, product, service)):
        raise ValueError(
            "a kahshe table mapping must name a logsource (category, product or service): a "
            "mapping that matches every rule points a whole library at one table")
    label = name or table.replace(".", "-")

    # A fresh condition per item, not one shared instance: pySigma binds a condition to the
    # pipeline that owns it and refuses a second binding, so reuse fails at construction.
    def matches() -> LogsourceCondition:
        return LogsourceCondition(category=category, product=product, service=service)

    attributes = {
        "kahshe_table": table,
        "kahshe_prefix": prefix,
    }
    if ts_column:
        attributes["kahshe_ts_column"] = ts_column

    items = [
        ProcessingItem(
            identifier=f"kahshe-{label}-{key.rsplit('_', 1)[-1]}",
            transformation=SetCustomAttributeTransformation(key, value),
            rule_conditions=[matches()],
        )
        for key, value in attributes.items()
    ]
    if field_mapping:
        items.append(
            ProcessingItem(
                identifier=f"kahshe-{label}-fields",
                transformation=FieldMappingTransformation(dict(field_mapping)),
                rule_conditions=[matches()],
            )
        )
    return items


def kahshe_pipeline(tables: list[ProcessingItem] | None = None) -> ProcessingPipeline:
    """A pipeline over one or more :func:`kahshe_table` mappings.

    Called with nothing it produces an EMPTY pipeline, which converts nothing and says why: the
    backend refuses a rule whose table it does not know rather than inventing one. That is the
    intended experience for someone who has not written their mapping yet — a named error, not a
    rules file pointed at a table that does not exist.
    """
    return ProcessingPipeline(
        name="kahshe",
        priority=20,
        items=list(tables or []),
    )
