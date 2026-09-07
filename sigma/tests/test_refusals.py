"""What will not convert, and why each refusal is louder than a warning.

A converted rule that quietly lost a clause is the worst artifact this backend could produce:
it loads, it reviews as correct, and it fires on rows the author did not ask for — or on none at
all. So every construct kahshe has no equivalent for raises, and the message names the construct
and, where one exists, what to write instead. The person reading it is holding a rule library
somebody else wrote.
"""

import pytest
from sigma.collection import SigmaCollection

from sigma.backends.kahshe.errors import KahsheConversionError


def convert(backend, detection):
    return backend.convert(SigmaCollection.from_yaml(f"""
title: Test rule
id: 00000000-0000-4000-8000-000000000002
logsource: {{ category: webserver }}
detection:
{detection}
"""))


@pytest.mark.parametrize("detection,expected", [
    ("""
    selection:
        cs-uri-query|cidr: 10.0.0.0/8
    condition: selection
""", "CIDR"),
    ("""
    selection:
        cs-uri-query: null
    condition: selection
""", "null test"),
    ("""
    selection:
        cs-uri-query|exists: true
    condition: selection
""", "exists"),
    ("""
    keywords:
        - some free text
    condition: keywords
""", "keyword search with no field"),
    ("""
    selection:
        cs-uri-query: 'a*b*c'
    condition: selection
""", "wildcard pattern"),
])
def test_a_construct_with_no_kahshe_equivalent_raises_and_names_itself(
        backend, detection, expected):
    with pytest.raises(KahsheConversionError) as excinfo:
        convert(backend, detection)
    assert expected in str(excinfo.value)


def test_a_rule_whose_logsource_maps_to_no_table_raises_rather_than_guessing(backend):
    """A rule library pointed at the wrong table converts cleanly and alerts on nothing."""
    with pytest.raises(KahsheConversionError) as excinfo:
        backend.convert(SigmaCollection.from_yaml("""
title: Windows rule against a webserver pipeline
id: 00000000-0000-4000-8000-000000000003
logsource: { product: windows, service: security }
detection:
    selection: { EventID: 4625 }
    condition: selection
"""))
    assert "table" in str(excinfo.value)
    assert "SetCustomAttributeTransformation" in str(excinfo.value)


CORRELATION = """
title: Base
name: base
id: 00000000-0000-4000-8000-000000000004
logsource: {{ category: webserver }}
detection:
    selection: {{ sc-status: 404 }}
    condition: selection
---
title: Correlated
id: 00000000-0000-4000-8000-000000000005
correlation:
    type: {type}
    rules: [base]
    timespan: 5m
    {extra}
"""


@pytest.mark.parametrize("type_,extra,expected", [
    ("value_count", "group-by: [c-ip]\n    condition: { gte: 5, field: c-ip }", "value_count"),
    ("temporal", "condition: { gte: 2 }", "temporal"),
    ("value_sum", "condition: { gte: 100, field: sc-bytes }", "value_sum"),
])
def test_a_correlation_kahshe_cannot_count_raises(backend, type_, extra, expected):
    with pytest.raises(KahsheConversionError) as excinfo:
        backend.convert(SigmaCollection.from_yaml(
            CORRELATION.format(type=type_, extra=extra)))
    assert expected in str(excinfo.value)


@pytest.mark.parametrize("op", ["lt", "lte", "eq"])
def test_an_event_count_asking_about_absence_raises(backend, op):
    """`fewer than N in T` needs to know the stream ended; a prospective counter never does."""
    with pytest.raises(KahsheConversionError) as excinfo:
        backend.convert(SigmaCollection.from_yaml(
            CORRELATION.format(type="event_count", extra=f"condition: {{ {op}: 5 }}")))
    message = str(excinfo.value)
    assert "event_count" in message
    assert "prospective counter cannot" in message


def test_a_correlation_over_several_rules_raises(backend):
    with pytest.raises(KahsheConversionError) as excinfo:
        backend.convert(SigmaCollection.from_yaml("""
title: A
name: a
id: 00000000-0000-4000-8000-000000000006
logsource: { category: webserver }
detection: { selection: { sc-status: 404 }, condition: selection }
---
title: B
name: b
id: 00000000-0000-4000-8000-000000000007
logsource: { category: webserver }
detection: { selection: { sc-status: 500 }, condition: selection }
---
title: Both
id: 00000000-0000-4000-8000-000000000008
correlation:
    type: event_count
    rules: [a, b]
    timespan: 5m
    condition: { gte: 3 }
"""))
    assert "over 2 rules" in str(excinfo.value)


def test_a_correlation_without_a_ts_column_raises(pipeline):
    """Sigma says WHEN implicitly; kahshe must name the column, and guessing one is a miss."""
    from sigma.backends.kahshe import KahsheBackend
    from sigma.pipelines.kahshe import kahshe_pipeline, kahshe_table

    no_ts = kahshe_pipeline(kahshe_table("logs.httplogs", category="webserver"))
    with pytest.raises(KahsheConversionError) as excinfo:
        KahsheBackend(no_ts).convert(SigmaCollection.from_yaml(
            CORRELATION.format(type="event_count", extra="condition: { gte: 3 }")))
    assert "event-time column" in str(excinfo.value)


def test_a_window_of_one_is_not_a_window(backend):
    with pytest.raises(KahsheConversionError) as excinfo:
        backend.convert(SigmaCollection.from_yaml(
            CORRELATION.format(type="event_count", extra="condition: { gte: 1 }")))
    assert "threshold of 1" in str(excinfo.value)
