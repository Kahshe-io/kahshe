"""What a Sigma rule becomes.

Every test that produces a rules file writes it to tests/generated/, which a Java test loads
through kahshe's own `WatchRules`. That round trip is the only thing that can say the output is
valid: a backend judged by its own expectations is a backend that agrees with itself.
"""

from sigma.collection import SigmaCollection

WEBSERVER = "logsource: { category: webserver }"


def convert(backend, detection: str, extra: str = "") -> str:
    return backend.convert(SigmaCollection.from_yaml(f"""
title: Test rule
id: 00000000-0000-4000-8000-000000000001
level: high
{WEBSERVER}
detection:
{detection}
{extra}
"""))


def test_a_selection_and_a_filter_become_two_selections_and_a_condition(backend, golden):
    out = golden(convert(backend, """
    selection:
        sc-status: 404
    filter:
        cs-uri-query|contains: .gif
    condition: selection and not filter
"""))
    assert "- { column: status, equals: [404] }" in out  # a number has no case
    assert "- { column: request, contains: [.gif] }" in out
    assert "condition: sel0 and not sel1" in out
    assert "table: logs.httplogs" in out


def test_several_values_on_one_field_are_one_entry_not_several_selections(backend, golden):
    """kahshe ORs the values inside an entry, which is what a person would have written."""
    out = golden(convert(backend, """
    selection:
        sc-status:
            - 404
            - 405
            - 410
    condition: selection
"""))
    assert "- { column: status, equals: [404, 405, 410] }" in out
    assert "condition: sel0" in out  # one selection, referenced once


def test_wildcards_become_the_three_operators_kahshe_has(backend, golden):
    out = golden(convert(backend, """
    prefix:
        cs-uri-query: /images*
    suffix:
        cs-uri-query: '*.gif'
    middle:
        cs-uri-query: '*admin*'
    condition: prefix or suffix or middle
"""))
    assert "starts_with: [/images]" in out
    assert "ends_with: [.gif]" in out
    assert "contains: [admin]" in out


def test_modifiers_map_to_kahshe_operators(backend, golden):
    out = golden(convert(backend, """
    selection:
        cs-uri-query|startswith: /admin
        cs-uri-query|endswith: .php
        sc-status|gte: 400
        sc-status|lt: 500
    pattern:
        cs-uri-query|re: '^/api/v[0-9]+/'
    condition: selection or pattern
"""))
    assert "starts_with: [/admin]" in out
    assert "ends_with: [.php]" in out
    # unquoted, because kahshe refuses a comparison whose value is not a number
    assert "gte: [400]" in out and "lt: [500]" in out
    assert "re: [" in out


def test_a_nested_condition_keeps_its_shape(backend, golden):
    out = golden(convert(backend, """
    errors:
        sc-status|gte: 400
    images:
        cs-uri-query|contains: .gif
    server:
        sc-status|gte: 500
    condition: (errors and not images) or server
"""))
    assert "condition: (sel0 and not sel1) or sel2" in out


def test_one_of_them_becomes_an_or_over_the_selections(backend, golden):
    out = golden(convert(backend, """
    sel_a:
        sc-status: 404
    sel_b:
        sc-status: 500
    condition: 1 of sel_*
"""))
    assert "condition:" in out
    assert "404" in out and "500" in out


def test_an_event_count_correlation_becomes_a_window_rule(backend, golden):
    out = golden(backend.convert(SigmaCollection.from_yaml("""
title: Client error
name: client-error
id: 00000000-0000-4000-8000-000000000010
logsource: { category: webserver }
detection:
    selection: { sc-status: 404 }
    condition: selection
---
title: Many client errors from one address
id: 00000000-0000-4000-8000-000000000011
level: medium
correlation:
    type: event_count
    rules: [client-error]
    group-by: [c-ip]
    timespan: 1h
    condition: { gte: 3 }
""")))
    assert "window:" in out
    assert "ts_column: ts" in out
    assert "timeframe: 3600s" in out
    assert "count: 3" in out
    assert "- clientip" in out          # group-by went through the field mapping


def test_gt_converts_as_gte_of_one_more(backend, golden):
    """`> 4` and `>= 5` are the same set over a count, so this converts rather than refusing."""
    out = golden(backend.convert(SigmaCollection.from_yaml("""
title: Client error
name: client-error
id: 00000000-0000-4000-8000-000000000012
logsource: { category: webserver }
detection:
    selection: { sc-status: 404 }
    condition: selection
---
title: More than four
id: 00000000-0000-4000-8000-000000000013
correlation:
    type: event_count
    rules: [client-error]
    timespan: 5m
    condition: { gt: 4 }
""")))
    assert "count: 5" in out


def test_a_value_that_looks_like_a_yaml_boolean_is_quoted(backend, golden):
    """`no` unquoted parses to False and would silently match the text "false"."""
    out = golden(convert(backend, """
    selection:
        cs-uri-query: 'no'
    condition: selection
"""))
    assert 'equals_ignore_case: ["no"]' in out


def test_a_plain_string_folds_case_and_cased_does_not(backend, golden):
    """Sigma's plain `field: value` is case-insensitive in most implementations, and kahshe's
    `equals` is not. Converting it to `equals` matched fewer rows than the same rule matches
    everywhere else, silently; `equals_ignore_case` is the operator that means what Sigma means.
    """
    out = golden(convert(backend, """
    folded:
        cs-uri-query: /Admin/Login
    exact:
        cs-uri-query|cased: /Admin/Login
    condition: folded or exact
"""))
    assert "- { column: request, equals_ignore_case: [/Admin/Login] }" in out
    assert "- { column: request, equals: [/Admin/Login] }" in out
