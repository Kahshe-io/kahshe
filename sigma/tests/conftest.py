import pathlib
import sys

import pytest

# The package under test lives beside this directory, not in site-packages: `sigma` is a
# namespace package, so the repository's copy merges with pySigma's own.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from sigma.pipelines.kahshe import kahshe_pipeline, kahshe_table  # noqa: E402

#: Where converted rules are written for the Java gate to load. The round trip is the point:
#: a backend that emits YAML kahshe rejects is worse than no backend, and only kahshe's own
#: loader can say. watch/src/test/.../SigmaGeneratedRulesTest.java reads every file here.
GENERATED = pathlib.Path(__file__).parent / "generated"


@pytest.fixture
def pipeline():
    """The lab's own table, so the generated rules are ones the watcher could actually run."""
    return kahshe_pipeline(
        kahshe_table(
            "logs.httplogs",
            category="webserver",
            ts_column="ts",
            field_mapping={
                "c-ip": "clientip",
                "cs-uri-query": "request",
                "sc-status": "status",
                "sc-bytes": "size",
            },
        )
    )


@pytest.fixture
def backend(pipeline):
    from sigma.backends.kahshe import KahsheBackend

    return KahsheBackend(pipeline)


@pytest.fixture
def golden(request):
    """Writes a converted rules file for the Java loader gate, named after the test."""

    def write(text: str) -> str:
        GENERATED.mkdir(exist_ok=True)
        name = request.node.name.replace("test_", "").replace("[", "-").replace("]", "")
        (GENERATED / f"{name}.yaml").write_text(text + "\n")
        return text

    return write
