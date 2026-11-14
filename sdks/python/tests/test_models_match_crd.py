"""The SDK's guarantee: the pydantic models produce output that validates against
AEI's *published* CRD schema, so the hand-authored Python models can never drift
from the controller's Go types. This is what lets an app trust the SDK instead of
hand-mirroring the CRD.
"""

from __future__ import annotations

import os
from pathlib import Path

import pydantic
import pytest

from aei import AgentRunSpec, Identity, Requirements, Result
from aei import _agentrun as generated

yaml = pytest.importorskip("yaml")
jsonschema = pytest.importorskip("jsonschema")


def _crd_path() -> Path:
    override = os.environ.get("AEI_CRD")
    if override:
        return Path(override)
    # sdks/python/tests → repo root → controller/config/crd
    root = Path(__file__).resolve().parents[3]
    return root / "controller" / "config" / "crd" / "agents.x-k8s.io_agentruns.yaml"


def _root_schema() -> dict:
    path = _crd_path()
    if not path.exists():
        pytest.skip(f"AgentRun CRD not found at {path}; set AEI_CRD to override")
    crd = yaml.safe_load(path.read_text())
    version = next(v for v in crd["spec"]["versions"] if v["name"] == "v1alpha1")
    return version["schema"]["openAPIV3Schema"]


def _spec_schema() -> dict:
    return _root_schema()["properties"]["spec"]


@pytest.mark.parametrize(
    ("model", "path"),
    [
        (generated.AgentRun, ()),
        (generated.Spec, ("spec",)),
        (generated.Identity, ("spec", "identity")),
        (generated.Requirements, ("spec", "requirements")),
        (generated.Status, ("status",)),
        (generated.Handle, ("status", "handle")),
        (generated.Timing, ("status", "timing")),
        (generated.Condition, ("status", "conditions", "items")),
    ],
)
def test_generated_mirror_has_every_schema_property(model, path):
    # The other tests only prove model output is *accepted* by the schema, which a
    # mirror missing fields still satisfies. This one fails when _agentrun.py is
    # stale in either direction: regenerate it with `sdks/generate.sh python`.
    node = _root_schema()
    for key in path:
        node = node["items"] if key == "items" else node["properties"][key]
    wire_names = {field.alias or name for name, field in model.model_fields.items()}
    assert wire_names == set(node["properties"])


def test_fully_populated_spec_validates_against_crd():
    schema = _spec_schema()
    spec = AgentRunSpec(
        provider_class_name="standard",
        task_ref="worklist://github:1",
        parameters={"kind": "converse", "itemId": "org/repo#7", "provider": "github"},
        isolation_class="gvisor",
        identity=Identity(subject="github:1", scopes=["signals:read", "metadata:write"], audience="pyfer-agent"),
        requirements=Requirements(runtime_lifecycle="any", isolation="preferred", state_persistence="none"),
        timeout_seconds=900,
    )
    jsonschema.validate(instance=spec.to_spec(), schema=schema)  # raises on drift


def test_empty_fields_are_omitted():
    # A minimal spec emits only what is set (the CRD is optional-everything).
    spec = AgentRunSpec(parameters={"kind": "ingest"})
    assert spec.to_spec() == {"parameters": {"kind": "ingest"}}


def test_invalid_enum_is_rejected_by_the_model():
    # The generated enum enforces the constraint client-side, at construction.
    with pytest.raises(pydantic.ValidationError):
        Requirements(isolation="bogus")


def test_invalid_enum_is_rejected_by_the_schema():
    # And the published schema rejects it independently of the model, so the
    # positive check above is non-vacuous.
    schema = _spec_schema()
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance={"requirements": {"isolation": "bogus"}}, schema=schema)


def test_result_reads_camelcase_status():
    result = Result.model_validate(
        {"phase": "Succeeded", "resultRef": "worklist://item/x", "timing": {"runtimeSeconds": 64.6}}
    )
    assert result.succeeded and result.done
    assert result.result_ref == "worklist://item/x"
    assert result.timing.runtime_seconds == 64.6
