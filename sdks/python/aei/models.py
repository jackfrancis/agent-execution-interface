"""The app-facing veneer over the generated AgentRun types.

[_agentrun.py](_agentrun.py) is generated verbatim from the CRD schema
(``sdks/generate.sh python``) and is never hand-edited — it is the type *mirror*.
This module is the thin, hand-written *ergonomics* on top of it, and it is the
only per-language code an SDK author writes for the models:

  * ``AgentRunSpec`` — the generated ``Spec`` plus ``to_spec()`` (the CRD
    ``spec`` object: camelCase keys, empty fields dropped);
  * ``Result`` — a curated read view of ``AgentRun.status`` (flattens the
    ``Finished`` condition to a ``message``, hides ``conditions``/``handle``);
  * re-exports of the generated ``Identity`` / ``Requirements`` / ``Timing`` so
    callers import everything from ``aei``.

Because the types are generated, they cannot drift from the controller's Go
structs; the veneer only adds sugar, so it stays tiny.
"""

from __future__ import annotations

from pydantic import Field

from ._agentrun import Identity, Requirements, Spec, Timing
from ._base import GeneratedModel

__all__ = ["AgentRunSpec", "Identity", "Requirements", "Result", "Timing"]


def _prune(value):
    """Recursively drop null/empty values — the JSON analog of Go's omitempty, so
    a dumped spec carries only what the app actually set."""
    if isinstance(value, dict):
        pruned = {k: _prune(v) for k, v in value.items()}
        return {k: v for k, v in pruned.items() if v not in (None, "", {}, [])}
    return value


class AgentRunSpec(Spec):
    """The app's intent, ready to dispatch. Inherits every field of the generated
    ``Spec`` (so it can never drift) and adds only the wire projection."""

    def to_spec(self) -> dict:
        """The CRD ``spec`` object: camelCase keys, enums as their string values,
        unset/empty fields omitted."""
        return _prune(self.model_dump(mode="json", by_alias=True, exclude_none=True))


class Result(GeneratedModel):
    """The observed outcome of a run, read from ``AgentRun.status``. ``result_ref``
    is an opaque pointer to where the result lives; AEI never parses it."""

    phase: str = ""
    result_ref: str = Field("", alias="resultRef")
    message: str = ""
    timing: Timing = Field(default_factory=Timing)

    @property
    def succeeded(self) -> bool:
        return self.phase == "Succeeded"

    @property
    def done(self) -> bool:
        return self.phase in ("Succeeded", "Failed")
