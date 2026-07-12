"""Agent Execution Interface — Python app-developer SDK.

An AEI *app developer* dispatches agent work by submitting runs to the AEI dispatch
API and reading their status (docs/dispatch-plane.md). This SDK is the whole of
what that takes in Python:

  * typed, validated ``AgentRunSpec`` models — generated from the CRD schema, not
    hand-rolled (``_agentrun.py`` + the veneer in ``models.py``);
  * a thin ``AeiClient`` over the dispatch API — a small HTTP/JSON contract.

There is deliberately no bespoke protocol and no Kubernetes client: the app POSTs a
run and gets an id, with **no per-run object**, so one app scales to thousands of
runs. Every language's AEI app SDK is this same pair — a typed model + an HTTP
client — so an app in any language dispatches the same way. Substrate/provider code
never appears here (a Go controller resolves the run's ``AgentProviderClass`` and
dispatches it).
"""

from .client import AeiClient
from .models import AgentRunSpec, Identity, Requirements, Result, Timing

__all__ = [
    "AeiClient",
    "AgentRunSpec",
    "Identity",
    "Requirements",
    "Result",
    "Timing",
]
