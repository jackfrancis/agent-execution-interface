"""AeiClient — the app side of the AEI data plane.

An app dispatches a run by POSTing to the AEI dispatch API and reads its lifecycle
by GETting it back; the wire is a small, stable HTTP/JSON contract, **not** the
Kubernetes API. So an app dispatches thousands of runs with no per-run object
(docs/dispatch-plane.md), and the only transport any language needs is a plain
HTTP client — even more universal than a Kubernetes client.

The caller authenticates with its projected ServiceAccount token; the endpoint
validates it (TokenReview) and enforces the app's ``AgentApp`` policy. There is no
shared secret.
"""

from __future__ import annotations

import os
import time

from .models import AgentRunSpec, Result

_BASE = "/aei/v1alpha1"
_SA_TOKEN = "/var/run/secrets/kubernetes.io/serviceaccount/token"  # nosec B105: path, not a secret


class AeiClient:
    """Submits runs to the AEI dispatch API and reads their status. Requires the
    endpoint URL and the app's ``AgentApp`` name; both default from the environment
    (``AEI_DISPATCH_URL`` / ``AEI_APP``) so an in-cluster app needs no wiring."""

    def __init__(self, endpoint: str | None = None, app: str | None = None, *, token: str | None = None, session=None):
        self._endpoint = (endpoint or os.environ.get("AEI_DISPATCH_URL", "")).rstrip("/")
        self._app = app or os.environ.get("AEI_APP", "")
        self._token = token
        self._session = session  # an injectable requests-like session (tests); else lazy requests

    def dispatch(self, spec: AgentRunSpec, *, app: str | None = None) -> str:
        """Submit a run; returns its id. No Kubernetes object is created — the run
        exists only as control-plane state addressable by this id."""
        body = {"app": app or self._app, **spec.to_spec()}
        resp = self._http().post(f"{self._endpoint}{_BASE}/dispatch", json=body, headers=self._auth(), timeout=30)
        resp.raise_for_status()
        return resp.json()["runId"]

    def get(self, run_id: str) -> Result:
        resp = self._http().get(f"{self._endpoint}{_BASE}/runs/{run_id}", headers=self._auth(), timeout=30)
        resp.raise_for_status()
        return Result.model_validate(resp.json())

    def wait(self, run_id: str, *, timeout: float = 900.0, poll: float = 2.0) -> Result:
        """Poll the run's lifecycle until it reaches a terminal phase or the timeout
        elapses. The run *result* itself lands in the app's own store; this only
        tracks completion."""
        deadline = time.time() + timeout
        while True:
            result = self.get(run_id)
            if result.done:
                return result
            if time.time() > deadline:
                return Result(phase="Failed", message="aei: timed out waiting for run")
            time.sleep(poll)

    # --- internals ---

    def _http(self):
        if self._session is None:
            import requests  # lazy: only the live path needs it

            self._session = requests.Session()
        return self._session

    def _auth(self) -> dict:
        return {"Authorization": f"Bearer {self._bearer()}"}

    def _bearer(self) -> str:
        if self._token:
            return self._token
        try:
            with open(_SA_TOKEN, encoding="utf-8") as f:
                return f.read().strip()
        except OSError:
            return ""
