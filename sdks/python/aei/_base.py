"""Base class the generated models ([_agentrun.py](_agentrun.py)) inherit, wired
via datamodel-code-generator's ``--base-class``. It supplies the two pydantic
settings the AgentRun schema needs that the generator cannot infer from JSON
Schema:

  * ``populate_by_name`` — let callers construct with the snake_case Python field
    names, not only the camelCase wire aliases;
  * ``protected_namespaces=()`` — allow the timing fields named ``model_*``
    (``modelSeconds``, ``modelCalls``) without pydantic's protected-namespace
    warning.

Keeping these here, off the generated file, is what lets ``_agentrun.py`` be
regenerated verbatim from the CRD schema (``sdks/generate.sh python``).
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class GeneratedModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, protected_namespaces=())
