#!/usr/bin/env bash
# generate.sh — regenerate an AEI app SDK's *typed models* for any language from
# the one published AgentRun CRD schema.
#
# This is the answer to "how does the SDK scale across N languages": you do NOT
# hand-author a model per language. The CRD's openAPIV3Schema is machine-readable
# (controller-gen already emits it from the Go structs), and every language's
# Kubernetes client already provides the wire — so adding a language is a
# generator invocation, not a new hand-written SDK. Same model the Kubernetes
# client libraries use: one OpenAPI spec, generated clients per language.
#
#   ./generate.sh python                 # pydantic v2 (datamodel-code-generator)
#   ./generate.sh go        [out.go]     # quicktype
#   ./generate.sh typescript [out.ts]    # quicktype
#   ./generate.sh rust|swift|csharp|kotlin|java|dart|ruby|... [out]
#
# The generated file is the raw type mirror; a language's SDK pairs it with its
# k8s client and a ~100-line idiomatic veneer (dispatch/wait/run_name). The
# reference Python SDK's veneer lives in aei/client.py.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
crd="$here/../controller/config/crd/agents.x-k8s.io_agentruns.yaml"
lang="${1:-}"
out="${2:-}"

if [[ -z "$lang" ]]; then
  echo "usage: generate.sh <python|go|typescript|rust|swift|csharp|kotlin|...> [outfile]" >&2
  echo "the wire is the language's Kubernetes client; this generates only the typed models" >&2
  exit 2
fi

# 1. Extract the machine-readable schema the controller already publishes. This
#    is the single source of truth for every language. A fixed basename in a
#    throwaway dir keeps the generated file's header deterministic across runs.
schema_dir="$(mktemp -d)"
schema="$schema_dir/agentrun.schema.json"
tmp_out=""
trap 'rm -rf "$schema_dir"; [[ -z "$tmp_out" ]] || rm -f "$tmp_out"' EXIT
python3 - "$crd" >"$schema" <<'PY'
import json, sys, yaml
crd = yaml.safe_load(open(sys.argv[1]))
ver = next(v for v in crd["spec"]["versions"] if v["name"] == "v1alpha1")
schema = ver["schema"]["openAPIV3Schema"]
schema["$schema"] = "http://json-schema.org/draft-07/schema#"
schema["title"] = "AgentRun"
json.dump(schema, sys.stdout)
PY

# 2. Generate typed models from that schema.
case "$lang" in
  python)
    # pydantic v2 models; the veneer (aei/models.py) adds ergonomics on top.
    # --base-class supplies populate_by_name + protected_namespaces=() (see
    # aei/_base.py), so the generated file needs no hand edits.
    #
    # The output depends on the generator and formatter versions, so the SDK's `gen`
    # extra pins all three (pip install -e 'python[gen]'), and the formatters are named
    # because the generator's default is about to change. Not exec'd, so the EXIT trap
    # still removes the temporary schema. It always writes inside the package and moves
    # the file afterwards, because isort decides that `aei` is first-party from the
    # output's location: written anywhere else, the imports are grouped differently.
    tmp_out="$here/python/aei/.agentrun.$$.py"
    datamodel-codegen \
      --input "$schema" --input-file-type jsonschema \
      --output-model-type pydantic_v2.BaseModel \
      --base-class aei._base.GeneratedModel \
      --snake-case-field --use-double-quotes \
      --use-schema-description --use-field-description \
      --disable-timestamp \
      --formatters black isort \
      --output "$tmp_out"
    mv "$tmp_out" "${out:-$here/python/aei/_agentrun.py}"
    ;;
  *)
    # quicktype covers a broad language matrix from the same JSON Schema.
    args=(-y quicktype --src "$schema" --src-lang schema --lang "$lang" --top-level AgentRun)
    [[ -n "$out" ]] && args+=(-o "$out")
    npx "${args[@]}"
    ;;
esac
