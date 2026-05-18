#!/usr/bin/env bash
set -euo pipefail

WORKFLOW_DIR=".github/workflows"
errors=0

if [ ! -d "$WORKFLOW_DIR" ]; then
  echo "ci-guard: workflow dir not found: $WORKFLOW_DIR" >&2
  exit 1
fi

while IFS= read -r -d '' workflow_file; do
  echo "Validating: $workflow_file"

  if rg -q "windows-latest" "$workflow_file"; then
    echo "  ✗ banned platform found: windows-latest" >&2
    errors=1
  fi

  if rg -q "working-directory: .*bindings/python|cd bindings/python|--manifest-path\\s+bindings/python|--directory\\s+bindings/python|py/[^[:space:]]+|crates/[^[:space:]]+/Makefile" "$workflow_file"; then
    echo "  ✗ stale internal path reference found" >&2
    errors=1
  fi
done < <(find "$WORKFLOW_DIR" -maxdepth 1 -type f \( -name '*.yml' -o -name '*.yaml' \) -print0)

if [ "$errors" -ne 0 ]; then
  echo "ci-guard: validation failed" >&2
  exit 1
fi

echo "ci-guard: validation passed"
