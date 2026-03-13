#!/usr/bin/env bash
set -euo pipefail

WORKFLOW_DIR="python-surface-archive/.github/workflows"
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

  if rg -q "working-directory: .*bindings/python|cd bindings/python|--manifest-path\\s+bindings/python|--directory\\s+bindings/python" "$workflow_file"; then
    echo "  ✗ stale monorepo path reference found: bindings/python" >&2
    errors=1
  fi

  if ! rg -q "python-surface-archive" "$workflow_file"; then
    echo "  ⚠ monorepo layout path missing in $workflow_file" >&2
    errors=1
  fi
done < <(find "$WORKFLOW_DIR" -maxdepth 1 -type f \( -name '*.yml' -o -name '*.yaml' \) -print0)

if rg -q "bindings/python|cd bindings/python" "python-surface-archive/README.md"; then
  echo "  ✗ stale monorepo path reference found in py README: bindings/python" >&2
  errors=1
fi

if [ "$errors" -ne 0 ]; then
  echo "ci-guard: validation failed" >&2
  exit 1
fi

echo "ci-guard: validation passed"
