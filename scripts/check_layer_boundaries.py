#!/usr/bin/env python3
"""
Fail on cross-layer imports that bypass the intended monorepo boundaries.
"""

from __future__ import annotations

from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]

PYTHON_ROOT = ROOT / "python-surface-archive/python/disruptor_rs"
MYELON_ROOT = ROOT / "crates/legacy-wip"

ALLOWED_PYTHON_INTERNAL_FILES = {
    Path("python-surface-archive/python/disruptor_rs/__init__.py"),
    Path("python-surface-archive/python/disruptor_rs/multiprocess.py"),
}

PYTHON_INTERNAL_PATTERNS = [
    re.compile(r"\bfrom\s+\.\s+import\s+_internal\b"),
    re.compile(r"\bfrom\s+\.\.\s+import\s+_internal\b"),
    re.compile(r"\bfrom\s+disruptor_rs\._internal\s+import\b"),
    re.compile(r"\bimport\s+disruptor_rs\._internal\b"),
    re.compile(r"\bfrom\s+\._internal\s+import\b"),
    re.compile(r"\bfrom\s+\.\._internal\s+import\b"),
]

PYTHON_RAW_PATTERNS = [
    re.compile(r"\bfrom\s+\.\s+import\s+_internal_raw\b"),
    re.compile(r"\bfrom\s+\.\.\s+import\s+_internal_raw\b"),
    re.compile(r"\bfrom\s+disruptor_rs\._internal_raw\s+import\b"),
    re.compile(r"\bimport\s+disruptor_rs\._internal_raw\b"),
    re.compile(r"\bfrom\s+\._internal_raw\s+import\b"),
    re.compile(r"\bfrom\s+\.\._internal_raw\s+import\b"),
]

PYTHON_RAW_ATTRIBUTE_PATTERNS = [
    re.compile(r"\b_internal\._raw\b"),
    re.compile(r"\bdisruptor_rs\._internal\._raw\b"),
]

RUST_PRIVATE_PATTERNS = [
    re.compile(r"\bdisruptor_mp::(api|builder|consumer|cursor|producer|ringbuffer|wait)\b"),
    re.compile(r"\buse\s+disruptor_mp\s+as\s+inner\b"),
    re.compile(r"\bcrate::inner::"),
    re.compile(r"\binner::(api|builder|consumer|cursor|producer|ringbuffer|wait)\b"),
]


def find_line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def check_python_boundaries(failures: list[str]) -> None:
    for path in sorted(PYTHON_ROOT.rglob("*.py")):
        rel_path = path.relative_to(ROOT)
        text = path.read_text(encoding="utf-8")
        if rel_path not in ALLOWED_PYTHON_INTERNAL_FILES:
            for pattern in PYTHON_INTERNAL_PATTERNS:
                match = pattern.search(text)
                if match is None:
                    continue
                failures.append(
                    f"{rel_path}:{find_line_number(text, match.start())}: direct _internal import "
                    "outside the approved boundary modules"
                )
        for pattern in PYTHON_RAW_PATTERNS:
            match = pattern.search(text)
            if match is None:
                continue
            failures.append(
                f"{rel_path}:{find_line_number(text, match.start())}: direct _internal_raw import "
                "is forbidden outside the hidden loader path"
            )
        for pattern in PYTHON_RAW_ATTRIBUTE_PATTERNS:
            match = pattern.search(text)
            if match is None:
                continue
                failures.append(
                    f"{rel_path}:{find_line_number(text, match.start())}: direct _internal._raw access "
                    "is forbidden; use the private _internal factory helpers instead"
                )


def check_rust_boundaries(failures: list[str]) -> None:
    for path in sorted(MYELON_ROOT.rglob("*.rs")):
        rel_path = path.relative_to(ROOT)
        if "tests/ui/" in rel_path.as_posix():
            continue
        text = path.read_text(encoding="utf-8")
        for pattern in RUST_PRIVATE_PATTERNS:
            match = pattern.search(text)
            if match is None:
                continue
            failures.append(
                f"{rel_path}:{find_line_number(text, match.start())}: uses private disruptor-mp module path"
            )


def main() -> int:
    failures: list[str] = []
    check_python_boundaries(failures)
    check_rust_boundaries(failures)

    if failures:
        print("layer-boundary-check: found cross-layer violations:")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    print("layer-boundary-check: monorepo layer boundaries are clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
