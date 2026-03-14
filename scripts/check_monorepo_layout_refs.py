#!/usr/bin/env python3
"""
Fail active user-facing files that still reference the pre-monorepo layout.
"""

from __future__ import annotations

from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]

ACTIVE_GLOBS = [
    "README.md",
    "python-surface-archive/README.md",
    "python-surface-archive/benchmarks/**/*.py",
    "python-surface-archive/examples/**/*.py",
    "python-surface-archive/examples/**/*.md",
    "python-surface-archive/tests/unit/README.md",
    "python-surface-archive/tools/validation/README.md",
    "python-surface-archive/tools/validation/**/*.py",
    "crates/disruptor-mp/examples/**/*.rs",
]

STALE_PATTERNS = {
    "bindings/python": re.compile(r"bindings/python"),
    "disruptor-rs-playground": re.compile(r"disruptor-rs-playground"),
    "disruptor-mp-playground": re.compile(r"disruptor-mp-playground"),
    "disruptor_mp_playground": re.compile(r"disruptor_mp_playground"),
}


def iter_active_files() -> list[Path]:
    files: set[Path] = set()
    for pattern in ACTIVE_GLOBS:
        files.update(ROOT.glob(pattern))
    return sorted(path for path in files if path.is_file())


def find_line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def main() -> int:
    failures: list[str] = []
    for path in iter_active_files():
        text = path.read_text(encoding="utf-8")
        rel_path = path.relative_to(ROOT)
        for label, pattern in STALE_PATTERNS.items():
            match = pattern.search(text)
            if match is None:
                continue
            failures.append(
                f"{rel_path}:{find_line_number(text, match.start())}: stale layout reference '{label}'"
            )

    if failures:
        print("layout-ref-check: found stale pre-monorepo paths:")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    print("layout-ref-check: active user-facing files use current monorepo paths")
    return 0


if __name__ == "__main__":
    sys.exit(main())
