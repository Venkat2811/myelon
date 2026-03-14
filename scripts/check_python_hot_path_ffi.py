#!/usr/bin/env python3
"""
Fail if hot-path per-event FFI calls spread beyond the explicit allowlist.
"""

from __future__ import annotations

from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]
PYTHON_ROOT = ROOT / "python-surface-archive/python/disruptor_rs"

ALLOWED_FILES = {
    Path("python-surface-archive/python/disruptor_rs/batching_producer.py"),
    Path("python-surface-archive/python/disruptor_rs/_dataplane_runtime.py"),
    Path("python-surface-archive/python/disruptor_rs/external_integrations/_hot_path_runtime.py"),
}

HOT_PATH_PATTERNS = [
    re.compile(
        r"self\._producer\.(publish|publish_fast|publish_zero_copy|publish_batch|"
        r"try_publish_batch|publish_batch_zero_copy|publish_batch_preallocated|"
        r"wait_for_consumers_ready|get_consumer_count)\("
    ),
    re.compile(
        r"self\._consumer\.(try_consume|try_consume_next_with_sequence|process_available|"
        r"process_available_optimized|process_available_zero_copy_batch|"
        r"process_available_bulk|process_available_and_count|process_available_batch|"
        r"process_available_stats|signal_ready)\("
    ),
    re.compile(
        r"self\.(producer|batching_producer)\.(publish|publish_batch|publish_zero_copy|"
        r"publish_batch_zero_copy|publish_batch_preallocated|wait_for_consumers_ready|"
        r"get_consumer_count)\("
    ),
    re.compile(
        r"self\.consumer\.(try_consume_next|process_available|process_available_and_count|"
        r"process_available_batch|process_available_stats|signal_ready)\("
    ),
]


def find_line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def main() -> int:
    failures: list[str] = []
    for path in sorted(PYTHON_ROOT.rglob("*.py")):
        rel_path = path.relative_to(ROOT)
        if rel_path in ALLOWED_FILES:
            continue
        text = path.read_text(encoding="utf-8")
        for pattern in HOT_PATH_PATTERNS:
            match = pattern.search(text)
            if match is None:
                continue
            failures.append(
                f"{rel_path}:{find_line_number(text, match.start())}: hot-path per-event FFI call "
                "must stay inside the current allowlist until N03 replaces it"
            )

    if failures:
        print("python-hot-path-ffi-check: found hot-path FFI calls outside allowlist:")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    print("python-hot-path-ffi-check: hot-path per-event FFI calls are constrained to the allowlist")
    return 0


if __name__ == "__main__":
    sys.exit(main())
