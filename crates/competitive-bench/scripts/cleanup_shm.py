#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

ROOT = Path('/dev/shm')
PATTERNS = [
    r'^boost_',
    r'^broadc',
    r'^bs[0-9a-z]+',
    r'^bc[0-9a-z]+',
    r'^cb[0-9a-z]+',
    r'^crossbar-',
    r'^ompi_',
    r'^shmipc',
    r'^zmq',
    r'^rusteron',
    r'^myelon',
    r'^disruptor_',
    r'^mp_',
    r'^mb_',
    r'^cpshm',
    r'^cmp',
]
REGEXES = [re.compile(p) for p in PATTERNS]


def matches(name: str) -> bool:
    return any(rx.match(name) for rx in REGEXES)


def main() -> int:
    parser = argparse.ArgumentParser(description='Remove competitive-bench-owned SHM artifacts from /dev/shm.')
    parser.add_argument('--dry-run', action='store_true', help='List matching files without deleting them.')
    args = parser.parse_args()

    if not ROOT.exists():
        print('/dev/shm does not exist on this host')
        return 0

    removed = 0
    bytes_removed = 0
    matched = 0
    for entry in ROOT.iterdir():
        if not matches(entry.name):
            continue
        matched += 1
        try:
            size = entry.stat().st_size
        except FileNotFoundError:
            continue
        if args.dry_run:
            print(f'{entry.name}\t{size}')
            continue
        try:
            entry.unlink()
            removed += 1
            bytes_removed += size
        except FileNotFoundError:
            continue
        except IsADirectoryError:
            continue
        except PermissionError:
            print(f'skip-permission\t{entry.name}')

    if args.dry_run:
        print(f'matched={matched}')
    else:
        print(f'removed_files={removed}')
        print(f'removed_bytes={bytes_removed}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
