#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def run_report(binary: str, args: list[str]) -> dict:
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        out_path = Path(tmp.name)
    cmd = [
        "cargo",
        "run",
        "-q",
        "-p",
        "perf-bench",
        "--profile",
        "competitive",
        "--bin",
        binary,
        "--",
        *args,
        "--json-out",
        str(out_path),
    ]
    proc = subprocess.run(
        cmd,
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"benchmark command failed: {' '.join(cmd)}\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    try:
        with out_path.open() as fh:
            return json.load(fh)
    finally:
        out_path.unlink(missing_ok=True)


def throughput_rows(report: dict) -> list[dict]:
    rows = []
    for scenario in report.get("scenarios", []):
        outcome = scenario.get("outcome", {})
        if "Throughput" in outcome:
            rows.append(scenario)
    if not rows:
        raise AssertionError("report did not contain throughput scenarios")
    return rows


def single_row(report: dict) -> dict:
    rows = throughput_rows(report)
    if len(rows) != 1:
        raise AssertionError(f"expected exactly one scenario, got {len(rows)}")
    return rows[0]


def producer_ops(row: dict) -> float:
    return row["outcome"]["Throughput"]["producer"]["throughput_ops_sec"]


def p95_ns(row: dict) -> int:
    latency = row["outcome"]["Throughput"].get("latency")
    if latency is None:
        raise AssertionError("expected latency stats in throughput outcome")
    return latency["p95_ns"]


def p9999_ns(row: dict) -> int:
    latency = row["outcome"]["Throughput"].get("latency")
    if latency is None:
        raise AssertionError("expected latency stats in throughput outcome")
    return latency["p9999_ns"]


def scenario_name(row: dict) -> str:
    return row["identity"]["scenario"]


def assert_ge(label: str, actual: float, floor: float) -> None:
    if actual < floor:
        raise AssertionError(f"{label}: expected >= {floor:.2f}, got {actual:.2f}")


def assert_le(label: str, actual: float, ceiling: float) -> None:
    if actual > ceiling:
        raise AssertionError(f"{label}: expected <= {ceiling:.2f}, got {actual:.2f}")


def framed_64_floor() -> None:
    thresholds = {
        "shm": (3_500_000.0, 50_000.0),
        "mmap": (3_200_000.0, 80_000.0),
    }
    for backend, (ops_floor, p9999_ceiling) in thresholds.items():
        row = single_row(
            run_report(
                "perf-bench-pingpong",
                [
                    "--layer",
                    "framed",
                    "--backend",
                    backend,
                    "--size",
                    "64",
                    "--wait-strategy",
                    "busyspin",
                    "--mode",
                    "throughput",
                    "--num-messages",
                    "50000",
                    "--warmup",
                    "5000",
                ],
            )
        )
        assert_ge(f"framed 64B {backend} ops/s", producer_ops(row), ops_floor)
        assert_le(f"framed 64B {backend} p99.99", p9999_ns(row), p9999_ceiling)
        print(
            f"[ok] framed 64B {backend}: {producer_ops(row):.1f} ops/s, "
            f"p99.99={p9999_ns(row)}ns ({scenario_name(row)})"
        )


def typed_zc_rkyv_floor() -> dict[str, float]:
    speeds: dict[str, float] = {}
    thresholds = {
        "shm": (1_500_000.0, 100_000.0),
        "mmap": (1_200_000.0, 120_000.0),
    }
    for backend, (ops_floor, p9999_ceiling) in thresholds.items():
        row = single_row(
            run_report(
                "perf-bench-pingpong",
                [
                    "--layer",
                    "typed_zc",
                    "--backend",
                    backend,
                    "--codec",
                    "rkyv",
                    "--batch-size",
                    "1",
                    "--wait-strategy",
                    "busyspin",
                    "--mode",
                    "throughput",
                    "--num-messages",
                    "50000",
                    "--warmup",
                    "5000",
                ],
            )
        )
        speeds[backend] = producer_ops(row)
        assert_ge(f"typed_zc rkyv batch1 {backend} ops/s", speeds[backend], ops_floor)
        assert_le(
            f"typed_zc rkyv batch1 {backend} p99.99", p9999_ns(row), p9999_ceiling
        )
        print(
            f"[ok] typed_zc rkyv batch1 {backend}: {speeds[backend]:.1f} ops/s, "
            f"p99.99={p9999_ns(row)}ns ({scenario_name(row)})"
        )
    return speeds


def codec_rkyv_speedup_floor(typed_zc_speeds: dict[str, float]) -> None:
    speedup_floor = 4.0
    for backend, typed_speed in typed_zc_speeds.items():
        row = single_row(
            run_report(
                "perf-bench-pingpong",
                [
                    "--layer",
                    "codec",
                    "--backend",
                    backend,
                    "--codec",
                    "rkyv",
                    "--batch-size",
                    "1",
                    "--wait-strategy",
                    "busyspin",
                    "--mode",
                    "throughput",
                    "--num-messages",
                    "50000",
                    "--warmup",
                    "5000",
                ],
            )
        )
        codec_speed = producer_ops(row)
        speedup = typed_speed / codec_speed
        assert_ge(f"typed_zc vs codec rkyv speedup {backend}", speedup, speedup_floor)
        print(
            f"[ok] typed_zc vs codec rkyv {backend}: "
            f"{typed_speed:.1f}/{codec_speed:.1f} ops/s = {speedup:.2f}x"
        )


def raw_wait_strategy_sanity() -> None:
    ops_floor = 1_000_000.0
    p95_ceiling = 20_000.0
    for backend in ("shm", "mmap"):
        for wait_strategy in ("sleep", "block"):
            row = single_row(
                run_report(
                    "perf-bench-pingpong",
                    [
                        "--layer",
                        "raw_ring",
                        "--backend",
                        backend,
                        "--size",
                        "64",
                        "--wait-strategy",
                        wait_strategy,
                        "--mode",
                        "throughput",
                        "--num-messages",
                        "50000",
                        "--warmup",
                        "5000",
                    ],
                )
            )
            ops = producer_ops(row)
            assert_ge(f"raw_ring {backend} {wait_strategy} ops/s", ops, ops_floor)
            assert_le(f"raw_ring {backend} {wait_strategy} p95", p95_ns(row), p95_ceiling)
            print(
                f"[ok] raw_ring {backend} {wait_strategy}: "
                f"{ops:.1f} ops/s, p95={p95_ns(row)}ns ({scenario_name(row)})"
            )


def main() -> int:
    typed_speeds = typed_zc_rkyv_floor()
    codec_rkyv_speedup_floor(typed_speeds)
    framed_64_floor()
    raw_wait_strategy_sanity()
    print("[ok] competitive perf floor gate passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"[fail] {exc}", file=sys.stderr)
        raise SystemExit(1)
