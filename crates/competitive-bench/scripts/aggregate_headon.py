#!/usr/bin/env python3
import json
import os
import sys
from collections import defaultdict

HEADON_DIR = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.getcwd(), "output", "headon")


DISPLAY_NAMES = {
    "competitive_shm": "disruptor-shm",
    "competitive_mmap": "disruptor-mmap",
    "competitive_raw_myelon_shm": "myelon-raw-shm",
    "competitive_raw_myelon_mmap": "myelon-raw-mmap",
    "crossbar": "crossbar-channel",
    "shmipc": "shmipc-rs",
    "boost": "boost-message-queue",
    "ompi": "ompi-vader-self",
    "rusteron": "rusteron-aeron-ipc",
    "zmq": "zeromq-ipc",
    "zmqabs": "zeromq-ipc-abs",
    "zmqtcp": "zeromq-tcp",
}


DELTA_COLUMNS = [
    ("p1", "ΔP1"),
    ("p10", "ΔP10"),
    ("p25", "ΔP25"),
    ("p50", "ΔP50"),
    ("p95", "ΔP95"),
    ("p99", "ΔP99"),
    ("p999", "ΔP99.9"),
    ("p99999", "ΔP99.999"),
]


def read_json(path):
    with open(path, "r", encoding="utf-8") as handle:
        text = handle.read()
    start = text.find("{")
    if start == -1:
        raise ValueError(f"no json object found in {path}")
    return json.loads(text[start:])


def fmt_rate(value):
    if value is None:
        return "-"
    value = float(value)
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.2f}K"
    return f"{value:.0f}"


def normalize_adapter(raw_name):
    return DISPLAY_NAMES.get(raw_name, raw_name)


def parse_canonical_measurement(measurement):
    if measurement == "MaxThroughput":
        return None
    if isinstance(measurement, dict):
        co = measurement.get("CoAware")
        if isinstance(co, dict):
            return co.get("target_rate")
    return None


def latency_map_from_internal(latency):
    return {
        "p1": latency.get("p1_ns"),
        "p10": latency.get("p10_ns"),
        "p25": latency.get("p25_ns"),
        "p50": latency.get("p50_ns"),
        "p95": latency.get("p95_ns"),
        "p99": latency.get("p99_ns"),
        "p999": latency.get("p999_ns"),
        "p99999": latency.get("p99999_ns"),
    }


def latency_map_from_external(latency):
    return {
        "p1": latency.get("p1"),
        "p10": latency.get("p10"),
        "p25": latency.get("p25"),
        "p50": latency.get("p50"),
        "p95": latency.get("p95"),
        "p99": latency.get("p99"),
        "p999": latency.get("p999"),
        "p99999": latency.get("p99999"),
    }


def parse(payload, fallback_adapter):
    if "scenarios" in payload:
        scenario = payload["scenarios"][0]
        identity = scenario["identity"]
        config = scenario["config"]
        outcome = scenario["outcome"]["Throughput"]
        return {
            "adapter": normalize_adapter(identity["benchmark"]),
            "size": config["workload"]["message_size_bytes"],
            "rate": parse_canonical_measurement(config["measurement"]),
            "ops": outcome["consumers"]["average_throughput_ops_sec"],
            "latency": latency_map_from_internal(outcome["latency"]),
        }

    mode = payload.get("measurement_mode", "max_throughput")
    rate = payload.get("target_rate") if mode == "fixed_rate" else None
    latency = payload.get("coordinated_omission_stats") or payload.get("latency_stats", {})
    return {
        "adapter": normalize_adapter(payload.get("adapter", fallback_adapter)),
        "size": payload.get("config", {}).get("message_size"),
        "rate": rate,
        "ops": payload.get("throughput"),
        "latency": latency_map_from_external(latency),
    }


def fmt_delta(left, right):
    if left in (None, 0) or right is None:
        return "-"
    delta = ((float(right) / float(left)) - 1.0) * 100.0
    return f"{delta:+.1f}%"


def print_table(title, headers, rows):
    widths = [len(head) for head in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def border(ch="-"):
        return "+" + "+".join(ch * (w + 2) for w in widths) + "+"

    print(title)
    print(border("="))
    print("|" + "|".join(f" {headers[i].ljust(widths[i])} " for i in range(len(headers))) + "|")
    print(border("-"))
    for row in rows:
        print("|" + "|".join(f" {row[i].ljust(widths[i])} " for i in range(len(row))) + "|")
    print(border("-"))
    print()


def main():
    grouped = defaultdict(dict)
    if not os.path.isdir(HEADON_DIR):
        print(f"missing headon dir: {HEADON_DIR}")
        return 1

    for adapter_dir_name in sorted(os.listdir(HEADON_DIR)):
        adapter_dir = os.path.join(HEADON_DIR, adapter_dir_name)
        if not os.path.isdir(adapter_dir):
            continue
        for name in sorted(os.listdir(adapter_dir)):
            if not name.endswith(".json"):
                continue
            payload = parse(read_json(os.path.join(adapter_dir, name)), adapter_dir_name)
            key = (payload["rate"], payload["size"])
            grouped[key][payload["adapter"]] = payload

    if not grouped:
        print("no head-on pairs found")
        return 0

    headers = ["Case", "Left", "Right", "Left Ops/s", "Right Ops/s", "ΔOps/s"] + [
        label for _, label in DELTA_COLUMNS
    ]
    throughput_rows = []
    fixed_rows = []

    for key in sorted(grouped.keys(), key=lambda item: ((item[0] is not None), item[0] or 0, item[1])):
        rate, size = key
        adapters = sorted(grouped[key].keys())
        if len(adapters) < 2:
            continue
        left = grouped[key][adapters[0]]
        right = grouped[key][adapters[1]]
        case = f"{size}B" if rate is None else f"{size}B @ {fmt_rate(rate)}/s"
        row = [
            case,
            adapters[0],
            adapters[1],
            fmt_rate(left["ops"]),
            fmt_rate(right["ops"]),
            fmt_delta(left["ops"], right["ops"]),
        ]
        for key_name, _label in DELTA_COLUMNS:
            row.append(fmt_delta(left["latency"].get(key_name), right["latency"].get(key_name)))
        if rate is None:
            throughput_rows.append(row)
        else:
            fixed_rows.append(row)

    if throughput_rows:
        print_table("Head-on Summary: Max Throughput", headers, throughput_rows)
    if fixed_rows:
        print_table("Head-on Summary: Fixed Rate (CO-aware)", headers, fixed_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
