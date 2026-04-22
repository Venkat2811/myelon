#!/usr/bin/env python3
import json
import os
import sys
from collections import defaultdict

OUTDIR = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.getcwd(), "output", "results")


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


PERCENTILES = [
    ("p1", "P1"),
    ("p10", "P10"),
    ("p25", "P25"),
    ("p50", "P50"),
    ("p90", "P90"),
    ("p95", "P95"),
    ("p99", "P99"),
    ("p999", "P99.9"),
    ("p9999", "P99.99"),
    ("p99999", "P99.999"),
    ("p999999", "P99.9999"),
]


BACKEND_GROUPS = [
    ("shm", "SHM / IPC"),
    ("mmap", "mmap"),
]


def read_jsons(outdir):
    rows = []
    if not os.path.isdir(outdir):
        return rows
    for name in sorted(os.listdir(outdir)):
        if not name.endswith(".json"):
            continue
        path = os.path.join(outdir, name)
        try:
            payload = load_json(path)
        except Exception:
            continue
        rows.append((name, payload))
    return rows


def load_json(path):
    with open(path, "r", encoding="utf-8") as handle:
        text = handle.read()
    start = text.find("{")
    if start == -1:
        raise ValueError(f"no json object found in {path}")
    return json.loads(text[start:])


def fmt_ns(ns):
    if ns is None:
        return "-"
    ns = float(ns)
    if ns >= 1_000_000:
        return f"{ns / 1_000_000:.2f} ms"
    if ns >= 1_000:
        return f"{ns / 1_000:.2f} us"
    return f"{int(ns)} ns"


def fmt_rate(value):
    if value is None:
        return "-"
    value = float(value)
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.2f}K"
    return f"{value:.0f}"


def parse_name_contract(name):
    stem = os.path.basename(name).replace(".json", "")
    parts = stem.split("_")
    numeric = [int(part) for part in parts if part.isdigit()]
    if len(numeric) >= 2:
        return numeric[-2], numeric[-1]
    if len(numeric) == 1:
        return numeric[0], None
    return None, None


def parse_canonical_measurement(measurement):
    if measurement == "MaxThroughput":
        return None
    if isinstance(measurement, dict):
        co = measurement.get("CoAware")
        if isinstance(co, dict):
            return co.get("target_rate")
    return None


def normalize_adapter(raw_name):
    return DISPLAY_NAMES.get(raw_name, raw_name)


def latency_map_from_internal(latency):
    return {
        "p1": latency.get("p1_ns"),
        "p10": latency.get("p10_ns"),
        "p25": latency.get("p25_ns"),
        "p50": latency.get("p50_ns"),
        "p90": latency.get("p90_ns"),
        "p95": latency.get("p95_ns"),
        "p99": latency.get("p99_ns"),
        "p999": latency.get("p999_ns"),
        "p9999": latency.get("p9999_ns"),
        "p99999": latency.get("p99999_ns"),
        "p999999": latency.get("p999999_ns"),
    }


def latency_map_from_external(latency):
    return {
        "p1": latency.get("p1"),
        "p10": latency.get("p10"),
        "p25": latency.get("p25"),
        "p50": latency.get("p50"),
        "p90": latency.get("p90"),
        "p95": latency.get("p95"),
        "p99": latency.get("p99"),
        "p999": latency.get("p999"),
        "p9999": latency.get("p9999"),
        "p99999": latency.get("p99999"),
        "p999999": latency.get("p999999"),
    }


def parse_row(name, payload):
    size_from_name, rate_from_name = parse_name_contract(name)
    if "scenarios" in payload:
        scenario = payload["scenarios"][0]
        identity = scenario["identity"]
        config = scenario["config"]
        outcome = scenario["outcome"]["Throughput"]
        workload = config["workload"]
        return {
            "adapter": normalize_adapter(identity["benchmark"]),
            "size": size_from_name or workload["message_size_bytes"],
            "rate": rate_from_name or parse_canonical_measurement(config["measurement"]),
            "ops": outcome["consumers"]["average_throughput_ops_sec"],
            "latency": latency_map_from_internal(outcome["latency"]),
            "backend": "mmap" if "mmap" in identity["benchmark"] else "shm",
        }

    config = payload.get("config", {})
    measurement = payload.get("measurement_mode")
    latency = payload.get("coordinated_omission_stats") or payload.get("latency_stats", {})
    base = os.path.basename(name).replace(".json", "")
    adapter_name = "_".join([part for part in base.split("_") if not part.isdigit()]) or base
    rate = rate_from_name
    if rate is None and measurement == "fixed_rate":
        rate = payload.get("target_rate")
    return {
        "adapter": normalize_adapter(payload.get("adapter", adapter_name)),
        "size": size_from_name or config.get("message_size"),
        "rate": rate,
        "ops": payload.get("throughput"),
        "latency": latency_map_from_external(latency),
        "backend": "mmap" if "mmap" in adapter_name else "shm",
    }


def print_table(title, headers, rows):
    widths = [len(head) for head in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    def border(ch="-"):
        return "+" + "+".join(ch * (width + 2) for width in widths) + "+"

    print(title)
    print(border("="))
    print("|" + "|".join(f" {headers[i].ljust(widths[i])} " for i in range(len(headers))) + "|")
    print(border("-"))
    for row in rows:
        print("|" + "|".join(f" {row[i].ljust(widths[i])} " for i in range(len(row))) + "|")
    print(border("-"))
    print()


def main():
    parsed = [parse_row(name, payload) for name, payload in read_jsons(OUTDIR)]
    by_backend = defaultdict(list)
    for row in parsed:
        by_backend[row["backend"]].append(row)

    throughput_headers = ["Adapter", "Size"] + [label for _, label in PERCENTILES] + ["Ops/s"]
    fixed_headers = ["Adapter", "Size", "Rate"] + [label for _, label in PERCENTILES] + ["Ops/s"]

    for backend_key, backend_label in BACKEND_GROUPS:
        backend_rows = by_backend.get(backend_key, [])
        if not backend_rows:
            continue
        throughput_rows = []
        fixed_rows = []
        for row in sorted(backend_rows, key=lambda item: (item["adapter"], item["size"] or 0, item["rate"] or 0)):
            cells = [row["adapter"], f"{row['size']}B"]
            if row["rate"] is not None:
                cells.append(fmt_rate(row["rate"]))
            for key, _label in PERCENTILES:
                cells.append(fmt_ns(row["latency"].get(key)))
            cells.append(fmt_rate(row["ops"]))
            if row["rate"] is None:
                throughput_rows.append(cells)
            else:
                fixed_rows.append(cells)

        if throughput_rows:
            print_table(
                f"[{backend_label}] Max Throughput",
                throughput_headers,
                throughput_rows,
            )
        if fixed_rows:
            print_table(
                f"[{backend_label}] Fixed Rate (CO-aware)",
                fixed_headers,
                fixed_rows,
            )


if __name__ == "__main__":
    main()
