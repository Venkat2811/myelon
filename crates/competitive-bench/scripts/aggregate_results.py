#!/usr/bin/env python3
import json
import os
import sys
from collections import defaultdict
from typing import Dict, Iterable, List, Optional

DEFAULT_OUTDIR = os.path.join(os.getcwd(), "output", "results")

DISPLAY_NAMES = {
    "competitive_shm": "disruptor-shm",
    "competitive_mmap": "disruptor-mmap",
    "competitive_raw_myelon_shm": "myelon-raw-shm",
    "competitive_raw_myelon_mmap": "myelon-raw-mmap",
    "raw_ring_shm": "disruptor-shm",
    "raw_ring_mmap": "disruptor-mmap",
    "crossbar": "crossbar-channel",
    "iceoryx2": "iceoryx2-shm",
    "shmipc": "shmipc-rs",
    "boost": "boost-message-queue",
    "ompi": "ompi-vader-self",
    "rusteron": "rusteron-aeron-ipc",
    "zmq": "zeromq-ipc",
    "zmqabs": "zeromq-ipc-abs",
    "zmqtcp": "zeromq-tcp",
    "iggy": "iggy-tcp",
    "redpanda": "redpanda-kafka",
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

PROTOCOL_DISPLAY = {
    "shm": "SHM",
    "mmap": "MMAP",
    "ipc": "IPC",
    "tcp": "TCP",
    "tcp_broker": "TCP / Brokered",
    "mpi": "MPI",
    "message_queue": "Message Queue",
}

PROTOCOL_ORDER = ["shm", "mmap", "ipc", "tcp", "tcp_broker", "mpi", "message_queue"]
FAMILY_DISPLAY = {
    "signal": "Signal",
    "pingpong": "Ping-Pong",
    "broadcast": "Broadcast",
}
FAMILY_ORDER = ["signal", "pingpong", "broadcast"]

PROTOCOL_BY_ADAPTER = {
    "disruptor-shm": "shm",
    "myelon-raw-shm": "shm",
    "shmipc-rs": "shm",
    "iceoryx2-shm": "shm",
    "disruptor-mmap": "mmap",
    "myelon-raw-mmap": "mmap",
    "crossbar-channel": "mmap",
    "crossbar-pubsub": "mmap",
    "rusteron-aeron-ipc": "ipc",
    "zeromq-ipc": "ipc",
    "zeromq-ipc-abs": "ipc",
    "zeromq-tcp": "tcp",
    "iggy-tcp": "tcp_broker",
    "redpanda-kafka": "tcp_broker",
    "ompi-vader-self": "mpi",
    "boost-message-queue": "message_queue",
}

ADAPTER_ORDER = [
    "disruptor-shm",
    "myelon-raw-shm",
    "shmipc-rs",
    "iceoryx2-shm",
    "disruptor-mmap",
    "myelon-raw-mmap",
    "crossbar-channel",
    "crossbar-pubsub",
    "rusteron-aeron-ipc",
    "zeromq-ipc",
    "zeromq-ipc-abs",
    "zeromq-tcp",
    "iggy-tcp",
    "redpanda-kafka",
    "ompi-vader-self",
    "boost-message-queue",
]


def default_outdir() -> str:
    return DEFAULT_OUTDIR


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        text = handle.read()
    start = text.find("{")
    if start == -1:
        raise ValueError(f"no json object found in {path}")
    return json.loads(text[start:])


def fmt_ns(ns: Optional[float]) -> str:
    if ns is None:
        return "-"
    ns = float(ns)
    if ns >= 1_000_000_000:
        return f"{ns / 1_000_000_000:.2f} s"
    if ns >= 1_000_000:
        return f"{ns / 1_000_000:.2f} ms"
    if ns >= 1_000:
        return f"{ns / 1_000:.2f} us"
    return f"{int(ns)} ns"


def fmt_rate(value: Optional[float]) -> str:
    if value is None:
        return "-"
    value = float(value)
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.2f}K"
    return f"{value:.0f}"


def parse_name_contract(name: str):
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


def normalize_adapter(raw_name: str) -> str:
    return DISPLAY_NAMES.get(raw_name, raw_name)


def latency_map_from_internal(latency: Optional[dict]) -> Dict[str, Optional[float]]:
    latency = latency or {}
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


def latency_map_from_external(latency: Optional[dict]) -> Dict[str, Optional[float]]:
    latency = latency or {}
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


def parse_row(name: str, payload: dict) -> dict:
    size_from_name, rate_from_name = parse_name_contract(name)
    if "scenarios" in payload:
        scenario = payload["scenarios"][0]
        identity = scenario["identity"]
        config = scenario["config"]
        outcome = scenario["outcome"]["Throughput"]
        workload = config["workload"]
        family = "pingpong"
        if identity["benchmark"] in {"raw_ring_shm", "raw_ring_mmap"}:
            scenario_name = str(identity.get("scenario", "")).lower()
            if os.path.basename(name).startswith("signal_") or "signal" in scenario_name:
                family = "signal"
        return {
            "adapter": normalize_adapter(identity["benchmark"]),
            "family": family,
            "size": size_from_name or workload["message_size_bytes"],
            "rate": rate_from_name or parse_canonical_measurement(config["measurement"]),
            "ops": outcome["consumers"]["average_throughput_ops_sec"],
            "fanout_ops": None,
            "consumer_count": workload.get("num_consumers") or workload.get("consumers"),
            "latency": latency_map_from_internal(outcome["latency"]),
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
        "family": payload.get("family", "pingpong"),
        "size": config.get("message_size") or size_from_name,
        "rate": rate,
        "ops": payload.get("throughput"),
        "fanout_ops": payload.get("fanout_throughput"),
        "consumer_count": payload.get("consumer_count") or config.get("consumers"),
        "latency": latency_map_from_external(latency),
    }


def protocol_for_adapter(adapter: str) -> str:
    return PROTOCOL_BY_ADAPTER.get(adapter, "other")


def adapter_sort_key(adapter: str):
    try:
        return (ADAPTER_ORDER.index(adapter), adapter)
    except ValueError:
        return (len(ADAPTER_ORDER), adapter)


def protocol_sort_key(protocol: str):
    try:
        return (PROTOCOL_ORDER.index(protocol), protocol)
    except ValueError:
        return (len(PROTOCOL_ORDER), protocol)


def family_sort_key(family: str):
    try:
        return (FAMILY_ORDER.index(family), family)
    except ValueError:
        return (len(FAMILY_ORDER), family)


def read_jsons(outdir: str):
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


def load_rows(outdir: str) -> List[dict]:
    rows = [parse_row(name, payload) for name, payload in read_jsons(outdir)]
    for row in rows:
        row["protocol"] = protocol_for_adapter(row["adapter"])
    return rows


def print_table(title: str, headers: List[str], rows: List[List[str]]) -> str:
    widths = [len(head) for head in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    def border(ch: str = "-") -> str:
        return "+" + "+".join(ch * (width + 2) for width in widths) + "+"

    lines = [title, border("=")]
    lines.append("|" + "|".join(f" {headers[i].ljust(widths[i])} " for i in range(len(headers))) + "|")
    lines.append(border("-"))
    for row in rows:
        lines.append("|" + "|".join(f" {row[i].ljust(widths[i])} " for i in range(len(row))) + "|")
    lines.append(border("-"))
    lines.append("")
    return "\n".join(lines)


def render_report(rows: List[dict]) -> str:
    by_family_protocol: Dict[str, Dict[str, List[dict]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        by_family_protocol[row.get("family", "pingpong")][row["protocol"]].append(row)

    throughput_headers = ["Adapter", "Size"] + [label for _, label in PERCENTILES] + ["Ops/s"]
    fixed_headers = ["Adapter", "Size", "Rate"] + [label for _, label in PERCENTILES] + ["Ops/s"]
    broadcast_throughput_headers = ["Adapter", "Size", "Consumers"] + [label for _, label in PERCENTILES] + ["Publish/s", "Fanout/s"]
    broadcast_fixed_headers = ["Adapter", "Size", "Consumers", "Rate"] + [label for _, label in PERCENTILES] + ["Publish/s", "Fanout/s"]

    any_throughput = any(row["rate"] is None for row in rows)
    any_fixed = any(row["rate"] is not None for row in rows)
    sections: List[str] = []

    for family in sorted(by_family_protocol.keys(), key=family_sort_key):
        by_protocol = by_family_protocol[family]
        family_label = FAMILY_DISPLAY.get(family, family)

        if any(row["rate"] is None for rows in by_protocol.values() for row in rows):
            sections.append(f"=== {family_label} Max Throughput By Protocol ===\n")
        for protocol in sorted(by_protocol.keys(), key=protocol_sort_key):
            throughput_rows: List[List[str]] = []
            for row in sorted(
                by_protocol[protocol],
                key=lambda item: (
                    adapter_sort_key(item["adapter"]),
                    item.get("consumer_count") or 0,
                    item["size"] or 0,
                    item["rate"] or 0,
                ),
            ):
                if row["rate"] is not None:
                    continue
                if family == "broadcast":
                    cells = [
                        row["adapter"],
                        f"{row['size']}B",
                        str(row.get("consumer_count") or "-"),
                    ]
                else:
                    cells = [row["adapter"], f"{row['size']}B"]
                for key, _label in PERCENTILES:
                    cells.append(fmt_ns(row["latency"].get(key)))
                if family == "broadcast":
                    cells.append(fmt_rate(row["ops"]))
                    cells.append(fmt_rate(row.get("fanout_ops")))
                else:
                    cells.append(fmt_rate(row["ops"]))
                throughput_rows.append(cells)
            if throughput_rows:
                sections.append(
                    print_table(
                        f"[{PROTOCOL_DISPLAY.get(protocol, protocol)}] {family_label} Max Throughput",
                        broadcast_throughput_headers if family == "broadcast" else throughput_headers,
                        throughput_rows,
                    )
                )

        if any(row["rate"] is not None for rows in by_protocol.values() for row in rows):
            sections.append(f"=== {family_label} Fixed Rate (CO-aware) By Protocol ===\n")
        for protocol in sorted(by_protocol.keys(), key=protocol_sort_key):
            fixed_rows: List[List[str]] = []
            for row in sorted(
                by_protocol[protocol],
                key=lambda item: (
                    adapter_sort_key(item["adapter"]),
                    item.get("consumer_count") or 0,
                    item["size"] or 0,
                    item["rate"] or 0,
                ),
            ):
                if row["rate"] is None:
                    continue
                if family == "broadcast":
                    cells = [
                        row["adapter"],
                        f"{row['size']}B",
                        str(row.get("consumer_count") or "-"),
                        fmt_rate(row["rate"]),
                    ]
                else:
                    cells = [row["adapter"], f"{row['size']}B", fmt_rate(row["rate"])]
                for key, _label in PERCENTILES:
                    cells.append(fmt_ns(row["latency"].get(key)))
                if family == "broadcast":
                    cells.append(fmt_rate(row["ops"]))
                    cells.append(fmt_rate(row.get("fanout_ops")))
                else:
                    cells.append(fmt_rate(row["ops"]))
                fixed_rows.append(cells)
            if fixed_rows:
                sections.append(
                    print_table(
                        f"[{PROTOCOL_DISPLAY.get(protocol, protocol)}] {family_label} Fixed Rate (CO-aware)",
                        broadcast_fixed_headers if family == "broadcast" else fixed_headers,
                        fixed_rows,
                    )
                )

    return "\n".join(section.rstrip() for section in sections if section).rstrip() + "\n"


def main(argv: Optional[List[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    outdir = argv[0] if argv else default_outdir()
    text = render_report(load_rows(outdir))
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
