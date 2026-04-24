#!/usr/bin/env python3
"""Generate line graphs from competitive-bench simple-smoke results."""
import json
import glob
import os
import sys
import re
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

OUTDIR = sys.argv[1] if len(sys.argv) > 1 else "output/simple-smoke"
GRAPH_DIR = os.path.join(OUTDIR, "graphs")
os.makedirs(GRAPH_DIR, exist_ok=True)

SIZES = [32, 64, 128, 1024, 2048, 4096]

# --- Adapter display config ---
ADAPTER_COLORS = {
    "disruptor-shm":      "#1f77b4",
    "disruptor-mmap":     "#aec7e8",
    "myelon-raw-shm":     "#ff7f0e",
    "myelon-raw-mmap":    "#ffbb78",
    "iceoryx2-shm":       "#2ca02c",
    "crossbar-channel":   "#d62728",
    "boost-message-queue":"#9467bd",
    "ompi-vader-self":    "#8c564b",
    "zeromq-ipc":         "#e377c2",
    "zeromq-ipc-abs":     "#f7b6d2",
    "zeromq-tcp":         "#7f7f7f",
    "iggy-tcp":           "#bcbd22",
    "redpanda-kafka":     "#17becf",
}

ADAPTER_MARKERS = {
    "disruptor-shm":      "o",
    "disruptor-mmap":     "s",
    "myelon-raw-shm":     "D",
    "myelon-raw-mmap":    "^",
    "iceoryx2-shm":       "v",
    "crossbar-channel":   "<",
    "boost-message-queue":">",
    "ompi-vader-self":    "p",
    "zeromq-ipc":         "h",
    "zeromq-ipc-abs":     "H",
    "zeromq-tcp":         "*",
    "iggy-tcp":           "X",
    "redpanda-kafka":     "P",
}

# --- File name to adapter mapping ---
FILE_PATTERNS = [
    (r"^disruptor_(\d+)\.json$",        "disruptor-shm"),
    (r"^disruptor_mmap_(\d+)\.json$",   "disruptor-mmap"),
    (r"^myelon_raw_(\d+)\.json$",       "myelon-raw-shm"),
    (r"^myelon_raw_mmap_(\d+)\.json$",  "myelon-raw-mmap"),
    (r"^iceoryx2_(\d+)\.json$",         "iceoryx2-shm"),
    (r"^crossbar_(\d+)\.json$",         "crossbar-channel"),
    (r"^boost_(\d+)\.json$",            "boost-message-queue"),
    (r"^ompi_(\d+)\.json$",             "ompi-vader-self"),
    (r"^zmq_(\d+)\.json$",             "zeromq-ipc"),
    (r"^zmqabs_(\d+)\.json$",          "zeromq-ipc-abs"),
    (r"^zmqtcp_(\d+)\.json$",          "zeromq-tcp"),
    (r"^iggy_(\d+)\.json$",            "iggy-tcp"),
    (r"^redpanda_(\d+)\.json$",        "redpanda-kafka"),
    # fixed-rate variants
    (r"^disruptor_(\d+)_(\d+)\.json$",        "disruptor-shm"),
    (r"^disruptor_mmap_(\d+)_(\d+)\.json$",   "disruptor-mmap"),
    (r"^myelon_raw_(\d+)_(\d+)\.json$",       "myelon-raw-shm"),
    (r"^myelon_raw_mmap_(\d+)_(\d+)\.json$",  "myelon-raw-mmap"),
    (r"^iceoryx2_(\d+)_(\d+)\.json$",         "iceoryx2-shm"),
    (r"^crossbar_(\d+)_(\d+)\.json$",         "crossbar-channel"),
    (r"^boost_(\d+)_(\d+)\.json$",            "boost-message-queue"),
    (r"^ompi_(\d+)_(\d+)\.json$",             "ompi-vader-self"),
    (r"^zmq_(\d+)_(\d+)\.json$",             "zeromq-ipc"),
    (r"^zmqabs_(\d+)_(\d+)\.json$",          "zeromq-ipc-abs"),
    (r"^zmqtcp_(\d+)_(\d+)\.json$",          "zeromq-tcp"),
    (r"^iggy_(\d+)_(\d+)\.json$",            "iggy-tcp"),
    (r"^redpanda_(\d+)_(\d+)\.json$",        "redpanda-kafka"),
]


def parse_file(filepath):
    """Parse a result JSON, handling both internal (perf-bench) and external formats."""
    fname = os.path.basename(filepath)

    # Skip broadcast files
    if fname.startswith("broadcast_") or fname.startswith("signal_"):
        return None

    adapter = None
    size = None
    is_fixed_rate = False

    for pattern, adapter_name in FILE_PATTERNS:
        m = re.match(pattern, fname)
        if m:
            adapter = adapter_name
            groups = m.groups()
            size = int(groups[0])
            is_fixed_rate = len(groups) > 1
            break

    if adapter is None:
        return None

    try:
        with open(filepath) as f:
            text = f.read().strip()
            # Internal format may have non-JSON prefix
            idx = text.find("{")
            if idx < 0:
                return None
            d = json.loads(text[idx:])
    except (json.JSONDecodeError, ValueError):
        return None

    throughput = None
    latency = {}

    # Internal format (perf-bench)
    if "scenarios" in d:
        for sc in d["scenarios"]:
            out = sc.get("outcome", {})
            tp = out.get("Throughput", {})
            cons = tp.get("consumers", {})
            throughput = cons.get("average_throughput_ops_sec")
            lat = cons.get("latency", {})
            if lat:
                latency = lat
            break
    else:
        # External format
        throughput = d.get("throughput")
        latency = d.get("latency_stats", {})

    if throughput is None:
        return None

    return {
        "adapter": adapter,
        "size": size,
        "throughput": throughput,
        "p50": latency.get("p50"),
        "p99": latency.get("p99"),
        "p999": latency.get("p999"),
        "is_fixed_rate": is_fixed_rate,
    }


def load_all(outdir):
    results = []
    for f in glob.glob(os.path.join(outdir, "*.json")):
        r = parse_file(f)
        if r:
            results.append(r)
    return results


def size_label(s):
    if s >= 1024:
        return f"{s//1024}KB"
    return f"{s}B"


def plot_line(ax, data, adapters, sizes, y_key, ylabel, title, log_y=False):
    """Plot a line chart: X=message size, Y=metric, one line per adapter."""
    for adapter in adapters:
        pts = [(r["size"], r[y_key]) for r in data if r["adapter"] == adapter and r[y_key] is not None]
        if not pts:
            continue
        pts.sort()
        xs, ys = zip(*pts)
        color = ADAPTER_COLORS.get(adapter, "#333333")
        marker = ADAPTER_MARKERS.get(adapter, "o")
        ax.plot(xs, ys, marker=marker, label=adapter, color=color, linewidth=2, markersize=7)

    ax.set_xscale("log", base=2)
    ax.set_xticks(sizes)
    ax.set_xticklabels([size_label(s) for s in sizes])
    ax.set_xlabel("Message Size")
    ax.set_ylabel(ylabel)
    ax.set_title(title, fontsize=13, fontweight="bold")
    if log_y:
        ax.set_yscale("log")
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=7, loc="best", ncol=2)


def fmt_throughput(val, _):
    if val >= 1e6:
        return f"{val/1e6:.1f}M"
    if val >= 1e3:
        return f"{val/1e3:.0f}K"
    return f"{val:.0f}"


def fmt_latency_ns(val, _):
    if val >= 1e6:
        return f"{val/1e6:.1f}ms"
    if val >= 1e3:
        return f"{val/1e3:.1f}us"
    return f"{val:.0f}ns"


# ---- Load data ----
results = load_all(OUTDIR)
throughput_data = [r for r in results if not r["is_fixed_rate"]]
fixed_rate_data = [r for r in results if r["is_fixed_rate"]]

# ---- Group adapters by class ----
SHM_ADAPTERS = ["disruptor-shm", "myelon-raw-shm", "iceoryx2-shm"]
MMAP_ADAPTERS = ["disruptor-mmap", "myelon-raw-mmap", "crossbar-channel"]
ALL_LOCAL = SHM_ADAPTERS + MMAP_ADAPTERS + ["boost-message-queue", "ompi-vader-self"]
IPC_ADAPTERS = ["zeromq-ipc", "zeromq-ipc-abs", "zeromq-tcp"]
BROKER_ADAPTERS = ["iggy-tcp", "redpanda-kafka"]
ALL_ADAPTERS = ALL_LOCAL + IPC_ADAPTERS + BROKER_ADAPTERS

print(f"Loaded {len(results)} results, {len(throughput_data)} throughput, {len(fixed_rate_data)} fixed-rate")

# ====================================================================
# Figure 1: Throughput overview (all adapters, log scale)
# ====================================================================
fig, ax = plt.subplots(figsize=(14, 8))
plot_line(ax, throughput_data, ALL_ADAPTERS, SIZES, "throughput",
          "Throughput (ops/s)", "Ping-Pong Throughput: All Adapters", log_y=True)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(fmt_throughput))
fig.tight_layout()
fig.savefig(os.path.join(GRAPH_DIR, "01_throughput_all.png"), dpi=150)
plt.close()
print("  -> 01_throughput_all.png")

# ====================================================================
# Figure 2: SHM adapters throughput (linear)
# ====================================================================
fig, ax = plt.subplots(figsize=(12, 7))
plot_line(ax, throughput_data, SHM_ADAPTERS, SIZES, "throughput",
          "Throughput (ops/s)", "SHM Ping-Pong Throughput")
ax.yaxis.set_major_formatter(ticker.FuncFormatter(fmt_throughput))
fig.tight_layout()
fig.savefig(os.path.join(GRAPH_DIR, "02_throughput_shm.png"), dpi=150)
plt.close()
print("  -> 02_throughput_shm.png")

# ====================================================================
# Figure 3: MMAP adapters throughput (linear)
# ====================================================================
fig, ax = plt.subplots(figsize=(12, 7))
plot_line(ax, throughput_data, MMAP_ADAPTERS, SIZES, "throughput",
          "Throughput (ops/s)", "MMAP Ping-Pong Throughput")
ax.yaxis.set_major_formatter(ticker.FuncFormatter(fmt_throughput))
fig.tight_layout()
fig.savefig(os.path.join(GRAPH_DIR, "03_throughput_mmap.png"), dpi=150)
plt.close()
print("  -> 03_throughput_mmap.png")

# ====================================================================
# Figure 4: P50 Latency all adapters (log scale)
# ====================================================================
fig, ax = plt.subplots(figsize=(14, 8))
plot_line(ax, throughput_data, ALL_ADAPTERS, SIZES, "p50",
          "P50 Latency (ns)", "Ping-Pong P50 Latency: All Adapters", log_y=True)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(fmt_latency_ns))
fig.tight_layout()
fig.savefig(os.path.join(GRAPH_DIR, "04_p50_latency_all.png"), dpi=150)
plt.close()
print("  -> 04_p50_latency_all.png")

# ====================================================================
# Figure 5: P50 Latency local IPC only
# ====================================================================
fig, ax = plt.subplots(figsize=(12, 7))
plot_line(ax, throughput_data, ALL_LOCAL, SIZES, "p50",
          "P50 Latency (ns)", "P50 Latency: Local IPC Adapters", log_y=True)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(fmt_latency_ns))
fig.tight_layout()
fig.savefig(os.path.join(GRAPH_DIR, "05_p50_latency_local.png"), dpi=150)
plt.close()
print("  -> 05_p50_latency_local.png")

# ====================================================================
# Figure 6: P99 Latency all adapters (log scale)
# ====================================================================
fig, ax = plt.subplots(figsize=(14, 8))
plot_line(ax, throughput_data, ALL_ADAPTERS, SIZES, "p99",
          "P99 Latency (ns)", "Ping-Pong P99 Latency: All Adapters", log_y=True)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(fmt_latency_ns))
fig.tight_layout()
fig.savefig(os.path.join(GRAPH_DIR, "06_p99_latency_all.png"), dpi=150)
plt.close()
print("  -> 06_p99_latency_all.png")

# ====================================================================
# Figure 7: P99 Latency local IPC only
# ====================================================================
fig, ax = plt.subplots(figsize=(12, 7))
plot_line(ax, throughput_data, ALL_LOCAL, SIZES, "p99",
          "P99 Latency (ns)", "P99 Latency: Local IPC Adapters", log_y=True)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(fmt_latency_ns))
fig.tight_layout()
fig.savefig(os.path.join(GRAPH_DIR, "07_p99_latency_local.png"), dpi=150)
plt.close()
print("  -> 07_p99_latency_local.png")

# ====================================================================
# Figure 8: Fixed-rate CO-aware P50 latency (all)
# ====================================================================
fig, ax = plt.subplots(figsize=(14, 8))
plot_line(ax, fixed_rate_data, ALL_ADAPTERS, SIZES, "p50",
          "CO-aware P50 Latency (ns)", "Fixed-Rate CO-aware P50 Latency: All Adapters", log_y=True)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(fmt_latency_ns))
fig.tight_layout()
fig.savefig(os.path.join(GRAPH_DIR, "08_co_p50_all.png"), dpi=150)
plt.close()
print("  -> 08_co_p50_all.png")

# ====================================================================
# Figure 9: Fixed-rate CO-aware P99 latency (all)
# ====================================================================
fig, ax = plt.subplots(figsize=(14, 8))
plot_line(ax, fixed_rate_data, ALL_ADAPTERS, SIZES, "p99",
          "CO-aware P99 Latency (ns)", "Fixed-Rate CO-aware P99 Latency: All Adapters", log_y=True)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(fmt_latency_ns))
fig.tight_layout()
fig.savefig(os.path.join(GRAPH_DIR, "09_co_p99_all.png"), dpi=150)
plt.close()
print("  -> 09_co_p99_all.png")

# ====================================================================
# Figure 10: Fixed-rate CO P50 local IPC only
# ====================================================================
fig, ax = plt.subplots(figsize=(12, 7))
plot_line(ax, fixed_rate_data, ALL_LOCAL, SIZES, "p50",
          "CO-aware P50 Latency (ns)", "Fixed-Rate CO-aware P50: Local IPC", log_y=True)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(fmt_latency_ns))
fig.tight_layout()
fig.savefig(os.path.join(GRAPH_DIR, "10_co_p50_local.png"), dpi=150)
plt.close()
print("  -> 10_co_p50_local.png")

# ====================================================================
# Figure 11: Throughput bar chart at 64B (grouped)
# ====================================================================
fig, ax = plt.subplots(figsize=(14, 6))
adapters_64 = []
vals_64 = []
colors_64 = []
for adapter in ALL_ADAPTERS:
    pts = [r["throughput"] for r in throughput_data if r["adapter"] == adapter and r["size"] == 64]
    if pts:
        adapters_64.append(adapter)
        vals_64.append(pts[0])
        colors_64.append(ADAPTER_COLORS.get(adapter, "#333"))

bars = ax.barh(range(len(adapters_64)), vals_64, color=colors_64)
ax.set_yticks(range(len(adapters_64)))
ax.set_yticklabels(adapters_64, fontsize=9)
ax.set_xlabel("Throughput (ops/s)")
ax.set_title("Ping-Pong Throughput at 64B", fontsize=13, fontweight="bold")
ax.xaxis.set_major_formatter(ticker.FuncFormatter(fmt_throughput))
ax.grid(True, axis="x", alpha=0.3)
for bar, val in zip(bars, vals_64):
    ax.text(bar.get_width() + max(vals_64) * 0.01, bar.get_y() + bar.get_height()/2,
            fmt_throughput(val, None), va="center", fontsize=8)
fig.tight_layout()
fig.savefig(os.path.join(GRAPH_DIR, "11_throughput_bar_64B.png"), dpi=150)
plt.close()
print("  -> 11_throughput_bar_64B.png")

# ====================================================================
# Figure 12: P50 Latency bar chart at 64B
# ====================================================================
fig, ax = plt.subplots(figsize=(14, 6))
adapters_64_lat = []
vals_64_lat = []
colors_64_lat = []
for adapter in ALL_ADAPTERS:
    pts = [r["p50"] for r in throughput_data if r["adapter"] == adapter and r["size"] == 64 and r["p50"]]
    if pts:
        adapters_64_lat.append(adapter)
        vals_64_lat.append(pts[0])
        colors_64_lat.append(ADAPTER_COLORS.get(adapter, "#333"))

bars = ax.barh(range(len(adapters_64_lat)), vals_64_lat, color=colors_64_lat)
ax.set_yticks(range(len(adapters_64_lat)))
ax.set_yticklabels(adapters_64_lat, fontsize=9)
ax.set_xlabel("P50 Latency (ns)")
ax.set_xscale("log")
ax.set_title("Ping-Pong P50 Latency at 64B", fontsize=13, fontweight="bold")
ax.xaxis.set_major_formatter(ticker.FuncFormatter(fmt_latency_ns))
ax.grid(True, axis="x", alpha=0.3)
for bar, val in zip(bars, vals_64_lat):
    ax.text(bar.get_width() * 1.1, bar.get_y() + bar.get_height()/2,
            fmt_latency_ns(val, None), va="center", fontsize=8)
fig.tight_layout()
fig.savefig(os.path.join(GRAPH_DIR, "12_p50_bar_64B.png"), dpi=150)
plt.close()
print("  -> 12_p50_bar_64B.png")

# ====================================================================
# Figure 13: Broadcast throughput (4c vs 8c)
# ====================================================================
broadcast_data = []
for f in glob.glob(os.path.join(OUTDIR, "broadcast_*.json")):
    fname = os.path.basename(f)
    # broadcast_disruptor_64_4c.json or broadcast_disruptor_64_4c_400000.json
    m = re.match(r"^broadcast_(\w+?)_(\d+)_(\d+)c\.json$", fname)
    if not m:
        continue
    adapter_raw, size_str, consumers = m.groups()
    adapter_map = {
        "disruptor": "disruptor-shm", "disruptor_mmap": "disruptor-mmap",
        "myelon_raw": "myelon-raw-shm", "myelon_raw_mmap": "myelon-raw-mmap",
        "crossbar": "crossbar-pubsub"
    }
    adapter = adapter_map.get(adapter_raw, adapter_raw)
    try:
        with open(f) as fh:
            text = fh.read().strip()
            idx = text.find("{")
            d = json.loads(text[idx:])
        if "scenarios" in d:
            for sc in d["scenarios"]:
                tp = sc.get("outcome", {}).get("Throughput", {}).get("consumers", {}).get("average_throughput_ops_sec")
                if tp:
                    broadcast_data.append({"adapter": adapter, "size": int(size_str), "consumers": int(consumers), "throughput": tp})
                break
        else:
            tp = d.get("throughput")
            if tp:
                broadcast_data.append({"adapter": adapter, "size": int(size_str), "consumers": int(consumers), "throughput": tp})
    except Exception:
        continue

BROADCAST_COLORS = {
    "disruptor-shm": "#1f77b4", "disruptor-mmap": "#aec7e8",
    "myelon-raw-shm": "#ff7f0e", "myelon-raw-mmap": "#ffbb78",
    "crossbar-pubsub": "#d62728",
}

for nc in [4, 8]:
    fig, ax = plt.subplots(figsize=(12, 7))
    subset = [r for r in broadcast_data if r["consumers"] == nc]
    adapters_bc = sorted(set(r["adapter"] for r in subset))
    for adapter in adapters_bc:
        pts = [(r["size"], r["throughput"]) for r in subset if r["adapter"] == adapter]
        pts.sort()
        if not pts:
            continue
        xs, ys = zip(*pts)
        color = BROADCAST_COLORS.get(adapter, "#333")
        ax.plot(xs, ys, marker="o", label=adapter, color=color, linewidth=2, markersize=7)
    ax.set_xscale("log", base=2)
    ax.set_xticks(SIZES)
    ax.set_xticklabels([size_label(s) for s in SIZES])
    ax.set_xlabel("Message Size")
    ax.set_ylabel("Throughput (ops/s)")
    ax.set_title(f"Broadcast Throughput ({nc} consumers)", fontsize=13, fontweight="bold")
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(fmt_throughput))
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=9)
    fig.tight_layout()
    fig.savefig(os.path.join(GRAPH_DIR, f"13_broadcast_{nc}c.png"), dpi=150)
    plt.close()
    print(f"  -> 13_broadcast_{nc}c.png")

print(f"\nAll graphs saved to {GRAPH_DIR}/")
