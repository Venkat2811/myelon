#!/usr/bin/env python3
import math
import os
import sys
from collections import defaultdict
from typing import Dict, Iterable, List, Tuple

import aggregate_results as agg

GRAPH_PROTOCOLS = ["shm", "mmap", "tcp", "tcp_broker"]
GRAPH_FAMILIES = ["signal", "pingpong", "broadcast"]
LATENCY_KEY = "p99"
WIDTH = 1280
HEIGHT = 820
MARGIN_LEFT = 110
MARGIN_RIGHT = 260
MARGIN_TOP = 90
MARGIN_BOTTOM = 90
PLOT_WIDTH = WIDTH - MARGIN_LEFT - MARGIN_RIGHT
PLOT_HEIGHT = HEIGHT - MARGIN_TOP - MARGIN_BOTTOM

ADAPTER_COLORS = {
    "disruptor-shm": "#1f77b4",
    "myelon-raw-shm": "#2ca02c",
    "shmipc-rs": "#9467bd",
    "iceoryx2-shm": "#14b8a6",
    "disruptor-mmap": "#1f77b4",
    "myelon-raw-mmap": "#2ca02c",
    "crossbar-channel": "#ff7f0e",
    "rusteron-aeron-ipc": "#d62728",
    "zeromq-ipc": "#8c564b",
    "zeromq-ipc-abs": "#bcbd22",
    "zeromq-tcp": "#8c564b",
    "iggy-tcp": "#06b6d4",
    "redpanda-kafka": "#ef4444",
    "ompi-vader-self": "#17becf",
    "boost-message-queue": "#7f7f7f",
}

MODES = [
    ("max_throughput", "Max Throughput", lambda row: row["rate"] is None),
    ("fixed_rate", "Fixed Rate (CO-aware)", lambda row: row["rate"] is not None),
]


def human_size(size: int) -> str:
    if size >= 1024 * 1024:
        return f"{size // (1024 * 1024)}MB"
    if size >= 1024:
        return f"{size // 1024}KB"
    return f"{size}B"


def log_ticks(min_value: float, max_value: float) -> List[float]:
    if min_value <= 0 or max_value <= 0:
        return []
    start = math.floor(math.log10(min_value))
    end = math.ceil(math.log10(max_value))
    ticks: List[float] = []
    for exp in range(start, end + 1):
        base = 10 ** exp
        for mult in (1, 2, 5):
            value = mult * base
            if min_value <= value <= max_value:
                ticks.append(float(value))
    if min_value not in ticks:
        ticks.append(min_value)
    if max_value not in ticks:
        ticks.append(max_value)
    return sorted(set(ticks))


def scale_log(value: float, min_value: float, max_value: float, span: float) -> float:
    lo = math.log10(min_value)
    hi = math.log10(max_value)
    cur = math.log10(value)
    if hi == lo:
        return 0.5 * span
    return (cur - lo) / (hi - lo) * span


def throughput_value(point: dict) -> float:
    if point.get("family") == "broadcast" and point.get("fanout_ops"):
        return float(point["fanout_ops"])
    return float(point["ops"])


def dominates(left: dict, right: dict) -> bool:
    return (
        left["metric_ops"] >= right["metric_ops"]
        and left["latency_ns"] <= right["latency_ns"]
        and (left["metric_ops"] > right["metric_ops"] or left["latency_ns"] < right["latency_ns"])
    )


def pareto_frontier(points: List[dict]) -> List[dict]:
    frontier = []
    for point in points:
        if any(dominates(other, point) for other in points if other is not point):
            continue
        frontier.append(point)
    return sorted(frontier, key=lambda item: item["ops"])


def normalize_points(rows: Iterable[dict]) -> List[dict]:
    points: List[dict] = []
    for row in rows:
        ops = row.get("ops")
        latency_ns = row.get("latency", {}).get(LATENCY_KEY)
        if not ops or not latency_ns:
            continue
        point = dict(row)
        point["ops"] = float(ops)
        point["metric_ops"] = throughput_value(point)
        point["latency_ns"] = float(latency_ns)
        point["label"] = point_label(point)
        points.append(point)
    return points


def point_label(point: dict) -> str:
    size = human_size(int(point["size"]))
    consumers = point.get("consumer_count")
    fanout = point.get("fanout_ops")
    suffix = ""
    if consumers:
        suffix += f" {consumers}c"
    if fanout:
        suffix += f" fanout={agg.fmt_rate(fanout)}"
    if point.get("rate") is None:
        return f"{point['adapter']} {size}{suffix}"
    return f"{point['adapter']} {size}{suffix} @ {agg.fmt_rate(point['rate'])}/s"


def protocol_mode_filename(family: str, protocol: str, mode_key: str) -> str:
    return f"pareto_{family}_{protocol}_{mode_key}_{LATENCY_KEY}.svg"


def draw_svg(family: str, protocol: str, mode_key: str, mode_title: str, points: List[dict], frontier: List[dict], out_path: str) -> None:
    x_min = min(point["metric_ops"] for point in points)
    x_max = max(point["metric_ops"] for point in points)
    y_min = min(point["latency_ns"] for point in points)
    y_max = max(point["latency_ns"] for point in points)

    x_ticks = log_ticks(x_min, x_max)
    y_ticks = log_ticks(y_min, y_max)

    def px(point: dict) -> float:
        return MARGIN_LEFT + scale_log(point["metric_ops"], x_min, x_max, PLOT_WIDTH)

    def py(point: dict) -> float:
        return MARGIN_TOP + (PLOT_HEIGHT - scale_log(point["latency_ns"], y_min, y_max, PLOT_HEIGHT))

    legend_adapters = sorted({point["adapter"] for point in points}, key=agg.adapter_sort_key)
    family_title = agg.FAMILY_DISPLAY.get(family, family)
    x_axis_label = "Fanout throughput (delivered msgs/s, log scale)" if family == "broadcast" else "Throughput (ops/s, log scale)"
    lines: List[str] = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}">')
    lines.append('<rect width="100%" height="100%" fill="#0f172a"/>')
    lines.append(f'<text x="{MARGIN_LEFT}" y="42" fill="#e2e8f0" font-size="28" font-family="monospace">{family_title} {agg.PROTOCOL_DISPLAY.get(protocol, protocol)} Pareto Frontier</text>')
    lines.append(f'<text x="{MARGIN_LEFT}" y="68" fill="#94a3b8" font-size="15" font-family="monospace">{mode_title} · X = {"fanout throughput" if family == "broadcast" else "throughput"} · Y = P99 latency (lower is better) · non-dominated frontier highlighted</text>')
    lines.append(f'<rect x="{MARGIN_LEFT}" y="{MARGIN_TOP}" width="{PLOT_WIDTH}" height="{PLOT_HEIGHT}" fill="#111827" stroke="#334155" stroke-width="1"/>')

    for tick in x_ticks:
        xpos = MARGIN_LEFT + scale_log(tick, x_min, x_max, PLOT_WIDTH)
        lines.append(f'<line x1="{xpos:.2f}" y1="{MARGIN_TOP}" x2="{xpos:.2f}" y2="{MARGIN_TOP + PLOT_HEIGHT}" stroke="#1e293b" stroke-width="1"/>')
        lines.append(f'<text x="{xpos:.2f}" y="{MARGIN_TOP + PLOT_HEIGHT + 24}" fill="#cbd5e1" font-size="12" text-anchor="middle" font-family="monospace">{agg.fmt_rate(tick)}</text>')

    for tick in y_ticks:
        ypos = MARGIN_TOP + (PLOT_HEIGHT - scale_log(tick, y_min, y_max, PLOT_HEIGHT))
        lines.append(f'<line x1="{MARGIN_LEFT}" y1="{ypos:.2f}" x2="{MARGIN_LEFT + PLOT_WIDTH}" y2="{ypos:.2f}" stroke="#1e293b" stroke-width="1"/>')
        lines.append(f'<text x="{MARGIN_LEFT - 14}" y="{ypos + 4:.2f}" fill="#cbd5e1" font-size="12" text-anchor="end" font-family="monospace">{agg.fmt_ns(tick)}</text>')

    lines.append(f'<text x="{MARGIN_LEFT + PLOT_WIDTH / 2:.2f}" y="{HEIGHT - 26}" fill="#e2e8f0" font-size="14" text-anchor="middle" font-family="monospace">{x_axis_label}</text>')
    lines.append(f'<text transform="translate(24 {MARGIN_TOP + PLOT_HEIGHT / 2:.2f}) rotate(-90)" fill="#e2e8f0" font-size="14" text-anchor="middle" font-family="monospace">P99 latency (log scale)</text>')

    for point in points:
        color = ADAPTER_COLORS.get(point["adapter"], "#f8fafc")
        lines.append(
            f'<circle cx="{px(point):.2f}" cy="{py(point):.2f}" r="5" fill="{color}" fill-opacity="0.72" stroke="#020617" stroke-width="1.2">'
            f'<title>{point["label"]} · ops {agg.fmt_rate(point["ops"])} · x-metric {agg.fmt_rate(point["metric_ops"])} · p99 {agg.fmt_ns(point["latency_ns"])} · p50 {agg.fmt_ns(point["latency"].get("p50"))}</title>'
            '</circle>'
        )

    if frontier:
        path = " ".join(
            ("M" if index == 0 else "L") + f" {px(point):.2f} {py(point):.2f}"
            for index, point in enumerate(frontier)
        )
        lines.append(f'<path d="{path}" fill="none" stroke="#f8fafc" stroke-width="2.4" stroke-dasharray="8 6"/>')
        for point in frontier:
            x = px(point)
            y = py(point)
            lines.append(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="7" fill="none" stroke="#f8fafc" stroke-width="2"/>')
            lines.append(
                f'<text x="{x + 10:.2f}" y="{y - 10:.2f}" fill="#f8fafc" font-size="12" font-family="monospace">'
                f'{human_size(int(point["size"]))}'
                + (f' @{agg.fmt_rate(point["rate"])}' if point.get("rate") is not None else '')
                + f' {point["adapter"]}</text>'
            )

    legend_x = MARGIN_LEFT + PLOT_WIDTH + 28
    legend_y = MARGIN_TOP + 8
    lines.append(f'<text x="{legend_x}" y="{legend_y}" fill="#e2e8f0" font-size="16" font-family="monospace">Adapters</text>')
    for index, adapter in enumerate(legend_adapters):
        y = legend_y + 28 + index * 24
        color = ADAPTER_COLORS.get(adapter, "#f8fafc")
        lines.append(f'<rect x="{legend_x}" y="{y - 10}" width="14" height="14" fill="{color}" rx="3" ry="3"/>')
        lines.append(f'<text x="{legend_x + 22}" y="{y + 1}" fill="#cbd5e1" font-size="13" font-family="monospace">{adapter}</text>')

    lines.append('</svg>')
    with open(out_path, 'w', encoding='utf-8') as handle:
        handle.write("\n".join(lines) + "\n")


def write_markdown(outdir: str, generated: List[Tuple[str, str, str]]) -> None:
    md_path = os.path.join(outdir, "pareto.md")
    with open(md_path, "w", encoding="utf-8") as handle:
        handle.write("# Pareto Frontier Graphs\n\n")
        handle.write("These graphs plot throughput against P99 latency. Higher throughput and lower latency are better. The dashed white line marks the non-dominated frontier.\n\n")
        current_section = None
        for family, protocol, mode_title, relative_path in generated:
            section = f"{agg.FAMILY_DISPLAY.get(family, family)} / {agg.PROTOCOL_DISPLAY.get(protocol, protocol)}"
            if section != current_section:
                if current_section is not None:
                    handle.write("\n")
                handle.write(f"## {section}\n\n")
                current_section = section
            handle.write(f"### {mode_title}\n\n")
            handle.write(f"![{section} {mode_title}]({relative_path})\n\n")


def main(argv: List[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    outdir = argv[0] if argv else agg.default_outdir()
    graph_dir = os.path.join(outdir, "graphs")
    os.makedirs(graph_dir, exist_ok=True)

    rows = agg.load_rows(outdir)
    generated: List[Tuple[str, str, str, str]] = []

    for family in GRAPH_FAMILIES:
        family_rows = [row for row in rows if row.get("family", "pingpong") == family]
        if not family_rows:
            continue
        for protocol in GRAPH_PROTOCOLS:
            protocol_rows = [row for row in family_rows if row.get("protocol") == protocol]
            for mode_key, mode_title, predicate in MODES:
                points = normalize_points(row for row in protocol_rows if predicate(row))
                if not points:
                    continue
                frontier = pareto_frontier(points)
                filename = protocol_mode_filename(family, protocol, mode_key)
                out_path = os.path.join(graph_dir, filename)
                draw_svg(family, protocol, mode_key, mode_title, points, frontier, out_path)
                generated.append((family, protocol, mode_title, os.path.join("graphs", filename)))
                print(f"wrote {out_path} ({len(points)} points, {len(frontier)} frontier)")

    if generated:
        write_markdown(outdir, generated)
        print(f"wrote {os.path.join(outdir, 'pareto.md')}")
    else:
        print(f"no pareto graphs generated from {outdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
