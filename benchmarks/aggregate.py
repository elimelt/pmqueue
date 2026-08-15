#!/usr/bin/env python3
"""Aggregate raw benchmark output into CSV, medians JSON, and summary.md.

Reads results/raw.jsonl (one JSON object per trial run) and writes:
  results/raw.csv       flat per-trial table
  results/medians.json  per-group medians/min/max for plotting
  results/summary.md    median comparison table, main vs this branch

Uses only the Python standard library.
"""
import csv
import json
import statistics
import sys
from pathlib import Path

SIDES = ("main", "branch")

# metric -> (label, unit, higher_is_better)
METRICS = {
    "msgs_per_sec": ("throughput", "msgs/s", True),
    "mib_per_sec": ("throughput", "MiB/s", True),
    "alloc_bytes_per_op": ("allocation", "B/op", False),
    "gc_count": ("gc collections", "count", False),
    "gc_time_ms": ("gc time", "ms", False),
    "heap_after_cycle_bytes": ("settled heap", "bytes", False),
    "peak_rss_bytes": ("peak RSS", "bytes", False),
    "p50_us": ("latency p50", "us", False),
    "p95_us": ("latency p95", "us", False),
    "p99_us": ("latency p99", "us", False),
    "mean_us": ("latency mean", "us", False),
}


def fmt_size(n):
    if n >= 1024 * 1024:
        return f"{n // (1024 * 1024)} MiB"
    if n >= 1024:
        return f"{n // 1024} KiB"
    return f"{n} B"


def fmt_val(metric, v):
    if v is None:
        return "-"
    if metric in ("msgs_per_sec",):
        return f"{v:,.0f}"
    if metric in ("mib_per_sec",):
        return f"{v:,.1f}"
    if metric in ("alloc_bytes_per_op",):
        return f"{v:,.0f}"
    if metric.endswith("_us"):
        return f"{v:,.1f}"
    if metric.endswith("_bytes"):
        return f"{v / (1024 * 1024):,.1f} MiB"
    return f"{v:,.0f}"


def main():
    results = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "results"
    rows = [json.loads(line) for line in (results / "raw.jsonl").read_text().splitlines() if line.strip()]
    env = json.loads((results / "env.json").read_text()) if (results / "env.json").exists() else {}

    # raw.csv
    fieldnames = []
    for r in rows:
        for k in r:
            if k not in fieldnames:
                fieldnames.append(k)
    with open(results / "raw.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    # group by (scenario, size, checksum)
    groups = {}
    for r in rows:
        key = (r["scenario"], r["size"], r["checksum"])
        groups.setdefault(key, {s: [] for s in SIDES})[r["side"]].append(r)

    out_groups = []
    for key in sorted(groups, key=lambda k: (k[0], k[1], not k[2])):
        scenario, size, checksum = key
        metrics = {}
        for metric in METRICS:
            stats = {}
            for side in SIDES:
                vals = [r[metric] for r in groups[key][side] if r.get(metric) is not None]
                if not vals:
                    continue
                stats[side] = {
                    "median": statistics.median(vals),
                    "min": min(vals),
                    "max": max(vals),
                    "n": len(vals),
                }
            if len(stats) == len(SIDES):
                metrics[metric] = stats
        out_groups.append({
            "scenario": scenario,
            "size": size,
            "checksum": checksum,
            "metrics": metrics,
        })

    (results / "medians.json").write_text(json.dumps({"env": env, "groups": out_groups}, indent=2))

    # summary.md
    trials = env.get("trials", "?")
    lines = []
    lines.append("# Benchmark summary: main vs this branch")
    lines.append("")
    lines.append(f"- JVM: {env.get('jvm', 'unknown')}, flags `{env.get('jvm_flags', '')}`")
    lines.append(f"- OS: {env.get('os', 'unknown')}, date {env.get('date', '')}")
    lines.append(f"- Trials: {trials} per side, interleaved (fresh JVM per trial). Values are medians.")
    lines.append(f"- Before = `main` ({env.get('before_ref', '')}), after = this branch ({env.get('after_ref', '')}).")
    lines.append("- Delta = (after - before) / before. Positive throughput delta is better;")
    lines.append("  positive latency/memory delta is worse. Deltas within the run-to-run noise")
    lines.append("  band (max spread across trials of either side) are marked `~` (equivalent).")
    lines.append("")

    headline = {
        "offer": ["msgs_per_sec", "mib_per_sec", "alloc_bytes_per_op", "gc_time_ms", "peak_rss_bytes"],
        "poll": ["msgs_per_sec", "mib_per_sec", "alloc_bytes_per_op", "gc_time_ms",
                 "heap_after_cycle_bytes", "peak_rss_bytes"],
        "latency": ["p50_us", "p95_us", "p99_us", "alloc_bytes_per_op"],
        "openclose": ["p50_us", "p95_us", "mean_us"],
    }
    scenario_title = {
        "offer": "offer() throughput",
        "poll": "poll() throughput",
        "latency": "Round-trip latency (offer+poll)",
        "openclose": "Open+close on populated file (5,000 x 1 KiB messages)",
    }

    for scenario in ("offer", "poll", "latency", "openclose"):
        sgroups = [g for g in out_groups if g["scenario"] == scenario]
        if not sgroups:
            continue
        lines.append(f"## {scenario_title[scenario]}")
        lines.append("")
        lines.append("| Case | Metric | main | this branch | delta |")
        lines.append("|---|---|---:|---:|---:|")
        for g in sgroups:
            case = f"{fmt_size(g['size'])}, checksum {'on' if g['checksum'] else 'off'}"
            for metric in headline[scenario]:
                if metric not in g["metrics"]:
                    continue
                st = g["metrics"][metric]
                m, b = st["main"]["median"], st["branch"]["median"]
                if metric in ("gc_count", "gc_time_ms") and max(m, b) < 5:
                    # both sides negligible; percentage deltas would mislead
                    continue
                unit = METRICS[metric][1]
                if m == 0:
                    delta = "-"
                else:
                    pct = (b - m) / m * 100
                    noise = max(
                        (st[s]["max"] - st[s]["min"]) / st[s]["median"] * 100
                        for s in SIDES if st[s]["median"]
                    )
                    mark = " ~" if abs(pct) <= noise else ""
                    delta = f"{pct:+.1f}%{mark}"
                lines.append(
                    f"| {case} | {metric} ({unit}) | {fmt_val(metric, m)} | {fmt_val(metric, b)} | {delta} |")
        lines.append("")

    lines.append("`~` = within run-to-run noise; treat as equivalent.")
    lines.append("")
    (results / "summary.md").write_text("\n".join(lines))
    print(f"wrote {results / 'raw.csv'}, {results / 'medians.json'}, {results / 'summary.md'}")


if __name__ == "__main__":
    main()
