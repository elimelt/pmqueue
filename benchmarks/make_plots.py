#!/usr/bin/env python3
"""Render comparison plots (PNG) from results/medians.json.

Requires matplotlib (run_benchmarks.sh installs it into a scratch venv).
Two series only: main (before) and this branch (after). Bars show medians
across trials; whiskers show the min-max spread across trials.
"""
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
SECONDARY = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
BASELINE = "#c3c2b7"
C_MAIN = "#2a78d6"    # series 1: main (before)
C_BRANCH = "#eb6834"  # series 2: this branch (after)

SIDES = ["main", "branch"]
SIDE_LABEL = {"main": "main", "branch": "this branch"}
SIDE_COLOR = {"main": C_MAIN, "branch": C_BRANCH}


def fmt_size(n):
    if n >= 1024 * 1024:
        return f"{n // (1024 * 1024)} MiB"
    if n >= 1024:
        return f"{n // 1024} KiB"
    return f"{n} B"


def fmt_val(v):
    if v >= 10000:
        return f"{v / 1000:,.1f}k"
    if v >= 100:
        return f"{v:,.0f}"
    if v >= 10:
        return f"{v:,.1f}"
    if v >= 0.1:
        return f"{v:,.2f}"
    return f"{v:,.3f}"


def style_axis(ax):
    ax.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(BASELINE)
    ax.spines["bottom"].set_linewidth(1)
    ax.tick_params(colors=MUTED, labelsize=8, length=0)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.10g}"))
    ax.yaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def paired_bars(ax, stats_by_side, label_values=True):
    """Two thin bars (main, branch) with min-max whiskers and tip labels."""
    xs = [0, 1]
    width = 0.55
    tops = []
    for x, side in zip(xs, SIDES):
        st = stats_by_side[side]
        ax.bar(x, st["median"], width=width, color=SIDE_COLOR[side], zorder=3)
        ax.vlines(x, st["min"], st["max"], color=SECONDARY, linewidth=1, zorder=4)
        tops.append(max(st["max"], st["median"]))
    top = max(tops) if tops else 1
    ax.set_ylim(0, top * 1.28)
    if label_values:
        for x, side in zip(xs, SIDES):
            st = stats_by_side[side]
            ax.annotate(fmt_val(st["median"]), (x, max(st["max"], st["median"])),
                        xytext=(0, 3), textcoords="offset points",
                        ha="center", va="bottom", fontsize=8, color=INK)
    ax.set_xticks(xs)
    ax.set_xticklabels([], fontsize=8)
    ax.set_xlim(-0.75, 1.75)
    style_axis(ax)


def grouped_bars(ax, group_labels, stats_list, unit):
    """Grouped bars: one group per label, two bars (main, branch) per group."""
    n = len(group_labels)
    width = 0.32
    gap = 0.04
    for gi, st in enumerate(stats_list):
        for si, side in enumerate(SIDES):
            s = st[side]
            x = gi + (si - 0.5) * (width + gap)
            ax.bar(x, s["median"], width=width, color=SIDE_COLOR[side], zorder=3)
            ax.vlines(x, s["min"], s["max"], color=SECONDARY, linewidth=1, zorder=4)
            ax.annotate(fmt_val(s["median"]), (x, max(s["max"], s["median"])),
                        xytext=(0, 3), textcoords="offset points",
                        ha="center", va="bottom", fontsize=8, color=INK)
    tops = [max(st[s]["max"], st[s]["median"]) for st in stats_list for s in SIDES]
    ax.set_ylim(0, max(tops) * 1.22)
    ax.set_xticks(range(n))
    ax.set_xticklabels(group_labels, fontsize=9, color=SECONDARY)
    ax.set_ylabel(unit, fontsize=9, color=SECONDARY)
    style_axis(ax)


def legend(fig):
    handles = [plt.Rectangle((0, 0), 1, 1, color=SIDE_COLOR[s]) for s in SIDES]
    fig.legend(handles, [SIDE_LABEL[s] for s in SIDES], loc="upper right",
               frameon=False, fontsize=9, labelcolor=SECONDARY,
               bbox_to_anchor=(0.99, 1.0))


def titled(fig, title, subtitle, sub_y=0.885):
    fig.suptitle(title, x=0.02, y=0.975, ha="left", va="top",
                 fontsize=13, color=INK, fontweight="bold")
    fig.text(0.02, sub_y, subtitle, ha="left", va="top",
             fontsize=9, color=SECONDARY)


def new_fig(w, h):
    fig = plt.figure(figsize=(w, h), dpi=160)
    fig.patch.set_facecolor(SURFACE)
    return fig


def group_key(g):
    return (g["scenario"], g["size"], g["checksum"])


def main():
    results = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "results"
    data = json.loads((results / "medians.json").read_text())
    groups = {group_key(g): g["metrics"] for g in data["groups"]}
    trials = data.get("env", {}).get("trials", "?")
    sizes = sorted({g["size"] for g in data["groups"] if g["scenario"] == "offer"})
    note = f"median of {trials} interleaved trials per side, whiskers = min-max across trials"

    # --- throughput: small multiples, offer row + poll row, per-size scale ---
    fig = new_fig(9, 5.2)
    axes = fig.subplots(2, len(sizes))
    for row, scenario in enumerate(("offer", "poll")):
        for col, size in enumerate(sizes):
            ax = axes[row][col]
            key = (scenario, size, True)
            if key not in groups:
                ax.axis("off")
                continue
            paired_bars(ax, groups[key]["mib_per_sec"])
            if row == 0:
                ax.set_title(fmt_size(size), fontsize=9, color=SECONDARY, pad=8)
            if col == 0:
                ax.set_ylabel(f"{scenario}()\nMiB/s", fontsize=9, color=SECONDARY)
    titled(fig, "Throughput by message size (payload MiB/s)",
           f"checksums on; {note};\n"
           "each panel has its own scale. poll() fsyncs its read position per message on both sides.")
    legend(fig)
    fig.tight_layout(rect=(0.01, 0.02, 0.99, 0.82))
    fig.savefig(results / "throughput.png", facecolor=SURFACE)
    plt.close(fig)

    # --- latency percentiles -------------------------------------------------
    fig = new_fig(7, 4.2)
    ax = fig.subplots(1, 1)
    lat = groups[("latency", 1024, True)]
    grouped_bars(ax, ["p50", "p95", "p99"],
                 [lat["p50_us"], lat["p95_us"], lat["p99_us"]],
                 "microseconds")
    titled(fig, "Round-trip latency, 1 KiB message",
           f"one offer() + one poll() per round trip; checksums on; lower is better;\n{note}",
           sub_y=0.87)
    legend(fig)
    fig.tight_layout(rect=(0.01, 0.02, 0.99, 0.78))
    fig.savefig(results / "latency.png", facecolor=SURFACE)
    plt.close(fig)

    # --- checksums on vs off --------------------------------------------------
    fig = new_fig(8, 4.2)
    ax_offer, ax_poll = fig.subplots(1, 2)
    for ax, scenario in ((ax_offer, "offer"), (ax_poll, "poll")):
        grouped_bars(ax, ["checksums on", "checksums off"],
                     [groups[(scenario, 1024, True)]["msgs_per_sec"],
                      groups[(scenario, 1024, False)]["msgs_per_sec"]],
                     "messages/s")
        ax.set_title(f"{scenario}()", fontsize=10, color=SECONDARY, pad=8)
    titled(fig, "Checksum cost at 1 KiB (messages/s)",
           f"higher is better;\n{note}", sub_y=0.87)
    legend(fig)
    fig.tight_layout(rect=(0.01, 0.02, 0.99, 0.76))
    fig.savefig(results / "checksum.png", facecolor=SURFACE)
    plt.close(fig)

    # --- open/close on populated file -----------------------------------------
    fig = new_fig(7, 4.2)
    ax = fig.subplots(1, 1)
    oc = groups[("openclose", 1024, True)]
    grouped_bars(ax, ["p50", "p95", "mean"],
                 [oc["p50_us"], oc["p95_us"], oc["mean_us"]],
                 "microseconds")
    titled(fig, "Open + close on a populated queue file",
           f"5,000 x 1 KiB messages on disk; 100 cycles/trial; lower is better;\n{note}", sub_y=0.87)
    legend(fig)
    fig.tight_layout(rect=(0.01, 0.02, 0.99, 0.78))
    fig.savefig(results / "open_close.png", facecolor=SURFACE)
    plt.close(fig)

    # --- memory: allocation per operation --------------------------------------
    fig = new_fig(9, 5.2)
    axes = fig.subplots(2, len(sizes))
    for row, scenario in enumerate(("offer", "poll")):
        for col, size in enumerate(sizes):
            ax = axes[row][col]
            key = (scenario, size, True)
            if key not in groups or "alloc_bytes_per_op" not in groups[key]:
                ax.axis("off")
                continue
            st = groups[key]["alloc_bytes_per_op"]
            kib = {s: {k: v / 1024 if k != "n" else v for k, v in st[s].items()} for s in SIDES}
            paired_bars(ax, kib)
            if row == 0:
                ax.set_title(fmt_size(size), fontsize=9, color=SECONDARY, pad=8)
            if col == 0:
                ax.set_ylabel(f"{scenario}()\nKiB alloc/op", fontsize=9, color=SECONDARY)
    titled(fig, "Heap allocation per operation (KiB, lower is better)",
           f"per-thread allocation counter over the measured window; checksums on;\n"
           f"{note}; each panel has its own scale")
    legend(fig)
    fig.tight_layout(rect=(0.01, 0.02, 0.99, 0.82))
    fig.savefig(results / "memory.png", facecolor=SURFACE)
    plt.close(fig)

    print(f"wrote plots to {results}")


if __name__ == "__main__":
    main()
