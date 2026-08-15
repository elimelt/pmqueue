# Benchmarks

Before/after benchmarks for the queue library: `main` (before) vs this
branch (after). No Maven, no JMH, no new dependencies.

Run everything with one command:

```bash
benchmarks/run_benchmarks.sh
```

The driver:

1. Reuses the Temurin JDK 21 that `run_tests.sh` downloads into `target/jdk/`
   (downloads it if missing).
2. Exports library sources for both sides with `git archive` into a scratch
   dir (`/tmp/pmqueue-bench`), so the working tree is never modified.
3. Compiles the same harness (`src/QueueBench.java`) once against each
   side's classes. The harness only uses APIs that exist on both sides.
4. Runs each scenario in a fresh JVM, one JSON line per run. Trials are
   interleaved: each trial runs both sides back to back, and the side that
   goes first alternates per trial to reduce machine-noise bias.
5. Aggregates medians (`aggregate.py`, stdlib only) and renders plots
   (`make_plots.py`, matplotlib from a scratch venv at `/tmp/benchvenv`).

Scenarios: `offer()` and `poll()` throughput across 64 B / 1 KiB / 8 KiB /
64 KiB, checksums on vs off at 1 KiB, single-message round-trip latency
percentiles, and open+close cost on a populated file. Memory is captured per
run: bytes allocated per operation (per-thread allocation counter), GC
count/time deltas, settled heap after a full produce/consume cycle, and peak
RSS (`/usr/bin/time -l` on macOS).

Knobs (environment variables): `TRIALS` (default 5), `BEFORE_REF` (default
`main`), `AFTER_REF` (default `HEAD`).

Outputs in `results/`: `raw.jsonl` and `raw.csv` (per-trial data),
`medians.json`, `summary.md`, `env.json`, and the `*.png` plots.

Note: `poll()` persists its read position with an fsync per message on both
sides, so poll throughput and round-trip latency are fsync-bound; op counts
are sized accordingly.
