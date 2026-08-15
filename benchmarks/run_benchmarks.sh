#!/usr/bin/env bash
# Before/after benchmark driver for the persistent message queue.
#
# Compares the library on `main` (before) against this branch (after).
# Library sources for each side are exported with `git archive` into a
# scratch dir, so the working tree is never touched. The same harness
# (benchmarks/src) is compiled once against each side's classes and the
# same scenarios run against both, interleaved per trial to reduce
# machine-noise bias.
#
# Reuses the Temurin JDK that run_tests.sh downloads into target/jdk/.
# Results land in benchmarks/results/ (raw JSONL + CSV, summary.md, PNGs).
#
# Usage: benchmarks/run_benchmarks.sh
#   TRIALS=5 (default) can be overridden via the environment.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BENCH="$ROOT/benchmarks"
RESULTS="$BENCH/results"
WORK="${BENCH_WORK:-/tmp/pmqueue-bench}"
TRIALS="${TRIALS:-5}"
BEFORE_REF="${BEFORE_REF:-main}"
AFTER_REF="${AFTER_REF:-HEAD}"
JAVA_RELEASE=21
JVM_FLAGS=(-Xms512m -Xmx512m)

# --- JDK (same download logic as run_tests.sh) ---------------------------
JDK_DIR="$ROOT/target/jdk"
find_javac() {
  find "$JDK_DIR" -type f -name javac -path '*/bin/*' 2>/dev/null | head -1
}
JAVAC="$(find_javac || true)"
if [ -z "${JAVAC:-}" ]; then
  case "$(uname -s)" in
    Darwin) os=mac ;;
    Linux) os=linux ;;
    *) echo "unsupported OS: $(uname -s)" >&2; exit 1 ;;
  esac
  case "$(uname -m)" in
    arm64|aarch64) arch=aarch64 ;;
    x86_64) arch=x64 ;;
    *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;;
  esac
  echo "Downloading Temurin $JAVA_RELEASE JDK ($os/$arch) into $JDK_DIR ..."
  mkdir -p "$JDK_DIR"
  curl -fsSL -o "$JDK_DIR/jdk.tar.gz" \
    "https://api.adoptium.net/v3/binary/latest/$JAVA_RELEASE/ga/$os/$arch/jdk/hotspot/normal/eclipse"
  tar -xzf "$JDK_DIR/jdk.tar.gz" -C "$JDK_DIR"
  rm "$JDK_DIR/jdk.tar.gz"
  JAVAC="$(find_javac)"
fi
[ -n "$JAVAC" ] || { echo "JDK setup failed" >&2; exit 1; }
JAVA="$(dirname "$JAVAC")/java"
"$JAVA" -version

# --- Export and compile both sides ---------------------------------------
rm -rf "$WORK"
mkdir -p "$WORK/data" "$RESULTS"

compile_side() {
  side="$1"
  ref="$2"
  echo "Exporting library sources for '$side' ($ref) ..."
  mkdir -p "$WORK/src-$side"
  git -C "$ROOT" archive "$ref" src/main/java | tar -x -C "$WORK/src-$side"
  echo "Compiling library for '$side' ..."
  mkdir -p "$WORK/classes-$side"
  find "$WORK/src-$side" -name '*.java' -print0 | xargs -0 "$JAVAC" \
    --release "$JAVA_RELEASE" -nowarn -d "$WORK/classes-$side"
  echo "Compiling harness against '$side' classes ..."
  mkdir -p "$WORK/bench-$side"
  "$JAVAC" --release "$JAVA_RELEASE" -nowarn -cp "$WORK/classes-$side" \
    -d "$WORK/bench-$side" "$BENCH"/src/*.java
}

compile_side main "$BEFORE_REF"
compile_side branch "$AFTER_REF"

# --- Scenario matrix ------------------------------------------------------
# scenario size ops warmup checksum
# poll() and the round trip fsync on every operation (library behavior on
# both sides), so those op counts are sized for ~200 ops/s on a laptop SSD.
SCENARIOS=(
  "offer 64 50000 10000 true"
  "offer 1024 50000 10000 true"
  "offer 8192 10000 2000 true"
  "offer 65536 2000 500 true"
  "poll 64 1500 300 true"
  "poll 1024 1500 300 true"
  "poll 8192 1500 300 true"
  "poll 65536 800 200 true"
  "offer 1024 50000 10000 false"
  "poll 1024 1500 300 false"
  "latency 1024 400 80 true"
  "openclose 1024 100 20 true"
)

RAW="$RESULTS/raw.jsonl"
: > "$RAW"

TIME_BIN=/usr/bin/time
HAVE_TIME_L=0
if [ "$(uname -s)" = "Darwin" ] && [ -x "$TIME_BIN" ]; then
  HAVE_TIME_L=1
fi

run_one() {
  side="$1" trial="$2" scenario="$3" size="$4" ops="$5" warmup="$6" checksum="$7"
  cp_arg="$WORK/bench-$side:$WORK/classes-$side"
  rm -rf "$WORK/data"
  mkdir -p "$WORK/data"
  args=(scenario="$scenario" side="$side" trial="$trial" size="$size" \
        ops="$ops" warmup="$warmup" checksum="$checksum" datadir="$WORK/data")
  if [ "$HAVE_TIME_L" = 1 ]; then
    line="$("$TIME_BIN" -l "$JAVA" "${JVM_FLAGS[@]}" -cp "$cp_arg" bench.QueueBench "${args[@]}" \
      2> "$WORK/time.out")"
    rss="$(awk '/maximum resident set size/ {print $1}' "$WORK/time.out")"
    line="${line%\}},\"peak_rss_bytes\":${rss:-0}}"
  else
    line="$("$JAVA" "${JVM_FLAGS[@]}" -cp "$cp_arg" bench.QueueBench "${args[@]}")"
  fi
  echo "$line" >> "$RAW"
  echo "  $side trial=$trial $scenario size=$size checksum=$checksum done"
}

echo "Running $TRIALS trials x ${#SCENARIOS[@]} scenarios x 2 sides ..."
for trial in $(seq 1 "$TRIALS"); do
  # alternate which side goes first each trial to reduce drift bias
  if [ $((trial % 2)) -eq 1 ]; then
    order=(main branch)
  else
    order=(branch main)
  fi
  for side in "${order[@]}"; do
    for row in "${SCENARIOS[@]}"; do
      # shellcheck disable=SC2086
      run_one "$side" "$trial" $row
    done
  done
done

# --- Environment metadata -------------------------------------------------
{
  echo "{"
  echo "  \"date\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\","
  echo "  \"os\": \"$(uname -sm)\","
  echo "  \"jvm\": \"$("$JAVA" -version 2>&1 | head -1 | sed 's/"/\\"/g')\","
  echo "  \"jvm_flags\": \"${JVM_FLAGS[*]}\","
  echo "  \"trials\": $TRIALS,"
  echo "  \"before_ref\": \"$(git -C "$ROOT" rev-parse --short "$BEFORE_REF")\","
  echo "  \"after_ref\": \"$(git -C "$ROOT" rev-parse --short "$AFTER_REF")\""
  echo "}"
} > "$RESULTS/env.json"

# --- Aggregate + plots ------------------------------------------------------
echo "Aggregating ..."
python3 "$BENCH/aggregate.py" "$RESULTS"

VENV="${BENCH_VENV:-/tmp/benchvenv}"
if [ ! -x "$VENV/bin/python" ]; then
  echo "Creating plot venv at $VENV ..."
  python3 -m venv "$VENV"
  "$VENV/bin/pip" -q install matplotlib
fi
if "$VENV/bin/python" -c "import matplotlib" 2>/dev/null; then
  echo "Plotting ..."
  "$VENV/bin/python" "$BENCH/make_plots.py" "$RESULTS"
else
  echo "matplotlib unavailable; skipping plots" >&2
fi

echo "Done. Results in $RESULTS"
