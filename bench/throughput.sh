#!/usr/bin/env bash
# Throughput vs worker threads for streamkit (milestone 3).
#
# Generates a 10M-event dirty stream once (cached in /private/tmp/adstack-sk),
# runs streamkit at --threads 1, 2, 4, 8, three runs each, and prints a
# markdown table of the median events/sec and the speedup over one thread.
# The machine is recorded at the top so the numbers can be reproduced or
# disputed. Any extra arguments are passed through to streamkit, which is how
# the variants in NOTES.md were measured (e.g. --reader-decode, --no-dedup).
#
#   bench/throughput.sh [build_dir] [extra streamkit flags...]
set -euo pipefail
B=${1:-build}
shift || true
EXTRA=("$@")

CACHE=/private/tmp/adstack-sk
N=10000000
SEED=1
RUNS=3
THREADS=(1 2 4 8)
mkdir -p "$CACHE"
IN="$CACHE/ev${N}_seed${SEED}.ads"
OUT="$CACHE/throughput_out.csv"

if [ ! -s "$IN" ]; then
  "$B/bin/gen" --n "$N" --seed "$SEED" --out "$IN"
fi

echo "## streamkit throughput"
echo
echo "- machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(sysctl -n hw.ncpu 2>/dev/null || nproc) hardware threads, $(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1024 / 1024 / 1024 )) GB"
echo "- os: $(uname -sr); compiler: $(cc --version 2>/dev/null | head -1)"
echo "- build: $B ($(grep -E '^CMAKE_BUILD_TYPE' "$B/CMakeCache.txt" | cut -d= -f2), sanitizer $(grep -E '^ADSTACK_SANITIZE' "$B/CMakeCache.txt" | cut -d= -f2))"
echo "- input: gen --n $N --seed $SEED (1% dups, 5% late up to 30s, 100K campaigns), $(stat -f %z "$IN" 2>/dev/null || stat -c %s "$IN") bytes"
echo "- command: streamkit --in \$IN --out \$OUT --lateness-s 60 --threads P ${EXTRA[*]:-}"
echo "- $RUNS runs per P, median reported; events/s = events / wall time including the final merge of per-worker part files"
echo "- date: $(date '+%Y-%m-%d %H:%M %Z')"
echo
echo "| threads | run 1 | run 2 | run 3 | median events/s | speedup vs 1 | reader stalls (median run) | worker idle waits (median run) |"
echo "|---|---|---|---|---|---|---|---|"

base=""
for P in "${THREADS[@]}"; do
  rates=()
  details=()
  for _ in $(seq "$RUNS"); do
    log=$("$B/bin/streamkit" --in "$IN" --out "$OUT" --lateness-s 60 --threads "$P" ${EXTRA[@]+"${EXTRA[@]}"} 2>&1 >/dev/null)
    rate=$(echo "$log" | sed -n 's/.* \([0-9]*\) events\/s.*/\1/p' | head -1)
    stalls=$(echo "$log" | sed -n 's/.*reader stalls \([0-9]*\).*/\1/p' | head -1)
    idle=$(echo "$log" | sed -n 's/.*worker idle waits \([0-9]*\).*/\1/p' | head -1)
    rates+=("$rate")
    details+=("$rate $stalls $idle")
  done
  # Median of three = the middle value after sorting.
  sorted=$(printf '%s\n' "${details[@]}" | sort -n)
  median_line=$(echo "$sorted" | sed -n 2p)
  median=$(echo "$median_line" | cut -d' ' -f1)
  m_stalls=$(echo "$median_line" | cut -d' ' -f2)
  m_idle=$(echo "$median_line" | cut -d' ' -f3)
  if [ -z "$base" ]; then base=$median; fi
  speedup=$(awk -v m="$median" -v b="$base" 'BEGIN { printf "%.2fx", m / b }')
  printf '| %s | %s | %s | %s | **%s** | %s | %s | %s |\n' \
    "$P" "${rates[0]}" "${rates[1]}" "${rates[2]}" "$median" "$speedup" "$m_stalls" "$m_idle"
done
rm -f "$OUT"
