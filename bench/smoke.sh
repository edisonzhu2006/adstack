#!/usr/bin/env bash
# End-to-end smoke: generate, batch-recompute, stream, compare.
# Uses the generator's default dirt (1% duplicates, 5% late up to 30s), which
# streamkit must match exactly from milestone 2 on. From milestone 3 the
# stream is run at 1, 2, 4 and 8 worker threads, tumbling (60s) and sliding
# (300s window, 60s slide), and every run must match the oracle.
#   bench/smoke.sh [build_dir] [n_events] [seed]
set -euo pipefail
B=${1:-build}
N=${2:-200000}
SEED=${3:-7}
D=$(mktemp -d); trap 'rm -rf "$D"' EXIT
CMP="$(dirname "$0")/compare.sh"

"$B/bin/gen" --n "$N" --seed "$SEED" --dup-frac 0.01 --late-frac 0.05 --max-late-s 30 --out "$D/events.ads"
"$B/bin/oracle" --in "$D/events.ads" --out "$D/truth_tumbling.csv"
"$B/bin/oracle" --in "$D/events.ads" --out "$D/truth_sliding.csv" --window-s 300 --slide-s 60

fail=0
for P in 1 2 4 8; do
  echo "== tumbling 60s, threads=$P"
  "$B/bin/streamkit" --in "$D/events.ads" --out "$D/results.csv" --lateness-s 60 \
    --late-out "$D/late.ads" --threads "$P"
  "$CMP" "$D/truth_tumbling.csv" "$D/results.csv" || fail=1

  echo "== sliding 300s/60s, threads=$P"
  "$B/bin/streamkit" --in "$D/events.ads" --out "$D/results.csv" --lateness-s 60 \
    --late-out "$D/late.ads" --threads "$P" --window-s 300 --slide-s 60
  "$CMP" "$D/truth_sliding.csv" "$D/results.csv" || fail=1
done

if [ "$fail" -ne 0 ]; then echo "SMOKE FAIL"; exit 1; fi
echo "SMOKE PASS: all thread counts match the oracle (tumbling and sliding)"
