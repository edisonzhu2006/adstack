#!/usr/bin/env bash
# End-to-end integration of the whole pipeline, one report, nonzero exit on
# any mismatch:
#
#   gen (dirty events) -> oracle (truth) -> streamkit (--threads 4, checkpoints on)
#       -> compare.sh (streamkit == oracle)
#       -> colstore ingest events.ads      -> verify every field   -> Q1-Q3 with --check
#       -> colstore ingest --in-csv aggregates.csv -> verify every field
#       -> colstore q on the aggregate table (top 10 campaigns by spend in one hour)
#          --check-csv against the CSV, and an awk recomputation from the ORACLE's
#          output (an engine-independent answer)
#
#   bench/e2e.sh [build_dir] [n_events] [work_dir] [threads]
#
# Defaults: build, 5,000,000 events, /private/tmp/adstack-cs/e2e, 4 threads.
# The generator runs at 1,000 events per simulated second so 5M events span
# 5,000 s (83 one-minute windows) and the "chosen hour" below selects 60 of
# them: the time predicate on the aggregate table then really filters and
# the zone maps really skip, instead of every row passing. Every stage's
# time is wall-clock around the whole process (fork to exit), measured by
# this script, plus the engine's own figure where it prints one.
set -uo pipefail
B=${1:-build}
N=${2:-5000000}
W=${3:-/private/tmp/adstack-cs/e2e}
THREADS=${4:-4}
SEED=5
RATE=1000
LATENESS=60
CKPT_EVERY=1000000
EPOCH=1767225600000000            # gen's default clock start, 2026-01-01T00:00:00Z
H0=$EPOCH                         # the chosen hour: [H0, H1)
H1=$((EPOCH + 3600000000))
CMP="$(cd "$(dirname "$0")" && pwd)/compare.sh"

mkdir -p "$W"
ADS="$W/events.ads"; TRUTH="$W/truth.csv"; AGG="$W/aggregates.csv"; LATE="$W/late.ads"
ECOL="$W/events.col"; ACOL="$W/aggregates.col"; CKPT="$W/ckpt"; LOG="$W/log"
mkdir -p "$LOG"
cleanup() { rm -rf "$ADS" "$TRUTH" "$AGG" "$LATE" "$ECOL" "$ACOL" "$CKPT" "$LOG"; }
trap cleanup EXIT

# Sub-second wall clock. perl's Time::HiRes is core on macOS and Linux;
# bash 3.2 (macOS) has no EPOCHREALTIME and BSD date has no %N.
now() { perl -MTime::HiRes=time -e 'printf "%.3f\n", time'; }
elapsed() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.3f", b - a}'; }
size() { stat -f%z "$1" 2>/dev/null || stat -c%s "$1"; }

# Report rows: "step|detail|seconds|status". Every step records its own
# status; the script never stops early so the report shows everything.
ROWS=()
FAILS=0
record() {  # step detail seconds status
  ROWS+=("$1|$2|$3|$4")
  [ "$4" = PASS ] || FAILS=$((FAILS + 1))
}
# Runs a command with its stderr captured to $LOG/<name>, times it, and
# records PASS/FAIL from its exit status. $DETAIL (set by the caller
# afterwards via detail_last) is filled from the log.
run_step() {  # name "command words..."
  local name=$1; shift
  local t0 t1 rc
  t0=$(now)
  "$@" > "$LOG/$name.out" 2> "$LOG/$name.err"
  rc=$?
  t1=$(now)
  LAST_SECS=$(elapsed "$t0" "$t1")
  LAST_RC=$rc
}

echo "## adstack end-to-end: gen -> streamkit -> colstore"
echo "- machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(sysctl -n hw.ncpu 2>/dev/null || nproc) hardware threads, load $(uptime | sed 's/.*load averages*: //')"
echo "- build: $B ($(grep -E '^CMAKE_BUILD_TYPE' "$B/CMakeCache.txt" | cut -d= -f2), sanitizer $(grep -E '^ADSTACK_SANITIZE' "$B/CMakeCache.txt" | cut -d= -f2))"
echo "- input: gen --n $N --seed $SEED --rate $RATE (1% dups, 5% late up to 30 s); window 60 s, lateness $LATENESS s"
echo "- streamkit: --threads $THREADS --ckpt-dir ... --checkpoint-every-events $CKPT_EVERY"
echo "- chosen hour for the aggregate query: [$H0, $H1) = 2026-01-01T00:00 to 01:00Z"
echo "- date: $(date '+%Y-%m-%d %H:%M %Z')"
echo

# ---- 1. generate ------------------------------------------------------------
run_step gen "$B/bin/gen" --n "$N" --seed "$SEED" --rate "$RATE" --out "$ADS"
record "gen" "$(sed -n 's/^gen: wrote //p' "$LOG/gen.err" | head -1)" "$LAST_SECS" "$([ $LAST_RC = 0 ] && echo PASS || echo FAIL)"

# ---- 2. oracle ---------------------------------------------------------------
run_step oracle "$B/bin/oracle" --in "$ADS" --out "$TRUTH"
record "oracle" "$(sed -n 's/^oracle: //p' "$LOG/oracle.err" | head -1)" "$LAST_SECS" "$([ $LAST_RC = 0 ] && echo PASS || echo FAIL)"

# ---- 3. streamkit with checkpointing -----------------------------------------
rm -rf "$CKPT"
run_step streamkit "$B/bin/streamkit" --in "$ADS" --out "$AGG" --lateness-s "$LATENESS" --late-out "$LATE" \
  --threads "$THREADS" --ckpt-dir "$CKPT" --checkpoint-every-events "$CKPT_EVERY"
sk_line=$(grep '^streamkit: [0-9]* events' "$LOG/streamkit.err" | head -1 | sed 's/^streamkit: //')
sk_ckpt=$(grep '^streamkit: checkpoints:' "$LOG/streamkit.err" | sed 's/^streamkit: checkpoints: //; s/, [0-9]* snapshots.*//')
record "streamkit" "$sk_line" "$LAST_SECS" "$([ $LAST_RC = 0 ] && echo PASS || echo FAIL)"
record "  checkpoints" "$sk_ckpt" "" "$([ -n "$sk_ckpt" ] && echo PASS || echo FAIL)"

# ---- 4. streamkit == oracle ---------------------------------------------------
run_step compare "$CMP" "$TRUTH" "$AGG"
record "compare streamkit vs oracle" "$(head -1 "$LOG/compare.out")" "$LAST_SECS" "$([ $LAST_RC = 0 ] && echo PASS || echo FAIL)"

# ---- 5. colstore: events --------------------------------------------------------
run_step ingest_events "$B/bin/colstore" ingest --in "$ADS" --out "$ECOL"
record "colstore ingest events.ads" "$(sed -n 's/^ingest: //p' "$LOG/ingest_events.err" | head -1); $(size "$ADS") -> $(size "$ECOL") bytes" "$LAST_SECS" "$([ $LAST_RC = 0 ] && echo PASS || echo FAIL)"

run_step verify_events "$B/bin/colstore" verify --col "$ECOL" --ads "$ADS"
record "colstore verify events" "$(sed -n 's/^verify: //p' "$LOG/verify_events.err" | head -1)" "$LAST_SECS" "$([ $LAST_RC = 0 ] && echo PASS || echo FAIL)"

# ---- 6. colstore: aggregates ----------------------------------------------------
run_step ingest_agg "$B/bin/colstore" ingest --in-csv "$AGG" --out "$ACOL"
record "colstore ingest aggregates.csv" "$(sed -n 's/^ingest: //p' "$LOG/ingest_agg.err" | head -1); $(size "$AGG") -> $(size "$ACOL") bytes" "$LAST_SECS" "$([ $LAST_RC = 0 ] && echo PASS || echo FAIL)"

run_step verify_agg "$B/bin/colstore" verify --col "$ACOL" --csv "$AGG"
record "colstore verify aggregates" "$(sed -n 's/^verify: //p' "$LOG/verify_agg.err" | head -1)" "$LAST_SECS" "$([ $LAST_RC = 0 ] && echo PASS || echo FAIL)"

# ---- 7. Q1-Q3 on the event table, each proved against the .ads file -------------
# The three benchmark queries of colstore/BENCHMARKS.md. Q2's hour is the
# same chosen hour. `--check` recomputes the whole answer from the .ads
# file with a std::map and exits 1 on any difference.
T0=$EPOCH
Q1W="country=840 AND event_type=1";                       Q1A="count()"
Q2W="event_time_us BETWEEN $T0 AND $((T0 + 3600000000))"; Q2G=campaign_id; Q2A="sum(cost_micros),count()"
Q3G=campaign_id
Q3A="count_if(event_type=0) AS impressions,count_if(event_type=1) AS clicks,count_if(event_type=2) AS conversions,sum(cost_micros) AS cost_micros"
q_events() {  # name label args...
  local name=$1 label=$2; shift 2
  run_step "$name" "$B/bin/colstore" q --in "$ECOL" --stats --threads "$THREADS" --check "$ADS" "$@"
  local engine rows chk
  engine=$(grep '^output:' "$LOG/$name.err" | sed 's/.*total //; s/ s$//')
  rows=$(($(wc -l < "$LOG/$name.out") - 1))
  chk=$(grep '^q --check:' "$LOG/$name.err" | sed 's/^q --check: //; s/; brute force[^)]*)/)/')
  record "$label" "$rows rows, engine $engine s at $THREADS threads; check $chk" "$LAST_SECS" "$([ $LAST_RC = 0 ] && echo PASS || echo FAIL)"
}
q_events q1 "Q1 events: point filter + count"      --where "$Q1W" --agg "$Q1A"
q_events q2 "Q2 events: 1h range, group campaign"  --where "$Q2W" --group "$Q2G" --agg "$Q2A"
q_events q3 "Q3 events: full group by, 4 aggs"     --group "$Q3G" --agg "$Q3A"

# ---- 8. the aggregate-table query: top 10 campaigns by spend in the hour ----------
# Proved twice: --check-csv recomputes it from the CSV inside colstore, and
# awk recomputes it from the ORACLE's CSV (no colstore or streamkit code),
# sorted by spend desc then campaign_id asc, the engine's tiebreak.
run_step qagg "$B/bin/colstore" q --in "$ACOL" --stats --threads "$THREADS" --check-csv "$AGG" \
  --where "window_start_us >= $H0 AND window_start_us < $H1" --group campaign_id \
  --agg "sum(cost_micros) AS spend" --order "spend desc" --limit 10
engine=$(grep '^output:' "$LOG/qagg.err" | sed 's/.*total //; s/ s$//')
zone=$(grep 'row groups:' "$LOG/qagg.err" | sed 's/^q: //; s/ skipped by zone map.*/ skipped by zone map/')
chk=$(grep '^q --check:' "$LOG/qagg.err" | sed 's/^q --check: //; s/; brute force[^)]*)/)/')
record "Q4 aggregates: top 10 spend in hour" "engine $engine s; $zone; check-csv $chk" "$LAST_SECS" "$([ $LAST_RC = 0 ] && echo PASS || echo FAIL)"

t0=$(now)
awk -F, -v h0="$H0" -v h1="$H1" 'NR > 1 && $1 >= h0 && $1 < h1 { s[$2] += $6 }
  END { for (c in s) printf "%d,%d\n", c, s[c] }' "$TRUTH" \
  | LC_ALL=C sort -t, -k2,2nr -k1,1n | head -10 > "$LOG/awk_top10.csv"
t1=$(now)
if diff <(tail -n +2 "$LOG/qagg.out") "$LOG/awk_top10.csv" > "$LOG/awk_diff" 2>&1; then
  record "  awk from oracle output" "top 10 identical: $(head -1 "$LOG/awk_top10.csv" | sed 's/,/ spend /')..." "$(elapsed "$t0" "$t1")" PASS
else
  record "  awk from oracle output" "DIFFER: $(head -3 "$LOG/awk_diff" | tr '\n' ' ')" "$(elapsed "$t0" "$t1")" FAIL
fi

# ---- report ----------------------------------------------------------------------
echo "| step | result | wall s | status |"
echo "|---|---|---:|:---:|"
for row in "${ROWS[@]}"; do
  IFS='|' read -r step detail secs status <<< "$row"
  echo "| $step | $detail | $secs | $status |"
done
echo
echo "top 10 campaigns by spend, $H0 <= window_start_us < $H1 (colstore q on the aggregate table):"
sed 's/^/    /' "$LOG/qagg.out"
echo
if [ "$FAILS" -ne 0 ]; then
  echo "E2E FAIL: $FAILS step(s) failed; logs in $LOG until this script exits"
  for f in "$LOG"/*.err; do
    if grep -q -i "fail\|error\|colstore .*:" "$f"; then echo "--- $f"; tail -5 "$f"; fi
  done
  exit 1
fi
echo "E2E PASS: streamkit matched the oracle, both tables verified field by field, Q1-Q3 and the aggregate query matched their brute-force recomputation"
