#!/usr/bin/env bash
# SIGKILL-and-resume harness for streamkit (milestone 4).
#
# Proves exactly-once checkpoint/restore the only way that counts: kill the
# process with SIGKILL at a random moment, restart it with --resume, repeat
# until it finishes, and diff the final output against the oracle. Any
# divergence (a double-counted event, a lost row, a half-merged file) fails
# the trial.
#
# Per trial:
#   1. fresh --ckpt-dir and --out
#   2. start streamkit (throttled so a 2M-event run lasts a few seconds and
#      checkpointing every 100K events, i.e. every quarter second or so)
#   3. after a random 0.2s..3.0s delay send SIGKILL; if it already exited 0,
#      the trial is done, otherwise restart with --resume and go to 3
#   4. bench/compare.sh truth results (same multiset of rows as the oracle)
#      and cmp against one uninterrupted run with the same flags, for both
#      the CSV and the --late-out file (byte for byte: the resumed run must
#      write exactly what a run that never died writes, row order included)
# At --lateness-s 60 the late file holds only its header (the generator's
# maximum delay is 30 s; the oracle comparison needs lateness >= that), so
# the late file's truncate-and-replay under SIGKILL is exercised but holds
# no records; the PipelineCheckpoint unit tests resume runs whose late file
# does hold records.
# The first start of every trial is also the one that must refuse to run on
# a stale checkpoint directory without --resume, so the harness removes the
# directory itself between trials (a leftover would be an error, by design).
#
#   bench/crash.sh [build_dir] [trials] [throttle_eps] [n_events]
# Inputs and the truth are cached in $ADSTACK_CACHE (default
# /private/tmp/adstack-sk); everything else goes to a private mktemp
# directory, so concurrent harness runs do not collide.
set -uo pipefail
B=${1:-build}
TRIALS=${2:-50}
THROTTLE=${3:-400000}
N=${4:-2000000}
SEED=11
EVERY=100000
THREADS=4
MAX_RESTARTS=40
CACHE=${ADSTACK_CACHE:-/private/tmp/adstack-sk}
CMP="$(cd "$(dirname "$0")" && pwd)/compare.sh"

mkdir -p "$CACHE"
IN="$CACHE/crash_ev${N}_seed${SEED}.ads"
TRUTH="$CACHE/crash_truth${N}_seed${SEED}.csv"
WORK=$(mktemp -d "$CACHE/crash.XXXXXX")
trap 'rm -rf "$WORK"' EXIT

# Private name + rename, so a concurrent harness never reads a half-written file.
if [ ! -s "$IN" ]; then "$B/bin/gen" --n "$N" --seed "$SEED" --out "$IN.tmp.$$" && mv -f "$IN.tmp.$$" "$IN"; fi
if [ ! -s "$TRUTH" ]; then "$B/bin/oracle" --in "$IN" --out "$TRUTH.tmp.$$" && mv -f "$TRUTH.tmp.$$" "$TRUTH"; fi

# The byte-for-byte reference: one run with the trial flags, never killed
# (not throttled; pacing does not change a byte of output).
REF="$WORK/reference.csv"
REF_LATE="$WORK/reference_late.ads"
if ! "$B/bin/streamkit" --in "$IN" --out "$REF" --late-out "$REF_LATE" --lateness-s 60 --threads "$THREADS" \
    --ckpt-dir "$WORK/reference_ckpt" --checkpoint-every-events "$EVERY" 2>"$WORK/reference.log"; then
  echo "reference run failed:"; cat "$WORK/reference.log"; exit 1
fi
if ! "$CMP" "$TRUTH" "$REF" >/dev/null; then echo "reference run differs from the oracle"; exit 1; fi

echo "## streamkit crash harness"
echo "- machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(sysctl -n hw.ncpu 2>/dev/null || nproc) hardware threads"
echo "- build: $B ($(grep -E '^CMAKE_BUILD_TYPE' "$B/CMakeCache.txt" | cut -d= -f2), sanitizer $(grep -E '^ADSTACK_SANITIZE' "$B/CMakeCache.txt" | cut -d= -f2))"
echo "- input: gen --n $N --seed $SEED (1% dups, 5% late up to 30s); truth: oracle"
echo "- command: streamkit --in \$IN --out \$OUT --late-out \$LATE --lateness-s 60 --threads $THREADS --ckpt-dir \$D --checkpoint-every-events $EVERY --throttle-eps $THROTTLE [--resume]"
echo "- check: compare.sh against the oracle, and cmp of the CSV and the late file against one uninterrupted run with the same flags"
echo "- kill: SIGKILL after 0.2s..3.0s (\$RANDOM), restart with --resume until exit 0, at most $MAX_RESTARTS restarts"
echo "- date: $(date '+%Y-%m-%d %H:%M %Z')"
echo

# Starts streamkit in the background, waits up to the delay, kills it if it
# is still running. Sets RC to the exit status (0 = finished, 137 = killed).
run_once() {  # $1 = extra flags ("" or --resume), $2 = delay in seconds
  # The braces-with-redirect and the 2>/dev/null on wait silence bash's own
  # "Killed: 9" job notices; streamkit's stderr still goes to the log.
  { "$B/bin/streamkit" --in "$IN" --out "$OUT" --late-out "$LATE" --lateness-s 60 --threads "$THREADS" \
      --ckpt-dir "$D" --checkpoint-every-events "$EVERY" --throttle-eps "$THROTTLE" $1 \
      2>>"$LOG" & } 2>/dev/null
  local pid=$!
  # Poll in 50ms steps so a run that finishes early is noticed without
  # waiting out the whole delay.
  local waited=0
  local limit
  limit=$(awk -v d="$2" 'BEGIN { printf "%d", d * 1000 }')
  while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt "$limit" ]; do
    sleep 0.05
    waited=$((waited + 50))
  done
  if kill -0 "$pid" 2>/dev/null; then
    kill -9 "$pid" 2>/dev/null
  fi
  wait "$pid" 2>/dev/null
  RC=$?
}

fail=0
total_kills=0
multi_kill_trials=0
for t in $(seq 1 "$TRIALS"); do
  D="$WORK/ckpt"
  OUT="$WORK/results.csv"
  LATE="$WORK/results_late.ads"
  LOG="$WORK/trial-$t.log"
  rm -rf "$D" "$OUT" "$OUT".part* "$OUT.tmp" "$LATE" "$LATE".part* "$LATE.tmp"
  : > "$LOG"
  kills=0
  flags=""
  result="FAIL(no exit)"
  for _ in $(seq 0 "$MAX_RESTARTS"); do
    # 0.2s .. 3.0s in 10ms steps.
    delay=$(awk -v r="$RANDOM" 'BEGIN { printf "%.2f", 0.2 + (r % 281) / 100 }')
    run_once "$flags" "$delay"
    if [ "$RC" -eq 0 ]; then
      result="exit 0"
      break
    elif [ "$RC" -eq 137 ]; then
      kills=$((kills + 1))
      flags="--resume"
    else
      result="FAIL(exit $RC)"
      break
    fi
  done
  if [ "$result" = "exit 0" ]; then
    if ! cmp_out=$("$CMP" "$TRUTH" "$OUT" 2>&1); then
      result="FAIL(compare)"
    elif ! cmp -s "$REF" "$OUT"; then
      result="FAIL(csv not byte-identical to the uninterrupted run)"
    elif ! cmp -s "$REF_LATE" "$LATE"; then
      result="FAIL(late file not byte-identical to the uninterrupted run)"
    else
      result="PASS ($(echo "$cmp_out" | sed -n 's/PASS: //p'), byte-identical)"
    fi
  fi
  # A trial that ends with a part file, a .tmp output or a checkpoint dir left
  # behind did not clean up after itself, which is its own kind of failure.
  if [ "${result#PASS}" != "$result" ]; then
    if [ -e "$D" ] || compgen -G "$OUT.part*" >/dev/null || compgen -G "$LATE.part*" >/dev/null || [ -e "$OUT.tmp" ] || [ -e "$LATE.tmp" ]; then
      result="FAIL(leftovers)"
    fi
  fi
  total_kills=$((total_kills + kills))
  if [ "$kills" -ge 2 ]; then multi_kill_trials=$((multi_kill_trials + 1)); fi
  # Only the run that finished prints a summary; it says which epoch it
  # picked up from (the epochs before it were committed by killed runs).
  from=$(sed -n 's/.*resumed from epoch \([0-9]*\) offset.*/\1/p' "$LOG" | tail -1)
  printf 'trial %2d: kills=%d final run resumed from epoch %-3s %s\n' "$t" "$kills" "${from:-none}" "$result"
  if [ "${result#PASS}" = "$result" ]; then
    fail=1
    echo "--- stderr of trial $t ---"
    tail -20 "$LOG"
    echo "---"
  fi
done

echo
echo "trials: $TRIALS, total kills: $total_kills, trials with >= 2 kills: $multi_kill_trials"
if [ "$fail" -ne 0 ]; then echo "CRASH HARNESS FAIL"; exit 1; fi
echo "CRASH HARNESS PASS: every trial's output identical to the oracle and byte-identical to an uninterrupted run"
