#!/usr/bin/env bash
# streamkit benchmark harness (milestones 3 and 5). Every streamkit number in
# streamkit/BENCHMARKS.md comes from this script; the machine, build, input
# and exact command are printed above each table so a number can be
# reproduced or disputed.
#
# Sections (all run by default; pick with --only):
#   threads     events/s at --threads 1, 2, 4, 8 on a 10M-event dirty stream
#   dedup       dedup on vs --no-dedup, on a dirty (1% dups) and a clean
#               (--dup-frac 0) stream, at 1 and 8 threads
#   checkpoint  events/s with no checkpoints vs --checkpoint-every-events
#               1000000 (the default interval) vs 100000 (the crash-harness
#               interval), 8 threads, plus the snapshot cost from the summary
#   memory      maximum resident set size (/usr/bin/time -l) at 1M and 10M
#               events, 1 and 8 threads, plus 8 threads with --queue-depth 256
#               to separate engine state from queue buffers
#   latency     emit latency under paced replay: gen --pace at 200K events/s
#               for 20 s piped into streamkit --out - --window-s 1
#               --lateness-s 2 --latency-stats, its stdout piped into cat,
#               1 and 8 threads; p50/p99 per run and the median over runs.
#               The metric is defined in streamkit/latency.h and NOTES.md
#               (m5, as revised in "Review fixes"): per window closed by the
#               watermark, wall time from read(2) returning the block holding
#               the closing event to the row having been written to stdout,
#               i.e. into the pipe a consumer reads. It excludes the
#               configured lateness and the producer's own buffering.
#
# Every cell is the median of three runs (the middle value); the three runs
# are printed too, so the spread is visible. Inputs are generated once and
# cached in $ADSTACK_CACHE (default /private/tmp/adstack-sk; about 1.1 GB for
# the 10M-event files); outputs, checkpoints and logs go to a private
# mktemp directory under it, so concurrent runs do not share or delete each
# other's files.
# Any extra arguments are passed through to every streamkit invocation, which
# is how the variants in NOTES.md were measured (e.g. --reader-decode).
#
#   bench/throughput.sh [build_dir] [--only threads,dedup,...] [extra streamkit flags...]
set -euo pipefail
B=${1:-build}
shift || true
ONLY="threads,dedup,checkpoint,memory,latency"
if [ "${1:-}" = "--only" ]; then ONLY=$2; shift 2; fi
EXTRA=("$@")

CACHE=${ADSTACK_CACHE:-/private/tmp/adstack-sk}
N=10000000
SEED=1
RUNS=3
THREADS=(1 2 4 8)
LAT_RATE=200000     # events/s of wall clock, gen --pace
LAT_SECS=20
mkdir -p "$CACHE"
IN="$CACHE/ev${N}_seed${SEED}.ads"
IN_CLEAN="$CACHE/ev${N}_seed${SEED}_clean.ads"
IN_1M="$CACHE/ev1000000_seed${SEED}.ads"
WORK=$(mktemp -d "$CACHE/throughput.XXXXXX")
OUT="$WORK/out.csv"
CKPT="$WORK/ckpt"
LOG="$WORK/run.log"
trap 'rm -rf "$WORK"' EXIT

want() { case ",$ONLY," in *",$1,"*) return 0 ;; *) return 1 ;; esac; }

# ---- helpers ----------------------------------------------------------------------

# Runs streamkit with the given flags; its stderr summary lands in $LOG.
sk() { "$B/bin/streamkit" "$@" ${EXTRA[@]+"${EXTRA[@]}"} 2>"$LOG" >/dev/null; }

# Pull one number out of the summary by the text that precedes or follows it.
eps()    { sed -n 's/.* \([0-9]*\) events\/s.*/\1/p' "$LOG" | head -1; }
stalls() { sed -n 's/.*reader stalls \([0-9]*\).*/\1/p' "$LOG" | head -1; }
idle()   { sed -n 's/.*worker idle waits \([0-9]*\).*/\1/p' "$LOG" | head -1; }
field()  { sed -n "s/.*$1 \([0-9.]*\).*/\1/p" "$LOG" | head -1; }   # field "peak ids" etc.

# Median of three numbers = the middle one after a numeric sort.
median3() { printf '%s\n' "$1" "$2" "$3" | sort -n | sed -n 2p; }

# One paced run for the latency section: gen --pace piped into streamkit,
# summary left in $LOG. Both exit statuses are checked, not just the
# pipeline's: gen dying of SIGPIPE (141) would mean streamkit closed the pipe
# early, and a run like that is not a sample. One full harness run was lost
# that way (2026-10-21, exit 141 with the log already deleted), so now a
# failed run prints both statuses and streamkit's stderr and is retried once
# instead of taking the whole harness down.
lat_run() {  # $1 = threads
  local attempt st
  for attempt in 1 2; do
    set +e
    "$B/bin/gen" --n $((LAT_RATE * LAT_SECS)) --rate "$LAT_RATE" --pace --seed "$SEED" --out - 2>"$LOG.gen" \
      | "$B/bin/streamkit" --in - --out - --window-s 1 --lateness-s 2 --threads "$1" --latency-stats \
          ${EXTRA[@]+"${EXTRA[@]}"} 2>"$LOG" \
      | cat > /dev/null
    st=("${PIPESTATUS[@]}")
    set -e
    if [ "${st[0]}" -eq 0 ] && [ "${st[1]}" -eq 0 ] && [ "${st[2]}" -eq 0 ]; then return 0; fi
    echo "latency run (threads $1, attempt $attempt) failed: gen exit ${st[0]}, streamkit exit ${st[1]}, cat exit ${st[2]}" >&2
    echo "--- gen stderr ---" >&2; cat "$LOG.gen" >&2
    echo "--- streamkit stderr ---" >&2; cat "$LOG" >&2
  done
  return 1
}

# Percentage change of $1 relative to $2, as "+x.x%" (positive = $1 is larger).
pct() { awk -v a="$1" -v b="$2" 'BEGIN { printf "%+.1f%%", (a - b) * 100.0 / b }'; }

header() {
  echo "- machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(sysctl -n hw.ncpu 2>/dev/null || nproc) hardware threads, $(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1024 / 1024 / 1024 )) GB"
  echo "- os: $(uname -sr); compiler: $(cc --version 2>/dev/null | head -1)"
  echo "- build: $B ($(grep -E '^CMAKE_BUILD_TYPE' "$B/CMakeCache.txt" | cut -d= -f2), sanitizer $(grep -E '^ADSTACK_SANITIZE' "$B/CMakeCache.txt" | cut -d= -f2))"
  echo "- load average at start: $(uptime | sed 's/.*load averages*: //')"
  echo "- extra streamkit flags: ${EXTRA[*]:-none}"
  echo "- $RUNS runs per cell, median (middle value) reported; events/s = events / wall time including the final merge of per-worker part files"
  echo "- date: $(date '+%Y-%m-%d %H:%M %Z')"
}

# ---- inputs -------------------------------------------------------------------------

# Generated to a private name and renamed, so a concurrent run never reads a
# half-written input (the rename is atomic; both writers produce the same
# bytes, so whichever lands last is fine).
gen_once() {  # $1 = path, rest = gen flags
  local f=$1; shift
  [ -s "$f" ] && return 0
  "$B/bin/gen" "$@" --out "$f.tmp.$$" && mv -f "$f.tmp.$$" "$f"
}
gen_once "$IN"       --n "$N" --seed "$SEED"
gen_once "$IN_CLEAN" --n "$N" --seed "$SEED" --dup-frac 0
gen_once "$IN_1M"    --n 1000000 --seed "$SEED"

echo "# streamkit benchmarks"
echo
header
echo "- dirty input: gen --n $N --seed $SEED (1% dups, 5% late up to 30s, 100K campaigns), $(stat -f %z "$IN" 2>/dev/null || stat -c %s "$IN") bytes"
echo "- clean input: same with --dup-frac 0"
echo

# ---- threads ------------------------------------------------------------------------

if want threads; then
  echo "## throughput vs threads"
  echo
  echo "- command: streamkit --in \$DIRTY --out \$OUT --lateness-s 60 --threads P"
  echo
  echo "| threads | run 1 | run 2 | run 3 | median events/s | speedup vs 1 | reader stalls (median run) | worker idle waits (median run) |"
  echo "|---|---|---|---|---|---|---|---|"
  base=""
  for P in "${THREADS[@]}"; do
    rates=(); details=()
    for _ in $(seq "$RUNS"); do
      sk --in "$IN" --out "$OUT" --lateness-s 60 --threads "$P"
      rates+=("$(eps)")
      details+=("$(eps) $(stalls) $(idle)")
    done
    median_line=$(printf '%s\n' "${details[@]}" | sort -n | sed -n 2p)
    median=$(echo "$median_line" | cut -d' ' -f1)
    if [ -z "$base" ]; then base=$median; fi
    speedup=$(awk -v m="$median" -v b="$base" 'BEGIN { printf "%.2fx", m / b }')
    printf '| %s | %s | %s | %s | **%s** | %s | %s | %s |\n' "$P" "${rates[0]}" "${rates[1]}" "${rates[2]}" \
      "$median" "$speedup" "$(echo "$median_line" | cut -d' ' -f2)" "$(echo "$median_line" | cut -d' ' -f3)"
  done
  echo
fi

# ---- dedup --------------------------------------------------------------------------

if want dedup; then
  echo "## dedup overhead"
  echo
  echo "- command: streamkit --in \$STREAM --out \$OUT --lateness-s 60 --threads P [--no-dedup]"
  echo "- --no-dedup skips the per-event hash-set insert (and counts duplicates twice; it exists only for this measurement)"
  echo "- overhead = (no-dedup median - dedup median) / no-dedup median, same stream and P"
  echo
  echo "| stream | threads | dedup | run 1 | run 2 | run 3 | median events/s | dedup overhead |"
  echo "|---|---|---|---|---|---|---|---|"
  for P in 1 8; do
    for stream in dirty clean; do
      if [ "$stream" = dirty ]; then f=$IN; else f=$IN_CLEAN; fi
      off=""
      for mode in off on; do
        rates=()
        for _ in $(seq "$RUNS"); do
          if [ "$mode" = off ]; then
            sk --in "$f" --out "$OUT" --lateness-s 60 --threads "$P" --no-dedup
          else
            sk --in "$f" --out "$OUT" --lateness-s 60 --threads "$P"
          fi
          rates+=("$(eps)")
        done
        median=$(median3 "${rates[@]}")
        if [ "$mode" = off ]; then
          off=$median; overhead="(baseline)"
        else
          overhead=$(awk -v on="$median" -v o="$off" 'BEGIN { printf "%.1f%%", (o - on) * 100.0 / o }')
        fi
        printf '| %s | %s | %s | %s | %s | %s | **%s** | %s |\n' "$stream" "$P" "$mode" "${rates[0]}" "${rates[1]}" "${rates[2]}" "$median" "$overhead"
      done
    done
  done
  echo
fi

# ---- checkpoint ---------------------------------------------------------------------

if want checkpoint; then
  echo "## checkpoint overhead"
  echo
  echo "- command: streamkit --in \$DIRTY --out \$OUT --lateness-s 60 --threads 8 [--ckpt-dir \$D --checkpoint-every-events N]"
  echo "- snapshot columns are from the summary's checkpoints: line of the median run (snapshots = per-worker snapshots written; slowest = one snapshot, fsyncs included; total = sum over all snapshots of all workers)"
  echo
  echo "| checkpoint interval | run 1 | run 2 | run 3 | median events/s | vs none | snapshots | slowest snapshot | snapshot time total |"
  echo "|---|---|---|---|---|---|---|---|---|"
  none=""
  for every in none 1000000 100000; do
    rates=(); details=()
    for _ in $(seq "$RUNS"); do
      rm -rf "$CKPT"
      if [ "$every" = none ]; then
        sk --in "$IN" --out "$OUT" --lateness-s 60 --threads 8
        details+=("$(eps) - - -")
      else
        sk --in "$IN" --out "$OUT" --lateness-s 60 --threads 8 --ckpt-dir "$CKPT" --checkpoint-every-events "$every"
        details+=("$(eps) $(sed -n 's/.*, \([0-9]*\) snapshots,.*/\1/p' "$LOG") $(field 'slowest snapshot')s $(field 'snapshot time total')s")
      fi
      rates+=("$(eps)")
    done
    median_line=$(printf '%s\n' "${details[@]}" | sort -n | sed -n 2p)
    median=$(echo "$median_line" | cut -d' ' -f1)
    if [ "$every" = none ]; then none=$median; vs="(baseline)"; label="none"; else vs=$(pct "$median" "$none"); label="every $every events"; fi
    printf '| %s | %s | %s | %s | **%s** | %s | %s | %s | %s |\n' "$label" "${rates[0]}" "${rates[1]}" "${rates[2]}" "$median" "$vs" \
      "$(echo "$median_line" | cut -d' ' -f2)" "$(echo "$median_line" | cut -d' ' -f3)" "$(echo "$median_line" | cut -d' ' -f4)"
  done
  echo
fi

# ---- memory -------------------------------------------------------------------------

if want memory; then
  echo "## memory"
  echo
  echo "- command: /usr/bin/time -l streamkit --in \$INPUT --out \$OUT --lateness-s 60 --threads P; maximum resident set size in bytes"
  echo "- peak dedup ids and peak open windows are summed over workers, from the median run's summary"
  echo
  echo "- the --queue-depth 256 rows exist to separate the engine's state (dedup set, open windows) from the reader-to-worker queues: the default 4096 slots x 128 events x 52 bytes = 27 MB per worker is touched only once the reader has run that far ahead"
  echo
  echo "| input | threads | queue depth | run 1 | run 2 | run 3 | median max RSS (bytes) | MiB | peak dedup ids | peak open windows |"
  echo "|---|---|---|---|---|---|---|---|---|---|"
  for input in 1M 10M; do
    if [ "$input" = 1M ]; then f=$IN_1M; else f=$IN; fi
    for cfg in "1 4096" "8 4096" "8 256"; do
      P=${cfg% *}; depth=${cfg#* }
      rss=(); details=()
      for _ in $(seq "$RUNS"); do
        /usr/bin/time -l "$B/bin/streamkit" --in "$f" --out "$OUT" --lateness-s 60 --threads "$P" --queue-depth "$depth" ${EXTRA[@]+"${EXTRA[@]}"} 2>"$LOG" >/dev/null
        r=$(awk '/maximum resident set size/ { print $1 }' "$LOG")
        rss+=("$r")
        details+=("$r $(field 'peak ids') $(field 'peak open windows')")
      done
      median_line=$(printf '%s\n' "${details[@]}" | sort -n | sed -n 2p)
      median=$(echo "$median_line" | cut -d' ' -f1)
      mib=$(awk -v b="$median" 'BEGIN { printf "%.1f", b / 1048576 }')
      printf '| %s events | %s | %s | %s | %s | %s | **%s** | %s | %s | %s |\n' "$input" "$P" "$depth" "${rss[0]}" "${rss[1]}" "${rss[2]}" "$median" "$mib" \
        "$(echo "$median_line" | cut -d' ' -f2)" "$(echo "$median_line" | cut -d' ' -f3)"
    done
  done
  echo
fi

# ---- latency ------------------------------------------------------------------------

if want latency; then
  echo "## emit latency under paced replay"
  echo
  echo "- command: gen --n $((LAT_RATE * LAT_SECS)) --rate $LAT_RATE --pace --seed $SEED --out - | streamkit --in - --out - --window-s 1 --lateness-s 2 --threads P --latency-stats | cat > /dev/null"
  echo "- metric (latency.h, NOTES.md m5 and Review fixes): per window closed by the watermark, wall time from read(2) returning the block that holds the event which pushed this worker's watermark past the window's end, to that window's row having been written (fwrite + fflush) to stdout, a pipe read by cat; nearest-rank percentiles over all such windows; windows closed by end of input are not counted"
  echo "- not included: the 2 s of allowed lateness (event-time), gen's own 1 MiB output buffering before the bytes reach the pipe, the consumer's own read"
  echo "- achieved rate = events / wall as reported by streamkit; input reads = read(2) calls that returned data, i.e. how many arrivals the stamps are taken at"
  echo
  echo "| threads | run | achieved events/s | watermark-closed windows | input reads (events/read) | min us | p50 us | p99 us | max us |"
  echo "|---|---|---|---|---|---|---|---|---|"
  for P in 1 8; do
    p50s=(); p99s=()
    for run in 1 2 3; do
      lat_run "$P"
      line=$(grep 'emit latency over' "$LOG")
      wins=$(echo "$line" | sed -n 's/.*over \([0-9]*\) watermark.*/\1/p')
      mn=$(echo "$line"   | sed -n 's/.*min \([0-9]*\) us.*/\1/p')
      p50=$(echo "$line"  | sed -n 's/.*p50 \([0-9]*\) us.*/\1/p')
      p99=$(echo "$line"  | sed -n 's/.*p99 \([0-9]*\) us.*/\1/p')
      mx=$(echo "$line"   | sed -n 's/.*max \([0-9]*\) us.*/\1/p')
      reads=$(echo "$line" | sed -n 's/.*input reads \([0-9]*\).*/\1/p')
      per_read=$(echo "$line" | sed -n 's/.*mean \([0-9]*\) events.*/\1/p')
      p50s+=("$p50"); p99s+=("$p99")
      printf '| %s | %s | %s | %s | %s (%s) | %s | %s | %s | %s |\n' "$P" "$run" "$(eps)" "$wins" "$reads" "$per_read" "$mn" "$p50" "$p99" "$mx"
    done
    printf '| %s | **median** | | | | | **%s** | **%s** | |\n' "$P" "$(median3 "${p50s[@]}")" "$(median3 "${p99s[@]}")"
  done
  echo
fi
