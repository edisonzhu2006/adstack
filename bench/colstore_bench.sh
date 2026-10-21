#!/usr/bin/env bash
# colstore benchmark harness. Every colstore number in docs/BENCHMARKS.md and
# colstore/README.md comes from this script.
#
#   bench/colstore_bench.sh [build_dir] [n_events] [work_dir]
#
# m1: generate n events, ingest to .col, verify every field against the
# source, print ingest time (best of 3, input in page cache) and file sizes.
# m2: ingest twice, once with --encoding plain (the baseline) and once with
# the default per-chunk encoding selection; verify both; print both sizes,
# both ingest times, and the per-column PLAIN vs encoded table.
# m3: four scan predicates, each verified against a brute-force pass over
# the .ads file (row count + column sums), then timed with --count (best of
# 3 printed as all 3); zone-map skip counts for 10 s / 60 s / 300 s time
# ranges; batch-size sweep (--vector 1 .. 65536) on a full-scan predicate;
# dictionary-code vs value filtering (auto file vs plain file).
# m4: the three benchmark queries (Q1 point filter + count, Q2 time-range
# GROUP BY, Q3 full GROUP BY with conditional aggregates), each recomputed
# from the .ads file with `q --check` first, then timed (3 runs, median)
# at 1 / 2 / 4 / 8 threads; sort cost of ORDER BY with and without LIMIT;
# batch size with the aggregate attached; a cold-cache run when `sudo -n
# purge` is available (it is not without a password, and then it says so).
# m5 appends the SQLite comparison.
set -euo pipefail
B=${1:-build}
N=${2:-10000000}
W=${3:-$(mktemp -d)}
mkdir -p "$W"
ADS="$W/bench_events.ads"
COL_PLAIN="$W/bench_events_plain.col"
COL="$W/bench_events.col"
cleanup() { rm -f "$ADS" "$COL_PLAIN" "$COL"; }
trap cleanup EXIT

echo "== machine"
echo "cpu:   $(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo | cut -d: -f2)"
echo "cores: $(sysctl -n hw.ncpu 2>/dev/null || nproc)"
echo "os:    $(uname -sr)"
echo "build: $B"

echo "== generate $N events (seed 1)"
"$B/bin/gen" --n "$N" --seed 1 --out "$ADS"

echo "== ingest --encoding plain (3 runs; take the best)"
for i in 1 2 3; do
  "$B/bin/colstore" ingest --in "$ADS" --out "$COL_PLAIN" --encoding plain 2>&1 | head -1
done

echo "== ingest --encoding auto (3 runs; take the best)"
for i in 1 2 3; do
  "$B/bin/colstore" ingest --in "$ADS" --out "$COL" 2>&1 | head -1
done

echo "== verify every field (plain, then auto)"
"$B/bin/colstore" verify --col "$COL_PLAIN" --ads "$ADS"
"$B/bin/colstore" verify --col "$COL" --ads "$ADS"

echo "== sizes"
size() { stat -f%z "$1" 2>/dev/null || stat -c%s "$1"; }
ads_bytes=$(size "$ADS")
plain_bytes=$(size "$COL_PLAIN")
col_bytes=$(size "$COL")
echo "ads bytes:        $ads_bytes"
echo "col plain bytes:  $plain_bytes"
echo "col auto bytes:   $col_bytes"
echo "ratio plain/ads:  $(awk -v a="$ads_bytes" -v c="$plain_bytes" 'BEGIN{printf "%.4f", c/a}')"
echo "ratio auto/ads:   $(awk -v a="$ads_bytes" -v c="$col_bytes" 'BEGIN{printf "%.4f", c/a}')"
echo "compression ads/auto: $(awk -v a="$ads_bytes" -v c="$col_bytes" 'BEGIN{printf "%.2fx", a/c}')"

echo "== per-column layout (auto)"
"$B/bin/colstore" info --in "$COL"

# ---- m3: scan / filter / project -------------------------------------------
# gen's clock starts at 2026-01-01T00:00:00Z = 1767225600000000 us and runs
# at 10,000 events/s, so +300 s is 1767225900000000 and the file covers
# N / 10,000 seconds. Predicates are chosen to exercise: a selective sorted
# range (zone maps skip), the same plus a dictionary equality, a full scan
# with one dictionary and one value predicate, and a narrow range on an
# unsorted column (zone maps cannot skip).
T0=1767225900000000
P1="event_time_us BETWEEN $T0 AND $((T0 + 60000000))"
P2="country=840 AND event_time_us BETWEEN $T0 AND $((T0 + 60000000))"
P3="event_type=1 AND cost_micros >= 1000"
P4="campaign_id BETWEEN 50000 AND 50100"
COLS=campaign_id,cost_micros

echo "== m3 verify: scan vs brute force over .ads (rows and column sums must match)"
for P in "$P1" "$P2" "$P3" "$P4"; do
  echo "-- where $P"
  "$B/bin/colstore" verify --col "$COL" --ads "$ADS" --where "$P" --cols "$COLS" 2>&1 | grep -E "rows scan|sum\(|verify --where: (PASS|FAIL)"
done

# Prints the scan-portion time of three runs (--count: no row formatting).
scan3() {
  local f=$1; shift
  local out=""
  for i in 1 2 3; do
    out="$out $("$B/bin/colstore" scan --in "$f" --count --stats "$@" 2>&1 >/dev/null | grep 'time:' | awk '{print $2}')"
  done
  echo "$out"
}

echo "== m3 scan times, s (3 runs), --count, cols $COLS"
for P in "$P1" "$P2" "$P3" "$P4"; do
  "$B/bin/colstore" scan --in "$COL" --count --stats --where "$P" --cols "$COLS" 2>&1 >/dev/null | grep -E "row groups|^rows" | tr '\n' ' '
  echo
  echo "   times:$(scan3 "$COL" --where "$P" --cols "$COLS")   <= $P"
done
echo "decode only (no where), cols $COLS:$(scan3 "$COL" --cols "$COLS")"
echo "decode only (no where), cols event_type,$COLS:$(scan3 "$COL" --cols event_type,$COLS)"

echo "== m3 zone-map skipping vs range width (sorted-by-ingest event_time_us, 5% late by up to 30 s)"
for D in 10 60 300; do
  "$B/bin/colstore" scan --in "$COL" --count --stats --where "event_time_us BETWEEN $T0 AND $((T0 + D * 1000000))" \
    --cols campaign_id 2>&1 >/dev/null | grep "row groups" | sed "s/^scan: /range ${D}s: /"
done

echo "== m3 batch size sweep on P3 (full scan), times s (3 runs)"
for V in 1 16 128 1024 8192 65536; do
  echo "vector=$V:$(scan3 "$COL" --where "$P3" --cols "$COLS" --vector "$V")"
done

echo "== m3 dictionary codes vs values: same predicate on the auto file (DICT/RLE_DICT chunks) and the plain file"
echo "country=840     auto :$(scan3 "$COL" --where "country=840" --cols country)"
echo "country=840     plain:$(scan3 "$COL_PLAIN" --where "country=840" --cols country)"
echo "no where        auto :$(scan3 "$COL" --cols country)"
echo "no where        plain:$(scan3 "$COL_PLAIN" --cols country)"
echo "event_type=1    auto :$(scan3 "$COL" --where "event_type=1" --cols event_type)"
echo "event_type=1    plain:$(scan3 "$COL_PLAIN" --where "event_type=1" --cols event_type)"

# ---- m4: hash aggregate, sort, limit ---------------------------------------
# The file spans N / 10,000 seconds of event time (1,000 s at 10M), so the
# one-hour range of Q2 from T0 = +300 s covers the last 700 s: the zone map
# skips the first ~45 of 153 groups and the rest is a 7M-row aggregate.
Q1W="country=840 AND event_type=1";                      Q1A="count()"
Q2W="event_time_us BETWEEN $T0 AND $((T0 + 3600000000))"; Q2G=campaign_id; Q2A="sum(cost_micros),count()"
Q3G=campaign_id
Q3A="count_if(event_type=0) AS impressions,count_if(event_type=1) AS clicks,count_if(event_type=2) AS conversions,sum(cost_micros) AS cost_micros"

echo "== m4 check: each query recomputed from .ads with a std::map and diffed (threads 1, then 8 with ORDER BY / LIMIT)"
"$B/bin/colstore" q --in "$COL" --where "$Q1W" --agg "$Q1A" --check "$ADS" 2>&1 >/dev/null | sed 's/^/Q1 /'
"$B/bin/colstore" q --in "$COL" --where "$Q2W" --group "$Q2G" --agg "$Q2A" --check "$ADS" 2>&1 >/dev/null | sed 's/^/Q2 /'
"$B/bin/colstore" q --in "$COL" --group "$Q3G" --agg "$Q3A" --check "$ADS" 2>&1 >/dev/null | sed 's/^/Q3 /'
"$B/bin/colstore" q --in "$COL" --where "$Q2W" --group "$Q2G" --agg "$Q2A" --order "sum_cost_micros desc" --limit 20 --threads 8 --check "$ADS" 2>&1 >/dev/null | sed 's/^/Q2 top-20 t=8 /'
"$B/bin/colstore" q --in "$COL" --group "$Q3G" --agg "$Q3A" --order "cost_micros desc" --limit 20 --threads 8 --check "$ADS" 2>&1 >/dev/null | sed 's/^/Q3 top-20 t=8 /'

# Total query time (scan + aggregate + sort, no CSV formatting counted) of
# three runs, printed as "a b c | median".
q3() {
  local out=""
  for i in 1 2 3; do
    out="$out $("$B/bin/colstore" q --in "$COL" --stats "$@" 2>&1 >/dev/null | grep '^output:' | sed 's/.*total //; s/ s$//')"
  done
  local med
  med=$(echo "$out" | tr ' ' '\n' | grep . | sort -g | sed -n 2p)
  echo "$out | median $med"
}

echo "== m4 query times, s (3 runs | median), warm page cache (file just written and read)"
for T in 1 2 4 8; do
  echo "threads=$T"
  echo "  Q1 point filter + count:            $(q3 --where "$Q1W" --agg "$Q1A" --threads "$T")"
  echo "  Q2 1h range, group campaign_id:     $(q3 --where "$Q2W" --group "$Q2G" --agg "$Q2A" --threads "$T")"
  echo "  Q3 full group by, 4 aggregates:     $(q3 --group "$Q3G" --agg "$Q3A" --threads "$T")"
done
echo "-- Q3 stats at threads=1 (groups, table, probes per row)"
"$B/bin/colstore" q --in "$COL" --group "$Q3G" --agg "$Q3A" --stats 2>&1 >/dev/null | grep -E "^aggregate|^output"
echo "-- Q2 stats at threads=1"
"$B/bin/colstore" q --in "$COL" --where "$Q2W" --group "$Q2G" --agg "$Q2A" --stats 2>&1 >/dev/null | grep -E "row groups|^aggregate|^output"

echo "== m4 ORDER BY on the 100K-group Q3 result: full sort vs LIMIT 20 (partial sort), sort seconds (3 runs)"
for L in 0 20; do
  out=""
  for i in 1 2 3; do
    out="$out $("$B/bin/colstore" q --in "$COL" --group "$Q3G" --agg "$Q3A" --order "cost_micros desc" --limit "$L" --stats 2>&1 >/dev/null | grep '^output:' | sed 's/.*sort //; s/ s, total.*//')"
  done
  echo "limit $L:$out"
done

echo "== m4 batch size with the aggregate attached (Q3, threads 1), total s (3 runs | median)"
for V in 1024 8192 65536; do
  echo "vector=$V: $(q3 --group "$Q3G" --agg "$Q3A" --vector "$V")"
done

echo "== m4 cold page cache"
if sudo -n purge 2>/dev/null; then
  echo "purged; one run each (threads 1)"
  echo "  Q1: $("$B/bin/colstore" q --in "$COL" --where "$Q1W" --agg "$Q1A" --stats 2>&1 >/dev/null | grep '^output:')"
  sudo -n purge
  echo "  Q2: $("$B/bin/colstore" q --in "$COL" --where "$Q2W" --group "$Q2G" --agg "$Q2A" --stats 2>&1 >/dev/null | grep '^output:')"
  sudo -n purge
  echo "  Q3: $("$B/bin/colstore" q --in "$COL" --group "$Q3G" --agg "$Q3A" --stats 2>&1 >/dev/null | grep '^output:')"
else
  echo "not measured: evicting the page cache needs 'sudo purge' and no password-less sudo is available;"
  echo "all times above are with the .col file in the page cache (it was written moments earlier)."
fi
