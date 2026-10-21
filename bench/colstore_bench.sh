#!/usr/bin/env bash
# colstore benchmark harness. Every colstore number in colstore/BENCHMARKS.md
# and colstore/README.md comes from this script.
#
#   bench/colstore_bench.sh [build_dir] [n_events] [work_dir] [stage]
#
#   build_dir  default build;  n_events default 10000000;
#   work_dir   default /private/tmp/adstack-cs: the generated .ads file is
#              CACHED there (gen is deterministic for seed 1, and 10M events
#              take a few seconds to write), everything else is deleted on exit;
#   stage      all (default) or m5: only the SQLite comparison.
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
# m5: the same rows loaded into SQLite by build/bin/colstore_sqlite (one
# rowid table, indexes on (event_time_us), (country, event_type),
# (campaign_id), ANALYZE; PRAGMAs printed), load time (3 runs, median) for
# both engines, storage sizes, Q1-Q3 on both (3 runs, median; SQLite with
# the planner's plan and with NOT INDEXED, the better one counts), the
# answers diffed, the zone-map skip ratio for Q2, and markdown tables.
# Skipped with a message when colstore_sqlite was not built (no sqlite3).
set -euo pipefail
B=${1:-build}
N=${2:-10000000}
W=${3:-/private/tmp/adstack-cs}
STAGE=${4:-all}
mkdir -p "$W"
ADS="$W/bench_events_${N}.ads"
COL_PLAIN="$W/bench_events_plain.col"
COL="$W/bench_events.col"
SQL="$W/bench_events.sqlite"
OUT="$W/bench_answers"
mkdir -p "$OUT"
cleanup() { rm -f "$COL_PLAIN" "$COL" "$SQL" "$OUT"/*.csv; rmdir "$OUT" 2>/dev/null || true; }
trap cleanup EXIT
size() { stat -f%z "$1" 2>/dev/null || stat -c%s "$1"; }

echo "== machine"
echo "cpu:   $(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo | cut -d: -f2)"
echo "cores: $(sysctl -n hw.ncpu 2>/dev/null || nproc)"
echo "os:    $(uname -sr)"
echo "build: $B"

# 8-byte header + 52 bytes per event (common/event.h); a file of the wrong
# size is a different N or a torn write, and is regenerated.
if [ -f "$ADS" ] && [ "$(size "$ADS")" -eq $((8 + 52 * N)) ]; then
  echo "== reuse cached $ADS ($N events, seed 1)"
else
  echo "== generate $N events (seed 1) -> $ADS"
  "$B/bin/gen" --n "$N" --seed 1 --out "$ADS"
fi

if [ "$STAGE" != "m5" ]; then

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

fi  # STAGE != m5

# ---- m5: SQLite comparison ---------------------------------------------------
# Same rows, same three queries. SQLite is a row store with B-tree indexes;
# colstore is a column store with zone maps. Each query runs on SQLite twice:
# as the planner chooses (with the indexes and ANALYZE statistics) and with
# `NOT INDEXED` (a plain table scan), and the faster of the two is the SQLite
# number in the table, because the point is the storage model, not a planner
# decision. colstore is shown at 1 thread (SQLite is single-threaded, so this
# is the like-for-like row) and at 8.
echo "== m5 SQLite comparison"
if [ ! -x "$B/bin/colstore_sqlite" ]; then
  echo "skipped: $B/bin/colstore_sqlite was not built (cmake did not find sqlite3)"
  exit 0
fi
T0=${T0:-1767225900000000}
Q1W="country=840 AND event_type=1";                      Q1A="count()"
Q2W="event_time_us BETWEEN $T0 AND $((T0 + 3600000000))"; Q2G=campaign_id; Q2A="sum(cost_micros),count()"
Q3G=campaign_id
Q3A="count_if(event_type=0) AS impressions,count_if(event_type=1) AS clicks,count_if(event_type=2) AS conversions,sum(cost_micros) AS cost_micros"
# Output column names are aliased to colstore's defaults so the CSVs diff.
S1="SELECT COUNT(*) AS count FROM events WHERE country = 840 AND event_type = 1"
S2="SELECT campaign_id, SUM(cost_micros) AS sum_cost_micros, COUNT(*) AS count FROM events WHERE event_time_us BETWEEN $T0 AND $((T0 + 3600000000)) GROUP BY campaign_id"
S3="SELECT campaign_id, COUNT(*) FILTER (WHERE event_type = 0) AS impressions, COUNT(*) FILTER (WHERE event_type = 1) AS clicks, COUNT(*) FILTER (WHERE event_type = 2) AS conversions, SUM(cost_micros) AS cost_micros FROM events GROUP BY campaign_id"

# median of three space-separated numbers
med3() { echo "$1" | tr ' ' '\n' | grep . | sort -g | sed -n 2p; }

echo "-- colstore ingest (auto), 3 runs"
cs_ingest=""
for i in 1 2 3; do
  t=$("$B/bin/colstore" ingest --in "$ADS" --out "$COL" 2>&1 | sed -n 's/.*rows in \([0-9.]*\) s.*/\1/p')
  cs_ingest="$cs_ingest $t"
done
cs_ingest_med=$(med3 "$cs_ingest")
echo "   seconds:$cs_ingest | median $cs_ingest_med"

echo "-- sqlite load (table, 3 indexes, ANALYZE), 3 runs; settings printed once"
sq_insert=""; sq_index=""; sq_analyze=""; sq_total=""
for i in 1 2 3; do
  log=$("$B/bin/colstore_sqlite" load --in "$ADS" --out "$SQL" 2>&1)
  [ "$i" = 1 ] && echo "$log" | sed 's/^/   /'
  line=$(echo "$log" | grep '^sqlite load:')
  sq_insert="$sq_insert $(echo "$line" | sed 's/.*insert \([0-9.]*\) s.*/\1/')"
  sq_index="$sq_index $(echo "$line" | sed 's/.*indexes \([0-9.]*\) s.*/\1/')"
  sq_analyze="$sq_analyze $(echo "$line" | sed 's/.*analyze \([0-9.]*\) s.*/\1/')"
  sq_total="$sq_total $(echo "$line" | sed 's/.*total \([0-9.]*\) s.*/\1/')"
done
sq_version=$(echo "$log" | sed -n 's/.*sqlite \([0-9.]*\)$/\1/p')
echo "   insert:$sq_insert | median $(med3 "$sq_insert")"
echo "   indexes:$sq_index | median $(med3 "$sq_index")"
echo "   analyze:$sq_analyze | median $(med3 "$sq_analyze")"
echo "   total:$sq_total | median $(med3 "$sq_total")"
sq_table_bytes=$(echo "$log" | sed -n 's/.*table only \([0-9]*\) bytes.*/\1/p')
sq_bytes=$(size "$SQL")
ads_bytes=$(size "$ADS")
col_bytes=$(size "$COL")

echo "-- answers: colstore q (1 thread) vs sqlite, CSVs sorted and diffed"
"$B/bin/colstore" q --in "$COL" --where "$Q1W" --agg "$Q1A" > "$OUT/cs_q1.csv"
"$B/bin/colstore" q --in "$COL" --where "$Q2W" --group "$Q2G" --agg "$Q2A" > "$OUT/cs_q2.csv"
"$B/bin/colstore" q --in "$COL" --group "$Q3G" --agg "$Q3A" > "$OUT/cs_q3.csv"
"$B/bin/colstore_sqlite" query --db "$SQL" --sql "$S1" --runs 1 > "$OUT/sq_q1.csv" 2>/dev/null
"$B/bin/colstore_sqlite" query --db "$SQL" --sql "$S2" --runs 1 > "$OUT/sq_q2.csv" 2>/dev/null
"$B/bin/colstore_sqlite" query --db "$SQL" --sql "$S3" --runs 1 > "$OUT/sq_q3.csv" 2>/dev/null
answers_ok=yes
for q in 1 2 3; do
  if diff <(LC_ALL=C sort "$OUT/cs_q$q.csv") <(LC_ALL=C sort "$OUT/sq_q$q.csv") > /dev/null; then
    echo "   Q$q: identical ($(($(wc -l < "$OUT/cs_q$q.csv") - 1)) rows)"
  else
    echo "   Q$q: DIFFER"; answers_ok=no
  fi
done

# SQLite: median of 3 same-connection runs after one untimed run; prints the plan.
sq_time() {  # $1 = sql; echoes "median" and prints plan + runs to stdout as a side line
  local log
  log=$("$B/bin/colstore_sqlite" query --db "$SQL" --sql "$1" --runs 3 --plan 2>&1 >/dev/null)
  echo "$log" | grep -E 'plan:|sqlite query' | sed 's/^ *//; s/^/      /' >&2
  echo "$log" | sed -n 's/.*| median \([0-9.]*\) s.*/\1/p'
}
# colstore: median of 3 `q --stats` totals (scan + aggregate + sort, no CSV).
cs_time() {
  local out=""
  for i in 1 2 3; do
    out="$out $("$B/bin/colstore" q --in "$COL" --stats "$@" 2>&1 >/dev/null | grep '^output:' | sed 's/.*total //; s/ s$//')"
  done
  echo "      colstore runs:$out" >&2
  med3 "$out"
}
better() { awk -v a="$1" -v b="$2" 'BEGIN{print (a < b) ? a : b}'; }
# a/b; two decimals below 1 so "SQLite faster" is still a number, n/a when b is 0.000 (tiny inputs)
ratio() { awk -v a="$1" -v b="$2" 'BEGIN{ if (b <= 0) printf "n/a"; else if (a / b < 1) printf "%.2f", a / b; else printf "%.1f", a / b }'; }

echo "-- query times (3 runs each, median; page cache warm)"
echo "   Q1 sqlite (planner):";    sq1=$(sq_time "$S1")
echo "   Q1 sqlite (NOT INDEXED):"; sq1n=$(sq_time "${S1/FROM events/FROM events NOT INDEXED}")
echo "   Q1 colstore 1 thread:";    cs1=$(cs_time --where "$Q1W" --agg "$Q1A" --threads 1)
echo "   Q1 colstore 8 threads:";   cs1p=$(cs_time --where "$Q1W" --agg "$Q1A" --threads 8)
echo "   Q2 sqlite (planner):";    sq2=$(sq_time "$S2")
echo "   Q2 sqlite (NOT INDEXED):"; sq2n=$(sq_time "${S2/FROM events/FROM events NOT INDEXED}")
echo "   Q2 colstore 1 thread:";    cs2=$(cs_time --where "$Q2W" --group "$Q2G" --agg "$Q2A" --threads 1)
echo "   Q2 colstore 8 threads:";   cs2p=$(cs_time --where "$Q2W" --group "$Q2G" --agg "$Q2A" --threads 8)
echo "   Q3 sqlite (planner):";    sq3=$(sq_time "$S3")
echo "   Q3 sqlite (NOT INDEXED):"; sq3n=$(sq_time "${S3/FROM events/FROM events NOT INDEXED}")
echo "   Q3 colstore 1 thread:";    cs3=$(cs_time --group "$Q3G" --agg "$Q3A" --threads 1)
echo "   Q3 colstore 8 threads:";   cs3p=$(cs_time --group "$Q3G" --agg "$Q3A" --threads 8)

zone=$("$B/bin/colstore" q --in "$COL" --where "$Q2W" --group "$Q2G" --agg "$Q2A" --stats 2>&1 >/dev/null | grep 'row groups:')
zone_total=$(echo "$zone" | sed 's/.*row groups: \([0-9]*\) total.*/\1/')
zone_skipped=$(echo "$zone" | sed 's/.*total, \([0-9]*\) skipped by zone map.*/\1/')
rows1=$(($(wc -l < "$OUT/cs_q1.csv") - 1)); rows2=$(($(wc -l < "$OUT/cs_q2.csv") - 1)); rows3=$(($(wc -l < "$OUT/cs_q3.csv") - 1))

echo
echo "== m5 results (markdown)"
echo "SQLite $sq_version; $N events; answers identical: $answers_ok"
echo
echo "| storage | bytes | vs .ads |"
echo "|---|---:|---:|"
echo "| .ads (raw 52-byte rows) | $ads_bytes | 1.00x |"
echo "| .col (colstore, auto encodings) | $col_bytes | $(awk -v a="$ads_bytes" -v b="$col_bytes" 'BEGIN{printf "%.2fx", b/a}') |"
echo "| .sqlite table only | $sq_table_bytes | $(awk -v a="$ads_bytes" -v b="$sq_table_bytes" 'BEGIN{printf "%.2fx", b/a}') |"
echo "| .sqlite with 3 indexes + ANALYZE | $sq_bytes | $(awk -v a="$ads_bytes" -v b="$sq_bytes" 'BEGIN{printf "%.2fx", b/a}') |"
echo
echo "| load (median of 3) | seconds |"
echo "|---|---:|"
echo "| colstore ingest (encode per chunk, write .col) | $cs_ingest_med |"
echo "| SQLite INSERT (one transaction, prepared statement) | $(med3 "$sq_insert") |"
echo "| SQLite 3 x CREATE INDEX | $(med3 "$sq_index") |"
echo "| SQLite ANALYZE | $(med3 "$sq_analyze") |"
echo "| SQLite total | $(med3 "$sq_total") |"
echo
echo "| query | rows out | SQLite planner | SQLite NOT INDEXED | colstore 1 thread | colstore 8 threads | colstore 1 thread vs best SQLite |"
echo "|---|---:|---:|---:|---:|---:|---:|"
echo "| Q1 point filter + count | $rows1 | $sq1 s | $sq1n s | $cs1 s | $cs1p s | $(ratio "$(better "$sq1" "$sq1n")" "$cs1")x |"
echo "| Q2 1h range, GROUP BY campaign_id | $rows2 | $sq2 s | $sq2n s | $cs2 s | $cs2p s | $(ratio "$(better "$sq2" "$sq2n")" "$cs2")x |"
echo "| Q3 full GROUP BY, 4 aggregates | $rows3 | $sq3 s | $sq3n s | $cs3 s | $cs3p s | $(ratio "$(better "$sq3" "$sq3n")" "$cs3")x |"
echo
echo "Q2 zone map: $zone_skipped of $zone_total row groups skipped ($(awk -v a="$zone_skipped" -v b="$zone_total" 'BEGIN{printf "%.1f", 100*a/b}')%)"
[ "$answers_ok" = yes ]
