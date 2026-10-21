# colstore

A columnar analytics store for adstack events, written in C++20 with the
standard library only. Ingests the `.ads` event files produced by `gen`,
streamkit's adapters, or any other adstack tool into a `.col` file: row
groups of 65,536 rows, one chunk per column, an encoding chosen per chunk
(PLAIN, DICT, DELTA_BP, RLE_DICT), footer with per-chunk metadata including
min/max. Queries run as a vectorized scan (batches of 1024 rows, selection
vectors, zone-map row-group skipping, dictionary predicates on codes) into
a hash aggregate (GROUP BY with count / sum / min / max and conditional
count_if / sum_if, open-addressing table, optionally one thread per set of
row groups) followed by ORDER BY / LIMIT. Every query can be re-derived
from the source `.ads` file with one flag. The file format is specified in
[FORMAT.md](FORMAT.md); the design decisions are in [NOTES.md](NOTES.md).

## Build

From the repository root (cmake and ninja live in
`~/.withai/runtimes/python/bin/` on this machine):

```
cmake -S . -B build-cs -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-cs
ctest --test-dir build-cs -R "Footer|RowGroup|Fixture|Codecs|Selector|BitPack|BitWidth|Dictionary|Predicate|ZoneMap|WhereParser|Exec|GroupTable|KeyPacker|Agg|HashAggregate|OrderParser|SortLimit|QueryFiles"
```

Targets: `colstore` (CLI), `colstore_core` (static library: format,
encodings, writer, reader), `colstore_tests`.

## Usage

```
# .ads -> .col; the writer picks an encoding per chunk unless told not to
build-cs/bin/colstore ingest --in events.ads --out events.col [--row-group 65536] [--encoding auto|plain]

# what is in the file: rows, row groups, per-column PLAIN vs stored bytes, encodings used
build-cs/bin/colstore info --in events.col [--groups]

# first n values of one column (decodes whatever encoding each chunk has)
build-cs/bin/colstore dump --in events.col --col campaign_id --n 20

# prove the round trip: every field of every row equal
build-cs/bin/colstore verify --col events.col --ads events.ads

# query: scan -> filter -> project. --where is "col op const" terms ANDed
# (ops < <= = >= > and BETWEEN a AND b); --cols picks the output columns
build-cs/bin/colstore scan --in events.col \
  --where "country=840 AND event_time_us BETWEEN 1767225900000000 AND 1767225960000000" \
  --cols campaign_id,cost_micros --limit 10 --stats
#   --limit 0   all rows      --count   print only the number of passing rows
#   --stats     row groups skipped by zone map / by dictionary, rows decoded, chunks read, time
#   --vector N  batch size (default 1024; 1 = row-at-a-time, for the measurement in NOTES.md)

# prove a query: the same predicate through the scan and through a brute-force
# loop over the .ads file; row counts and per-column sums must agree
build-cs/bin/colstore verify --col events.col --ads events.ads \
  --where "event_type=1 AND cost_micros >= 1000" --cols campaign_id,cost_micros

# aggregate query: scan -> filter -> hash aggregate -> order/limit -> CSV on stdout
build-cs/bin/colstore q --in events.col \
  --where "event_time_us BETWEEN 1767225900000000 AND 1767229500000000" \
  --group campaign_id --agg "sum(cost_micros),count()" \
  --order "sum_cost_micros desc" --limit 20 --threads 4 --stats
#   --group a,b       group columns (omit for one global row); any columns whose widths fit in 128 bits
#   --agg "..."       count(), sum(col), min(col), max(col), count_if(cond), sum_if(col, cond),
#                     each optionally "AS name"; cond is one --where term, e.g. event_type=1
#                     default names: count, sum_<col>, min_<col>, max_<col>, count_if_<col>_<op>_<v>, ...
#   --order "a desc, b"  output column names; group columns are appended as a tiebreak,
#                     so the output is identical at any --threads
#   --limit N         0 = all rows (default). With no --order the rows come out in first-seen order
#   --threads T       row groups t, t+T, ... per thread, one hash table each, merged at the end
#   --stats           groups skipped, rows, groups, table slots and doublings, probes per row, times
#   --check events.ads  recompute the whole GROUP BY from the .ads file (std::map, no shared code
#                     with the engine) and diff; with --order/--limit also diff the emitted rows
#                     against a plain std::stable_sort. Exit 1 on any difference.

# the three benchmark queries (bench/colstore_bench.sh runs them with --check, then times them)
build-cs/bin/colstore q --in events.col --where "country=840 AND event_type=1" --agg "count()"
build-cs/bin/colstore q --in events.col --where "event_time_us BETWEEN $T0 AND $((T0+3600000000))" \
  --group campaign_id --agg "sum(cost_micros),count()"
build-cs/bin/colstore q --in events.col --group campaign_id \
  --agg "count_if(event_type=0) AS impressions,count_if(event_type=1) AS clicks,count_if(event_type=2) AS conversions,sum(cost_micros) AS cost_micros"
```

Column names: `event_id event_time_us ingest_time_us event_type
advertiser_id campaign_id video_id country device cost_micros`.

## Reader API

`src/reader.h`:

```cpp
colstore::ColReader r("events.col");
for (size_t g = 0; g < r.num_row_groups(); ++g) {
  std::vector<uint32_t> campaigns = r.read_column<uint32_t>(g, colstore::ColumnId::CampaignId);
  std::vector<int64_t> costs = r.read_column<int64_t>(g, colstore::ColumnId::CostMicros);
  const colstore::ChunkMeta& m = r.chunk(g, colstore::ColumnId::EventTimeUs);  // offset, length, encoding, min, max
}
```

`read_column<T>` throws if `T` does not have the column's width; it
decodes any encoding. `read_column_bits` returns 64-bit patterns for
callers that pick the column at run time. `read_events(g)` rebuilds whole
`adstack::Event`s and exists for `verify`.

## Query API

`src/exec.h` and `src/predicate.h`:

```cpp
colstore::ColReader r("events.col");
colstore::Conjunction where = colstore::parse_where("country=840 AND cost_micros > 1000");
std::vector<colstore::ColumnId> cols = {colstore::ColumnId::CampaignId, colstore::ColumnId::CostMicros};
colstore::QueryStats q = colstore::run_query(r, where, cols, [&](const colstore::Batch& b) {
  for (uint32_t k = 0; k < b.selected(); ++k) {           // selection vector: indices of passing rows
    uint64_t campaign = b.cols[size_t(cols[0])].at(b.row(k));
    int64_t cost = colstore::as_signed(b.cols[size_t(cols[1])].at(b.row(k)));
  }
  return true;  // false stops early (LIMIT)
});
// q.scan.row_groups_skipped_zone, q.scan.chunks_decoded, q.rows_passed, q.seconds
```

`Scanner` walks row groups, consults the zone maps (per-chunk min/max)
before reading anything, decodes only the columns the query touches, and
hands out `Batch`es of up to 1024 rows whose column vectors point into the
decoded row group. `apply_filter` fills the selection vector branch-free;
`gather` densifies one column of the survivors. Dictionary-coded chunks
are kept as codes and filtered on a code range computed once per row
group.

## Aggregate API

`src/query.h`, `src/aggregate.h`, `src/sort.h`:

```cpp
colstore::QueryPlan plan;
plan.where = colstore::parse_where("event_type=1");
plan.group = {colstore::ColumnId::CampaignId};
plan.aggs = colstore::parse_aggs("sum(cost_micros) AS cost, count()");
plan.order = colstore::parse_order("cost desc", colstore::output_columns(plan.group, plan.aggs));
plan.limit = 20;
plan.threads = 4;
colstore::QueryResult r = colstore::run_aggregate_query("events.col", plan);
for (uint32_t row : r.rows) {                       // output order
  uint32_t campaign = static_cast<uint32_t>(r.table.at(0, row));
  int64_t cost = colstore::as_signed(r.table.at(1, row));
}
colstore::write_csv(stdout, r);
colstore::CheckReport c = colstore::check_against_ads("events.ads", plan, r);  // c.ok, c.detail
```

`HashAggregate` is the operator: `consume(const Batch&)` reads the
selected rows, `merge(other)` folds another thread's table in, `finish()`
returns a `ResultTable` (one row per group, group columns then aggregates,
raw 64-bit patterns typed by `schema`). `GroupTable` (`src/hash_table.h`)
is the open-addressing table underneath, mapping a packed 128-bit
`GroupKey` to a dense group index; `KeyPacker` packs the group columns
into that key. `sort_limit()` returns the output permutation.

## Encodings

Chosen per chunk by `encode_smallest()` in `src/encoding.cpp`: every
candidate is actually encoded and the smallest kept (ties keep the earlier
of PLAIN, DELTA_BP, DICT, RLE_DICT). The dictionary codecs are skipped
when a running lower bound during the distinct-value scan proves they
cannot win, which is what keeps ingest fast on high-cardinality columns;
a test checks the result equals trying all four.

| encoding | for | layout (details in FORMAT.md) |
|---|---|---|
| PLAIN | anything, fallback | values at natural width |
| DICT | low cardinality (`country`, `device`) | sorted dictionary + bit-packed codes |
| DELTA_BP | near-monotone ints (`event_id`, timestamps), any high-cardinality int | blocks of 128: first value, min delta, bit-packed deltas |
| RLE_DICT | long runs (`event_type`) | dictionary + bit-packed run codes and run lengths |

## Layout

```
colstore/
  src/format.h/.cpp    file layout constants, column table, encoding ids, footer encode/decode
  src/bitpack.h        BitWriter / BitReader: n values of w bits in ceil(n*w/8) bytes
  src/encoding.h/.cpp  PLAIN, DICT, DELTA_BP, RLE_DICT codecs; per-chunk selector; min/max
  src/writer.h/.cpp    ColWriter: buffer a row group, encode + write chunks, write footer
  src/reader.h/.cpp    ColReader: open, footer, read + decode chunk by (row group, column)
  src/predicate.h/.cpp Predicate (one [lo, hi] range over an order key), --where parser, zone-map decision
  src/exec.h/.cpp      Scanner (zone maps, per-group decode, 1024-row batches, row-group stride
                       for threads), apply_filter (selection vector), gather (project), run_query, stats
  src/hash_table.h/.cpp GroupTable: open addressing, linear probing, doubling at 1/2 load,
                       128-bit GroupKey -> dense group index
  src/aggregate.h/.cpp KeyPacker (group columns -> GroupKey), --agg parser, HashAggregate
                       (consume / merge / finish), ResultTable
  src/sort.h/.cpp      --order parser, sort_limit (std::sort, or std::partial_sort under LIMIT)
  src/query.h/.cpp     QueryPlan -> threads -> merge -> sort -> QueryResult; CSV; --check brute force
  src/main.cpp         CLI: ingest, info, dump, scan, q, verify
  tests/               GoogleTest: footer, row-group boundaries, chunk addressing,
                       bit packing, codec round trips, corruption, selector,
                       predicates + parser, zone-map boundaries, scan vs brute force,
                       hash table growth / collisions / vs unordered_map, key packing,
                       aggregates on hand-built batches, merge, ORDER BY / LIMIT vs full sort,
                       end-to-end queries vs brute force at 1..17 threads, --check failure modes
  FORMAT.md            byte-level format specification
  NOTES.md             decisions and alternatives
bench/colstore_bench.sh   gen -> ingest plain x3 -> ingest auto x3 -> verify both -> sizes + info
                          -> 4 predicates verified vs brute force -> scan times -> zone-map skip
                          counts vs range width -> batch-size sweep -> codes vs values
                          -> Q1/Q2/Q3 --check -> Q1/Q2/Q3 times at 1/2/4/8 threads -> sort cost
                          with and without LIMIT -> batch size with the aggregate -> cold cache
```

## Numbers (m2)

`bench/colstore_bench.sh build-cs 10000000` on an Apple M5 Pro, macOS
25.4, Apple clang 21, Release, input in page cache. 10,000,000 events from
`gen --seed 1` (1% duplicates, 5% late), 153 row groups of 65,536.

| metric | PLAIN (`--encoding plain`) | auto (default) |
|---|---|---|
| ingest, best of 3 | 0.427 s (23.4 M rows/s) | 1.183 s (8.45 M rows/s) |
| verify (read + decode all 10 columns, compare every field) | 0.330 s, PASS | 0.467 s, PASS |
| output `.col` bytes | 520,074,096 | 160,200,997 |
| ratio vs 520,000,008-byte `.ads` input | 1.0001 | 0.308 (**3.25x smaller**) |

Encoding costs 0.756 s of CPU per 10M rows (76 ns per row, 0.49 ms per
chunk) and 0.137 s to decode on read. Before the early-exit dictionary
scan, auto ingest of 1M rows took 0.704 s against 0.121 s after (PLAIN
0.051 s); see NOTES.md.

Per column, from `colstore info` on the auto file:

| column | type | PLAIN bytes | encoded bytes | ratio | bytes/row | encoding (all 153 chunks) |
|---|---|---:|---:|---:|---:|---|
| event_id | u64 | 80,000,000 | 16,043,404 | 4.99x | 1.60 | delta_bp |
| event_time_us | i64 | 80,000,000 | 33,512,388 | 2.39x | 3.35 | delta_bp |
| ingest_time_us | i64 | 80,000,000 | 1,328,737 | 60.21x | 0.13 | delta_bp |
| event_type | u8 | 10,000,000 | 2,048,858 | 4.88x | 0.20 | rle_dict |
| advertiser_id | u32 | 40,000,000 | 19,923,780 | 2.01x | 1.99 | delta_bp |
| campaign_id | u32 | 40,000,000 | 23,672,487 | 1.69x | 2.37 | delta_bp |
| video_id | u64 | 80,000,000 | 27,422,487 | 2.92x | 2.74 | delta_bp |
| country | u16 | 20,000,000 | 5,004,437 | 4.00x | 0.50 | dict |
| device | u8 | 10,000,000 | 2,501,836 | 4.00x | 0.25 | dict |
| cost_micros | i64 | 80,000,000 | 28,668,487 | 2.79x | 2.87 | delta_bp |
| **total** | | 520,000,000 | 160,126,901 | 3.25x | 16.01 | + 74,076-byte footer |

The 60x on `ingest_time_us` is partly a property of the synthetic
generator (it emits at a fixed 100 us step, so nearly every delta is
100); real ingest clocks are less regular. The 2.39x on `event_time_us`
is the honest number for a timestamp with 5% of values up to 30 s late.

## Numbers (m3)

Same machine, build and 10M-row file; scan times are the engine portion
from `scan --count --stats` (no row formatting), three runs, all within
the range shown. Full output: `bench/colstore_bench.sh build-cs 10000000`.

| query (project `campaign_id, cost_micros`) | rows passed | row groups scanned | chunks decoded | time |
|---|---:|---:|---:|---:|
| `event_time_us BETWEEN t AND t+60s` | 599,888 | 15 of 153 (138 skipped by zone map) | 43 | 0.006 s |
| `country=840 AND event_time_us BETWEEN t AND t+60s` | 180,421 | 15 of 153 | 58 | 0.008 s |
| `event_type=1 AND cost_micros >= 1000` | 899,878 | 153 of 153 | 459 | 0.061-0.063 s |
| `campaign_id BETWEEN 50000 AND 50100` (unsorted column) | 10,284 | 153 of 153 (0 skipped) | 306 | 0.041-0.042 s |
| no predicate, same two columns (decode-only reference) | 10,000,000 | 153 | 306 | 0.036-0.039 s |

All four agree with the brute-force pass over the `.ads` file on row count
and both column sums (`verify --where`, PASS). The filter itself costs 6 to
7 ms per 10M rows; the rest is decode.

Zone-map skipping on `event_time_us` (stored in arrival order, 5% of rows
up to 30 s late): a 10 s range scans 7 groups, 60 s scans 15, 300 s scans
52, against ideals of 2, 10 and 46; the late events widen every group's
`[min, max]` by about 30 s, a constant 5 extra groups per query (NOTES.md).

Batch size (`--vector`), full-scan query `event_type=1 AND cost_micros >= 1000`:

| rows per batch | 1 | 16 | 128 | 1024 (default) | 8192 | 65536 |
|---|---:|---:|---:|---:|---:|---:|
| time | 0.136-0.141 s | 0.068-0.072 s | 0.065-0.066 s | 0.061-0.064 s | 0.061-0.062 s | 0.061-0.065 s |

Dictionary codes vs values, `country=840` projecting `country` only:
0.028-0.029 s on the auto file (DICT chunks, filtered on codes, column
never expanded) against 0.024-0.025 s on the PLAIN file (filtered on
values); the no-predicate decode of that column is 0.024-0.027 s and
0.019-0.020 s respectively, so the filter adds 3 ms vs 5 ms.

## Benchmark queries (m4)

The three queries every later number refers to. `T0 = 1767225900000000`
(2026-01-01T00:05:00Z, 300 s into the generated stream). `gen` emits
10,000 events per second of event time, so the 10M-event file spans
1,000 s and the one-hour range of Q2 covers its last 700 s (the zone map
skips the first 45 of 153 row groups, the rest is a 6.99M-row aggregate).

| | SQL | colstore |
|---|---|---|
| Q1 | `SELECT COUNT(*) WHERE country=840 AND event_type=1` | `q --where "country=840 AND event_type=1" --agg "count()"` |
| Q2 | `SELECT campaign_id, SUM(cost_micros), COUNT(*) WHERE event_time_us BETWEEN T0 AND T0+3600s GROUP BY campaign_id` | `q --where "event_time_us BETWEEN $T0 AND $((T0+3600000000))" --group campaign_id --agg "sum(cost_micros),count()"` |
| Q3 | `SELECT campaign_id, COUNT(*) FILTER (WHERE event_type=0), ... =1, ... =2, SUM(cost_micros) GROUP BY campaign_id` | `q --group campaign_id --agg "count_if(event_type=0) AS impressions,count_if(event_type=1) AS clicks,count_if(event_type=2) AS conversions,sum(cost_micros) AS cost_micros"` |

Each is verified with `--check` (the whole GROUP BY recomputed from the
`.ads` file with a `std::map`, 1 / 100,000 / 100,000 groups identical) and,
with `--order ... --limit 20 --threads 8`, the 20 emitted rows are also
identical to a plain `std::stable_sort` of the brute-force table.

## Numbers (m4)

Same machine (Apple M5 Pro, 18 cores), build and 10M-row file; the time is
`total` from `q --stats` (scan + filter + aggregate + merge + sort, no CSV
formatting), median of 3 runs, file in the page cache. Cold-cache times are
not measured: evicting the cache needs `sudo purge`, which the script tries
and reports as unavailable. Full output: `bench/colstore_bench.sh build-cs 10000000`.

| threads | Q1 point filter + count | Q2 1h range, GROUP BY campaign_id | Q3 full GROUP BY, 4 aggregates |
|---:|---:|---:|---:|
| 1 | 0.051 s | 0.092 s | 0.177 s |
| 2 | 0.026 s | 0.055 s | 0.113 s |
| 4 | 0.014 s | 0.042 s | 0.084 s |
| 8 | 0.008 s | 0.040 s | 0.068 s |

Where Q3's 0.177 s goes at one thread: decoding the three columns is
0.055 s (`scan --count`), `GROUP BY campaign_id` with a bare `count()` is
0.104 s (so the hash lookup costs about 5 ns per row at 1.32 probes per
row, 100,000 groups in a 262,144-slot table after 8 doublings), and the
three `count_if` plus the `sum` add 0.073 s, about 1.8 ns per row per
aggregate. Thread scaling is 6x on 8 threads for Q1 (one group) but 2.6x
for Q3, because every thread builds its own full 100,000-group table and
the merge is serial: 1.2 ms per merged table, 8 ms at 8 threads, 18 ms at
16 (where it is 28% of the total). NOTES.md has the breakdown and what
would fix it.

ORDER BY on the 100,000-row Q3 result: full sort 7.1 ms; `LIMIT 20` 0.2 ms;
`LIMIT 1000` 0.6 ms (`std::partial_sort`, a heap of N). Batch size with
the aggregate attached (`--vector`): 1024 / 8192 / 65536 rows give
0.180 / 0.184 / 0.184 s on Q3, a 2% spread, so the m3 guess that 1024
would clearly win once an operator re-reads the batch did not hold; the
table's random access dominates, not the batch's cache residency.

## Status

- [x] m1: row-group file format, PLAIN encoding, ingest from `.ads`,
      `info` / `dump` / `verify`, reader API, ASan clean,
      10M rows round-trip identical
- [x] m2: per-chunk encodings (DICT, DELTA_BP, RLE_DICT, PLAIN fallback)
      with size-based selection, per-chunk min/max for every integer
      column, `--encoding plain|auto`, 45 colstore unit tests, ASan+UBSan
      clean on the 10M file, 3.25x smaller than the input, verify PASS
- [x] m3: scan / filter / project over 1024-row batches with selection
      vectors, zone-map row-group skipping (skip / all / some), dictionary
      predicates on codes, `--where` parser, `colstore scan`,
      `verify --where` against brute force (4 predicates PASS on 10M rows),
      66 colstore unit tests, ASan+UBSan clean; 60 s time range skips 138
      of 153 groups and runs in 6 ms
- [x] m4: hash aggregate (own open-addressing table, packed 128-bit keys,
      count / sum / min / max / count_if / sum_if), ORDER BY / LIMIT with
      partial sort, `colstore q` with CSV output and `--check` against a
      brute-force recompute from `.ads`, optional row-group-parallel scan
      with per-thread tables merged at the end, 105 colstore unit tests,
      ASan+UBSan and TSan clean on the 10M file; Q1 / Q2 / Q3 in
      0.051 / 0.092 / 0.177 s single-threaded, 0.008 / 0.040 / 0.068 s on 8
      threads, all three `--check` PASS
- [ ] m5: benchmark vs SQLite; `bench/colstore_bench.sh` extended
