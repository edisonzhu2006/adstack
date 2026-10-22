# colstore vs SQLite (m5)

Every number here comes from `bench/colstore_bench.sh` runs made on
2026-10-21 between 12:36 and 13:34 EDT, from a clean rebuild of the current
tree. The headline tables are the seventh and quietest run of that session
(log: `/private/tmp/adstack-cs/bench_m5_reverify7_2026-10-21.txt`); the
other six runs are listed in full under "Seven runs, one session" so the
spread is visible. Nothing is estimated. The m1-m4 numbers the script also
prints are in [README.md](README.md). Re-run the command below to
reproduce; the `.ads` input is cached, so a re-run takes about three
minutes.

## Machine

| | |
|---|---|
| CPU | Apple M5 Pro, 18 cores (`sysctl -n machdep.cpu.brand_string hw.ncpu`) |
| memory | 24 GiB (`sysctl -n hw.memsize` = 25,769,803,776); 59% free during the runs (`memory_pressure`), swap 2.4 GB used of a 4 GB file (`sysctl vm.swapusage`) |
| OS | macOS 26.4.1, Darwin 25.4.0 (`sw_vers`, `uname -sr`), uptime 131 days |
| compiler | Apple clang 21.0.0 (clang-2100.0.123.102), `-std=c++20`, CMake `Release` (`-O3`) |
| SQLite | 3.51.0, the library in the macOS SDK (`libsqlite3.tbd`), `THREADSAFE=2`, `MAX_MMAP_SIZE=1073741824`, `ENABLE_DBSTAT_VTAB` (`PRAGMA compile_options`) |
| background | `fseventsd` at 84-89% of one core for the whole session (`ps -r`), a standing condition on this machine; the streamkit track's `bench/crash.sh` and `bench/throughput.sh` during runs 1-4; `systemstats` at 100% of a core during run 5; a Chrome helper at 40-54% at the start of runs 6 and 7. Load average at the start of run 7: 2.78 (`uptime`) |
| thermal | no thermal or performance warning recorded (`pmset -g therm`) |
| storage | the files live in `/private/tmp/adstack-cs/` on the internal SSD and were in the page cache for every timed run (written or read moments earlier); a cold-cache run needs `sudo purge`, which this account cannot run, so no cold number is given |

## Commands

```
export PATH="$HOME/.withai/runtimes/python/bin:$PATH"    # cmake, ninja
cmake -S . -B build-cs -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-cs
bench/colstore_bench.sh build-cs 10000000 /private/tmp/adstack-cs m5   # the tables below
bench/colstore_bench.sh build-cs 10000000 /private/tmp/adstack-cs       # m1-m5, about four minutes
```

Data: `gen --n 10000000 --seed 1` (10,000,000 events, 9,899,652 unique,
100,348 duplicates, 496,040 late by up to 30 s, 10,000 events per second
of event time, 520,000,008 bytes raw). The same file feeds both engines.

## What SQLite was given

The schema, indexes and PRAGMAs are code in `src/sqlite_loader.cpp` and
are printed by `colstore_sqlite load`; this is what the run printed.

```
CREATE TABLE events (event_id INTEGER NOT NULL, event_time_us INTEGER NOT NULL,
  ingest_time_us INTEGER NOT NULL, event_type INTEGER NOT NULL, advertiser_id INTEGER NOT NULL,
  campaign_id INTEGER NOT NULL, video_id INTEGER NOT NULL, country INTEGER NOT NULL,
  device INTEGER NOT NULL, cost_micros INTEGER NOT NULL)
CREATE INDEX idx_events_time ON events(event_time_us)
CREATE INDEX idx_events_country_type ON events(country, event_type)
CREATE INDEX idx_events_campaign ON events(campaign_id)
ANALYZE
```

- Load: `journal_mode = OFF`, `synchronous = OFF`, `cache_size = -1048576`
  (1 GiB), `temp_store = MEMORY`, `locking_mode = EXCLUSIVE`; one
  transaction, one prepared `INSERT` bound and reset per row, through the
  C API (no CSV round trip); indexes created after the load (faster than
  maintaining them during it), then `ANALYZE` so the planner has
  statistics. Page size is the default 4096.
- Query: `query_only = 1`, `cache_size = -1048576`, `mmap_size =
  1073741824` (the maximum this build allows), `temp_store = MEMORY`.
  Each query runs once untimed (its rows written as CSV), then three
  times on the same connection; the median is reported. The same
  connection means SQLite's own page cache is warm from the second run
  on, which favours SQLite. (When the settings were chosen, a fresh
  connection per run and `mmap_size = 0` were both tried and each moved
  Q2 by less than the run-to-run spread; see the m5 entry in NOTES.md.)
- Every query is run twice on SQLite: as the planner chooses (with the
  indexes and statistics) and with `NOT INDEXED` (a plain table scan). The
  faster of the two is "best SQLite" in the ratio column, because the
  question is the storage model, not a planner decision.
- colstore: `q --stats` total (scan + filter + aggregate + merge + sort, no
  CSV formatting), three separate processes, median. SQLite is
  single-threaded per query, so the 1-thread colstore column is the
  like-for-like one; 8 threads is shown because the machine has them.
- Answers: the CSV of each query from both engines, sorted and diffed by
  the script. All three identical in every one of the seven runs
  (1 / 100,000 / 100,000 rows). The colstore answers were separately
  proven against a brute-force recompute from the `.ads` file (`q
  --check`, run again this session: PASS for Q1, Q2, Q3 and the Q3 top-20
  at 8 threads), so the two engines agree with each other and with the
  oracle.

The two `u64` columns (`event_id`, `video_id`) are stored in SQLite as the
same 64 bits reinterpreted as `INTEGER`; no benchmark query reads them.

## Storage

Byte counts are identical in all seven runs (the files are deterministic).

| storage | bytes | vs `.ads` | bytes per row |
|---|---:|---:|---:|
| `.ads` (raw 52-byte rows) | 520,000,008 | 1.00x | 52.0 |
| `.col` (colstore, auto encodings, format v2) | 160,201,178 | 0.31x | 16.0 |
| `.sqlite` table only | 488,329,216 | 0.94x | 48.8 |
| `.sqlite` with 3 indexes + ANALYZE | 899,682,304 | 1.73x | 90.0 |

Per object (`dbstat`): table 488,325,120; `idx_events_time` 172,388,352;
`idx_events_country_type` 120,160,256; `idx_events_campaign` 118,800,384.
The three indexes add 411,348,992 bytes, 84% of the table, 41 bytes per
row. SQLite's varint record format saves 6.1% against the fixed 52-byte
rows; colstore's per-chunk encodings save 69.2% (`info` has the per-column
table in README.md). The `.col` file is 3.05x smaller than the SQLite
table alone and 5.62x smaller than the database the queries ran against.
The `.col` file is 181 bytes larger than in the first m5 write-up: format
version 2 stores the table schema in the footer (integration entry in
NOTES.md); the column data is byte-identical.

## Load

Run 7; the individual runs are in parentheses.

| load, median of 3 | seconds | M rows/s |
|---|---:|---:|
| colstore ingest (choose and apply an encoding per chunk, write `.col`) | 1.215 (1.260, 1.196, 1.215) | 8.23 |
| SQLite INSERT (one transaction, prepared statement) | 2.781 (2.755, 2.781, 2.797) | 3.60 |
| SQLite 3 x CREATE INDEX | 11.870 (11.938, 11.870, 11.869) | |
| SQLite ANALYZE | 0.552 (0.549, 0.552, 0.557) | |
| SQLite total | 15.223 (15.243, 15.203, 15.223) | 0.66 |

colstore's ingest is 2.29x faster than SQLite's insert alone and 12.5x
faster than the load the queries needed; 78% of SQLite's load is building
the three B-trees, each a sort of 10M keys. colstore has no index to
build: the zone maps are a min and a max per chunk, computed while
encoding.

## Queries

`T0 = 1767225900000000` (300 s into the stream); the file spans 1,000 s,
so Q2's one-hour range covers the last 700 s.

| | SQL (SQLite) | colstore |
|---|---|---|
| Q1 | `SELECT COUNT(*) FROM events WHERE country = 840 AND event_type = 1` | `q --where "country=840 AND event_type=1" --agg "count()"` |
| Q2 | `SELECT campaign_id, SUM(cost_micros), COUNT(*) FROM events WHERE event_time_us BETWEEN T0 AND T0+3600s GROUP BY campaign_id` | `q --where "event_time_us BETWEEN T0 AND T0+3600000000" --group campaign_id --agg "sum(cost_micros),count()"` |
| Q3 | `SELECT campaign_id, COUNT(*) FILTER (WHERE event_type=0), ... =1, ... =2, SUM(cost_micros) FROM events GROUP BY campaign_id` | `q --group campaign_id --agg "count_if(event_type=0) AS impressions,count_if(event_type=1) AS clicks,count_if(event_type=2) AS conversions,sum(cost_micros) AS cost_micros"` |

Run 7, median of 3:

| query | rows out | SQLite, planner's plan | SQLite, `NOT INDEXED` | colstore 1 thread | colstore 8 threads | colstore 1 thread vs best SQLite |
|---|---:|---:|---:|---:|---:|---:|
| Q1 point filter + count | 1 | **0.0028 s** | 0.2664 s | 0.063 s | 0.009 s | 0.04x (SQLite 22.5x faster) |
| Q2 1 h range, GROUP BY campaign_id | 100,000 | 5.1766 s | **4.1553 s** | 0.098 s | 0.040 s | **42.4x** |
| Q3 full GROUP BY, 4 aggregates | 100,000 | **5.4385 s** | 6.8925 s | 0.181 s | 0.068 s | **30.0x** |

The three runs behind each median: SQLite Q1 0.0028 / 0.0028 / 0.0028 and
0.2552 / 0.2664 / 0.2723; Q2 5.5342 / 5.0881 / 5.1766 and 4.1900 / 4.1553
/ 4.1349; Q3 5.4304 / 5.4820 / 5.4385 and 6.8003 / 6.8925 / 7.0407.
colstore one thread Q1 0.063 / 0.063 / 0.063, Q2 0.106 / 0.097 / 0.098,
Q3 0.213 / 0.180 / 0.181; eight threads 0.009 / 0.009 / 0.009, 0.040 /
0.039 / 0.043, 0.065 / 0.068 / 0.076. Against the planner's own choice
the ratios are 52.8x (Q2) and 30.0x (Q3); against `NOT INDEXED` Q3 is
38.1x. At 8 threads colstore is 103.9x (Q2) and 80.0x (Q3) faster than
the best SQLite plan; 8 threads give colstore 7.0x on Q1 and 2.66x on Q3
(the serial merge of eight 100K-group tables, 9.1-9.5 ms, is most of the
gap; m4 entry in NOTES.md).

Query plans (`EXPLAIN QUERY PLAN`, printed by the script):

| | plan |
|---|---|
| Q1 | `SEARCH events USING COVERING INDEX idx_events_country_type (country=? AND event_type=?)` |
| Q2 | `SEARCH events USING INDEX idx_events_time (event_time_us>? AND event_time_us<?)`; `USE TEMP B-TREE FOR GROUP BY` |
| Q3 | `SCAN events USING INDEX idx_events_campaign` (index order supplies the GROUP BY, no temp B-tree) |
| `NOT INDEXED` | `SCAN events`; `USE TEMP B-TREE FOR GROUP BY` |

Zone map on Q2: 45 of 153 row groups skipped (29.4%), 2,949,120 rows never
read; the other 108 groups (7,050,880 rows) are decoded (`campaign_id`,
`cost_micros`, `event_time_us`) and filtered in 13.9 ns per scanned row.
Identical in all seven runs: this is a property of the file and the
predicate, not of the machine.

## Seven runs, one session

The script was run seven times between 12:36 and 13:34 EDT because the
machine was shared: the streamkit track ran its crash and throughput
harnesses during runs 1-4, a system daemon (`systemstats`) pinned a core
during run 5, and a browser process spiked at the start of runs 6 and 7.
Every run reproduced the counts, the sizes, the plans, the zone-map skip
and the cross-engine diff exactly; only wall-clock moved. The table shows
each run's medians (SQLite is the better of its two plans) so the band is
on record. The one-minute load average at the start of each run is a weak
indicator here: `fseventsd` and `systemstats` are single-threaded and
barely move it, and runs 4 and 5 started at load 3.3-3.4 and were the
slowest.

| run | start | load | colstore ingest | SQLite load | Q1 SQLite / colstore | Q2 SQLite / colstore | Q2 ratio | Q3 SQLite / colstore | Q3 ratio |
|---|---|---:|---:|---:|---|---|---:|---|---:|
| 1 | 12:36 | 15.75 | 1.273 | 21.641 | 0.0038 / 0.088 | 5.0677 / 0.152 | 33.3x | 6.9420 / 0.313 | 22.2x |
| 2 | 12:40 | 3.66 | 1.231 | 16.607 | 0.0047 / 0.079 | 4.9071 / 0.098 | 50.1x | 8.3116 / 0.955 | 8.7x |
| 3 | 12:44 | 3.89 | 1.377 | 26.175 | 0.0064 / 0.108 | 5.9188 / 0.155 | 38.2x | 8.2033 / 0.407 | 20.2x |
| 4 | 12:52 | 3.29 | 1.214 | 24.520 | 0.0038 / 0.079 | 9.1612 / 0.317 | 28.9x | 15.0127 / 0.739 | 20.3x |
| 5 | 13:18 | 3.39 | 1.860 | 22.150 | 0.0060 / 0.103 | 7.2890 / 0.175 | 41.7x | 10.7934 / 0.549 | 19.7x |
| 6 | 13:26 | 2.87 | 1.233 | 15.273 | 0.0027 / 0.051 | 4.8285 / 0.213 | 22.7x | 9.5284 / 0.313 | 30.4x |
| 7 | 13:30 | 2.78 | 1.215 | 15.223 | 0.0028 / 0.063 | 4.1553 / 0.098 | 42.4x | 5.4385 / 0.181 | 30.0x |

Reading the band: both engines slow down under contention, and not by
the same factor on the same query, so the ratio moves as well, from 23x
to 50x on Q2 and from 9x to 30x on Q3. A ratio of 20x or more held in 13
of the 14 GROUP BY cells; the one exception (run 2, Q3 at 8.7x) is a
colstore median of 0.955 s from runs of 1.341 / 0.955 / 0.803 s, five
times its quiet value, while SQLite's three runs in the same minute moved
by a third. The quiet-machine controls inside each run are the colstore
ingest (1.21-1.23 s in the quiet runs, 1.86 s in run 5) and SQLite's index
build (11.9 s quiet, 13-24 s loaded); run 7 is the only one where every
control sat at its quiet value from start to finish, which is why its
table is the headline. An inline re-time at 13:26, between runs 5 and 6,
gave Q3 at one thread 0.174 / 0.178 / 0.173 s and at eight threads 0.067 /
0.065 / 0.066 s, and the three-column decode alone 0.056 / 0.055 / 0.056 s
(`scan --count --cols event_type,campaign_id,cost_micros`), which is the
same picture as run 7.

A hypothesis for why colstore's Q3 suffers more than SQLite's under
contention, not yet measured: Q3's 100,000-group hash table (262,144 slots of
24 bytes after eight doublings, 6.3 MB, plus 3.2 MB of aggregate state;
m4 entry in NOTES.md) is about 9.5 MB, larger than any private cache
level, so its random probes depend on the shared cache and on memory
bandwidth, which are exactly what a neighbouring process takes away;
SQLite's index-ordered scan is a sequential walk with no such table. The
first item under "what I would change next" in NOTES.md is the measurement
that would confirm or refute this.

## Where each one wins, and why

colstore wins Q2 and Q3 by 30x to 42x for three reasons that stack. No
row reconstruction: Q3 touches three of ten columns, and SQLite has to
walk each row's record header to find them (544 ns per row on its best
plan, 689 ns on the scan plan), while colstore reads three contiguous
chunk sequences and never sees the other seven columns (18.1 ns per row).
Encoding-aware scans: `event_type` stays run-length coded and the
predicate runs on dictionary codes; `campaign_id` and `cost_micros` are
delta-coded and decode at about a nanosecond per value; the whole 10M-row
Q3 input is 54.4 MB of chunks (2,048,858 + 23,672,487 + 28,668,487 bytes,
from `info`) instead of 488 MB of records, 9.0x less to read. Zone maps:
Q2 skips 29% of the file from the footer alone, where SQLite's index plan
performs an index range scan followed by 7M rowid lookups into the table
and a temp B-tree for the GROUP BY (740 ns per matched row), which is why
its planner's choice is slower than a plain scan (416 ns per row) here.
On top of that, the hash aggregate with a dense group index is cheaper
than SQLite's sorted temp B-tree for 100,000 groups, and colstore can use
the other 17 cores.

SQLite wins Q1, and not narrowly: 2.8 ms against 63 ms, 22.5x. The
`(country, event_type)` index is a covering index for `COUNT(*)`, so the
query is a B-tree range count over the 269,609 matching entries (10.4 ns
each) and never touches the table. colstore has no secondary index; it
decodes the `country` and `event_type` chunks of all 153 row groups
(7.1 MB; every group contains country 840, so no zone map can skip one)
and tests 10M rows at 6.3 ns each. This is the general shape of the
trade-off: a column store makes "read a few columns of most rows" cheap
and has nothing special for "find the few rows that match an equality",
which an index does in time proportional to the answer. The same applies
to point lookups by primary key (`WHERE event_id = ?`): SQLite's rowid
B-tree answers in microseconds; colstore would decode a 64K-row chunk per
row group that the `event_id` zone map cannot exclude. Updates and
deletes: SQLite does them in place with transactions; colstore's chunks
are immutable and would need a rewrite of the row group or a delete
bitmap. Concurrency: SQLite gives readers and a writer at once, with a
rollback journal or WAL; colstore has one writer, no locks, and no
story for a reader opening a file mid-write beyond the trailer check.
Maturity: SQLite is one of the most tested pieces of software in the
world, with a full SQL frontend, a planner, types, NULLs, strings,
transactions and 20 years of users; colstore is ten integer columns, a
conjunction of range predicates and a hand-written GROUP BY, built in a
week. A fair summary is that colstore is faster at the one job it was
built for, analytic scans and aggregates over immutable event data, by
one to two orders of magnitude, and SQLite is better at everything else.

Two things that would narrow the gap on SQLite's side, not measured
because the brief fixed the indexes: a covering index on
`(event_time_us, campaign_id, cost_micros)` would turn Q2 into an
index-only range scan with no rowid lookups, and a larger page size
(`page_size = 65536`) would make the table scans read fewer, larger
pages. Neither changes the row-reconstruction cost or the lack of column
pruning, so neither is expected to close a 30x to 42x gap, but the
honest statement is that they were not tried.
