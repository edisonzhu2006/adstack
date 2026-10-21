# colstore vs SQLite (m5)

Every number here comes from one run of `bench/colstore_bench.sh` on
2026-10-21 (log: `/private/tmp/adstack-cs/bench_m5_final_2026-10-21.txt`);
the m1-m4 numbers it also prints are in [README.md](README.md). Nothing is
estimated. Re-run the command below to reproduce; the `.ads` input is
cached, so a re-run takes about three minutes.

## Machine

| | |
|---|---|
| CPU | Apple M5 Pro, 18 cores (`sysctl -n machdep.cpu.brand_string hw.ncpu`) |
| memory | 24 GiB (`sysctl -n hw.memsize` = 25,769,803,776) |
| OS | macOS 26.4.1, Darwin 25.4.0 (`sw_vers`, `uname -sr`) |
| compiler | Apple clang 21.0.0 (clang-2100.0.123.102), `-std=c++20`, CMake `Release` (`-O3`) |
| SQLite | 3.51.0, the library in the macOS SDK (`libsqlite3.tbd`), `THREADSAFE=2`, `MAX_MMAP_SIZE=1073741824`, `ENABLE_DBSTAT_VTAB` (`PRAGMA compile_options`) |
| load | load average 2.8 to 4.2 during the run (`uptime`); the streamkit track was building on the same machine |
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
  on, which favours SQLite. Checked while choosing the settings (before
  the script run, same binary and file): a fresh connection per run gave
  Q2 5.17 s vs 5.21 s, and `mmap_size = 0` gave 5.33 s vs 4.89 s on the
  index plan and 4.03 s vs 4.25 s on the scan plan, all inside the
  run-to-run spread, so neither setting changes the picture.
- Every query is run twice on SQLite: as the planner chooses (with the
  indexes and statistics) and with `NOT INDEXED` (a plain table scan). The
  faster of the two is "best SQLite" in the ratio column, because the
  question is the storage model, not a planner decision.
- colstore: `q --stats` total (scan + filter + aggregate + merge + sort, no
  CSV formatting), three separate processes, median. SQLite is
  single-threaded per query, so the 1-thread colstore column is the
  like-for-like one; 8 threads is shown because the machine has them.
- Answers: the CSV of each query from both engines, sorted and diffed by
  the script. All three identical (1 / 100,000 / 100,000 rows). The
  colstore answers were separately proven against a brute-force
  recompute from the `.ads` file (`q --check`, m4), so the two engines
  agree with each other and with the oracle.

The two `u64` columns (`event_id`, `video_id`) are stored in SQLite as the
same 64 bits reinterpreted as `INTEGER`; no benchmark query reads them.

## Storage

| storage | bytes | vs `.ads` | bytes per row |
|---|---:|---:|---:|
| `.ads` (raw 52-byte rows) | 520,000,008 | 1.00x | 52.0 |
| `.col` (colstore, auto encodings) | 160,200,997 | 0.31x | 16.0 |
| `.sqlite` table only | 488,329,216 | 0.94x | 48.8 |
| `.sqlite` with 3 indexes + ANALYZE | 899,682,304 | 1.73x | 90.0 |

Per object (`dbstat`): table 488,325,120; `idx_events_time` 172,388,352;
`idx_events_country_type` 120,160,256; `idx_events_campaign` 118,800,384.
The three indexes add 411,348,992 bytes, 84% of the table, 41 bytes per
row. SQLite's varint record format saves 6% against the fixed 52-byte
rows; colstore's per-chunk encodings save 69% (`info` has the per-column
table in README.md). The `.col` file is 3.05x smaller than the SQLite
table alone and 5.62x smaller than the database the queries ran against.

## Load

| load, median of 3 | seconds | M rows/s |
|---|---:|---:|
| colstore ingest (choose and apply an encoding per chunk, write `.col`) | 1.313 | 7.62 |
| SQLite INSERT (one transaction, prepared statement) | 3.066 | 3.26 |
| SQLite 3 x CREATE INDEX | 12.878 | |
| SQLite ANALYZE | 0.585 | |
| SQLite total | 16.529 | 0.60 |

colstore's ingest is 2.3x faster than SQLite's insert alone and 12.6x
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

| query | rows out | SQLite, planner's plan | SQLite, `NOT INDEXED` | colstore 1 thread | colstore 8 threads | colstore 1 thread vs best SQLite |
|---|---:|---:|---:|---:|---:|---:|
| Q1 point filter + count | 1 | **0.0028 s** | 0.2361 s | 0.055 s | 0.008 s | 0.05x (SQLite 19.6x faster) |
| Q2 1 h range, GROUP BY campaign_id | 100,000 | 5.0051 s | **4.0084 s** | 0.094 s | 0.042 s | **42.6x** |
| Q3 full GROUP BY, 4 aggregates | 100,000 | **5.2669 s** | 7.5260 s | 0.188 s | 0.069 s | **28.0x** |

The three runs behind each median are in the log; the colstore spreads
were 0.055-0.093, 0.094-0.105 and 0.187-0.287 s (the first run of each
set was the slow one, with the other track's build on the machine), the
SQLite spreads 4.87-5.92 s on Q2's index plan and 5.19-5.43 s on Q3's.
Against the planner's own choice the ratios are 53.2x (Q2) and 28.0x
(Q3); at 8 threads colstore is 95x and 76x faster than the best SQLite
plan.

Query plans (`EXPLAIN QUERY PLAN`, printed by the script):

| | plan |
|---|---|
| Q1 | `SEARCH events USING COVERING INDEX idx_events_country_type (country=? AND event_type=?)` |
| Q2 | `SEARCH events USING INDEX idx_events_time (event_time_us>? AND event_time_us<?)`; `USE TEMP B-TREE FOR GROUP BY` |
| Q3 | `SCAN events USING INDEX idx_events_campaign` (index order supplies the GROUP BY, no temp B-tree) |
| `NOT INDEXED` | `SCAN events`; `USE TEMP B-TREE FOR GROUP BY` |

Zone map on Q2: 45 of 153 row groups skipped (29.4%), 2,949,120 rows never
read; the other 108 groups are decoded (`campaign_id`, `cost_micros`,
`event_time_us`) and filtered in 13 ns per row.

## Where each one wins, and why

colstore wins Q2 and Q3 by 28x to 43x for three reasons that stack. No
row reconstruction: Q3 touches three of ten columns, and SQLite has to
walk each row's record header to find them (527 ns per row on its best
plan, 753 ns on the scan plan), while colstore reads three contiguous
chunks and never sees the other seven columns (18.8 ns per row). Encoding
aware scans: `event_type` stays run-length coded and the predicate runs
on dictionary codes; `campaign_id` and `cost_micros` are delta-coded and
decode at about a nanosecond per value; the whole 10M-row Q3 input is
54 MB of chunks instead of 488 MB of records. Zone maps: Q2 skips 29% of
the file from the footer alone, where SQLite's index plan performs an
index range scan followed by 7M rowid lookups into the table and a temp
B-tree for the GROUP BY (716 ns per matched row), which is why its
planner's choice is slower than a plain scan here. On top of that, the
hash aggregate with a dense group index is cheaper than SQLite's sorted
temp B-tree for 100,000 groups, and colstore can use the other 17 cores.

SQLite wins Q1, and not narrowly: 2.8 ms against 55 ms, 19.6x. The
`(country, event_type)` index is a covering index for `COUNT(*)`, so the
query is a B-tree range count over the 269,609 matching entries (10 ns
each) and never touches the table. colstore has no secondary index; it
decodes the `country` and `event_type` chunks of all 153 row groups
(every group contains country 840, so no zone map can skip one) and
tests 10M rows at 5.5 ns each. This is the general shape of the
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
pruning, so neither is expected to close a 28x to 43x gap, but the
honest statement is that they were not tried.
