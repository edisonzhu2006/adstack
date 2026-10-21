# streamkit benchmarks

Every number on this page comes from a command run on 2026-10-21 between
01:09 and 01:40 EDT on the machine below, on the tree as it stands after
milestone 5. The tables in sections 1 to 5 are the output of
`bench/throughput.sh build-sk` (sections 1, 2, 3, 5 from one run started at
01:15, section 4 from `--only memory` at 01:21 after the queue-depth rows
were added, plus a second `--only threads` run at 01:32). The decomposition
experiments in section 5 and the regression results in section 6 are
inline commands, written out next to their results.

## Machine and build

- Apple M5 Pro, 18 cores (6 "Super" + 12 "Performance" per
  `sysctl hw.perflevel0/1`), 24 GB, macOS 26.4.1 (Darwin 25.4.0), APFS on
  the internal SSD
- Apple clang 21.0.0 (clang-2100.0.123.102), `-O3` (CMake Release),
  project flags `-Wall -Wextra -Wpedantic -Wshadow -Wconversion`
- `build-sk` configured with
  `cmake -S . -B build-sk -G Ninja -DCMAKE_BUILD_TYPE=Release`
- The machine was not idle: another agent was building and testing the
  colstore track in a sibling directory throughout. Load averages at the
  start of each harness run are printed by the harness and quoted below
  (3.5 to 4.5 on 18 cores). The run-to-run spread is shown in every table
  so the noise is visible rather than hidden behind a median.
- Inputs: `gen --n 10000000 --seed 1` (520,000,008 bytes: 1% duplicates, 5%
  late by 1 to 30 s, 100K campaigns, 10K events per simulated second),
  the same with `--dup-frac 0` ("clean"), and `gen --n 1000000 --seed 1`.
  Cached in `/private/tmp/adstack-sk` by the harness.
- Method: three runs per cell, the median (middle value) in bold, all three
  shown. Events/s is events divided by wall time including the final merge
  of the per-worker part files, as reported on streamkit's own stderr
  summary.

## 1. Throughput vs threads

```
streamkit --in $DIRTY --out $OUT --lateness-s 60 --threads P
```

Run at 01:15 EDT, load average 3.46:

| threads | run 1 | run 2 | run 3 | median events/s | speedup vs 1 | reader stalls (median run) | worker idle waits (median run) |
|---|---|---|---|---|---|---|---|
| 1 | 5,040,788 | 5,057,507 | 5,054,775 | **5,054,775** | 1.00x | 71,647 | 1 |
| 2 | 9,417,748 | 9,155,871 | 9,348,048 | **9,348,048** | 1.85x | 32,251 | 2 |
| 4 | 18,826,122 | 14,578,356 | 12,207,640 | **14,578,356** | 2.88x | 10,035 | 4 |
| 8 | 24,142,013 | 27,268,312 | 27,525,467 | **27,268,312** | 5.39x | 1,699 | 158 |

Run at 01:32 EDT, load average 3.66:

| threads | run 1 | run 2 | run 3 | median events/s | speedup vs 1 | reader stalls (median run) | worker idle waits (median run) |
|---|---|---|---|---|---|---|---|
| 1 | 4,332,307 | 4,641,018 | 4,448,270 | **4,448,270** | 1.00x | 71,671 | 1 |
| 2 | 8,677,059 | 8,759,776 | 9,038,989 | **8,759,776** | 1.97x | 32,000 | 2 |
| 4 | 18,372,739 | 18,220,569 | 18,348,488 | **18,348,488** | 4.12x | 11,572 | 4 |
| 8 | 26,361,292 | 26,250,707 | 26,172,427 | **26,250,707** | 5.90x | 1,909 | 247 |

Reading it: the stable cells are 1 thread at 4.4 to 5.1M events/s, 2 at 8.8
to 9.3M, 8 at 26 to 27M (5.4x to 5.9x). The 4-thread cell of the first run
(12.2M to 18.8M across three runs taken seconds apart) is the clearest
picture of the noise on a shared machine; the second run's 18.2 to 18.4M is
the quiet value and matches the 18.5M measured on 2026-10-21 00:44 and the
19.9M of 2026-10-19 on an idle machine (NOTES.md, m3). Reader stalls fall
and worker idle waits rise with P: at 8 threads the workers are still the
bottleneck, but only just. The 19th's idle-machine table (5.20M / 9.41M /
19.9M / 30.9M, 5.94x) is the best measurement of the code; today's two
runs bound what a loaded machine gives.

## 2. Dedup overhead

```
streamkit --in $STREAM --out $OUT --lateness-s 60 --threads P [--no-dedup]
```

`--no-dedup` skips the per-event hash-set insert (and therefore counts
duplicates twice; it exists only for this measurement). Overhead =
(no-dedup median - dedup median) / no-dedup median, same stream and P.
01:15 EDT.

| stream | threads | dedup | run 1 | run 2 | run 3 | median events/s | dedup overhead |
|---|---|---|---|---|---|---|---|
| dirty (1% dups) | 1 | off | 6,727,607 | 6,727,638 | 6,819,683 | **6,727,638** | (baseline) |
| dirty (1% dups) | 1 | on | 5,045,959 | 4,996,566 | 5,006,735 | **5,006,735** | 25.6% |
| clean (0 dups) | 1 | off | 6,649,391 | 6,703,295 | 6,738,020 | **6,703,295** | (baseline) |
| clean (0 dups) | 1 | on | 4,994,284 | 4,885,024 | 4,989,659 | **4,989,659** | 25.6% |
| dirty (1% dups) | 8 | off | 30,290,061 | 33,177,491 | 30,480,580 | **30,480,580** | (baseline) |
| dirty (1% dups) | 8 | on | 27,122,141 | 25,404,729 | 25,529,229 | **25,529,229** | 16.2% |
| clean (0 dups) | 8 | off | 29,311,051 | 30,441,169 | 29,989,376 | **29,989,376** | (baseline) |
| clean (0 dups) | 8 | on | 27,493,418 | 27,055,261 | 26,412,604 | **27,055,261** | 9.8% |

In absolute terms at 1 thread: 1/5,006,735 - 1/6,727,638 = 199.7 - 148.6 =
51 ns per event on the dirty stream and 200.4 - 149.2 = 51 ns on the clean
one. That is one hash-set insert whose table does not fit in cache, and it
costs the same whether or not a duplicate is found: the clean stream has 1%
more unique ids to store and no hits, and the number does not move. At 8
threads the per-event cost is hidden behind the reader for part of the run
and the relative overhead drops to 10 to 16%, with the two streams 6 points
apart inside a run-to-run spread of 10%.

Why 25.6% here against 13% in the m2 entry: m2 measured the whole
single-threaded loop (read, decode, process) with and without dedup. Since
m3 the read is on another thread, so the no-dedup worker is faster (6.7M
against 5.9M) and the same 51 ns is a bigger fraction of a smaller number.
The nanoseconds, not the percentage, are the stable description of the
cost.

## 3. Checkpoint overhead

```
streamkit --in $DIRTY --out $OUT --lateness-s 60 --threads 8 [--ckpt-dir $D --checkpoint-every-events N]
```

Snapshot columns are from the `checkpoints:` line of the median run:
snapshots = per-worker snapshots written (8 per epoch); slowest = one
snapshot, its three fsyncs included; total = sum over all snapshots of all
workers. 01:15 EDT.

| checkpoint interval | run 1 | run 2 | run 3 | median events/s | vs none | snapshots | slowest snapshot | snapshot time total |
|---|---|---|---|---|---|---|---|---|
| none | 28,761,649 | 25,379,192 | 28,216,499 | **28,216,499** | (baseline) | - | - | - |
| every 1,000,000 events (default, 10 epochs) | 14,151,907 | 15,692,454 | 14,906,651 | **14,906,651** | -47.2% | 80 | 54.7 ms | 2.591 s |
| every 100,000 events (crash-harness setting, 100 epochs) | 2,396,257 | 2,283,333 | 2,467,905 | **2,396,257** | -91.5% | 800 | 65.1 ms | 27.890 s |

The arithmetic: 2.591 s / 80 = 32 ms per snapshot at the default interval;
10 epochs x 32 ms = 0.32 s, and the wall time went from 10M / 28.2M = 0.354 s
to 10M / 14.9M = 0.671 s, a 0.317 s difference. So the whole cost is the
synchronous snapshot (serialize 2.3 MB per worker, three F_FULLFSYNCs, eight
workers hitting the same drive at once); nothing else slowed down. At
benchmark speed the default interval is a checkpoint every 35 ms of wall
time, which no one would run; at the generator's own rate (10K events/s
simulated, which `--pace` replays) the same interval is one checkpoint every
100 s and 32 ms per 100 s is 0.03%. The 100K row is the density the crash
harness needs so that a random SIGKILL lands mid-epoch; it is not a
configuration to run in.

## 4. Memory

```
/usr/bin/time -l streamkit --in $INPUT --out $OUT --lateness-s 60 --threads P --queue-depth Q
```

Maximum resident set size in bytes. Peak dedup ids and peak open windows
are from the median run's summary, summed over workers. 01:21 EDT.

| input | threads | queue depth | run 1 | run 2 | run 3 | median max RSS (bytes) | MiB | peak dedup ids | peak open windows |
|---|---|---|---|---|---|---|---|---|---|
| 1M events | 1 | 4096 | 97,058,816 | 97,042,432 | 97,042,432 | **97,042,432** | 92.5 | 989,273 | 197,664 |
| 1M events | 8 | 4096 | 135,757,824 | 136,265,728 | 136,396,800 | **136,265,728** | 130.0 | 989,273 | 197,664 |
| 1M events | 8 | 256 | 91,291,648 | 91,340,800 | 91,308,032 | **91,308,032** | 87.1 | 989,273 | 197,664 |
| 10M events | 1 | 4096 | 107,593,728 | 107,102,208 | 107,347,968 | **107,347,968** | 102.4 | 1,189,660 | 199,484 |
| 10M events | 8 | 4096 | 356,401,152 | 349,945,856 | 349,372,416 | **349,945,856** | 333.7 | 1,195,108 | 199,569 |
| 10M events | 8 | 256 | 163,840,000 | 141,377,536 | 143,589,376 | **143,589,376** | 136.9 | 1,195,108 | 199,569 |

(The first harness run at 01:15, before the queue-depth rows existed, gave
96,976,896 / 135,921,664 / 107,593,728 / 361,234,432 bytes for the four
default-depth cells, i.e. the same picture.)

Reading it: ten times the events costs 10.7% more RSS at 1 thread (92.5 to
102.4 MiB), and the state counters say why it is not more: the dedup set
holds 1.19M ids at 10M events against 0.99M at 1M (the horizon is
lateness + window = 120 s of event time, so a 100 s stream never expires
anything while a 1000 s stream is in steady state at about 120 s x 9,900
unique ids/s), and the open-window map holds ~200K windows in both. At 8
threads the default queues dominate: 4096 slots x 128 events x 52 bytes =
27 MB per worker, touched only once the reader has run that far ahead,
which at 1M events (7.8K batches over 32K slots) it never fully does, and
at 10M events it always does. `--queue-depth 256` takes the queues out of
the picture and gives 87 to 137 MiB at 8 threads. The remaining 50 MiB
between 1M and 10M at 8 threads with shallow queues is 10 MiB of extra
dedup ids (206K x 48 bytes) and allocator slack: at 1M events no dedup
bucket ever expires, so nothing is freed; at 10M each worker frees and
re-allocates about 1,200 ids per event-second and the per-thread malloc
caches keep what they freed. Whether that slack keeps growing past 10M is
not measured here (m2 measured 1M against 10M at 1 thread and found 1.15x).

## 5. Emit latency under paced replay

### The metric

For each window that a worker's watermark closes, the sample is

```
t_row - t_block
```

where `t_block` is `steady_clock` read by the reader thread immediately
after `read(2)` returned the block of input bytes that contains the event
which pushed this worker's watermark past the window's end (the earliest
instant at which any engine with this partitioning could have known the
window was closable), and `t_row` is `steady_clock` read by the worker
immediately after `fprintf` appended that window's CSV row to the worker's
output stream (a 1 MiB stdio buffer on the part file). p50 and p99 are
nearest-rank percentiles over all such samples in the run; min and max are
the extreme samples.

What it is not: it does not include the allowed lateness (2 s of event
time here: a window [s, s+1) cannot close before an event with time >=
s+3 arrives, by design); it does not include the producer's buffering
before the bytes reach the pipe (gen's writer holds 1 MiB, measured below);
it does not include the stdio flush of the row to disk, fsync, or the
final merge of part files; and windows closed by end of input are not
sampled. Rows are stamped after each `fprintf`, so a row that is behind
others in the same closing call is charged for the rows written before it;
that is deliberate (it is when the row became available) and it is what
makes the distribution look the way it does below. The code is
`streamkit/latency.h` and the three stamping lines in `pipeline.cpp`;
nothing is read or stored unless `--latency-stats` is given.

### The producer's burst pattern

`gen --pace` sleeps per event so ingest time tracks the wall clock, but its
writer buffers 1 MiB before writing to the pipe. Measured by a Python
consumer timing its reads (`gen --n 400000 --rate 200000 --pace --seed 1
--out - | python3 -c '...os.read(0, 1<<20)...'`): 337 reads totalling
20,800,008 bytes, the largest 65,536 bytes (the pipe's capacity), 317 of
them exactly 64 KiB; 20 gaps longer than 10 ms between reads, median 100.7
ms, min 84.2 ms, max 100.7 ms; about 990,477 bytes per burst. So at 200K
events/s streamkit receives ~20K events every ~100 ms, in 64 KiB pieces
(1,186 events per `read(2)`, the "input reads" column). The stamp is taken
per piece, so the clock starts when the piece lands in the process, not
when gen generated the event.

### Main table (harness, 20 s runs)

```
gen --n 4000000 --rate 200000 --pace --seed 1 --out - \
  | streamkit --in - --out $OUT --window-s 1 --lateness-s 2 --threads P --latency-stats
```

01:16 to 01:18 EDT. 100K campaigns, so each 1 s window has about 86.3K
distinct campaigns and each worker closes 86.3K / P windows in one call
when its watermark crosses a second boundary.

| threads | run | achieved events/s | watermark-closed windows | input reads (events/read) | min us | p50 us | p99 us | max us |
|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 199,055 | 1,460,439 | 3,372 (1,186) | 2,723 | 13,441 | 23,136 | 24,943 |
| 1 | 2 | 199,046 | 1,460,439 | 3,372 (1,186) | 2,494 | 13,313 | 23,328 | 24,487 |
| 1 | 3 | 199,046 | 1,460,439 | 3,372 (1,186) | 2,578 | 14,021 | 23,961 | 25,374 |
| 1 | **median** | | | | | **13,441** | **23,328** | |
| 8 | 1 | 199,021 | 1,460,444 | 3,372 (1,186) | 41 | 6,614 | 12,750 | 13,173 |
| 8 | 2 | 198,998 | 1,460,444 | 3,372 (1,186) | 52 | 6,702 | 12,863 | 13,368 |
| 8 | 3 | 199,019 | 1,460,444 | 3,373 (1,185) | 38 | 6,672 | 12,919 | 13,786 |
| 8 | **median** | | | | | **6,672** | **12,863** | |

So the engine adds **p50 6.7 ms, p99 12.9 ms at 8 threads** and **p50 13.4
ms, p99 23.3 ms at 1 thread** on top of the configured 2 s lateness, at
200K events/s with 86K windows closing per second. The p50 is half the
p99 in every run: the samples are spread almost uniformly from the min to
the max, which is the signature of rows written one after another in a
single closing call.

### Decomposition (10 s runs, 2M events, one run each)

Same command with `--n 2000000` and the stated changes. 01:19 to 01:35
EDT. "rows per worker per boundary" is the run's total windows / 10 s / P
(100K campaigns: 862,950 windows in 10 s; 200K: 1,232,411; 50K: 504,381;
10K: 108,910; 1K: 11,294). "us per row" is (p99 - floor) / rows, with
floor = the p99 of the 1K-campaign run at that thread count (3.0 ms at
P=1, 1.3 ms at P=8; the P=8 floor is used for P >= 2).

| variant | threads | rows per worker per boundary | min us | p50 us | p99 us | max us | us per row at p99 | per-worker busy (whole run) |
|---|---|---|---|---|---|---|---|---|
| 1K campaigns (`gen --campaigns 1000`) | 8 | 141 | 27 | 796 | 1,274 | 1,293 | (floor) | |
| 1K campaigns | 1 | 1,129 | 868 | 2,416 | 3,001 | 3,014 | (floor) | |
| 10K campaigns | 8 | 1,361 | 34 | 1,006 | 1,863 | 1,958 | 0.41 | |
| 50K campaigns | 8 | 6,305 | 29 | 3,618 | 7,406 | 7,986 | 0.97 | 0.08 to 0.09 s |
| 100K campaigns (baseline) | 8 | 10,787 | 57 | 6,295 | 12,725 | 13,068 | 1.06 | |
| 100K campaigns, repeat | 8 | 10,787 | 30 | 6,673 | 13,308 | 13,946 | 1.11 | 0.14 to 0.15 s |
| 200K campaigns | 8 | 15,405 | 50 | 8,578 | 16,697 | 17,337 | 1.00 | 0.16 to 0.17 s |
| 100K, `--no-dedup` | 8 | 10,787 | 18 | 5,789 | 11,880 | 12,717 | 0.98 | |
| 100K, `--batch 16` | 8 | 10,787 | 24 | 6,306 | 12,964 | 13,484 | 1.08 | |
| 100K campaigns | 1 | 86,295 | (see main table) | 13,441 | 23,328 | | 0.24 | |
| 100K campaigns | 2 | 43,147 | 107 | 7,198 | 12,373 | 13,013 | 0.26 | |
| 100K campaigns | 4 | 21,574 | 17 | 4,555 | 8,419 | 8,853 | 0.33 | 0.15 s (repeat: 21 / 4,566 / 8,480 / 9,376) |
| 100K campaigns | 5 | 17,259 | 16 | 5,542 | 11,516 | 12,388 | 0.59 | 0.15 s |
| 100K campaigns | 6 | 14,383 | 30 | 6,011 | 11,493 | 12,013 | 0.71 | 0.14 to 0.15 s |
| 100K campaigns | 12 | 7,191 | 35 | 6,206 | 12,483 | 13,130 | 1.55 | 0.12 to 0.13 s |

What the table says, in order:

1. **The floor is the producer's burst.** With 1K campaigns (141 rows per
   worker per boundary, nothing to speak of) the p99 is still 1.27 ms at 8
   threads and 3.0 ms at 1 thread, and the distribution is again uniform
   from the min. That is the closing event waiting behind the other events
   of the same ~20K-event burst from gen: one worker processes 20K events
   in about 3 ms (6.7M events/s, the no-dedup single-worker rate), eight
   workers 2.5K each in about 1.3 ms (slower per event than at full load:
   the workers wake from a 50 us nap into cold caches). An engine measured
   against a producer that flushed per event would not show this term.
2. **The rest is rows, written one after another.** 86K windows close on
   the same event-time boundary, so each worker writes 86K / P rows in one
   `close_ready` call and the last row waits for all the others. `--no-dedup`
   (removes the per-second dedup bucket expiry from the call) and `--batch 16`
   change the p99 by under 1 ms, so neither is the cost.
3. **The per-row cost is 0.24 us when one worker writes and 1.0 to 1.1 us
   when eight write at once**, and at eight threads it is the same 1.0 us
   per row at 6.3K, 10.8K and 15.4K rows per burst, so it is a per-row cost
   that grows with the number of workers emitting concurrently, not a fixed
   per-burst cost (a serialized 1 MiB `write(2)` per worker was the other
   candidate: it would have shown up as a constant, and it does not). The
   per-worker busy time says the same thing from the other side: the same
   total work costs 4 x 0.15 s of worker time at P=4 and 8 x 0.145 s at
   P=8. What is in the row path is `fprintf` into the worker's own FILE
   (which takes that FILE's lock and runs libc's formatter) and
   `std::map::erase`, which frees a node through the process-wide
   allocator. One of those two scales badly across threads on this libc;
   telling them apart needs a code change (a hand-rolled integer formatter
   into the worker's own buffer, or a bulk erase) and is the first item
   under "what I would change" in NOTES.md.
4. **The consequence is a sweet spot at 4 threads for this workload**: p99
   8.4 ms at P=4 against 12.4 ms at P=2, 11.5 ms at P=5 and 6, 12.9 ms at
   P=8, 12.5 ms at P=12. Beyond four workers the drop in rows per worker is
   cancelled by the rise in cost per row. The 6 + 12 core split of the M5
   Pro was checked as an explanation and does not fit: P=5 (six threads
   with the reader, fitting the six top-tier cores) and P=6 (seven) give
   the same 11.5 ms.

### Under the sanitizers (5 s at 100K events/s, 500K events, 4 and 8 threads)

Same command with `--n 500000 --rate 100000` against the ASan+UBSan and
TSan binaries, 01:26 to 01:28 EDT. Exit 0 and zero lines matching
`Sanitizer|runtime error|data race|WARNING` in all four runs; 310,446
windows written and 131,486 sampled in each.

| build | threads | min us | p50 us | p99 us | max us |
|---|---|---|---|---|---|
| ASan+UBSan | 4 | 129 | 5,705 | 10,890 | 11,252 |
| ASan+UBSan | 8 | 116 | 4,748 | 9,921 | 10,352 |
| TSan | 4 | 169 | 22,276 | 42,832 | 44,042 |
| TSan | 8 | 279 | 24,656 | 50,986 | 52,540 |

(The numbers are the instrumented binaries' own latency and are listed
only to show the path ran clean under both tools.)

## 6. Correctness regression on the final tree

- **Warnings.** `touch` every source this track owns, then
  `cmake --build <dir> --target streamkit streamkit_tests adstack_tests
  oracle gen evdump` in `build-sk`, `build-sk-asan`, `build-sk-tsan`: 10
  translation units recompiled per tree before the m5 edits and 5 after,
  0 lines matching `warning:` and 0 matching `error:` in all six builds.
- **Unit tests.** `build-sk/bin/adstack_tests` and
  `build-sk/bin/streamkit_tests` run directly, and the same binaries in the
  ASan+UBSan and TSan trees: 16 + 63 = 79 passed, 0 failed, in all three
  builds (8 of them `Latency.*`, one new this milestone:
  `InputReadsCountsBlocksOnAFile`).
- **`bench/smoke.sh build-sk`** (200K events, seed 7, 1% dups), 01:26 EDT:
  PASS at 1, 2, 4 and 8 threads, tumbling 60 s (92,609 windows identical to
  the oracle) and sliding 300 s / 60 s (436,985 identical); 1,951 dups in
  every run, equal to the oracle's 1,951 dropped.
- **`bench/crash.sh build-sk 50`** (2M events seed 11, 4 threads,
  checkpoint every 100K events, reader throttled to 400K events/s, SIGKILL
  after 0.2 to 3.0 s, `--resume` until exit 0, `compare.sh` against the
  oracle), 01:26 to 01:31 EDT: **50/50 PASS, 392,549 windows identical in
  every trial, 125 kills in total, 44 of 50 trials killed at least twice**
  (6 trials killed once, 22 twice, 14 three times, 7 four times, 1 five
  times; none finished unkilled). No trial left a part file, a `.tmp` output
  or a checkpoint directory behind.
- **Sanitizers on the paced path**: section 5, last table.

## 7. Reproducing

```
cmake -S . -B build-sk -G Ninja -DCMAKE_BUILD_TYPE=Release && cmake --build build-sk
bench/throughput.sh build-sk                      # sections 1 to 5, about 3.5 minutes
bench/throughput.sh build-sk --only threads       # any subset: threads,dedup,checkpoint,memory,latency
bench/throughput.sh build-sk --reader-decode      # extra flags go to every streamkit run
bench/smoke.sh build-sk
bench/crash.sh build-sk 50
```

The harness prints the machine, build, load average, input and exact
command above every table, caches the generated inputs in
`/private/tmp/adstack-sk` (1.1 GB), and removes its own outputs on exit.
