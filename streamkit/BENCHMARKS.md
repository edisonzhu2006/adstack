# streamkit benchmarks

Every number on this page comes from a command run on 2026-10-21 between
12:30 and 13:20 EDT on the machine below, on the tree as it stands at the
end of milestone 5. Sections 1 to 5 are the output of
`bench/throughput.sh build-sk` (the harness prints the machine, build, load
average, input and exact command above every table) plus three paced runs
written out in section 5; the regression results in section 6 are from
`bench/smoke.sh`, `bench/crash.sh`, `ctest` and the sanitizer builds.
Nothing from an earlier session is quoted here; the decision log in
[NOTES.md](NOTES.md) is where the history lives.

## Machine, build, and the load during the runs

- Apple M5 Pro, 18 cores (6 "Super" + 12 "Performance" per
  `sysctl hw.perflevel0/1`), 24 GB, macOS 26.4.1 (Darwin 25.4.0), APFS on
  the internal SSD
- Apple clang 21.0.0 (clang-2100.0.123.102), `-O3` (CMake Release),
  project flags `-Wall -Wextra -Wpedantic -Wshadow -Wconversion`
- `build-sk` configured with
  `cmake -S . -B build-sk -G Ninja -DCMAKE_BUILD_TYPE=Release`
- **The machine was shared for most of the session.** The other track's
  benchmark (`build-cs/bin/colstore_sqlite`) ran at 100% of a core from
  before 12:30 until about 13:00, and `fseventsd` at 85 to 90% throughout
  (checked with `ps` before each run). A background poll waited 14 minutes
  for the other benchmark to stop and the load to fall below 3 before
  starting run E; the neighbour's activity came back intermittently after
  that (run E's dedup and checkpoint sections, run F's 8-thread clean
  cells). The load average was a poor predictor: the run that started at
  3.77 produced the slowest cells. So the timing sections were run several
  times; every run is listed, the run-to-run spread is in every table, and
  the text under each table says which cells are stable and which are not.
- Inputs: `gen --n 10000000 --seed 1` (520,000,008 bytes: 1% duplicates,
  5% late by 1 to 30 s, 100K campaigns, 10K events per simulated second),
  the same with `--dup-frac 0` ("clean"), and `gen --n 1000000 --seed 1`
  (52,000,008 bytes). Generated at 12:31 and cached in
  `/private/tmp/adstack-sk` by the harness.
- Method: three runs per cell, the median (middle value) in bold, all three
  shown. Events/s is events divided by wall time including the final merge
  of the per-worker part files, as reported on streamkit's own stderr
  summary.

The harness runs this page draws on:

| run | start (EDT) | sections | load average at start | what else was running |
|---|---|---|---|---|
| A | 12:31 | all five (the latency section stopped after the second 8-thread run, see section 5) | 5.24 | colstore_sqlite 100%, fseventsd 87% |
| B | 12:37 | latency | 6.49 | same |
| C | 12:42 | threads, dedup, checkpoint | 4.74 | same |
| D | 12:54 | threads, dedup, checkpoint, latency | 3.77 | same |
| E | 13:13 | threads, dedup, checkpoint, latency | 2.96 | fseventsd 87%; colstore_sqlite gone at start, something returned during the dedup section |
| F | 13:17 | dedup | 3.93 | fseventsd 88% |

## 1. Throughput vs threads

```
streamkit --in $DIRTY --out $OUT --lateness-s 60 --threads P
```

Run E (13:13), the quietest, with the three runs per cell within 2% of
each other at 1 and 8 threads:

| threads | run 1 | run 2 | run 3 | median events/s | speedup vs 1 | reader stalls (median run) | worker idle waits (median run) |
|---|---|---|---|---|---|---|---|
| 1 | 4,908,875 | 4,968,692 | 4,991,532 | **4,968,692** | 1.00x | 71,556 | 1 |
| 2 | 8,962,893 | 9,360,907 | 9,444,794 | **9,360,907** | 1.88x | 32,073 | 2 |
| 4 | 18,682,390 | 18,530,253 | 18,664,712 | **18,664,712** | 3.76x | 11,555 | 4 |
| 8 | 27,990,228 | 27,983,923 | 28,475,649 | **27,990,228** | 5.63x | 1,948 | 227 |

The same section in runs A, C and D (medians; all three values per cell
are in the harness outputs):

| threads | run A (12:31) | speedup | run C (12:42) | speedup | run D (12:54) | speedup |
|---|---|---|---|---|---|---|
| 1 | **4,584,638** | 1.00x | **2,923,134** | 1.00x | **2,632,637** | 1.00x |
| 2 | **8,859,232** | 1.93x | **5,468,398** | 1.87x | **4,808,859** | 1.83x |
| 4 | **15,418,282** | 3.36x | **8,950,094** | 3.06x | **7,416,702** | 2.82x |
| 8 | **21,290,249** | 4.64x | **13,470,720** | 4.61x | **11,888,600** | 4.52x |

Reading it: **4.97M events/s at 1 thread, 9.36M at 2, 18.7M at 4, 28.0M at
8 (5.63x)** on the quiet run. The shape is the same in all four runs (1.83
to 1.93x at 2 threads, 2.8 to 3.8x at 4, 4.5 to 5.6x at 8; reader stalls
fall from ~72K to ~2K and idle waits rise, so the workers are the
bottleneck until 8 threads and then only just), while the absolute level
moved by almost 2x between runs A, C and D on the same binary and input.
A single thread does not care about a load average of 4 on 18 cores, so
what moved it is the neighbour's effect on the memory system and the SSD,
not CPU time; run E is what the engine does when that stops.

## 2. Dedup overhead

```
streamkit --in $STREAM --out $OUT --lateness-s 60 --threads P [--no-dedup]
```

`--no-dedup` skips the per-event hash-set insert (and therefore counts
duplicates twice; it exists only for this measurement). Overhead =
(no-dedup median - dedup median) / no-dedup median, same stream and P.

Run F (13:17), the one run whose 1-thread cells agree to within 2% across
the three runs:

| stream | threads | dedup | run 1 | run 2 | run 3 | median events/s | dedup overhead |
|---|---|---|---|---|---|---|---|
| dirty (1% dups) | 1 | off | 6,736,434 | 6,789,975 | 6,778,647 | **6,778,647** | (baseline) |
| dirty (1% dups) | 1 | on | 5,035,224 | 5,022,584 | 5,085,722 | **5,035,224** | 25.7% |
| clean (0 dups) | 1 | off | 6,743,228 | 6,654,838 | 6,727,945 | **6,727,945** | (baseline) |
| clean (0 dups) | 1 | on | 4,991,555 | 4,973,327 | 4,960,226 | **4,973,327** | 26.1% |
| dirty (1% dups) | 8 | off | 31,446,467 | 31,517,124 | 31,982,307 | **31,517,124** | (baseline) |
| dirty (1% dups) | 8 | on | 25,078,540 | 26,838,460 | 27,028,062 | **26,838,460** | 14.8% |
| clean (0 dups) | 8 | off | 25,979,582 | 23,748,614 | 22,104,247 | **23,748,614** | (baseline) |
| clean (0 dups) | 8 | on | 18,170,429 | 16,573,663 | 17,237,147 | **17,237,147** | 27.4% |

In absolute terms at 1 thread: 1/5,035,224 - 1/6,778,647 = 198.6 - 147.5 =
**51 ns per event on the dirty stream** and 201.1 - 148.6 = **52 ns on the
clean one**. That is one hash-set insert whose table does not fit in cache,
and it costs the same whether or not a duplicate is found: the clean
stream has 1% more unique ids to store and no hits, and the number does
not move. At 8 threads the dirty cells are tight (31.4 to 32.0M off, 25.1
to 27.0M on, 14.8%) while the clean cells drifted during the run (22.1 to
26.0M off), which is the neighbour, not the stream: the two 8-thread
"on" medians differ by 9.6M for a 1% difference in input.

The same section in the four earlier runs (medians), kept so the noise is
on the record rather than filtered out:

| stream | threads | run A off / on (overhead) | run C | run D | run E |
|---|---|---|---|---|---|
| dirty | 1 | 3,130,390 / 2,762,974 (11.7%) | 4,461,515 / 2,840,935 (36.3%) | 3,109,027 / 1,710,762 (45.0%) | 6,699,658 / 3,107,624 (53.6%) |
| clean | 1 | 2,779,872 / 3,468,539 (-24.8%) | 4,009,599 / 3,000,783 (25.2%) | 2,497,986 / 1,563,279 (37.4%) | 3,460,977 / 3,056,602 (11.7%) |
| dirty | 8 | 19,203,727 / 7,697,820 (59.9%) | 21,882,917 / 10,647,476 (51.3%) | 12,326,437 / 7,528,256 (38.9%) | 22,373,394 / 7,367,691 (67.1%) |
| clean | 8 | 19,177,118 / 13,433,989 (29.9%) | 17,096,554 / 13,526,989 (20.9%) | 11,890,404 / 8,445,476 (29.0%) | 18,050,228 / 15,630,184 (13.4%) |

Run E's dirty / 1 / on cell is the clearest picture of what the noise
looked like: 4,659,314, 3,107,624 and 1,366,939 events/s for three
identical runs a few seconds apart, right after the threads section of
the same harness run had given 1-thread cells within 2% of each other.
A 1-thread dedup cell is a cache-missing hash insert per event, the
operation most sensitive to a neighbour thrashing memory, which is why
this section needed six attempts for one clean table and the threads
section only two.

## 3. Checkpoint overhead

```
streamkit --in $DIRTY --out $OUT --lateness-s 60 --threads 8 [--ckpt-dir $D --checkpoint-every-events N]
```

Snapshot columns are from the `checkpoints:` line of the median run:
snapshots = per-worker snapshots written (8 per epoch); slowest = one
snapshot, its three fsyncs included; total = sum over all snapshots of all
workers.

Run E (13:13):

| checkpoint interval | run 1 | run 2 | run 3 | median events/s | vs none | snapshots | slowest snapshot | snapshot time total |
|---|---|---|---|---|---|---|---|---|
| none | 14,340,081 | 13,168,321 | 14,197,552 | **14,197,552** | (baseline) | - | - | - |
| every 1,000,000 events (default, 10 epochs) | 10,390,298 | 9,696,183 | 9,340,744 | **9,696,183** | -31.7% | 80 | 57.5 ms | 3.111 s |
| every 100,000 events (crash-harness setting, 100 epochs) | 2,246,229 | 2,187,303 | 2,338,665 | **2,246,229** | -84.2% | 800 | 64.8 ms | 27.580 s |

Runs A, C and D (medians):

| checkpoint interval | run A | vs none | slowest / total | run C | vs none | slowest / total | run D | vs none | slowest / total |
|---|---|---|---|---|---|---|---|---|---|
| none | **13,704,742** | | | **14,837,748** | | | **8,426,988** | | |
| every 1,000,000 | **10,749,669** | -21.6% | 44.3 ms / 2.418 s | **10,039,172** | -32.3% | 56.5 ms / 2.673 s | **6,561,309** | -22.1% | 58.0 ms / 3.169 s |
| every 100,000 | **2,312,846** | -83.1% | 61.7 ms / 26.752 s | **2,302,177** | -84.5% | 59.1 ms / 27.195 s | **1,733,205** | -79.4% | 68.8 ms / 33.883 s |

Two things to read here. First, the "none" baseline is the identical
command to the 8-thread row of section 1, and in run E it gave 14.2M
against 28.0M two minutes earlier in the same harness run (the dedup
section between them had already gone noisy, see above); the neighbour's
return is what the percentages in the "vs none" column are measuring as
much as the checkpoints, which is why they range from -22% to -32% for
the same work. Second, the stable number is the cost of one snapshot:
total / 80 = 30.2 ms (A), 33.4 (C), 39.6 (D), 38.9 (E) at the default
interval and 33.4 / 34.0 / 42.4 / 34.5 ms at the 100K interval (serialize
~2.3 MB per worker, three F_FULLFSYNCs, eight workers hitting the same
SSD at once). The arithmetic that says the snapshot is the whole cost, per
run: 10 epochs x per-snapshot cost = 0.30 / 0.33 / 0.40 / 0.39 s, against
a wall-time increase of 0.20 / 0.32 / 0.34 / 0.33 s (10M events at the
"none" rate against the 1M-interval rate), so nothing else slowed down and
part of the snapshot time overlapped with the reader filling the queues.
At benchmark speed the default interval is a checkpoint every 70 to 120 ms
of wall time, which no one would run; at the generator's own rate (10K
events/s simulated, which `--pace` replays) the same interval is one
checkpoint every 100 s and 30 to 40 ms per 100 s is 0.03 to 0.04%. The
100K row is the density the crash harness needs so that a random SIGKILL
lands mid-epoch; it is not a configuration to run in.

## 4. Memory

```
/usr/bin/time -l streamkit --in $INPUT --out $OUT --lateness-s 60 --threads P --queue-depth Q
```

Maximum resident set size in bytes. Peak dedup ids and peak open windows
are from the median run's summary, summed over workers. Run A (started
12:31); RSS does not depend on the load, and the three values per cell
agree to within 1% except the 10M / 8 / 256 cell (118 to 136 MB, allocator
slack, see below), so this section was run once.

| input | threads | queue depth | run 1 | run 2 | run 3 | median max RSS (bytes) | MiB | peak dedup ids | peak open windows |
|---|---|---|---|---|---|---|---|---|---|
| 1M events | 1 | 4096 | 96,993,280 | 97,009,664 | 96,993,280 | **96,993,280** | 92.5 | 989,273 | 197,664 |
| 1M events | 8 | 4096 | 136,052,736 | 136,544,256 | 136,167,424 | **136,167,424** | 129.9 | 989,273 | 197,664 |
| 1M events | 8 | 256 | 91,619,328 | 91,357,184 | 91,291,648 | **91,357,184** | 87.1 | 989,273 | 197,664 |
| 10M events | 1 | 4096 | 109,068,288 | 108,167,168 | 107,741,184 | **108,167,168** | 103.2 | 1,189,660 | 199,484 |
| 10M events | 8 | 4096 | 362,496,000 | 358,318,080 | 364,642,304 | **362,496,000** | 345.7 | 1,195,108 | 199,569 |
| 10M events | 8 | 256 | 135,708,672 | 118,128,640 | 124,338,176 | **124,338,176** | 118.6 | 1,195,108 | 199,569 |

Reading it: ten times the events costs 11.5% more RSS at 1 thread (92.5
to 103.2 MiB), and the state counters say why it is not more: the dedup
set holds 1.19M ids at 10M events against 0.99M at 1M (the horizon is
lateness + window = 120 s of event time, so a 100 s stream never expires
anything while a 1000 s stream is in steady state at about 120 s x 9,900
unique ids/s), and the open-window map holds ~200K windows in both. At 8
threads the default queues dominate: 4096 slots x 128 events x 52 bytes =
27,262,976 bytes per worker, touched only once the reader has run that far
ahead, which at 1M events (7.8K batches over 32K slots) it never fully
does, and at 10M events it always does. `--queue-depth 256` takes the
queues out of the picture and gives 87 to 119 MiB at 8 threads. The
remaining ~31 MiB between 1M and 10M at 8 threads with shallow queues is
~10 MiB of extra dedup ids (206K x 48 bytes) and allocator slack: at 1M
events no dedup bucket ever expires, so nothing is freed; at 10M each
worker frees and re-allocates about 1,200 ids per event-second and the
per-thread malloc caches keep what they freed. Whether that slack keeps
growing past 10M is not measured here.

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
before the bytes reach the pipe (gen's writer holds 1 MiB, so at 200K
events/s the pipe receives ~1 MB bursts every ~100 ms which the reader
consumes in 64 KiB pieces, the "input reads" column: 3,372 reads of 1,186
events for 4M events); it does not include the stdio flush of the row to
disk, fsync, or the final merge of part files; and windows closed by end
of input are not sampled. Rows are stamped after each `fprintf`, so a row
that is behind others in the same closing call is charged for the rows
written before it; that is deliberate (it is when the row became
available) and it is what makes the distribution uniform from min to max.
The code is `streamkit/latency.h` and the three stamping lines in
`pipeline.cpp`; nothing is read or stored unless `--latency-stats` is
given. The full definition and the reasons for it are in NOTES.md (m5).

### Main table (harness, 20 s runs)

```
gen --n 4000000 --rate 200000 --pace --seed 1 --out - \
  | streamkit --in - --out $OUT --window-s 1 --lateness-s 2 --threads P --latency-stats
```

100K campaigns, so each 1 s window has about 86.3K distinct campaigns and
each worker closes 86.3K / P windows in one call when its watermark
crosses a second boundary. Run E (13:13), the quietest:

| threads | run | achieved events/s | watermark-closed windows | input reads (events/read) | min us | p50 us | p99 us | max us |
|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 199,005 | 1,460,439 | 3,372 (1,186) | 2,773 | 14,355 | 25,800 | 27,098 |
| 1 | 2 | 198,916 | 1,460,439 | 3,372 (1,186) | 2,831 | 14,735 | 25,496 | 26,687 |
| 1 | 3 | 198,981 | 1,460,439 | 3,372 (1,186) | 3,026 | 13,907 | 24,391 | 25,596 |
| 1 | **median** | | | | | **14,355** | **25,496** | |
| 8 | 1 | 198,956 | 1,460,444 | 3,373 (1,185) | 32 | 8,004 | 16,290 | 17,924 |
| 8 | 2 | 198,718 | 1,460,444 | 3,372 (1,186) | 41 | 7,906 | 16,094 | 16,722 |
| 8 | 3 | 198,759 | 1,460,444 | 3,372 (1,186) | 45 | 7,465 | 14,272 | 15,331 |
| 8 | **median** | | | | | **7,906** | **16,094** | |

So the engine adds **p50 7.9 ms, p99 16.1 ms at 8 threads** and **p50 14.4
ms, p99 25.5 ms at 1 thread** on top of the configured 2 s lateness, at
200K events/s with 86K windows closing per second. The p50 is about half
the p99 in every run: the samples are spread almost uniformly from the min
to the max, the signature of rows written one after another in a single
closing call.

The same section in runs A, B and D (p50 / p99 in microseconds per run,
then the medians):

| run | threads | run 1 | run 2 | run 3 | median p50 | median p99 |
|---|---|---|---|---|---|---|
| A (started 12:31) | 1 | 14,094 / 24,120 | 14,198 / 25,557 | 14,464 / 24,515 | **14,198** | **24,515** |
| A (started 12:31) | 8 | 8,861 / 48,223 | 11,577 / 23,877 | (not run, see below) | | |
| B (12:37) | 1 | 16,974 / 30,632 | 16,391 / 31,007 | 18,955 / 34,284 | **16,974** | **31,007** |
| B (12:37) | 8 | 9,538 / 21,391 | 10,065 / 21,078 | 8,825 / 19,528 | **9,538** | **21,078** |
| D (started 12:54) | 1 | 29,433 / 49,733 | 28,057 / 55,132 | 27,216 / 48,123 | **28,057** | **49,733** |
| D (started 12:54) | 8 | 11,475 / 26,380 | 11,999 / 23,788 | 11,425 / 23,900 | **11,475** | **23,900** |

Two more 8-thread runs taken by hand at 12:36 while diagnosing run A's
exit (same command, exit statuses gen 0 / streamkit 0 both times): p50
8,042 / p99 30,075 us and p50 11,348 / p99 22,459 us.

Across the thirteen 20 s 8-thread runs the p50 stayed between 7.5 and 12.0 ms
and the p99 between 14.3 and 26.4 ms, with one outlier at 30 and one at
48 ms during the noisiest minutes; the 1-thread cells moved with the
machine exactly as the throughput cells did (p50 14 ms in A and E, 28 ms
in D). With eight workers each one's share of a 20K-event burst is 2.5K
events, so most of a sample is the producer's 100 ms burst pattern plus
the rows written before it in the same closing call, neither of which is
CPU time; with one worker the whole burst is CPU time and the neighbour
shows.

Run A's latency section stopped after the second 8-thread run: the
harness exited 141 (SIGPIPE) and its trap had already removed the log.
`gen | streamkit` was rerun by hand eight times afterwards (20 s and 5 s
runs) with both exit statuses captured and never failed, so the cause is
not known; the harness now checks both statuses of the paced pipeline,
prints them with streamkit's stderr if a run fails, and retries once
(`lat_run` in `bench/throughput.sh`), so a recurrence will be diagnosable
and will not lose the rest of the run.

### Decomposition (10 s runs, 2M events, one run each, 13:18 EDT)

Same command with `--n 2000000` and `--campaigns` as stated. "rows per
worker per boundary" is the run's total windows / 10 s / P (100K campaigns:
862,948 windows in 10 s, so 86,295 per second; 1K campaigns: 11,293, so
1,129). "us per row at p99" is (p99 - floor) / rows, with floor = the p99
of the 1K-campaign run at that thread count (4.6 ms at P=1, 1.7 ms at
P=8; the P=8 floor is used for P=4). The 100K-campaign P=1 and P=8 rows
are run E's medians from the table above.

| variant | threads | rows per worker per boundary | min us | p50 us | p99 us | max us | us per row at p99 |
|---|---|---|---|---|---|---|---|
| 1K campaigns (`--campaigns 1000`) | 1 | 1,129 | 1,349 | 3,608 | 4,647 | 4,667 | (floor) |
| 1K campaigns | 8 | 141 | 43 | 1,076 | 1,689 | 1,715 | (floor) |
| 100K campaigns | 1 | 86,295 | (run E) | 14,355 | 25,496 | | 0.24 |
| 100K campaigns | 4 | 21,574 | 32 | 6,182 | 11,209 | 11,623 | 0.44 |
| 100K campaigns | 8 | 10,787 | (run E) | 7,906 | 16,094 | | 1.34 |

What the table says:

1. **The floor is the producer's burst.** With 1K campaigns (141 rows per
   worker per boundary, nothing to speak of) the p99 is still 1.7 ms at 8
   threads and 4.6 ms at 1 thread, and the distribution is again uniform
   from the min. That is the closing event waiting behind the other events
   of the same ~20K-event burst from gen: one worker processes 20K events
   in about 3 to 4 ms, eight workers 2.5K each in about 1.5 ms (slower
   per event than at full load: the workers wake from a 50 us nap into
   cold caches). An engine measured against a producer that flushed per
   event would not show this term.
2. **The rest is rows, written one after another.** 86K windows close on
   the same event-time boundary, so each worker writes 86K / P rows in one
   `close_ready` call and the last row waits for all the others.
3. **The per-row cost is 0.24 us when one worker writes, 0.44 us when four
   do, and 1.34 us when eight write at once.** A per-row cost that grows
   with the number of workers emitting concurrently is a contention, not
   a fixed per-burst cost. What is in the row path is `fprintf` into the
   worker's own FILE (which takes that FILE's lock and runs libc's
   formatter) and `std::map::erase`, which frees a node through the
   process-wide allocator; one of those scales badly across threads on
   this libc, and telling them apart needs a code change (NOTES.md, "what
   I would change next").
4. **The consequence is that 4 threads give the lowest p99 for this
   workload**: 11.2 ms at P=4 against 16.1 ms at P=8 and 25.5 ms at P=1.
   Past four workers the drop in rows per worker is cancelled by the rise
   in cost per row. Throughput and emit latency have different optimal P
   on this machine, and the harness is what shows it.

### Under the sanitizers (5 s at 100K events/s, 500K events, 4 and 8 threads)

Same command with `--n 500000 --rate 100000 --late-out`, against the
ASan+UBSan and TSan binaries, 12:51 EDT. Exit 0 for gen and streamkit and
zero lines matching `Sanitizer|runtime error|data race|WARNING` in all
four runs; 310,446 windows written and 131,486 sampled in each.

| build | threads | min us | p50 us | p99 us | max us |
|---|---|---|---|---|---|
| ASan+UBSan | 4 | 156 | 7,893 | 15,282 | 15,762 |
| ASan+UBSan | 8 | 183 | 6,326 | 13,151 | 13,674 |
| TSan | 4 | 223 | 64,499 | 153,302 | 157,154 |
| TSan | 8 | 416 | 38,470 | 79,898 | 82,241 |

(The numbers are the instrumented binaries' own latency and are listed
only to show the path ran clean under both tools.)

## 6. Correctness regression on the final tree

- **Warnings.** `touch` every source this track owns (`streamkit/*.cpp`,
  `streamkit/*.h`, `streamkit/tests/*.cpp`, `common/*`, `tools/oracle.cpp`,
  `tests/*.cpp`), then `cmake --build <dir> --target streamkit
  streamkit_tests adstack_tests oracle gen evdump` in `build-sk`,
  `build-sk-asan`, `build-sk-tsan` at 12:30: 16 translation units
  recompiled per tree, 0 lines matching `warning:` and 0 matching `error:`
  in all three builds.
- **Unit tests.** `build-sk/bin/adstack_tests` and
  `build-sk/bin/streamkit_tests` run directly, and the same binaries in
  the ASan+UBSan and TSan trees: 16 + 63 = 79 passed, 0 failed, 0
  sanitizer lines, in all three builds (8 of them `Latency.*`). `ctest
  --test-dir <dir> -R` over this track's suites: 100% of 79 passed in each
  of the three trees.
- **`bench/smoke.sh build-sk`** (200K events, seed 7, 1% dups), 12:44 EDT:
  PASS at 1, 2, 4 and 8 threads, tumbling 60 s (92,609 windows identical to
  the oracle) and sliding 300 s / 60 s (436,985 identical); 1,951 dups in
  every run, equal to the oracle's 1,951 dropped; 0 late (lateness 60 s
  covers the generator's 30 s maximum).
- **`bench/crash.sh build-sk 50`** (2M events seed 11, 4 threads,
  checkpoint every 100K events, reader throttled to 400K events/s, SIGKILL
  after 0.2 to 3.0 s, `--resume` until exit 0, `compare.sh` against the
  oracle), 12:44 to 12:50 EDT: **50/50 PASS, 392,549 windows identical in
  every trial, 138 kills in total, 47 of 50 trials killed at least twice**
  (3 trials killed once, 21 twice, 16 three times, 6 four times, 3 five
  times, 1 six times; none finished unkilled). No trial left a part file,
  a `.tmp` output or a checkpoint directory behind.
- **Sanitizers on the paced path**: section 5, last table.

## 7. Reproducing

```
cmake -S . -B build-sk -G Ninja -DCMAKE_BUILD_TYPE=Release && cmake --build build-sk
bench/throughput.sh build-sk                      # sections 1 to 5, about 4 minutes
bench/throughput.sh build-sk --only threads       # any subset: threads,dedup,checkpoint,memory,latency
bench/throughput.sh build-sk --reader-decode      # extra flags go to every streamkit run
bench/smoke.sh build-sk
bench/crash.sh build-sk 50
```

The harness prints the machine, build, load average, input and exact
command above every table, caches the generated inputs in
`/private/tmp/adstack-sk` (1.1 GB), and removes its own outputs on exit.
Check `ps` for other benchmarks before trusting a timing section (the
1-thread dedup cells are the first to go); the memory section and the
crash harness do not care.
