# streamkit

Streaming aggregation engine for ad events: reads events in ingest order,
drops duplicates, assigns each event to its window(s) by event time, closes
windows with an event-time watermark, and writes one CSV row per
(window, campaign). Events whose window already closed are written to a side
file and counted, never dropped silently (but see "The late side file"
below: it is not deduplicated past the dedup horizon). Output is byte-compatible with
`tools/oracle`, the batch ground truth, so the two can be diffed.

From m3 the engine is multithreaded: one reader thread hash-partitions
events by `campaign_id` into bounded single-producer single-consumer queues,
and P worker threads each own a dedup set, an open-window map, a watermark
and an output part file. No mutable state of ours is shared on the hot
path; the process-wide allocator and libc (malloc/free per dedup id and
window, `fprintf` per row) still are, and NOTES.md m5 measures a per-row
cost that grows with the worker count, which that shared machinery is the
likely cause of.

From m4 the engine checkpoints: every N events the reader drops a marker
into every queue, each worker snapshots its state atomically when the marker
reaches it, and a MANIFEST rename commits the epoch. `--resume` continues
from the last commit. `bench/crash.sh` SIGKILLs the process at random
points, resumes until it exits 0, and checks two things per trial: the
same rows as the oracle (`compare.sh`, which sorts) and byte identity of
the CSV and the late file against one uninterrupted run with the same
flags (`cmp`). The MANIFEST pins the input (size plus a hash of its first
and last MiB), the thread count, the window parameters, the checkpoint
interval and whether `--late-out` is on; a resume that differs in any of
them is refused with the field named.

Design decisions are in [NOTES.md](NOTES.md); every measured number, with
the machine, load, and command that produced it, is in
[BENCHMARKS.md](BENCHMARKS.md).

## Usage

```
streamkit --in FILE --out FILE [--window-s 60] [--slide-s WINDOW] [--lateness-s 0]
          [--late-out FILE] [--threads P] [--queue-depth 4096] [--batch 128]
          [--dedup-bucket-s 1] [--no-dedup] [--reader-decode]
          [--ckpt-dir DIR] [--checkpoint-every-events 1000000] [--resume] [--throttle-eps N]
          [--no-fsync] [--latency-stats] [--fail-after-checkpoint K]
```

Unknown flags and malformed numbers (`--threds 4`, `--lateness-s 1.5`) are
errors with exit 2; durations are whole seconds in [1, 2^50 us].

| flag | meaning |
|---|---|
| `--in` | event file in the shared `.ads` format, or `-` for stdin |
| `--out` | CSV output (`window_start_us,campaign_id,impressions,clicks,conversions,cost_micros`), or `-` for stdout. A file appears only when the run commits at end of input (parts are merged and renamed); with `-` each batch's rows are written and flushed to stdout as their windows close, so a consumer sees them during the run (no checkpoints in that mode) |
| `--window-s` | window length in seconds (default 60) |
| `--slide-s` | window hop in seconds; `< --window-s` gives overlapping sliding windows, each event counted in `window/slide` of them (default = window, i.e. tumbling) |
| `--lateness-s` | how long after a window's end events for it are still accepted; `watermark = max(event_time) - lateness`, per worker (default 0, close immediately) |
| `--late-out` | where events older than the watermark are written; if omitted they are only counted |
| `--threads` | worker threads; events are hash-partitioned by `campaign_id` (default: hardware threads, clamped to 8) |
| `--queue-depth` | batches buffered per worker before the reader waits; this is the backpressure, nothing is dropped (default 4096) |
| `--batch` | events per batch handed from the reader to a worker (default 128) |
| `--dedup-bucket-s` | width of the event-time buckets the dedup set expires by (default 1) |
| `--no-dedup` | count duplicates twice; exists only to measure the overhead of dedup |
| `--reader-decode` | decode records in the reader instead of the workers; exists only to measure where decode belongs |
| `--ckpt-dir` | write checkpoints here (`MANIFEST` + `epoch-k/worker-i.snap`); needs a seekable `--in` and a file `--out`; the directory is removed when the run completes |
| `--checkpoint-every-events` | input events between checkpoint markers (default 1,000,000) |
| `--resume` | continue from `--ckpt-dir`'s `MANIFEST` if there is one, otherwise start from byte 0; without this flag a checkpoint directory that holds state is an error |
| `--throttle-eps` | cap the reader at N events/s so a run lasts long enough to be killed mid-flight (crash tests; 0 = unlimited) |
| `--no-fsync` | skip every fsync in the checkpoint path, to measure what durability costs; a checkpoint then survives SIGKILL but not power loss |
| `--fail-after-checkpoint` | test hook: the reader aborts the run (exit 1) right after injecting marker K, leaving a checkpoint to `--resume` from; used by the unit tests and to drive resume cases by hand (0 = off) |
| `--latency-stats` | print min/p50/p99/max emit latency at exit: per window closed by the watermark, wall time from `read(2)` returning the input block that holds the closing event to the row being written to stdout (`--out -`) or appended to the part file's buffer (`--out FILE`, not yet visible to anyone) (`latency.h`, NOTES.md m5 and "Review fixes" for the exact definition), plus how many `read(2)` calls the input arrived in; zero cost when off |

The stderr summary has three lines (four with `--ckpt-dir`): totals
(events, dups, late and how many of those were past the dedup horizon,
windows, wall time, events/s over the events this run read, threads); the scaling
diagnostics (per-worker event counts and busy time, batches, reader stalls =
reader found a queue full, worker idle waits = worker found its queue empty,
merge time); the configuration with the dedup and open-window peaks; and the
checkpoint line (whether the run resumed and from which epoch and offset,
epochs committed, last snapshot size and open windows summed over workers,
slowest single snapshot, total snapshot and manifest time).

Typical loop (from the repo root, binaries in `build-sk/bin`):

```
build-sk/bin/gen --n 1000000 --seed 1 --out events.ads          # 1% dups, 5% late
build-sk/bin/oracle --in events.ads --out truth.csv
build-sk/bin/streamkit --in events.ads --out results.csv --lateness-s 60 --late-out late.ads --threads 8
bench/compare.sh truth.csv results.csv                           # PASS: N windows identical

# sliding: 5-minute windows every minute, oracle and engine take the same flags
build-sk/bin/oracle --in events.ads --out truth_sliding.csv --window-s 300 --slide-s 60
build-sk/bin/streamkit --in events.ads --out sliding.csv --lateness-s 60 --window-s 300 --slide-s 60
bench/compare.sh truth_sliding.csv sliding.csv

# checkpointed: kill it whenever you like, then add --resume and run the same command again
build-sk/bin/streamkit --in events.ads --out results.csv --lateness-s 60 --threads 8 \
    --ckpt-dir ckpt --checkpoint-every-events 100000
build-sk/bin/streamkit --in events.ads --out results.csv --lateness-s 60 --threads 8 \
    --ckpt-dir ckpt --checkpoint-every-events 100000 --resume
bench/compare.sh truth.csv results.csv
```

`bench/smoke.sh build-sk [n] [seed]` runs the oracle comparison at 1, 2, 4
and 8 threads. `bench/crash.sh build-sk [trials] [throttle_eps] [n_events]`
is the SIGKILL-and-resume harness (default 50 trials on a 2M-event stream).

`bench/throughput.sh build-sk [--only sections] [extra streamkit flags]` is
the benchmark harness; it prints markdown with the machine, build, load
average and exact command above every table, three runs per cell with the
median in bold:

```
bench/throughput.sh build-sk                          # all five sections, several minutes
bench/throughput.sh build-sk --only threads,latency   # a subset
bench/throughput.sh build-sk --reader-decode          # extra flags go to every streamkit run
```

| section | what it measures |
|---|---|
| `threads` | events/s at 1, 2, 4, 8 threads on a 10M-event dirty stream |
| `dedup` | dedup on vs `--no-dedup` on a dirty and a clean stream, 1 and 8 threads |
| `checkpoint` | no checkpoints vs every 1M vs every 100K events, 8 threads, with the snapshot cost |
| `memory` | max RSS (`/usr/bin/time -l`) at 1M and 10M events, 1 and 8 threads, and 8 threads with `--queue-depth 256` |
| `latency` | `gen --pace` at 200K events/s for 20 s piped into `--out - --window-s 1 --lateness-s 2 --latency-stats` and on into `cat`, 1 and 8 threads, min/p50/p99/max per run and the median over runs |

The inputs (1.1 GB) are generated once by seed and cached in
`$ADSTACK_CACHE` (default `/private/tmp/adstack-sk`); outputs, checkpoints
and logs go to a private `mktemp` directory, so concurrent harness runs do
not collide. The latency section checks gen's and streamkit's
exit statuses separately (a SIGPIPE in gen would mean streamkit closed the
pipe early), prints both with streamkit's stderr if a run fails, and
retries that run once. Timing sections are only as good as the machine:
check `ps` for other benchmarks first; the memory section and the crash
harness do not care.

## The late side file, and what it does not promise

Every event diverted as late is counted and, with `--late-out`, written.
Events past the dedup horizon (Dedup's `kExpired`, reported as "past the
dedup horizon" on stderr) are diverted without an id check, because the
set no longer remembers them. The aggregates are exactly right regardless
(proof in `dedup.h`), but when the stream re-delivers events later than
window + lateness the late file can contain re-deliveries of events that
were already counted, and repeats of one id. A consumer that re-adds late
events must deduplicate them by `event_id` against the output first. The
count is zero whenever the horizon covers the re-delivery delay (the
generator's defaults).

Event times must lie in [-2^62, 2^62] us (about 146,000 years either side
of 1970); a record outside fails the run with its event_id rather than
feeding signed overflow into the window arithmetic, and the oracle rejects
it the same way. A per-window cost sum that overflows int64 also fails
loudly. There is no bound on how far *ahead* a timestamp may be: one
far-future record advances its worker's watermark for good (standard
max-watermark semantics), and every later real event of that partition is
then late. A guard (maximum watermark advance per event) is listed in
NOTES.md as future work.

## How it works

```
reader (main thread)                     workers (one thread each)
  read(2) 1 MiB blocks                     pop batch
  peek campaign_id at byte 29              marker? -> checkpoint (below)
  partition_of(campaign) -> p              decode each record
  memcpy record into batch[p]              m2 engine per event:
  publish batch when full                    1. advance own watermark
  (wait if queue p is full)                  2. dedup (own bucketed set)
  every N events: flush partial              3. late check (earliest window closed?)
    batches, publish a marker                4. add to every window containing it
    (epoch k, offset X) to every queue       5. close windows the watermark passed
  at EOF: flush partials, send eof         append rows to <out>.part<p>
                                           (--out -: write them to stdout per batch)
main thread after join:
  late parts  -> <late>.tmp  -> rename
  header + out parts -> <out>.tmp -> rename      (the commit of the run)
  remove MANIFEST (synced), then the checkpoint dir, then the parts
```

Per event in a worker, in this order:

1. If the event's time is the largest this worker has seen, advance its
   watermark and let the dedup set drop buckets that fell behind the horizon.
2. Dedup (`dedup.h`): an exact set of event_ids bucketed by event_time. An
   id is forgotten once `event_time < floor_to_bucket(watermark - window)`,
   because any later copy of it would be rejected as late anyway (proof in
   `dedup.h`, extended to sliding windows). Duplicates are counted and skipped.
3. Late check: if the event's *earliest* window has ended at or before the
   watermark, write it to `--late-out` and count it. The whole event goes to
   the side file, never part of it.
4. Add to the open window for every `(window_start, campaign)` containing
   the event (`common/agg.h` enumerates them; one for tumbling); if the
   watermark moved, emit and erase every window whose end it passed.

Checkpoint, when a worker dequeues the marker for epoch k (`checkpoint.h`
has the full protocol and the file format):

1. `fflush` + fsync its part files, so the lengths it is about to record are
   really on disk.
2. Serialize (open windows, dedup buckets, part file lengths, watermark,
   cumulative counters) to `epoch-k/worker-i.tmp`, fsync, rename to
   `worker-i.snap`, fsync the directory.
3. Report to the coordinator. The last of the P workers to report writes
   `manifest.tmp` {epoch k, offset X, P, window parameters, interval,
   late-out flag, input fingerprint}, fsyncs, renames
   it to `MANIFEST`, and deletes `epoch-(k-1)`. The rename is the commit.

Resume (`--resume`): read `MANIFEST`, delete every epoch directory other
than the one it names (partial newer epochs, undeleted older ones), load
each worker's snapshot, truncate each part file to its recorded length, seek
the input to X, continue. Rows written after the checkpoint by the run that
died are gone with the truncate and come back from the replayed events, so
the final output is byte-identical to an uninterrupted run.

## Files

```
main.cpp            flags, runs the pipeline, stderr summary
pipeline.h / .cpp   reader loop, hash partitioning, markers, Worker (the per-partition engine),
                    snapshot/restore, atomic merge of parts
checkpoint.h / .cpp durable writes (fsync, F_FULLFSYNC, tmp + rename), snapshot codec, MANIFEST,
                    resume preparation, epoch coordinator
spsc_queue.h        bounded single-producer single-consumer ring (claim/publish, front/pop)
dedup.h / .cpp      time-bucketed exact dedup set, with export/import for snapshots
latency.h           --latency-stats: the metric's definition and the percentile arithmetic
tests/              GoogleTest unit tests (globbed by CMakeLists.txt)
CMakeLists.txt      streamkit_core library + streamkit binary + streamkit_tests
NOTES.md            decision log, one entry per milestone
BENCHMARKS.md       every measured number, with machine, load, command and the run-to-run spread
```

## Status

- [x] m1: single-threaded tumbling windows, watermark, late side output; matches the oracle on duplicate-free streams
- [x] m2: bounded exact dedup; matches the oracle with 1% duplicates (seeds 1..5 at 1M events), RSS 61.6 MiB at 1M events vs 71.0 MiB at 10M, 5.19M events/s single-threaded vs 5.94M without dedup
- [x] m3: reader thread + hash-partitioned workers over SPSC queues, per-worker watermark and part files, sliding windows (`--slide-s`, oracle too); matches the oracle at 1/2/4/8 threads, tumbling and sliding; 10M events: 5.20M events/s at 1 thread, 9.41M at 2, 19.9M at 4, 30.9M at 8 (5.94x); ASan and TSan clean. Re-verified 2026-10-21 on the finished tree (m4 and the m5 plumbing included): 78/78 tests, smoke PASS at 1/2/4/8 threads tumbling and sliding, ASan and TSan clean on 1M-event runs, 0 warnings; 27.1M events/s at 8 threads (5.38x) on a loaded machine, see NOTES.md for the evidence
- [x] m4: marker-based checkpoints (per-worker atomic snapshots, MANIFEST rename as commit, F_FULLFSYNC on Apple) and `--resume` with truncate-and-replay output; `bench/crash.sh`: 50/50 SIGKILL-and-resume trials identical to the oracle on 2026-10-19 (139 kills, 46 trials killed 2+ times) and twice again on 2026-10-21 on the finished tree (148 kills / 46 trials killed 2+ times, then 136 kills / 43 on the final binary, then a third pass from a fresh configure in the afternoon: 149 kills / 47 trials killed 2+ times, every trial killed at least once); checkpoint cost: 9.07 MB and 28 to 34 ms per snapshot at 98K open windows (9 to 15 ms of it fsync, across the morning and afternoon measurements), ~4.2 MB and 21 to 24 ms per worker per checkpoint at 4 threads; ASan and TSan clean on kill-and-resume runs; the output file's rename is now followed by a directory fsync like every checkpoint rename (NOTES.md, 2026-10-21 m4 entry)
- [x] integration (2026-10-21): the output CSV is colstore's second input kind (`colstore ingest --in-csv`); `bench/e2e.sh` runs gen -> oracle -> streamkit (4 threads, checkpoints on) -> compare -> colstore on both tables with every answer brute-force checked, exit 1 on any mismatch; NOTES.md says why the boundary is CSV and what a binary or Arrow-style one would buy; re-verified 2026-10-21 13:50 EDT from fresh configures: e2e PASS at 5M (49,791 dups, 3,735,840 windows, 5 epochs, 6.36M events/s at 4 threads), ASan and TSan e2e at 1M with zero reports, forced mismatch exits 1
- [x] m5: `bench/throughput.sh` with five sections (threads, dedup, checkpoint, memory, latency) and `BENCHMARKS.md`, every number re-measured on 2026-10-21 12:30 to 13:20 on a machine shared with the other track's benchmark (100% of a core until 13:00) and `fseventsd` (85 to 90% throughout), six harness runs with every run and its load on the page: quietest run 4.97M / 9.36M / 18.7M / 28.0M events/s at 1 / 2 / 4 / 8 threads (5.63x, runs within 2% per cell; the scaling shape was the same in the noisy runs while the level halved, so the page says which cells to trust); dedup 51 ns per event, the same on clean and dirty streams (25.7% / 26.1% of a single thread); checkpoints cost 30 to 40 ms per snapshot and nothing else (0.03 to 0.04% at the generator's native rate); max RSS 92.5 MiB at 1M events vs 103.2 MiB at 10M at 1 thread, 87 vs 119 MiB at 8 threads with `--queue-depth 256`; emit latency under `gen --pace` at 200K events/s, 1 s windows, 2 s lateness: p50 7.9 ms / p99 16.1 ms at 8 threads, p50 14.4 / p99 25.5 ms at 1 thread (metric defined in NOTES.md and BENCHMARKS.md section 5), decomposed into a 1.7 to 4.6 ms producer-burst floor plus 0.24 / 0.44 / 1.34 us per row at 1 / 4 / 8 workers, so 4 threads give the lowest p99 (11.2 ms); regression on the same tree: smoke PASS at 1/2/4/8, crash harness 50/50 (138 kills, 47 trials killed twice or more), 79/79 tests and 0 warnings in Release, ASan+UBSan and TSan, sanitizers clean on the paced path; the harness now checks both exit statuses of the paced pipeline and keeps the logs of a failed run
