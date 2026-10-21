# streamkit

Streaming aggregation engine for ad events: reads events in ingest order,
drops duplicates, assigns each event to its window(s) by event time, closes
windows with an event-time watermark, and writes one CSV row per
(window, campaign). Events whose window already closed are written to a side
file and counted, never dropped silently. Output is byte-compatible with
`tools/oracle`, the batch ground truth, so the two can be diffed.

From m3 the engine is multithreaded: one reader thread hash-partitions
events by `campaign_id` into bounded single-producer single-consumer queues,
and P worker threads each own a dedup set, an open-window map, a watermark
and an output part file. Nothing on the hot path is shared.

From m4 the engine checkpoints: every N events the reader drops a marker
into every queue, each worker snapshots its state atomically when the marker
reaches it, and a MANIFEST rename commits the epoch. `--resume` continues
from the last commit with byte-identical output, which `bench/crash.sh`
proves by SIGKILLing the process at random points and diffing the final
result against the oracle.

Design decisions and measurements are in [NOTES.md](NOTES.md).

## Usage

```
streamkit --in FILE --out FILE [--window-s 60] [--slide-s WINDOW] [--lateness-s 0]
          [--late-out FILE] [--threads P] [--queue-depth 4096] [--batch 128]
          [--dedup-bucket-s 1] [--no-dedup] [--reader-decode]
          [--ckpt-dir DIR] [--checkpoint-every-events 1000000] [--resume] [--throttle-eps N]
```

| flag | meaning |
|---|---|
| `--in` | event file in the shared `.ads` format, or `-` for stdin |
| `--out` | CSV output (`window_start_us,campaign_id,impressions,clicks,conversions,cost_micros`), or `-` for stdout |
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

The stderr summary has three lines (four with `--ckpt-dir`): totals
(events, dups, late, windows, wall time, events/s, threads); the scaling
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
and 8 threads. `bench/throughput.sh build-sk [extra streamkit flags]` prints
the throughput table (10M events, median of three runs per thread count).
`bench/crash.sh build-sk [trials] [throttle_eps] [n_events]` is the
SIGKILL-and-resume harness (default 50 trials on a 2M-event stream).

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
main thread after join:
  late parts  -> <late>.tmp  -> rename
  header + out parts -> <out>.tmp -> rename      (the commit of the run)
  remove the checkpoint dir, then the parts
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
   `manifest.tmp` {epoch k, offset X, P, window parameters}, fsyncs, renames
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
tests/              GoogleTest unit tests (globbed by CMakeLists.txt)
CMakeLists.txt      streamkit_core library + streamkit binary + streamkit_tests
NOTES.md            decision log, one entry per milestone
```

## Status

- [x] m1: single-threaded tumbling windows, watermark, late side output; matches the oracle on duplicate-free streams
- [x] m2: bounded exact dedup; matches the oracle with 1% duplicates (seeds 1..5 at 1M events), RSS 61.6 MiB at 1M events vs 71.0 MiB at 10M, 5.19M events/s single-threaded vs 5.94M without dedup
- [x] m3: reader thread + hash-partitioned workers over SPSC queues, per-worker watermark and part files, sliding windows (`--slide-s`, oracle too); matches the oracle at 1/2/4/8 threads, tumbling and sliding; 10M events: 5.20M events/s at 1 thread, 9.41M at 2, 19.9M at 4, 30.9M at 8 (5.94x); ASan and TSan clean
- [x] m4: marker-based checkpoints (per-worker atomic snapshots, MANIFEST rename as commit, F_FULLFSYNC on Apple) and `--resume` with truncate-and-replay output; `bench/crash.sh`: 50/50 SIGKILL-and-resume trials identical to the oracle (139 kills, 46 trials killed 2+ times); checkpoint cost: ~2.3 MB and ~35 ms per worker per checkpoint at 8 threads (28 ms of it fsync), 9.1 MB at 98K open windows; ASan and TSan clean on resume runs
- [ ] m5: benchmark table (events/s vs P, p50/p99 emit latency under `--pace`, memory, dedup overhead) via `bench/throughput.sh`
