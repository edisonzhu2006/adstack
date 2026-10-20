# adstack build plan

Created: 2026-10-19 20:40 EDT

## Objective

Two C++20 systems that together form a miniature ads data pipeline, built so
that every line can be explained in an interview and every number on the
resume can be reproduced with a script.

```
events  -->  streamkit (streaming windowed aggregation)  -->  colstore (columnar store + queries)
```

Target reader of the finished work: a Google infra interviewer (YouTube Ads
data infra or similar) asking "why did you do X instead of Y" and "how did
you measure that".

## Ground rules for every milestone

1. **Explainability is a deliverable.** Each milestone appends an entry to
   its project's `NOTES.md` (streamkit/NOTES.md, colstore/NOTES.md): the
   decision, the alternative considered, why this one, and what it costs.
   If an entry cannot be written, the design is not done.
2. **Every number is scripted.** Anything that will appear on the resume
   comes from a script in `bench/` with the machine spec and command in
   `docs/BENCHMARKS.md`. No numbers from memory.
3. **Correctness is proven against the oracle.** `tools/oracle` is the
   ground truth. Streamkit output is diffed with `bench/compare.sh`.
4. **Warnings clean, sanitizer clean.** `-Wall -Wextra -Wpedantic -Wshadow
   -Wconversion`, ASan for everything, TSan for the multithreaded code.
5. **Tests per milestone.** GoogleTest under `streamkit/tests/` and
   `colstore/tests/`; `common/` tests stay in `tests/`.
6. Plain C++20 and the standard library. Only dependencies: GoogleTest,
   nlohmann/json (adapters only), sqlite3 (benchmark comparison only).

## Track A: streamkit

| milestone | what | done when |
|---|---|---|
| m1 (done) | single-threaded tumbling windows, watermark, late side-output | matches oracle on dup-free stream |
| m2 | bounded exact dedup: time-bucketed set of event_ids, buckets expire once older than the accept horizon | matches oracle with 1% dups; RSS flat from 1M to 10M events |
| m3 | reader thread + P hash-partitioned workers over bounded SPSC queues, per-worker state and watermark, sliding windows (`--slide-s`), oracle gains `--slide-s` | matches oracle at P=1,2,4,8; throughput table |
| m4 | marker-based checkpoints: reader injects a barrier at input offset X, each worker snapshots (open windows, dedup buckets, output length) atomically (tmp, fsync, rename), manifest commits the epoch; `--resume` seeks input to X, truncates per-worker output to the recorded length, reloads state | `bench/crash.sh`: SIGKILL at random points 50 times, restart each time, final output identical to oracle |
| m5 | benchmarks: throughput vs P, p50/p99 emit latency under paced replay, memory, dedup overhead; `docs/BENCHMARKS.md` | table reproducible by `bench/throughput.sh` |

## Track B: colstore

| milestone | what | done when |
|---|---|---|
| m1 | row-group file format (64K rows), one column chunk per field, footer with per-chunk metadata, plain encoding, ingest from `.ads` | ingest 10M events and read every column back identically |
| m2 | encodings chosen per chunk: dictionary (low cardinality), delta + bit-packing (monotone-ish ints), run-length over dictionary, plain fallback | bytes per column before and after, in `colstore/README.md` |
| m3 | scan, filter, project with zone-map (min/max) row-group skipping; vectors of 1024 values | skipped-row-group count for a selective time-range query |
| m4 | hash aggregate (GROUP BY with sum/count) and sort/limit; CLI `colstore q --where ... --group ... --agg ...` | the three benchmark queries run end to end |
| m5 | benchmark vs SQLite (same data, sensible indexes): storage size, three query times; `docs/BENCHMARKS.md` | table reproducible by `bench/colstore_bench.sh` |

## Integration

- `colstore ingest` accepts both raw events (`.ads`) and streamkit aggregate
  CSV, so the pipeline runs end to end: `gen | streamkit | colstore`.
- `bench/e2e.sh` runs the whole thing and prints one report.

## Verification and review

After both tracks land: independent reviewers per track rebuild from a
clean directory under ASan and TSan, rerun every harness, and check that
every number in the docs reproduces. Findings are fixed before the docs
are finalized.

## Final documents

- `docs/EXPLAINER.md`: study guide. Architecture walkthroughs, every
  design decision as a Q&A, every benchmark with how it was measured,
  likely interview questions with answers, and "what I would change".
- `docs/RESUME.md`: the resume bullets, each traceable to a benchmark row.
- `docs/BENCHMARKS.md`: all numbers, machine spec, commands.

## Ownership (so parallel work does not collide)

- Track A edits: `streamkit/`, `common/`, `tools/oracle.cpp`, `bench/smoke.sh`,
  `bench/compare.sh`, new `bench/throughput.sh`, `bench/crash.sh`.
  Builds in `build-sk/`.
- Track B edits: `colstore/`, new `bench/colstore_bench.sh`. Does not touch
  `common/` or root `CMakeLists.txt`. Builds in `build-cs/`.
- Root `README.md`, `.github/workflows/ci.yml`, `docs/`: the final docs pass.

## Resume mapping (targets; replaced by measured values at the end)

- streamkit: "Built a multithreaded streaming aggregation engine in C++20
  computing windowed ad metrics over out-of-order, duplicated streams with
  exactly-once checkpoint/restore; X events/s on N cores, zero divergence
  from a batch recompute across 50 SIGKILL-and-resume trials."
- colstore: "Built a columnar analytics store in C++20 with dictionary,
  delta and run-length encoding, zone-map pruning and vectorized hash
  aggregation; Y to 1 compression and Z times faster than SQLite on GROUP BY
  over 10M+ ad events."
