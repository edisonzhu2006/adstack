# streamkit design notes

Running log of decisions and dead ends for the streaming engine. Same rule
as the root NOTES.md: every entry should answer a "why did you do X instead
of Y" question directly, and every number comes from a command that is
written down next to it.

## 2026-10-19: m2, bounded exact deduplication

**Problem.** The input re-delivers some events (same `event_id`, same
`event_time`, new `ingest_time`). m1 counted them twice, so impressions and
cost were overstated in exactly the rows that had duplicates. In ads this is
double billing. The oracle fixes it with one `unordered_set` of every id it
has ever seen, which is correct for a batch job over a file and wrong for a
stream: memory grows without bound.

**Decision: an exact set of event_ids bucketed by event_time, whole buckets
dropped once they fall behind the accept horizon.** `streamkit/dedup.h`.

### The horizon, derived rather than guessed

An id only has to be remembered for as long as a second copy of it could
still be counted. Streamkit rejects an event as late when its window has
already closed:

```
window_start(event_time) + window_us <= watermark
```

Let `H = watermark - window_us`. For any event with `event_time = t < H`:

```
window_start(t) <= t < H   =>   window_start(t) + window_us < watermark
```

so the event is late no matter which window it belongs to. A duplicate
carries the same `event_time` as its original, so once `t < H` every copy of
that id is diverted to the late output and never aggregated. Forgetting the
id changes nothing in the output. The dedup structure rounds `H` down to a
bucket boundary (`forget_before = floor_to_bucket(watermark - window)`),
which is conservative: it keeps up to one extra bucket of ids, never fewer.

Three verdicts fall out of this: `kNew` (stored), `kDuplicate` (dropped,
counted in `dups`), `kExpired` (`event_time < forget_before`, not stored).
`kExpired` is not an error path: by the proof above the late check that
follows is guaranteed to divert the event, so `main.cpp` just passes it
through. The unit test `ExpiredImpliesLateForEveryTimestampNearTheHorizon`
checks this implication exhaustively for every microsecond in a band around
the horizon, for several watermarks and bucket widths including an odd one
(7.000003s), so no rounding corner is taken on faith.

A tighter horizon exists: the exact late line is `t < window_start(watermark)`,
which is between `H` and `H + window`. I kept `H` because it is the line the
plan specifies and the proof is one inequality; moving to the exact line
would save up to one window of ids (about half the retained set at
window = lateness = 60s). Noted under "what I would change".

### Why advance before insert

`main.cpp` calls `dedup.advance(watermark)` before `dedup.insert(...)` for
the same event. Order matters for memory: if one event jumps the watermark
far ahead (the Wikimedia adapter can do this), advancing first drops every
stale bucket before the new bucket is appended, so the retained span is
always at most `(lateness + window) / bucket + 2` buckets. Inserting first
would briefly hold both the whole old span and the new bucket. The event
that raises the watermark can never be late under the watermark it just set
(`window_end > event_time >= watermark`), so doing this before the late
check changes nothing there.

### Alternatives considered and rejected

- **Unbounded `unordered_set<uint64_t>` (what the oracle does).** Ten lines,
  obviously correct, and memory is proportional to the length of the stream.
  At the generator's 10K events/s it is about 48 bytes per id (measured
  below), so roughly 1.7 GB per hour, forever. Fine for the oracle, which is
  deliberately never optimized; disqualifying for a long-running engine.
- **Ring of bloom filters, one per time slice.** Constant memory and cheap,
  but a bloom filter has false positives, and here a false positive means a
  real, never-seen, billable event is silently dropped as a duplicate. The
  error is invisible (no row disagrees loudly, a count is just one short),
  cannot be audited after the fact, and is money. The whole point of the
  late side-file is that nothing is dropped silently; a probabilistic dedup
  would undo that. A bloom filter in front of the exact set as a fast
  negative check would be sound, but the exact set already answers "not
  seen" in one hash lookup, so it would add work without removing any.
- **Per-id expiry (min-heap or ordered map on event_time).** Exact and
  bounded, but it pays O(log n) per insert and per expiry, and expiry walks
  one id at a time. Bucketing expires thousands of ids in one `pop_front`,
  and the insert is one hash-set insert. The cost is granularity: we keep up
  to one bucket (1s default) of ids longer than strictly needed.
- **`std::map`/`std::set` per bucket.** Ordered lookup buys nothing; ids are
  only ever asked "have I seen you". Hash set.

### Smaller choices

- **`std::deque<unordered_set>` for the buckets.** Append at the back as
  event time advances, prepend for an out-of-order event older than anything
  retained, pop from the front to expire. A `std::vector` would need shifting
  on `pop_front`; a true ring buffer would need a fixed capacity, and the
  span is bounded but not fixed (it depends on lateness, window and bucket
  width, all runtime flags).
- **Bucket reuse through a small free list (`kMaxFree = 4`).** Buckets are
  created at the same rate they expire, so an expired set is `clear()`ed and
  handed back as the next bucket instead of allocating a fresh table every
  second of event time. `clear()` frees the nodes but keeps the bucket
  array, which is the part worth keeping. The test
  `RecycledBucketDoesNotRememberExpiredIds` pins that a reused table comes
  back empty.
- **A new bucket reserves the size of its neighbour.** At a steady event
  rate consecutive buckets hold about the same number of ids, so this skips
  the rehash cascade of growing from empty without asking the user to guess
  the rate. Minimum reserve of 16.
- **`--dedup-bucket-s` is a flag, `--no-dedup` exists only for measurement.**
  The bucket width trades memory granularity against bucket count; 1s is the
  default because at 60s window and 60s lateness it means 121 buckets, which
  is nothing. `--no-dedup` is how the overhead number below was taken; it is
  not a mode anyone should run in production.

### What it costs

Measured on an Apple M5 Pro (18 cores, 24 GB), Apple clang 21, Release
(`-O3`), single-threaded, input `gen --n 1000000 --seed 1` (defaults: 1%
dups, 5% late up to 30s, 10K events/s simulated), `--lateness-s 60`,
output to `/dev/null`.

Throughput, three runs each (`streamkit --in ev1m.ads --out /dev/null
--lateness-s 60 [--no-dedup]`; `--no-dedup` is the m1 code path and the
"before" number, since m1 and m2 differ only by the dedup call):

| mode | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| `--no-dedup` (m1 equivalent) | 6,229,525 | 5,942,683 | 5,937,450 | 5.94M events/s |
| dedup on (m2) | 5,194,919 | 5,202,342 | 5,190,762 | 5.19M events/s |

So exact dedup costs about 13% of single-thread throughput: one hash-set
insert per event, which is one cache miss into a table that does not fit
in L2.

Memory, `/usr/bin/time -l` maximum resident set size, same flags:

| input | dedup on | `--no-dedup` | ids retained at end | buckets |
|---|---|---|---|---|
| 1M events (`ev1m.ads`) | 64,585,728 B (61.6 MiB) | 17,514,496 B | 989,273 | 121 |
| 10M events (`ev10m.ads`) | 74,481,664 B (71.0 MiB) | 17,678,336 B | 1,189,164 (peak 1,189,660) | 121 |

Ten times the events, 1.15 times the RSS. Why the numbers look the way they
do:

- The generator runs at 10K events/s of simulated time, so 1M events is
  100s of event time and 10M is 1000s. The horizon is `lateness + window =
  120s` (121 buckets of 1s). A 100s stream never reaches the horizon, so at
  1M nearly every unique id is still retained (989,273 of 989,957 unique;
  the 684 missing are the earliest late events, 20-30s before the first
  on-time one, whose buckets had already expired). At 10M the structure has
  been in steady state for 880s and holds about 120s x 9,900 unique/s =
  1.19M ids, and would hold the same at 100M.
- Dedup memory is about (74.5 MB - 17.7 MB) / 1.19M = 48 bytes per
  retained id. That is a libc++ `unordered_set` node (next pointer, cached
  hash, 8-byte key = 24 bytes, rounded to 32 by malloc) plus the bucket
  array at load factor 1 (8 bytes per id) plus allocator slack. A flat
  open-addressing set would roughly halve this; see "what I would change".
- The 17.5 MB baseline without dedup is the open-window map (about 100K
  campaigns x 2 to 3 open minutes) plus stdio buffers, and it is flat
  between 1M and 10M because the watermark closes windows as it goes.

### How it was verified

- `ctest --test-dir build-sk`: 38/38, 14 of them `Dedup.*` in
  `streamkit/tests/dedup_test.cpp` (duplicate inside the horizon, expiry
  forgets, expired copy is consistent with the late rule, boundary
  microseconds on both sides of a bucket edge, negative timestamps, horizon
  never moves backwards, gaps become empty buckets, recycled buckets are
  empty, retained span stays bounded on a 400s simulated dirty stream).
- `bench/smoke.sh build-sk` (200K, seed 7, 1% dups): PASS, 92,609 windows
  identical, and streamkit's `1951 dups` equals the oracle's `1951 dups
  dropped`.
- `bench/smoke.sh build-sk 1000000 <seed>` for seeds 1..5: PASS on all five
  (205,806 / 205,938 / 205,935 / 205,785 / 206,079 windows identical), dup
  counts equal to the oracle's on every seed.
- ASan+UBSan (`build-sk-asan`, `-DADSTACK_SANITIZE=address`): `ctest` 38/38,
  `bench/smoke.sh build-sk-asan` PASS, 1M seed 1 identical to the oracle,
  and a run with `--lateness-s 0 --dedup-bucket-s 5` (17,054 late events, 13
  buckets, continuous expiry) clean. Note `ASAN_OPTIONS=detect_leaks=1`
  aborts on macOS; the leak checker is not supported there.
- TSan not run: m2 is single-threaded. It joins the matrix in m3.
- Warnings: all project translation units compile clean under `-Wall
  -Wextra -Wpedantic -Wshadow -Wconversion`. The one warning in a full build
  is clang 21's `-Wcharacter-conversion` inside googletest's own
  `gtest-printers.h` while compiling `gtest-all.cc`, because the root
  `add_compile_options` also applies to the fetched library. That is a root
  CMake concern (mark the dependency's include dirs SYSTEM or scope the
  flags to our targets), out of this track's ownership.

### What I would change

- Use the exact late line `window_start(watermark)` as the horizon instead
  of `watermark - window`. Same proof shape, up to one window less of
  retained ids.
- Replace `std::unordered_set` with a flat open-addressing set (keys are
  already 64-bit and uniform, so no separate hash is needed). Roughly half
  the bytes per id and no per-insert allocation. Deferred because the
  standard container is explainable in one sentence and the measured
  overhead (13% throughput, 48 B/id) is acceptable for now; it is the first
  thing to revisit if m5's memory table says dedup dominates.
- In m3 each worker gets its own `Dedup` keyed by its partition, so no
  sharing is needed; the structure already has no global state.
- In m4 the checkpoint has to carry the buckets (ids plus `front_start_`
  plus `forget_before_`), because after a restart replayed events must hit
  the same set. The introspection accessors are the start of that interface.

## 2026-10-19: m3, hash-partitioned workers and sliding windows

**Problem.** m2 is one thread at 5.2M events/s. The per-event cost is two
random memory accesses (the dedup hash insert and the open-window map
update) and no amount of tuning makes one core do that much faster. The
machine has 18 cores. Splitting the work across threads without sharing
state is the only way to use them without paying for a lock per event.

**Decision: one reader thread, P worker threads, events hash-partitioned by
`campaign_id`, one bounded SPSC queue per worker, no shared mutable state on
the hot path.** `streamkit/pipeline.h`, `streamkit/spsc_queue.h`.

### Why partition by campaign_id

Every output row is keyed by `(window_start, campaign_id)`, so if all events
of a campaign go to one worker, that worker can compute the row alone. The
same fact makes dedup exact without a shared set: a duplicate is a
byte-for-byte copy of its original, so it has the same `campaign_id` and
lands on the same worker, which still remembers the id. Partitioning by
anything else (round robin, by `event_id`) would need a shared dedup set and
a merge of partial rows per window.

`partition_of()` is the splitmix64 finalizer over the id, reduced into
`[0, P)` by multiply-and-shift. Not `campaign_id % P`, for two reasons: ids
with structure (all multiples of 8, say) would all land on one worker, and
the test `Partition.SpreadsSequentialAndStructuredIds` checks both the
sequential and the stride-8 case stay within 3% of even. It is deterministic
across runs and machines on purpose: m4's resume has to put a campaign back
on the worker whose part file holds its state. On the generator's 10M
stream the eight workers got between 1,231,823 and 1,264,230 events, a 2.6%
spread.

### Why SPSC queues and not a shared work queue or a mutex

With one reader and one owner per partition, every queue has exactly one
producer and one consumer. That restriction removes the need for a lock:
the producer is the only writer of `tail_`, the consumer the only writer of
`head_`, and a release store of an index after writing the slot, paired
with an acquire load of that index before reading the slot, is all the
synchronization there is (`spsc_queue.h` spells the ordering out). A shared
MPMC queue would need CAS loops and would put every event's handoff on one
contended cache line. A mutex-protected `std::deque` would be simpler to
write and roughly ten times slower per handoff, and would serialize the
reader against eight workers.

Three details worth defending:

- **Batches, not events.** The queue carries batches of `--batch` (default
  128) events, so the cross-core cache-line transfer that announces new data
  happens once per 128 events, not once per event. Measured: `--batch 16`,
  `128` and `1024` all give 30-31M events/s at P=8, so the queue is not the
  bottleneck at any of those sizes. 128 keeps the per-worker buffer small
  (6.6 KB per slot) and the live-stream latency low.
- **Slots are reused in place.** The producer claims a slot, fills the
  `std::vector` that already lives there, publishes; the consumer reads it
  in place and pops. No allocation per batch and no cross-thread free, which
  on macOS's allocator is not cheap. First version appended with
  `vector::insert(end, rec, rec+52)`; that turned out to be slower than
  decoding the record (see "where to decode"), so batches are now sized once
  and filled with `memcpy`.
- **Cached indices.** Each side keeps a private copy of the other side's
  index and only re-reads the shared atomic when the queue looks full or
  empty. Without it, every `try_claim` would read the consumer's cache line
  and every `try_front` the producer's, and the two cores would ping-pong
  those lines on every batch.

**Backpressure, never drop.** When a queue is full the reader re-checks 64
times, then yields up to 1024 times, then sleeps 50us between checks until a
slot frees. Workers wait the same way on an empty queue, so a paced live
stream at 10K events/s does not burn eight cores spinning. The test
`Pipeline.TinyQueueAndBatchExerciseBackpressure` runs with 2 slots of 1
event each (the reader waits on every single event) and the output is still
exact. `reader stalls` and `worker idle waits` in the stderr summary count
these waits and are the first thing to read when scaling looks off: stalls
and no idle waits means the workers are the bottleneck; idle waits and no
stalls means the reader is.

### Per-worker watermark, not global

Each worker computes `watermark = max(event_time seen by this worker) -
lateness` and closes its own windows against it. The alternative is a
global watermark = min over workers, which every worker would read (an
atomic per worker that the reader or a coordinator folds).

Per worker won on three counts. There is nothing to share, so no cross-
thread read on the hot path. A partition whose traffic stalls (one hot
advertiser pauses) delays only its own windows; under a global watermark the
slowest partition would hold every partition's windows open, growing state
everywhere. And the late rule stays exact per campaign: an event is late iff
its own worker has seen something `lateness` newer, which is the m2 rule
applied per key range.

What it costs: the output is interleaved across partitions (part files are
concatenated in partition order, so rows are sorted within a partition but
not globally); `compare.sh` sorts, and colstore ingest does not care about
row order. The test `Pipeline.LateEventsEndUpInOneMergedSideFile` documents
the one behaviour that surprises people: an on-time event for campaign 3 can
be late if campaign 0, on the same worker, has already pushed that worker's
watermark past it. That is correct (the stream as a whole has moved on), and
with P=1 it is exactly m2's behaviour.

### Sliding windows: one code path, shared with the oracle

`--slide-s` (default = window) makes windows overlap. An event belongs to
every window start `s` that is a multiple of `slide` with `s <= t < s +
window`; `common/agg.h` has `first_window_start`, `last_window_start` and
`for_each_window_start`, and both streamkit and the oracle enumerate windows
through them, so they cannot disagree on membership. Tumbling is the
`slide == window` case of the same loop; there is no separate tumbling path.
`tests/window_test.cpp` brute-forces the enumeration against the definition
over a grid of times including negative ones and a window that is not a
multiple of the slide.

Two consequences inside the worker:

- **Closing is unchanged.** All windows have the same length, so the
  smallest start in the `std::map` is also the earliest end, and "pop from
  the front while `start + window <= watermark`" still works. The map key
  did not change.
- **Late means "earliest window already closed".** An event with five
  windows could have two of them closed and three open. Counting it in the
  open three and writing it to the side file would make the output and the
  side file both claim part of it. So the rule is: late if
  `first_window_start(t) + window <= watermark`, then the whole event goes
  to the side file. This is also what keeps the m2 dedup proof valid with
  no change to the horizon; the derivation is in `dedup.h`. With `lateness
  >= max delay` (how the oracle comparison is run) the partial case never
  happens, which is why the comparison is exact.

Cost: an event with `window/slide = 5` does five map updates, and the open
state is five times larger. Measured on 10M events, `--window-s 300
--slide-s 60`: 0.82M events/s at P=1 and 8.3M events/s at P=8 (2,105,868
windows, identical to the oracle). The P=8 number is 10.1x the P=1 number,
more than linear, because at P=1 the single map holds about a million
entries and misses cache on nearly every update, while each of eight maps
is small enough to behave.

### Per-worker output files

Each worker appends its CSV rows to `<out>.part<i>` through a 1 MiB stdio
buffer; the main thread writes the header and concatenates the parts in
partition order when the workers are done (late events the same way, with
the `.ads` header kept once). A single shared output would need a lock per
row; more importantly, m4's checkpoint will record "worker i's output is N
bytes long" and truncate back to N on resume, which only means something
if worker i is the only writer of that file.

Cost: the merge copies the output once. At P=8 on 10M events that is
0.035 s of a 0.32 s run (11% of wall time; 68 MB of CSV at about 2 GB/s).
The reported events/s includes it. If it ever matters, the consumer
(colstore) can read the parts directly, or the merge can overlap with the
next epoch; noted under "what I would change".

### Where to decode: reader or worker

The reader has to see `campaign_id` to partition, nothing else. It peeks the
4 bytes at `kWireOffsetCampaignId` (pinned to `decode()` by a test) and
copies the raw 52-byte record into the batch; the worker decodes. The
`--reader-decode` flag exists only to measure the alternative.

Measured (10M events, `--lateness-s 60`, output to a file):

| P | raw batches (default) | `--reader-decode` | who is the bottleneck |
|---|---|---|---|
| 8 | 31.3M events/s (stalls 2934, idle 8) | 31.1M (stalls 2542, idle 1021) | workers; a wash |
| 16 | 34.6M / 34.0M (two runs; stalls 0, idle 3160 / 3536) | 34.0M / 33.6M (stalls 0, idle 6798 / 6538) | reader; decode there costs ~2% and doubles worker idle time |

So at the default P the choice does not matter, and when the reader is the
bottleneck the raw path is the right one, by a small margin. The first
version of the raw path was *slower* than `--reader-decode` at P=8 (29.6M
vs 33.4M) because it appended with `vector::insert(end, rec, rec + 52)`;
replacing that with a pre-sized buffer and `memcpy`, and the 64-bit `%` in
`partition_of` with a multiply-shift, moved the reader from the bottleneck
(0 stalls, 5564 idle waits at P=8) to comfortably ahead (2934 stalls, 8
idle waits). Lesson written down so it is not relearned: the reader touches
every byte once, so anything generic in its inner loop shows up.

### Throughput (`bench/throughput.sh build-sk`)

Apple M5 Pro, 18 hardware threads, 24 GB, Apple clang 21, Release (`-O3`),
input `gen --n 10000000 --seed 1` (1% dups, 5% late up to 30s, 100K
campaigns), `--lateness-s 60`, output to a file, three runs each, median:

| threads | run 1 | run 2 | run 3 | median events/s | speedup vs 1 | reader stalls | worker idle waits |
|---|---|---|---|---|---|---|---|
| 1 | 5,200,554 | 5,277,291 | 5,095,853 | **5.20M** | 1.00x | 71,053 | 1 |
| 2 | 9,512,899 | 9,405,758 | 9,192,105 | **9.41M** | 1.81x | 32,257 | 2 |
| 4 | 20,402,246 | 19,870,905 | 19,934,752 | **19.9M** | 3.83x | 12,583 | 4 |
| 8 | 31,176,597 | 30,868,540 | 29,773,819 | **30.9M** | 5.94x | 3,069 | 8 |

P=1 matches m2 (5.19M), so the queue and the thread handoff cost nothing
measurable at one worker. Scaling is near linear to 4 and 5.94x at 8. Where
the other 2x went, from the per-worker busy times the summary prints:

- At P=8 the wall time is 0.32 s. The slowest worker was busy 0.28 s and the
  fastest 0.25 s; each processed about 1.25M events, so a worker runs at
  4.5 to 5.0M events/s next to seven others, versus 5.2M alone: 5 to 13%
  per-worker slowdown from sharing memory bandwidth and last-level cache
  (eight dedup sets plus eight maps is ~300 MB of randomly accessed state).
- The merge is 0.035 s, 11% of wall, and is serial.
- The spread between the fastest and slowest worker (events 2.6%, speed the
  rest) means seven workers idle while the slowest finishes.
- Processing alone (10M / 0.28 s) is 35.7M events/s = 6.9x. The gap from 6.9x
  to 5.94x is the merge; from 8x to 6.9x is cache sharing plus skew.

Beyond 8: P=12 gives 34.1M and P=16 34.2M events/s with 0 reader stalls and
3-4K worker idle waits, so the reader (one thread doing read(2), a 4-byte
peek, a hash and a 52-byte memcpy per event, about 40M events/s) is the
ceiling on this machine. The default `--threads` is clamped to 8 because
that is where the workers stop being the bottleneck.

`--no-dedup` at P=8: 38.2M events/s, so dedup costs about 19% at P=8 (13% at
P=1 in m2): the hash insert's cache miss hurts more when eight cores share
the cache.

### Memory

`/usr/bin/time -l`, maximum resident set size, 10M events, `--lateness-s 60`:

| threads | `--queue-depth 4096` (default) | `--queue-depth 256` |
|---|---|---|
| 1 | 106,954,752 B (102 MiB) | 78,446,592 B (75 MiB) |
| 8 | 344,244,224 B (328 MiB) | 146,817,024 B (140 MiB) |

The default queue is the dominant term: 4096 slots x 128 events x 52 bytes
= 27 MB per worker once the reader has filled it, which it does whenever it
is ahead (always, at P <= 8). The 75 MiB at P=1 with a shallow queue is m2's
71 MiB plus the queue. At P=8 the per-worker state is about 8 x (dedup
~7 MB + map ~2 MB) on top. Depth 256 costs throughput, though: 26.2M at P=8
instead of 31M (7,399 stalls and 922 idle waits in the same run: the
shallow queue exposes the reader to every hiccup of every worker). 4096 is
kept as the default per the plan; it is a knob, and m5's memory table
should decide whether 1024 is the better trade.

### Alternatives considered and rejected

- **One shared `std::map` and dedup set behind a mutex.** Simplest, and
  every event would take the lock twice. Measured nothing because the
  outcome is known: slower than one thread.
- **Concurrent hash map (sharded locks or lock-free).** Would scale the
  dedup set, but the windows still need ordered closing per key, and there
  is no lock-free ordered map in the standard library. Partitioning gets
  the same parallelism with plain single-threaded containers, which is the
  whole appeal: each worker is literally the m2 engine.
- **Round-robin or `event_id` partitioning with a final merge of partial
  rows.** Spreads load perfectly even under key skew, but needs a shared
  dedup set (a duplicate could land anywhere) and a reduce step per window.
  Key-hash partitioning is what Flink/Beam do for keyed state for the same
  reason; skew is handled by picking a key with enough cardinality
  (100K campaigns here).
- **A global watermark.** Discussed above: more coupling, worse behaviour
  under a stalled partition, and the only thing it buys is globally ordered
  output, which nothing downstream needs.
- **One output file with a mutex, or workers sending rows back to the main
  thread.** A lock per row, or a second queue per worker in the other
  direction; both more code than concatenating files, and both lose the
  property m4 needs (one writer per file).
- **Workers read the file themselves (each scans everything, keeps its own
  keys).** No reader thread and no queues, but P workers read and hash the
  whole input, P x 520 MB of memory traffic instead of one, and it cannot
  work on a pipe. Rejected for the pipe case alone.

### How it was verified

- `ctest --test-dir build-sk` (excluding colstore's tests, whose source is
  mid-edit by the other track and does not compile right now): 56/56. New:
  7 `SpscQueue.*` (capacity rounding, FIFO, full/empty, wraparound, claim
  idempotence, slot reuse, and two two-thread tests moving 2M integers and
  20K in-place batches), 3 `Partition.*`, 4 `Worker.*` (m2 semantics,
  sliding membership, sliding late rule, window closing), 6 `Pipeline.*`
  (brute-force match at 1/2/4/8 threads tumbling and 1/3/8 sliding, a
  2-slot 1-event queue, late side file merged from four workers, bad config,
  missing input does not hang), 6 `Sliding.*` in `tests/window_test.cpp`,
  and `Wire.CampaignIdOffsetMatchesDecode`.
- `bench/smoke.sh build-sk 200000 7`: PASS for threads 1, 2, 4, 8, tumbling
  (92,609 windows identical) and sliding 300s/60s (436,985 windows
  identical); dup count 1,951 equals the oracle's in every run.
- 10M events (`--seed 1`), P=8: 1,701,892 windows tumbling and 2,105,868
  sliding, both identical to the oracle (the smoke loop's compare).
- ASan+UBSan (`build-sk-asan`): ctest 50/50, `bench/smoke.sh` PASS at all
  thread counts, and a 1M-event sliding run at 4 threads clean.
- TSan (`build-sk-tsan`): ctest clean, 1M events at `--threads 4` clean,
  1M events at `--threads 8` with `--batch 4 --queue-depth 4` (maximum
  handoff frequency) clean, `bench/smoke.sh` on the TSan build PASS.
- Warnings: every translation unit of this track compiles clean under
  `-Wall -Wextra -Wpedantic -Wshadow -Wconversion`; checked by touching all
  sources and rebuilding. (The one known warning in a full build is still
  clang 21 inside googletest's own headers; root CMake concern, out of this
  track's ownership.)

### What I would change

- Overlap the merge with processing, or let colstore ingest the part files
  directly; that is the 11% at P=8.
- Lower the default `--queue-depth` once m5's memory table is in; 4096 x 128
  events per worker is more buffer than a file source ever needs.
- The reader is the ceiling past 12 workers at ~40M events/s. Two readers
  over two halves of a file would double it; for a pipe the fix would be
  moving the hash into the workers (reader round-robins raw blocks, each
  worker forwards foreign campaigns to their owner), which is a second
  queue layer and only worth it on a bigger machine.
- Replace `std::unordered_set` in the dedup set and `std::map` for open
  windows with flat structures; at P=8 the cache-miss per event is now the
  cost that shows (dedup went from 13% to 19%).

## 2026-10-19: m4, checkpoints and exactly-once resume

**Problem.** A streaming job runs for days and the process will die: a
crash, a deploy, SIGKILL from the OOM killer, a reboot. It has to come back
and continue without counting any event twice or losing any, and the output
it already wrote has to end up byte-identical to what an uninterrupted run
would have produced. m3 had no story for this: a restart meant a rerun from
byte 0 and a second copy of every row already emitted.

**Decision: marker-based checkpoints (Chandy-Lamport style), per-worker
atomic snapshots, a MANIFEST rename as the commit point, and
truncate-and-replay of the per-worker output files on resume.**
`streamkit/checkpoint.h`, `pipeline.cpp` (`Worker::checkpoint`,
`Reader::inject_marker`, `plan_start`).

### The protocol, in plain language

Every N input events (`--checkpoint-every-events`, default 1,000,000) the
reader knows "I have handed out every byte before input offset X". It
flushes its partial batches and drops a marker (epoch k, offset X) into
every worker's queue, in the same queue the events travel in, right behind
the last event before X. Queues are FIFO, so when worker i dequeues the
marker it has processed exactly its share of the events before X and
nothing after. At that instant it:

1. `fflush`es and fsyncs its part files, so the lengths it is about to
   record are really on disk;
2. serializes (open windows, dedup buckets, part file lengths, watermark,
   cumulative counters) to `epoch-k/worker-i.tmp`, fsyncs, renames to
   `worker-i.snap`, fsyncs the directory;
3. reports to the coordinator. The last of the P workers to report writes
   `manifest.tmp` {epoch k, offset X, P, window parameters}, fsyncs, renames
   it to `MANIFEST`, and deletes `epoch-(k-1)`.

Resume (`--resume`) reads `MANIFEST`, deletes every other `epoch-*`
directory, loads each worker's snapshot, truncates each part file to its
recorded length, seeks the input to X and continues. Rows the dead run wrote
after the checkpoint are gone with the truncate and come back from the
replayed events.

Nothing ever stops anything else: the marker rides in the data stream, so a
worker that is behind simply reaches it later, and the reader keeps reading
the next block while workers snapshot.

### Why markers and not "stop the world and snapshot"

Stop-the-world means a barrier on the hot path: the reader stops, every
worker drains its queue, all P threads rendezvous, snapshot, restart. While
the slowest worker drains (queues hold up to 4096 x 128 events each), P-1
cores idle; and the barrier itself is a shared synchronization point on a
pipeline whose whole design is "nothing shared on the hot path" (m3). The
marker costs one extra queue slot per worker per epoch and nothing else.
Both give the same consistency, "state = everything before X", because the
only communication between threads is the queue itself: there are no
in-flight messages besides the ones sitting in the queues, and the marker's
position in each queue captures those exactly. That is the Chandy-Lamport
observation applied to a pipeline with a single source.

### Why per-worker output files plus truncation, not deduplicated output rows

The alternative to truncation is to let the resumed run re-emit rows and
have something downstream (or a final pass) drop the second copy of each
`(window_start, campaign)`. That needs a memory of what was emitted, either
in the consumer or as a set of keys in the engine, and it changes the
semantics of the output file from "append-only rows" to "rows, some of
which are retractions of earlier rows". Truncation needs no memory at all:
one integer per worker (file length at the marker) and the fact that the
worker is the only writer of its file (which is why m3 gave each worker a
part file instead of a shared output with a lock). After `truncate(2)` the
file is in exactly the state it was in at the marker, and the replayed
events rewrite the tail deterministically: same events, same partitioning
(`partition_of` is deterministic across runs and machines on purpose), same
per-worker order, same bytes. The late side file is handled the same way
(`EventWriter` gained an append mode and a `bytes()` counter for it), so the
audit trail is exactly-once too.

### Why rename is the commit, and what each fsync buys

`rename(2)` is atomic on POSIX: a reader opening the final name sees either
the old file or the new one, never a half-written one. So every file in the
protocol is written to a `.tmp` name and renamed, and the question "did
this checkpoint happen" has a yes/no answer: `MANIFEST` names epoch k or it
does not. Snapshots are renamed before the manifest is written, so when the
manifest says k, all P snapshots of k exist.

`fsync(fd)` on the file makes its bytes durable before the rename, so a
power loss cannot leave a renamed-but-empty file. `fsync` on the directory
makes the rename itself durable: a directory entry is data of the directory,
and on ext4/APFS a crash shortly after a rename can otherwise keep the file
but forget the name. On macOS `fsync` only pushes data to the drive's write
cache; `fcntl(F_FULLFSYNC)` asks the drive to flush that cache too, so
`durable_sync()` uses it on Apple and falls back to `fsync` where the file
system refuses (some network and external volumes). The part files get the
same treatment before their lengths are recorded.

The checksum (FNV-1a over the snapshot) is belt and braces: rename already
guarantees a `.snap` is complete, but a 64-bit hash turns a bug or a disk
fault into a loud error instead of a silently wrong resume. The test
`Snapshot.TruncationAndCorruptionAreLoudErrors` flips single bits and cuts
the file at every interesting place.

### Crash points, and what happens at each

| crash while | on disk | on resume |
|---|---|---|
| snapshots of epoch k being written | `epoch-k/` partial, MANIFEST says k-1 | `prepare_resume` deletes `epoch-k/`, resumes from k-1 |
| all snapshots of k written, manifest not yet renamed | `epoch-k/` complete, MANIFEST says k-1 | same: only the rename commits; k is deleted |
| after MANIFEST rename, before deleting k-1 | `epoch-(k-1)/` and `epoch-k/`, MANIFEST says k | k-1 deleted, resume from k |
| final merge | `<out>.tmp` partial, parts intact, MANIFEST says last k | merge redone from the parts after replaying the tail |
| after output rename, before deleting the checkpoint dir | output complete, MANIFEST says k | tail replayed and output rewritten with the same bytes (idempotent) |
| after deleting the checkpoint dir, before deleting parts | output complete, no MANIFEST | `--resume` finds nothing committed and starts from byte 0, rewriting the same bytes |
| before the first epoch commits | no MANIFEST | start from byte 0 |

The merge order in `run_pipeline` (late file, output, checkpoint dir, parts)
is chosen so every row of that table holds; the comment there spells it out.
Without `--resume`, a checkpoint directory that holds state is an error, not
silently ignored: starting over on top of a checkpoint would hide a bug.

### What if the input is a pipe

A resume has to rewind the input to offset X; a pipe (stdin, the Wikimedia
adapter) cannot be rewound, so `--ckpt-dir` with a non-seekable input fails
at startup with "needs a seekable input file; stdin or a pipe cannot be
rewound on resume" (`BlockReader::seekable`, test
`PipelineCheckpoint.PipeInputCannotBeCheckpointed` uses a FIFO). The real
fix for a pipe is upstream: a replayable log (Kafka-style offsets) in front
of the engine, so that "offset X" means something after a restart. The
engine's half of that contract is already in place: the manifest records an
offset, not a count of events processed. The same check also refuses
`--out -`, since stdout cannot be truncated.

### Progress needs the checkpoint interval shorter than the time between failures

Seen in the TSan run: with a kill every 2 s and TSan slowing the process so
that a resume (reload 16 MB of snapshots, rebuild a million-entry hash set)
plus 2 s of processing covered less than one 100K-event epoch, the run never
got past epoch 9. Each resume started from the same place. Not a bug, but
the thing to remember when choosing N: the harness's 100K events every
~0.25 s against kills every 0.2 to 3 s satisfies it, and every one of its
trials converged in at most 6 kills. The TSan run converged with 4 s between
kills.

### What a checkpoint costs

Apple M5 Pro (18 cores, 24 GB, APFS on the internal SSD), Apple clang 21,
Release, input `gen --n 10000000 --seed 1`, `--lateness-s 60`, output to a
file. Every number is from the `checkpoints:` line of the stderr summary or
from `/private/tmp/.../fsync_bench` (a 20-line C++ microbenchmark: write a
file, optionally fsync or F_FULLFSYNC, rename; 20 iterations).

Snapshot size, 8 threads: 18,221,744 bytes for 197,718 open windows plus
~990K retained dedup ids summed over workers, i.e. about 2.3 MB per worker.
At 1 thread with `--lateness-s 0`, which is the "100K open windows" case
asked for in the plan (97,952 open windows, 1M-event input): 9,072,065
bytes, slowest snapshot 35.5 ms, 4 snapshots in 125 ms total. The bytes are
44 per open window and 8 per dedup id; the dedup ids are half the file.

Throughput, 10M events, 8 threads, three runs each:

| mode | wall | events/s | per-snapshot (per worker) | slowest snapshot |
|---|---|---|---|---|
| no checkpoints | 0.36 s | 26.3M / 28.6M / 28.0M | n/a | n/a |
| `--ckpt-dir`, every 1M events (10 epochs) | 0.71 / 0.71 / 0.75 s | 14.1M / 14.1M / 13.4M | 34 to 38 ms (2.7 to 3.1 s total / 80) | 67 to 74 ms |
| same, `--no-fsync` | 0.46 / 0.44 / 0.41 s | 21.7M / 22.8M / 24.6M | 6 to 7 ms (0.50 to 0.56 s / 80) | 8 to 11 ms |
| `--ckpt-dir`, every 100K events (100 epochs) | 4.5 / 4.3 / 4.2 s | 2.2M / 2.3M / 2.4M | 35 to 36 ms (28 to 29 s / 800) | 75 to 86 ms |

At 1 thread (18 MB per snapshot): no checkpoints 2.06 s; with fsync 2.79 s,
70 ms per snapshot; `--no-fsync` 2.55 s, 49 ms per snapshot.

Reading the table:

- A checkpoint stalls each worker for about 35 ms at P=8, and the wall-time
  cost is exactly that: 10 checkpoints x 35 ms = 0.35 s, which is the
  0.36 s to 0.71 s difference. Nothing else slowed down; the summary's
  busy times outside snapshots are unchanged.
- Of those 35 ms, fsync is 28 ms and serializing 2.3 MB is 7 ms
  (`--no-fsync` isolates it). The microbenchmark says why: F_FULLFSYNC of a
  2 MB file is 6 ms alone (plain `fsync` 0.3 ms, no sync 0.4 ms), and each
  snapshot does three of them (part file, snapshot file, directory) while
  seven other workers are doing the same, so the drive serializes 24 cache
  flushes per epoch. At P=1 the fsync share is 21 ms of 70 ms and the
  encode of 18 MB (iterating a million-node hash set, FNV over the bytes)
  is the rest.
- The interval is in events, so at benchmark speed the default is a
  checkpoint every 36 ms, which is 10 to 100 times more often than anyone
  would checkpoint a real stream. At the generator's own rate (10K events/s
  simulated, which is what `--pace` replays) the default is one checkpoint
  every 100 s, and 35 ms every 100 s is 0.035%. The 100K-event row is the
  harness setting and exists to make the crash test dense, not to run in.
- `--no-fsync` is a measurement knob like `--no-dedup`: a checkpoint written
  without it still survives SIGKILL (the kernel has the bytes) but not a
  power loss. It is documented as such and off by default.

### Alternatives considered and rejected

- **Stop-the-world snapshot.** Above: a barrier on the hot path and idle
  cores for no gain in consistency.
- **Deduplicating output rows instead of truncating.** Above: needs memory
  of what was emitted and changes the output contract.
- **Write-ahead log of every event (or every emitted row).** Durable per
  event, so recovery replays only the tail, but it doubles the write
  traffic on the hot path for every event forever. The input file already
  is the log; we only need an offset into it. For a pipe source this is the
  thing the upstream has to provide (see above).
- **Incremental snapshots (only windows and dedup buckets that changed since
  the last epoch).** Smaller files, but the resume would have to replay a
  chain of deltas and every delta is a correctness risk. At 2.3 MB per
  worker per checkpoint a full snapshot is cheap enough that the simpler
  thing wins; revisit if the open-window state grows a hundredfold.
- **A coordinator thread with a condition variable.** The "last arriver
  commits" pattern under one mutex does the same with no extra thread and
  no wake-up latency, and the mutex is taken P times per epoch, i.e. once
  per 125K events per worker at the default interval.
- **Text (CSV/JSON) snapshots.** Readable, but 3 to 5 times the bytes and a
  parser on the resume path. The manifest is text because it is eight lines
  and `cat MANIFEST` is the first thing anyone does when debugging; the
  snapshots are binary because they are megabytes.
- **Bloom-filter dedup state in the snapshot.** Same argument as m2: a false
  positive after resume is a silently lost billable event.

### How it was verified

- `ctest --test-dir build-sk` (this track's tests): 70/70. New in
  `streamkit/tests/checkpoint_test.cpp`: 4 `Snapshot.*` (exact round trip
  with the byte count pinned, empty state, every truncation point and
  single-bit flips rejected, a doctored window count cannot cause a huge
  allocation, atomic write leaves no `.tmp`), 2 `Manifest.*` (round trip,
  five kinds of malformed input), 4 `Resume.*` (empty dir, partial epoch
  with no manifest, committed epoch kept while older/newer/complete-but-
  uncommitted epochs are removed, missing snapshot in a committed epoch is
  corruption), 1 `Coordinator.*` (last arriver commits, previous epoch
  deleted), 2 `DedupState.*`, 2 `WorkerResume.*` (truncate both part files
  and replay to byte-identical output, with a post-checkpoint duplicate
  proving the dedup state came back; a part shorter than its snapshot is an
  error), 6 `PipelineCheckpoint.*` (checkpointed run is byte-identical to a
  plain run and removes its directory; fail after marker 7 then resume at 1
  and 4 threads is byte-identical to the plain run for both the CSV and the
  late file, and the stale directory without `--resume` and a resume with a
  different thread count are errors; crash, resume, crash again, resume
  again; nothing committed means start over; a FIFO input is refused with a
  message naming "seekable"). `Pipeline.MissingInputThrowsBeforeAnyWorkerStarts`
  replaces the m3 test of the same scenario (the input is now opened before
  the workers exist) and `Pipeline.TornInputIsReportedNotHung` keeps the
  "reader fails mid-stream, nothing hangs" property pinned.
- `bench/crash.sh build-sk 50` (2M events seed 11, 4 threads, checkpoint
  every 100K events, throttle 400K events/s, SIGKILL after 0.2 to 3.0 s,
  restart with `--resume` until exit 0, then `compare.sh` against the
  oracle): **50/50 PASS, 392,549 windows identical in every trial, 139 kills
  in total, 46 of 50 trials killed at least twice, at most 6 kills in one
  trial, no trial finished without being killed.** The harness also fails a
  trial that leaves a part file, a `.tmp` output or the checkpoint directory
  behind; none did.
- `bench/smoke.sh build-sk 200000 7`: PASS at 1/2/4/8 threads, tumbling
  and sliding, unchanged from m3.
- Checkpointing does not change the output: 1M and 10M events at 8 threads
  with `--ckpt-dir` compared identical to the oracle (and the unit test
  compares bytes against a non-checkpointed run).
- ASan+UBSan (`build-sk-asan`): ctest 70/70; 1M events at 4 threads with
  checkpoints every 100K, SIGKILLed every 3 s (two kills) and resumed each
  time, identical to the oracle, zero sanitizer reports. (At 1.5 s spacing
  the ASan-slowed run could not complete an epoch between kills and never
  converged; see "progress needs the checkpoint interval" above.)
- TSan (`build-sk-tsan`): ctest 70/70; same kill/resume run (three kills at
  4 s spacing), identical to the oracle, zero reports. The coordinator's
  mutex and the per-worker files are the only cross-thread state the
  checkpoint adds; TSan sees the mutex and the queue's acquire/release.
- Warnings: every translation unit of this track (streamkit, common, the
  oracle, both test binaries) compiles clean under `-Wall -Wextra
  -Wpedantic -Wshadow -Wconversion`, checked by touching every source and
  rebuilding in `build-sk`.

### What I would change

- **Take the fsync off the worker's critical path.** Encode in the worker
  (7 ms), hand the bytes to a writer thread that does the part-file fsync,
  the snapshot write, the renames and the coordinator report. The worker's
  stall would drop from ~35 ms to ~7 ms per checkpoint at P=8. Not done in
  m4 because the measured cost at any realistic interval is already below
  0.1%, and the synchronous version is the one a reader can follow in one
  sitting.
- **Batch the F_FULLFSYNCs.** Three per worker per epoch, 24 per epoch at
  P=8, all hitting the same drive. One `F_FULLFSYNC` after all P snapshots
  are written (the drive flush is global, not per file) would be as durable
  and 8 times cheaper; it needs the coordinator to own the final sync.
- **Snapshot the dedup set more compactly.** The ids are half the bytes.
  Sorting each bucket and delta-encoding would cut that by 4 to 5 times;
  it costs a sort per bucket at snapshot time.
- **Time-based checkpoint interval** (`--checkpoint-every-s`) alongside the
  event-based one, so a slow stream still checkpoints regularly and a fast
  one does not checkpoint every 36 ms.
- **Resume on a pipe** via an upstream replayable log; the manifest already
  stores an offset, so only the reader's "seek" changes.

## 2026-10-21: m3 re-verified on the finished tree

**Why this entry exists.** The m3 entry above was written on 2026-10-19
while m3 was the newest code. Since then m4 (checkpoints) landed in the
same reader and worker, and the m5 emit-latency plumbing (`latency.h`,
`--latency-stats`) was wired in. This session re-ran every m3 "done when"
check on the tree as it stands, so the m3 claims are known to hold for the
code that will actually ship, not just the code that existed when they
were first made. No design changed; the diff of this session is one file
mode and the docs.

### One defect found and fixed

`bench/throughput.sh` was created on the 19th without the execute bit
(`-rw-r--r--`), so `bench/throughput.sh build-sk` failed with "permission
denied"; the m3 table must have been produced by invoking it through
`bash`. Fixed with `chmod +x`, which is how the other four harnesses are
stored. The README's command line now works as written.

### What was re-run, and the result

- **Warnings.** `touch` every source this track owns, then
  `cmake --build build-sk --target streamkit streamkit_tests adstack_tests
  oracle gen evdump`: 16 translation units recompiled, 0 warnings under
  `-Wall -Wextra -Wpedantic -Wshadow -Wconversion`. The fresh ASan and TSan
  trees (17 TUs each, googletest included) each show exactly one warning,
  and it is in googletest's own `gtest-all.cc` (`gtest-printers.h:524`,
  `-Wcharacter-conversion`, a clang 21 diagnostic). Our test TUs include
  gtest through `-isystem` and are clean; silencing the library's own
  compile belongs in the root CMake `FetchContent`, which this track does
  not own.
- **Unit tests.** `ctest --test-dir build-sk -R '^(Window|Agg|Sliding|...
  |Latency)\.'` (this track's suites only; colstore's are the other track's):
  78/78 (16 in `adstack_tests`, 62 in `streamkit_tests`).
- **Oracle match, `bench/smoke.sh build-sk 200000 7`:** PASS at 1, 2, 4
  and 8 threads, tumbling (92,609 windows identical) and sliding 300s/60s
  (436,985 windows identical); 1,951 dups in every run, equal to the
  oracle's.
- **ASan+UBSan (`build-sk-asan`):** 16/16 and 62/62 tests; 1M events
  (`gen --seed 1`) at `--threads 4`, tumbling: 205,806 windows identical to
  the oracle, sliding 300s/60s: 605,807 identical; stderr holds the three
  summary lines and nothing else (`grep -c 'Sanitizer|runtime error'` = 0).
- **TSan (`build-sk-tsan`):** 62/62 tests; 1M events at `--threads 4`
  identical to the oracle, 0 reports; and the maximum-handoff-rate
  configuration, 1M events sliding at `--threads 8 --batch 4
  --queue-depth 4`, identical to the oracle, 0 reports (3.29 s under TSan:
  250K batches through eight queues, which is the hardest case for the
  release/acquire pairing in `spsc_queue.h`).

### Throughput today, and why it is lower than on the 19th

`bench/throughput.sh build-sk`, same machine (Apple M5 Pro, 18 hardware
threads, 24 GB), same input (`gen --n 10000000 --seed 1`), same command
(`--lateness-s 60`), three runs per P, median:

| run | threads | run 1 | run 2 | run 3 | median events/s | speedup | reader stalls | worker idle waits |
|---|---|---|---|---|---|---|---|---|
| 00:44 EDT | 1 | 5,057,341 | 5,032,221 | 5,029,794 | **5,032,221** | 1.00x | 71,612 | 1 |
| | 2 | 8,970,398 | 8,740,039 | 9,338,358 | **8,970,398** | 1.78x | 32,392 | 2 |
| | 4 | 18,686,740 | 18,528,303 | 18,391,435 | **18,528,303** | 3.68x | 11,916 | 4 |
| | 8 | 27,098,227 | 27,124,952 | 27,033,898 | **27,098,227** | 5.38x | 1,891 | 161 |
| 00:48 EDT | 1 | 4,640,864 | 4,959,435 | 4,975,646 | **4,959,435** | 1.00x | 71,641 | 1 |
| | 2 | 9,317,083 | 9,452,555 | 9,133,616 | **9,317,083** | 1.88x | 32,144 | 2 |
| | 4 | 18,042,246 | 17,557,405 | 17,092,314 | **17,557,405** | 3.54x | 11,499 | 4 |
| | 8 | 21,588,898 | 23,669,106 | 25,958,136 | **23,669,106** | 4.77x | 1,937 | 62 |

And `bench/throughput.sh build-sk --reader-decode` (00:45 EDT): 5,064,618 /
9,328,241 / 17,045,726 / 27,734,074 events/s at P = 1/2/4/8, with 937
worker idle waits at P=8 against 161 on the default path in the run just
before it. That is the same picture as the 19th: at P=8 the two are a wash
and the raw-record path keeps the workers better fed.

On the 19th the same harness gave 5.20M / 9.41M / 19.9M / 30.9M (5.94x).
Today's P=8 is 12% lower on the first run and 23% lower on the second, and
the two runs four minutes apart disagree by 13% at P=8. Before blaming the
code, the evidence from the same minutes:

- `uptime` load averages went 4.37 (00:45) to 7.07 (00:48) on a machine
  that was otherwise idle on the 19th; `ps -r` showed `fseventsd` at
  84.5% to 88.9% CPU throughout (the other track's agent was building and
  this session had just written a 520 MB input, both of which the file
  event daemon indexes) and a Chrome renderer at 23%.
- The per-worker busy times at P=8 were 0.26 to 0.32 s against 0.25 to
  0.28 s on the 19th: the workers themselves run at the same speed, within
  the noise.
- The serial merge took 0.071 to 0.095 s against 0.035 s on the 19th.
  That is 68 MB of CSV copied through a file system whose event daemon is
  busy; it is 0.04 to 0.06 s of the 0.05 s gap between a 0.32 s and a
  0.37 s wall time at P=8, i.e. nearly all of it.
- P=1 single runs taken during the same minute were 4.43M and 4.53M against
  the harness's 5.03M median a few minutes earlier, so even the
  single-thread number moved 12% with load; the P=8 gap is the same noise
  applied to a 0.37 s run.

So the difference is accounted for by I/O and load on the measuring
machine, not by code: the workers' compute is unchanged and the extra wall
time is in the merge. The one hot-path change since the m3 numbers is
m4's reader, which gained two per-event branches (the marker check
`handed_ == next_marker_at_` and the `--throttle-eps` test). Their cost
cannot be separated from today's noise; a quiet-machine rerun is the
right test and is the first step of m5's benchmark pass, together with the
"what I would change" item of hoisting both checks to once per block if
they show.

What to quote until then: the 19th's table was taken on a quiet machine
and is the better measurement; today's confirms correctness and the shape
of the scaling (3.5 to 3.7x at 4 threads, 4.8 to 5.4x at 8) on the final
code, and shows the harness's run-to-run spread under load, which m5's
write-up has to state next to any single number.

### Files touched this session

`bench/throughput.sh` (mode only), `streamkit/README.md` (the two flags
main.cpp already had but the README did not list, `--no-fsync` and
`--latency-stats`, and the m3 status line), this file.

## 2026-10-21: m4 re-verified on the finished tree, one durability gap closed

**Why this entry exists.** The m4 entry above was written on 2026-10-19.
This session re-ran every m4 "done when" check on the tree as it stands
today (with the m5 emit-latency plumbing wired in), re-measured the
checkpoint cost, and read the resume path once more with the crash table
in hand. Every number below is from a command run in this session; the
19th's numbers stay above as the first measurement.

### One gap found and closed: the output rename was not made durable

`merge_atomic` (pipeline.cpp) wrote `<out>.tmp`, fsync'ed it, renamed it
over `<out>`, and returned. Every rename in `checkpoint.cpp` is followed by
an fsync of the parent directory, for the reason given in the m4 entry: the
rename lives in the directory's data, and until the directory is fsync'ed a
power loss can keep the file and forget the name. The output rename skipped
that step, and it is the one rename where the omission has teeth, because
`run_pipeline` deletes the checkpoint directory right after it. Against a
power loss in that window the file system was free to persist the deletion
and lose the rename, leaving neither an output file nor a MANIFEST to resume
from; the next `--resume` would start over from byte 0 (correct, but the
whole run's work gone), and a user who did not pass `--resume` would see no
output and no checkpoint and no explanation.

Why SIGKILL never caught it: a killed process loses nothing the kernel
already has, and `rename` is in the kernel's cache the moment it returns.
Only a power cut or kernel panic between the rename and the next journal
flush exposes the ordering, which no software harness on one machine can
produce. The fix is one `durable_sync_dir(parent)` after the rename, the
same call the checkpoint files already use, with a comment that says why.
Cost: one directory fsync per run, after the last byte of output, which
is below the run-to-run noise (1M events at 4 threads with checkpoints
took 0.31 to 0.33 s wall with the fsync in place; the only pre-edit runs
were taken under harness load and are not a fair comparison).

Why not also fsync the directory after `remove_all(ckpt_dir)` and the part
deletions? Losing those deletions is harmless: a stale checkpoint directory
next to a finished output is exactly the "after output rename, before
deleting the checkpoint dir" row of the crash table, and `--resume` replays
the tail and rewrites the same bytes. The row that mattered was the one
where the output could vanish.

### What was re-run, and the result

- **Warnings.** `touch` every source this track owns, then
  `cmake --build build-sk --target streamkit streamkit_tests adstack_tests
  oracle gen evdump`: 16 translation units recompiled, 0 lines matching
  `warning:`; the ASan and TSan trees, configured fresh in this session,
  also 0. Rebuilt again after the `merge_atomic` edit: 0.
- **Unit tests.** `ctest --test-dir build-sk -R '^(...this track's
  suites...)'`: 78/78 (16 in `adstack_tests`, 62 in `streamkit_tests`), in
  Release, ASan+UBSan and TSan, before and after the edit. The m4 suites
  are the 20 tests in `checkpoint_test.cpp` (4 `Snapshot.*`, 2
  `Manifest.*`, 4 `Resume.*`, 1 `Coordinator.*`, 2 `DedupState.*`, 2
  `WorkerResume.*`, 5 `PipelineCheckpoint.*`), covering the four things the
  plan names: snapshot round trip, truncate-on-resume, manifest parsing,
  partial-epoch detection.
- **`bench/smoke.sh build-sk 200000 7`:** PASS at 1/2/4/8 threads,
  tumbling and sliding, before and after the edit.
- **`bench/crash.sh build-sk 50`, run twice** (2M events seed 11, 4
  threads, checkpoint every 100K events, throttle 400K events/s, SIGKILL
  after 0.2 to 3.0 s, `--resume` until exit 0, `compare.sh` against the
  oracle):
  - 00:52 EDT, binary before the edit: **50/50 PASS, 392,549 windows
    identical in every trial, 148 kills, 46 of 50 trials killed at least
    twice, 1 trial not killed at all, at most 5 kills in one trial.**
  - 00:58 EDT, final binary: **50/50 PASS, 136 kills, 43 of 50 trials
    killed at least twice, 1 trial not killed, at most 6 kills.**
  The final run of every trial reports which epoch it resumed from (10 to
  20 of the 20 epochs in the stream), so the kills landed throughout the
  run, not only at the start. No trial left a part file, a `.tmp` output or
  the checkpoint directory behind (the harness checks).
- **Edge cases by hand** (final binary): `--in -` piped from `cat` with
  `--ckpt-dir` exits with "needs a seekable input file; stdin or a pipe
  cannot be rewound on resume"; a directory left by a run killed after
  epoch 3 is refused without `--resume` ("holds a previous run; pass
  --resume to continue it or remove it"); `--resume` with a different
  `--lateness-s` is refused ("flags differ from the checkpoint's"); with
  matching flags it resumes from epoch 3 at offset 15,600,008 and finishes;
  `cat MANIFEST` shows the eight `key value` lines.
- **ASan+UBSan (`build-sk-asan`)**, final binary: 1M events (`gen --seed
  1`) at 4 threads, `--lateness-s 60 --late-out`, checkpoint every 100K
  events, throttled to 150K events/s, SIGKILLed every 3 s: 2 kills, resumed
  from epoch 8, 205,806 windows identical to the oracle, 0 lines matching
  `Sanitizer|runtime error`.
- **TSan (`build-sk-tsan`)**, final binary: same input and flags at 100K
  events/s, SIGKILLed every 4 s: 3 kills, resumed from epoch 9, 205,806
  windows identical, 0 lines matching
  `Sanitizer|ThreadSanitizer|runtime error|data race`.

### Checkpoint cost, re-measured

Apple M5 Pro, 18 hardware threads, APFS on the internal SSD, Release,
`build-sk/bin/streamkit`, input `gen --n 1000000 --seed 1`, load average
4.9 at the time (the other track's agent was active; a quieter machine than
during the harness runs, which ran at the same time as a first attempt at
these numbers and inflated them by 40 to 80%). Three runs each; the
`checkpoints:` line of the stderr summary is the source.

The plan's "100K open windows" case, 1 thread, `--lateness-s 0`,
`--checkpoint-every-events 250000` (4 snapshots per run):

| | snapshot bytes | open windows | slowest snapshot | all 4 snapshots | wall |
|---|---|---|---|---|---|
| with fsync (default) | 9,072,065 | 97,952 | 31.2 / 31.2 / 31.9 ms | 119 / 112 / 111 ms | 0.31 / 0.29 / 0.29 s |
| `--no-fsync` | 9,072,065 | 97,952 | 17.1 / 17.1 / 19.6 ms | 58 / 60 / 66 ms | n/a |
| no checkpoints | | | | | 0.19 / 0.18 / 0.18 s |

So a 9.07 MB snapshot of 98K open windows costs about 28 to 30 ms, of
which 13 to 15 ms is the three durable syncs (part file, snapshot,
directory) and the rest is serializing 98K windows plus ~500K dedup ids and
hashing the bytes. The size is deterministic (same bytes in every run and
in the 19th's measurement): 44 bytes per open window and 8 per dedup id.

The harness setting, 4 threads, `--lateness-s 60`, every 100K events (10
epochs, 40 snapshots per run): 16,615,820 bytes per epoch summed over the
four workers (197,664 open windows summed, ~4.2 MB per worker), slowest
single snapshot 33.7 / 36.5 / 37.0 ms, 0.86 / 0.97 / 0.95 s of snapshot
time in total (21 to 24 ms per snapshot), wall 0.31 / 0.33 / 0.33 s against
0.07 / 0.08 / 0.07 s without checkpoints. That is the cost of checkpointing
every 30 ms of input at benchmark speed, which is the density the crash
test needs and 10 to 100 times more often than anyone would run; the m4
entry's arithmetic for realistic intervals still holds.

### Files touched this session

`streamkit/pipeline.cpp` (the directory fsync in `merge_atomic` and its
comment), `streamkit/README.md` (the m4 status line), this file.

## 2026-10-21 (afternoon): m4 re-verified from a fresh configure, third crash-harness pass

**Why this entry exists.** This session was handed the m4 milestone with the
tree already at m5 plus the colstore integration. Rather than re-implement
anything, it checked every item of the m4 brief against the code and the two
entries above, then re-ran every "done when" check from a fresh
`cmake` configure of all three trees. Nothing in the code needed changing;
this entry is the evidence for today's tree and one measurement lesson. The
numbers from the 19th and this morning stay above as earlier measurements.

### The brief, item by item, and where each lives

| brief item | where | checked by |
|---|---|---|
| marker every N events carrying (epoch, offset X), in-band after the last event before X | `Reader::inject_marker` flushes partial batches, then one marker batch per queue | `PipelineCheckpoint.*`, the harness |
| worker snapshots (open windows, dedup buckets, part length) to `.tmp`, fsync, rename `.snap` | `Worker::checkpoint`, `atomic_write_file`; format documented at the top of `checkpoint.h` | `Snapshot.*`, `WorkerResume.*` |
| manifest written when all P snapshots exist, rename is the commit, k-1 deleted | `CheckpointCoordinator::snapshot_written` (last arriver commits) | `Coordinator.LastArriverCommitsAndDeletesPreviousEpoch` |
| `--resume` loads snapshots, truncates parts, seeks input; stale dir without `--resume` is an error | `plan_start`, `open_outputs_resumed`, `truncate_part` | `PipelineCheckpoint.FailAfterCheckpointThenResumeIsByteIdentical`, by hand below |
| partial epoch ignored; k-1 left behind cleaned; concat to tmp then rename; input exhausted still atomic | `prepare_resume`, `merge_atomic`, merge order in `run_pipeline` | `Resume.*`, `PipelineCheckpoint.CheckpointsCommitAndDirectoryIsRemovedOnSuccess` |
| `--throttle-eps` | `Reader::run`, checked every 1024 events | the harness kills 149 times in 50 trials |
| `bench/crash.sh`, 50 trials, per-trial kills and result, nonzero exit on any failure | `bench/crash.sh` | run below |
| pipe input fails clearly | `BlockReader::seekable` in `plan_start` | `PipelineCheckpoint.PipeInputCannotBeCheckpointed`, by hand below |
| F_FULLFSYNC on Apple | `durable_sync`, `EventWriter::sync` | read; the `--no-fsync` column below is what it costs |

### What was re-run, and the result

- **Configure and warnings.** All three trees re-configured with
  `cmake -S . -B <dir> -G Ninja -DCMAKE_BUILD_TYPE=Release
  -DADSTACK_SANITIZE=<none|address|thread>`, every source this track owns
  touched, then `cmake --build <dir> --target streamkit streamkit_tests
  adstack_tests oracle gen evdump`: 16 translation units recompiled per
  tree, 0 lines matching `warning:` in each.
- **Unit tests.** `build-*/bin/adstack_tests` and `build-*/bin/streamkit_tests`
  run directly: 16 + 63 = 79 passed in Release, ASan+UBSan and TSan, 0
  lines matching `Sanitizer|runtime error|data race` in any of the six
  runs. (`ctest -R` with a prefix list matched only 64 of them, which is
  why the binaries were run directly; the ctest subset also passed 64/64
  in each tree.)
- **`bench/smoke.sh build-sk 200000 7`:** PASS at 1/2/4/8 threads, 92,609
  windows identical tumbling and 436,985 sliding in every run.
- **`bench/crash.sh build-sk 50`** at 12:20 EDT (2M events seed 11, 4
  threads, checkpoint every 100K events, throttle 400K events/s, SIGKILL
  after 0.2 to 3.0 s, `--resume` until exit 0, `compare.sh` against the
  oracle): **50/50 PASS, 392,549 windows identical in every trial, 149
  kills in total, 47 of 50 trials killed at least twice, every trial
  killed at least once (3 trials exactly once), at most 6 kills in one
  trial (trial 26).** The finishing run of each trial resumed from epochs
  10 through 20 of the 20, so the kills landed across the whole run. No
  trial left a part file, a `.tmp` output or the checkpoint directory
  behind. The run shared the machine with the other track's agent (load
  average 4.7 to 7.7 over the run).
- **Edge cases by hand**, `build-sk/bin/streamkit` on `gen --n 1000000
  --seed 1`, exit codes captured under bash:
  `cat ev.ads | streamkit --in - --ckpt-dir D` exits 1 with "needs a
  seekable input file; stdin or a pipe cannot be rewound on resume";
  `--out - --ckpt-dir D` exits 1 ("needs --out to be a file"); a run
  SIGKILLed after 0.8 s exits 137 and leaves `MANIFEST` naming epoch 2
  (`cat` shows the nine `key value` lines); the same command without
  `--resume` exits 1 ("holds a previous run; pass --resume to continue it
  or remove it"); `--resume` with `--lateness-s 30` or `--threads 2` exits
  1 ("flags differ from the checkpoint's"); `--resume` with matching flags
  exits 0, 205,806 windows identical to the oracle, checkpoint directory
  and parts gone.
- **ASan+UBSan (`build-sk-asan`)**: 1M events at 4 threads, `--lateness-s
  60 --late-out`, checkpoint every 100K events, throttled to 150K
  events/s, SIGKILLed every 3 s: 2 kills, final run resumed from epoch 8,
  205,806 windows identical, 0 sanitizer lines.
- **TSan (`build-sk-tsan`)**: same flags at 100K events/s, SIGKILLed every
  4 s: 3 kills, resumed from epoch 8, 205,806 windows identical, 0 lines
  matching `Sanitizer|ThreadSanitizer|runtime error|data race`.

### Checkpoint cost at 100K open windows, re-measured

Apple M5 Pro, 18 hardware threads, 24 GB, APFS on the internal SSD,
Release `build-sk/bin/streamkit`, input `gen --n 1000000 --seed 1`, load
average 5.6 to 5.7 (the other track's agent was building and testing the
whole time, so wall times carry more noise than the per-snapshot timers,
which clock only the worker's own checkpoint call). Three runs per row;
the `checkpoints:` line of the stderr summary is the source.

The plan's case, 1 thread, `--lateness-s 0`, `--checkpoint-every-events
250000` (4 snapshots per run, 97,952 open windows and 595,368 dedup ids at
the last one):

| | snapshot bytes | slowest snapshot | all 4 snapshots | 4 manifests | wall |
|---|---|---|---|---|---|
| with fsync (default) | 9,072,065 | 34.8 / 37.6 / 40.1 ms | 126 / 126 / 137 ms | 14.7 / 14.7 / 14.4 ms | 0.33 / 0.35 / 0.39 s |
| `--no-fsync` | 9,072,065 | 25.4 / 25.5 / 27.2 ms | 88 / 86 / 94 ms | 0.8 / 0.7 / 0.8 ms | 0.37 / 0.35 / 0.38 s |
| no checkpoints | | | | | 0.29 / 0.30 / 0.31 s |

So a 9.07 MB snapshot of 98K open windows costs 31 to 34 ms, of which 9 to
11 ms is the three durable syncs and the rest serializing 98K windows and
~600K ids plus the FNV pass. The byte count is identical to the two
earlier measurements (44 bytes per window, 8 per id: deterministic). The
wall-time columns overlap under this load and are reported for honesty,
not as evidence; the per-snapshot timers are the number to quote.

The harness setting (4 threads, `--lateness-s 60`, every 100K events, 10
epochs, 40 snapshots per run): 16,615,820 bytes per epoch summed over the
four workers (197,664 open windows summed), slowest single snapshot 55.7 /
48.3 / 42.8 ms, 1.028 / 0.953 / 0.959 s of snapshot time per run (24 to
26 ms per snapshot), wall 0.39 / 0.36 / 0.38 s against 0.13 / 0.13 / 0.13
s without checkpoints. Same shape as this morning's numbers, a little
slower on a busier machine.

### A measurement dead end worth one paragraph

The first attempt at the table above ran the flag variants from a zsh
loop with the extra flags in an unquoted `$extra` variable. zsh does not
word-split an unquoted variable, so `--ckpt-dir cost-ckpt
--checkpoint-every-events 250000` reached the binary as a single
argument, the arg parser saw an unknown flag with no value, and all three
"modes" ran without any checkpoint at all, producing three identical
columns of wall times and no `checkpoints:` line. The tell was the missing
summary line, not the numbers. The loop was redone under `bash -c`, and
the table reports only those runs. Lesson for the harnesses, which already
follow it: `bench/*.sh` run under `#!/usr/bin/env bash`, never from an
interactive zsh loop, and a measurement without its summary line is not a
measurement.

### Files touched this session

`streamkit/README.md` (the m4 status line gains today's harness numbers),
this file. No source file changed; the tree that passed is the tree that
was handed over.

## 2026-10-21: m5, benchmarks and the emit-latency metric

**Problem.** The plan's m5 is a table: throughput vs threads, p50/p99
emit latency under paced replay, memory, dedup overhead, every cell
reproducible by `bench/throughput.sh`. Two of those are plain
measurements. The third, latency, first needs a definition that cannot be
mistaken for something it is not: an engine that closes windows on a
watermark has no single "the event was processed" moment to time, and
every obvious definition measures something other than the engine. This
entry records the definition, why it is that one, how the harness is
built, what the numbers turned out to mean (one of them was a surprise),
and what I would change next. The numbers themselves, with the machine,
load and command for each, are in `BENCHMARKS.md`; this entry quotes only
the ones it argues about.

### Decision 1: emit latency is read(2)-to-row, per watermark-closed window, per worker

For every window that a worker's watermark closes, the sample is
`t_row - t_block`: `t_block` is `steady_clock` read by the reader right
after `read(2)` returned the block of bytes holding the event that pushed
this worker's watermark past the window's end; `t_row` is `steady_clock`
read by the worker right after `fprintf` appended that window's row to its
output stream. p50/p99 are nearest-rank percentiles over the run's samples.
It excludes the allowed lateness (event time, configured, not a cost of
the engine), the producer's buffering before the bytes reach the process,
the stdio flush to disk, the final merge, and windows closed by end of
input. In one sentence for an interviewer: it is the time from "the
process could first have known the window was closable" to "the row
exists", and nothing else.

Why that and not the alternatives:

- **Not `t_row - ingest_time` of the closing event.** `ingest_time` is
  data. With `gen --pace` it tracks the wall clock within gen's own
  buffering, so this would measure gen's pacing accuracy plus its 1 MiB
  write buffer (measured: ~100 ms bursts, below) ahead of anything the
  engine does; with the Criteo file it would be days. A benchmark number
  that depends on the producer's clock is not a number about the engine.
- **Not a stamp per event.** That is 8 more bytes per event through the
  queue and a clock read per event (tens of nanoseconds against a ~200 ns
  per-event budget), on by default or behind a branch in the hot loop
  either way. A stamp per input block is one clock read per `read(2)` and
  loses no precision about arrival: every event in a block entered the
  process at the same instant, which is the instant stamped. On a file the
  reader runs ahead of the workers and a batch can straddle two blocks; it
  carries the earlier stamp, so a sample is never understated (pipeline.h
  says so at the field). On a pipe every short read flushes the partial
  batches, so batches never straddle.
- **Per-worker watermark, not "when could any worker have known".** The
  engine closes a window when *its* partition has seen an event past the
  end plus lateness; that is the semantics being measured. A global
  watermark (min over workers) closes later, never earlier, so the
  per-worker instant is the earliest any engine with this partitioning
  could close the window, which is what "could first have passed" has to
  mean. The gap to the first qualifying event in *any* partition is one
  partition inter-arrival time, about P x 5 us at 200K events/s.
- **Stop the clock after `fprintf`, not after flush or fsync.** The row is
  available to the engine's consumer once it is in the stream; the 1 MiB
  flush happens every ~23K rows and belongs to no particular window, and
  fsync is a checkpoint matter. Timing the flush would make the p99 a
  statement about stdio's buffer size.
- **Exact samples, not a histogram.** 4 bytes per closed window while the
  flag is on (5.8 MB for the 20 s run); nearest-rank percentiles are
  always a value that was observed, which is easier to defend than an
  interpolated one; a histogram needs bucket boundaries chosen before the
  distribution is known. `latency.h` has the arithmetic and
  `tests/latency_test.cpp` pins it on inputs whose answer is known.
- **Flag-gated, zero cost off.** `Config::latency_stats` is tested in the
  reader once per block and in `close_ready` once per call; with it off no
  clock is read and nothing is stored (`Latency.OffByDefaultRecordsNothing`).
- **End-of-input closings are not samples.** They are not watermark
  closings; including them would add one sample per still-open window at
  EOF with a value that means "how long since the last block", which is
  not latency.

Two additions after the first run, both so the write-up could be checked
rather than believed: `min` next to p50/p99/max, because the samples
turned out to be spread uniformly from the min (the first row of a
closing burst, the pure path through queue, decode and close) to the max
(the last row, charged for every row before it), and `input reads` on the
same line, because the arrival granularity of the stamps on a pipe is the
producer's write pattern and should be printed, not assumed.

### Decision 2: the harness

`bench/throughput.sh` grew from one table to five sections (`threads`,
`dedup`, `checkpoint`, `memory`, `latency`), selectable with `--only`, with
the machine, build, load average, input and exact command printed above
every table, three runs per cell with the median in bold and all three
shown, and the cached inputs generated once by seed.

- **Runtime flags, not build flags, for the variants.** `--no-dedup`,
  `--checkpoint-every-events`, `--queue-depth` compare one binary with one
  variable changed. A build flag compares two binaries whose code
  generation differs everywhere, and the difference measured is then the
  compiler's, not the feature's. The cost is that `--no-dedup` exists in a
  shipping binary; it is documented as a measurement knob and produces
  wrong output on purpose.
- **Medians of three with the spread shown**, because the machine was
  never idle (the other track's agent built and tested in a sibling
  directory the whole time; load average 3.5 to 4.5 on 18 cores). The
  4-thread cell of the first run (12.2M, 14.6M, 18.8M events/s within
  seconds of each other) is the argument for showing all three: a median
  alone would have hidden a 50% spread.
- **`gen --pace` into a pipe for latency, not `--throttle-eps` on a file.**
  `--throttle-eps` paces the reader *after* `read(2)`, so the stamp would
  sit before the throttle sleep and the samples would include it;
  `--pace` puts the pacing upstream of the stamp, and it exercises the live
  path (short reads, partial-batch flushes) rather than the file path.
- **200K events/s, 1 s windows, 2 s lateness, 20 s.** Twenty times the
  generator's native rate, chosen so that something closes: with 100K
  campaigns about 86K windows close per second of stream, which is the
  emission path under load. A 60 s window would close nothing in 20 s;
  with 1 s / 2 s the first closing is at t = 3 s and the run has 17
  boundaries and 1.46M samples. A first 3 s trial run produced only the
  sparse negative-time windows from late events and a p99 of 2 ms that
  meant nothing; the harness runs 20 s for that reason.

### What the numbers say

Full tables in `BENCHMARKS.md`. The parts that needed interpreting:

- **Throughput.** Two harness runs, 17 minutes apart: 5.05M / 9.35M /
  14.6M / 27.3M and 4.45M / 8.76M / 18.3M / 26.3M events/s at 1 / 2 / 4 / 8
  threads (5.4x and 5.9x at 8). The 19th's idle-machine table (5.20M /
  9.41M / 19.9M / 30.9M) remains the best measurement of the code; today's
  runs bound what a loaded machine gives and are reported as such.
- **Dedup costs 51 ns per event**, the same on a dirty and on a clean
  stream (1/5.01M - 1/6.73M and 1/4.99M - 1/6.70M at 1 thread): one hash
  insert that misses cache, paid whether or not a duplicate is found. The
  m2 entry said 13% and this says 25.6%, and both are right: m2's baseline
  was the whole single-threaded loop including the read; since m3 the read
  is on another thread, the no-dedup worker is faster (6.7M against 5.9M),
  and the same 51 ns is a larger share of a smaller number. Quote the
  nanoseconds.
- **Checkpoints cost exactly their snapshots.** 2.591 s of snapshot time
  over 80 snapshots is 32 ms each; 10 epochs x 32 ms = 0.32 s, and the wall
  time grew by 0.317 s (0.354 to 0.671 s). At any interval a real stream
  would use (the default is one checkpoint per 100 s at the generator's
  native rate) that is 0.03%.
- **State is flat; the queues are not.** 1M to 10M events at 1 thread is
  92.5 to 102.4 MiB with the dedup set at 0.99M to 1.19M ids and the
  open-window map at ~200K in both. At 8 threads the default 4096-slot
  queues (27 MB per worker) are touched only when the reader runs that far
  ahead, which it never fully does on 1M events and always does on 10M, so
  130 to 334 MiB; `--queue-depth 256` gives 87 to 137 MiB. The remaining
  50 MiB at 8 threads with shallow queues is 10 MiB of ids plus allocator
  slack from the bucket expiry that only begins once the stream is longer
  than the 120 s horizon; whether it keeps growing past 10M is not
  measured and is listed below.
- **Emit latency: p50 6.7 ms, p99 12.9 ms at 8 threads; p50 13.4 ms, p99
  23.3 ms at 1 thread**, on top of the 2 s lateness, at 200K events/s with
  86K windows closing per second. The decomposition, each step from a run
  in `BENCHMARKS.md` section 5:
  1. The producer's pattern is measured, not assumed: gen's writer buffers
     1 MiB, so at 200K events/s the pipe receives ~990 KB bursts every
     ~100 ms, which the reader consumes in 64 KiB pieces (1,186 events per
     `read(2)`, the "input reads" column).
  2. With 1K campaigns (141 rows per worker per boundary) the p99 is still
     1.27 ms at 8 threads and 3.0 ms at 1: that is the closing event
     waiting behind the rest of its ~20K-event burst, which one worker
     processes in ~3 ms and eight in ~1.3 ms each. This floor is the
     producer's burst, not the queue; a producer that flushed per event
     would not show it.
  3. Everything above the floor is rows written one after another: all of
     a worker's windows for a second close in one call. `--no-dedup` and
     `--batch 16` move the p99 by under 1 ms, so bucket expiry and batching
     are not it.
  4. The surprise: **the cost per row is 0.24 us when one worker writes
     and 1.0 to 1.1 us when eight write at once**, and at eight threads it
     is the same ~1.0 us per row at 6.3K, 10.8K and 15.4K rows per burst
     (50K, 100K, 200K campaigns). So it is a per-row cost that grows with
     the number of concurrently emitting workers, not a per-burst fixed
     cost (which rules out the other candidate, one serialized 1 MiB
     `write(2)` per worker per flush: that would be a constant). The
     busy-time counters agree: the same work costs 4 x 0.15 s of worker
     time at P=4 and 8 x 0.145 s at P=8. Two things are in the row path,
     libc `fprintf` (the FILE lock plus the formatter) and `std::map::erase`
     (a node free through the process-wide allocator), and one of them
     scales badly across threads on this libc; telling them apart needs a
     code change and is the first item below. The core layout of the M5
     Pro (6 + 12) was checked as an explanation and rejected: P=5 (six
     threads with the reader) and P=6 (seven) both give 11.5 ms.
  5. The consequence is a sweet spot at 4 threads for this workload (p99
     8.4 ms, against 12.4 ms at 2, 11.5 at 5 and 6, 12.9 at 8, 12.5 at 12):
     past four workers the fall in rows per worker is cancelled by the
     rise in cost per row. Throughput and emit latency have different
     optimal P on this machine, and the harness is what shows it.

### What it costs

- Measurement: with `--latency-stats`, one `steady_clock` read per input
  block in the reader, one per emitted row in the worker, and 4 bytes per
  watermark-closed window held until exit (5.8 MB for the 20 s run). Off,
  one predictable branch per block and per `close_ready` call. The
  `input_reads` counter is one increment per block, always on.
- Harness: about 3.5 minutes for all five sections and 1.1 GB of cached
  inputs in `/private/tmp/adstack-sk`. Removed after this session's runs.

### How it was verified

- Warnings: 0 across `build-sk`, `build-sk-asan`, `build-sk-tsan` after
  touching every source this track owns (10 translation units per tree
  before the m5 edits, 5 after). Tests: 16 + 63 = 79 passed in each of the
  three builds, run from the test binaries directly; the 8 `Latency.*`
  tests cover the percentile arithmetic, the clamp, one sample per
  watermark-closed window and none at EOF, nothing recorded when the flag
  is off, samples from every worker, and (new) the block counter on a
  file larger than one block.
- `bench/smoke.sh build-sk`: PASS at 1/2/4/8 threads, tumbling (92,609
  windows identical) and sliding (436,985), 1,951 dups in every run equal
  to the oracle's.
- `bench/crash.sh build-sk 50`: 50/50 PASS, 125 kills, 44 trials killed
  at least twice, no leftovers.
- ASan+UBSan and TSan on the paced latency path (500K events at 100K
  events/s, 4 and 8 threads, `--latency-stats --late-out`): exit 0, zero
  sanitizer lines, 310,446 windows written in each run.
- The harness's parsing was checked against the raw stderr lines in the
  inline experiments (the same `emit latency` line, read by eye).

### What I would change next

1. **Format rows without libc `printf`, and erase closed windows in bulk.**
   A hand-rolled unsigned-to-ASCII into a worker-owned buffer with a
   `write(2)` when full removes the FILE lock and the formatter from the
   row path; `open_.erase(begin, it)` after the loop replaces one free per
   row with a batch of frees (or, further, an arena for map nodes so no
   free happens at all until the arena is recycled). The target is the
   P=1 per-row cost (0.24 us) at P=8, which would bring the p99 at this
   workload from 12.9 ms to about 4 ms and restore the expectation that
   more workers means lower emit latency. The harness's latency section is
   the acceptance test, and the first run tells which of the two was the
   culprit.
2. **A replayable-log source (Kafka) with offset commit tied to the
   checkpoint epoch.** The manifest already records "input offset X"; for
   a partitioned log it records a map of partition to offset, captured by
   the reader when it injects the marker, and the consumer group's offsets
   are committed only after the MANIFEST rename, never before. A crash
   between snapshot and commit then replays from the last committed
   offsets into the same truncate-and-replay that resume already does, so
   the pipeline is exactly-once end to end without idempotent sinks. It
   needs one marker per source partition and the reader to hold new
   partition assignments until the epoch boundary. This is the change
   that turns the engine from a file tool into a service.
3. **Incremental snapshots.** Half of every snapshot is dedup ids that
   barely change between epochs (one bucket appended, one expired), and the
   open windows change mostly at the front. A base snapshot plus per-epoch
   deltas (new buckets, count of expired buckets, windows touched since the
   last epoch, part-file lengths), compacted into a new base every k
   epochs, cuts the bytes per epoch by roughly the horizon / interval ratio.
   The cost is a chain to validate on resume (each delta hashed and bound
   to its predecessor) and resume time that grows with k. Not worth it at
   2.3 MB per worker per epoch; it is the right design once open state is
   a hundred times larger (long windows, many keys), which m4's
   "stop-the-world" note already said.
4. **Watermark alignment across partitions for ordered output.** Each
   worker publishes its watermark through one atomic; a merger pulls
   closed rows from P per-worker queues and emits them in window-start
   order only up to the minimum watermark. The output becomes globally
   time-ordered, which lets colstore ingest it pre-sorted and prune by
   zone map without a sort, and it is also the natural place to move the
   checkpoint fsync off the workers. The cost is that the slowest
   partition holds every partition's rows back, so emit latency becomes
   the max over partitions; on this workload that is bounded by the burst
   anyway, but a stalled partition would stall the output, which is
   exactly the trade the m3 entry chose against for the per-worker
   watermark. Worth doing behind a flag.
5. **Make `gen --pace` flush on every sleep** (`tools/gen.cpp`, root-owned,
   so not changed here). The 1 MiB write buffer turns a paced stream into
   100 ms bursts and puts a 1.3 to 3 ms floor under every latency sample;
   with per-sleep flushes the pipe would carry events as they are produced
   and the measurement would show the queue handoff itself.
6. **A 100M-event memory run** to settle whether the 50 MiB of allocator
   slack at 8 threads is bounded; and the flat open-addressing dedup set
   from the m2 entry, which would halve the bytes per id that the memory
   table is made of.

### Files touched this session

`bench/throughput.sh` (five sections, `--only`), `streamkit/latency.h`
(`min_us`), `streamkit/pipeline.h` and `pipeline.cpp` (`input_reads`),
`streamkit/main.cpp` (the latency line), `streamkit/tests/latency_test.cpp`
(min, block counter, one new test), `streamkit/BENCHMARKS.md` (new),
`streamkit/README.md` (status, harness usage, files), this file.

## 2026-10-21: integration with colstore, the CSV boundary

**What landed.** `bench/e2e.sh` runs the whole pipeline on 5M dirty events
(1% duplicates, 5% late up to 30 s, `--rate 1000` so the stream spans 83
one-minute windows): `gen -> oracle -> streamkit --threads 4 --ckpt-dir ...
--checkpoint-every-events 1000000 -> bench/compare.sh -> colstore ingest`
of both the raw events and streamkit's output `-> verify -> Q1-Q3 -> top-10
campaigns by spend in one hour on the aggregate table`, with every answer
checked against a brute-force recomputation and the final top-10 also
recomputed by awk from the ORACLE's CSV. One report, exit 1 on any
mismatch (proved by a one-off run with a wrapper `oracle` that added 1 to
one cost: `E2E FAIL`, exit 1). Nothing in streamkit changed; the only
edits on this side are this entry and one README line.

**The boundary between streamkit and colstore is streamkit's CSV, as is.**
colstore learned to ingest it (`--in-csv`, a 6-integer-column table with
the schema carried in the `.col` footer) rather than streamkit learning
to write `.col` or a shared binary record. Why CSV, for now:

- It already existed and was already proved: `bench/compare.sh` diffs it
  against the oracle byte for byte, so the integration added no new
  format and no new way to be wrong between the two engines. colstore's
  `verify --csv` then proves its own copy against the same file, and the
  e2e's awk step recomputes the final answer from the oracle's CSV with
  no code from either engine.
- The checkpoint/resume design depends on the output being an append-only
  byte stream whose length is the state (snapshot records `bytes written`,
  resume truncates to it, m4 entry). A row-oriented text file has exactly
  that property. A `.col` output would not: the footer is written last,
  row groups are 65,536 rows, and a crash mid-group would need its own
  recovery story, which is a second checkpoint protocol.
- Cost, measured in the e2e: streamkit writes 3,735,840 rows (127 MB of
  text) in 0.60 s wall for 5M events; colstore parses them with
  `std::from_chars` at 7.2M rows/s (0.52 s) into 16.6 MB of `.col`, i.e.
  the CSV is 7.7x the columnar bytes. Both numbers are below the oracle's
  own 1.4 s and far below the 10 s `compare.sh` spends sorting the same
  rows twice. At this scale the text boundary is not where the time goes.

**What a binary or Arrow-style boundary would buy.** Three things,
roughly in order of value:

1. *Bytes and parse time.* A fixed-width 36-byte record (i64, u32, 3 x
   u64, i64, mirroring `common/event.h`) is 134 MB for these rows, barely
   smaller than the 127 MB of text because decimal integers are short;
   the real saving is the parse: a `memcpy` per record instead of six
   `from_chars`, which at 7.2M rows/s today is about half of colstore's
   ingest time for the aggregate table. An Arrow IPC stream (one record
   batch per flushed group of windows, columnar inside) goes further:
   colstore could take each batch's columns without a transpose and
   encode them directly.
2. *Types travel with the data.* The CSV's schema is a convention held in
   two places (`write_csv_header` in `common/agg.h` and
   `aggregate_schema()` in colstore's `format.h`) and enforced by a header
   string compare. A self-describing boundary carries names and types in
   the stream, so a new column in streamkit's output would be a new column
   in colstore's table instead of a rejected header.
3. *Streaming, not files.* With an Arrow stream colstore could ingest from
   streamkit's stdout as windows close, which is the actual pipeline
   shape; today the CSV is complete before ingest starts.

What it would cost: a third format in the repository (or a dependency on
Arrow, which the plan forbids), a resume rule for a block-structured
output (snapshot the block boundary, not a byte length), and a second
diff tool, since `compare.sh` reads text. None of that is needed for a
pipeline whose stages run one after another on one machine, which is
why the boundary stays as it is and this entry records what would change
it: either an ingest that has to keep up with streamkit live, or a column
added on one side that the other must pick up without a code change.

**Verified.** `bench/e2e.sh build-int` (Release): E2E PASS, streamkit
5,000,000 events, 49,791 dups, 3,735,840 windows in 0.60 s at 4 threads,
5 checkpoint epochs committed, `compare.sh` PASS 3,735,840 windows
identical; the same at 1M events under ASan+UBSan and TSan trees with 0
reports (colstore's NOTES entry has the full table and the colstore-side
costs). The run uses `--lateness-s 60` against a 30 s maximum lateness,
so 0 events were late-dropped; the dedup count equals the oracle's.

## 2026-10-21 (afternoon): m5 re-run on the finished tree, every number re-measured on a shared machine

**What was asked, and what was already there.** The m5 brief: extend the
harness with dedup, checkpoint and memory sections, add `--latency-stats`,
write `BENCHMARKS.md`, rerun the regression, and record what I would
change next. All of that had landed in the morning (the m5 entry above:
`latency.h`, the five-section harness, the first `BENCHMARKS.md`). So this
session is the one the plan's rule 2 exists for: rebuild everything from
the sources, rerun every harness, and replace every number with one from
a command run today, on a machine that turned out to be far from idle.
No engine code changed.

### Decision 1: `BENCHMARKS.md` holds this session's numbers only

The alternative was to keep the morning's tables (taken at load 3.5 to
4.5 with the other track building, quieter than most of today) and add
today's as a second set. Rejected: a page that mixes two machine states
invites quoting the better one without its caveat, and the brief's rule is
that every number reported comes from a command run in the session that
reports it. The morning's conclusions survive in the m5 entry above, and
today reproduced the ones that matter (below). Cost: the morning's full
decomposition table (five campaign counts, seven thread counts) is no
longer on the page; today's page has the three points that carry the
argument.

### The emit-latency metric, restated so it cannot be misread

For every window that a worker's watermark closes, the sample is
`t_row - t_block`: `t_block` is `steady_clock` read by the reader thread
right after `read(2)` returned the block of input bytes holding the event
that pushed that worker's watermark past the window's end; `t_row` is
`steady_clock` read by the worker right after `fprintf` appended that
window's row to its output stream. p50 and p99 are nearest-rank
percentiles over all such samples in a run (always a value that was
observed). It is the time from "this process could first have known the
window was closable" to "the row exists". It excludes: the configured
lateness (event time, by design); the producer's buffering before the
bytes reach the pipe (gen holds 1 MiB, so at 200K events/s the pipe
delivers ~1 MB bursts every ~100 ms); the stdio flush of the row to disk,
any fsync, and the final merge; and windows closed by end of input. Rows
are stamped one by one, so a row behind others in the same closing call is
charged for them, which is deliberate. Off by default; with the flag off
no clock is read and nothing is stored (`Latency.OffByDefaultRecordsNothing`).
The reasons for each choice are in the m5 entry above; the arithmetic is
`latency.h`.

### Decision 2: the harness checks both exit statuses of the paced pipeline

The first full harness run of the session died in the latency section
with exit 141 (SIGPIPE) after the second 8-thread run, and its `trap` had
already deleted streamkit's stderr. Eight reruns of the same `gen | sk`
pipeline by hand (20 s and 5 s) with both statuses captured never failed,
and the `sed | head -1` helpers were checked (2,000 iterations, 0
failures), so the cause is unknown. Rather than guess, `lat_run` in
`bench/throughput.sh` now runs the pipeline with `set +e`, reads
`PIPESTATUS` for gen and streamkit separately, prints both statuses and
both stderr logs if either is non-zero, and retries once. Alternatives:
ignore gen's status (a streamkit crash would then look like a clean run
with no rows: hidden); keep aborting (loses the rest of a four-minute run
and the evidence, which is what just happened). Cost: 20 lines of bash
and one extra log file. Four harness runs went through the edited script,
two of them through `lat_run`, without a failure.

### What the numbers say today, and what they do not

Everything is in `BENCHMARKS.md`, with the run-to-run spread; the parts
that needed reading:

- **The machine.** `build-cs/bin/colstore_sqlite` (the other track's
  benchmark) sat at 100% of a core until about 13:00 and `fseventsd` at
  85 to 90% for the whole session, checked with `ps` before each run. A
  background poll waited 14 minutes for the benchmark to stop and the load
  to fall below 3, then ran harness E at 13:13; the neighbour's activity
  came back intermittently afterwards. Load average at the start of a run
  was a poor guide (the run that started at 3.77 was the slowest). A
  single thread's throughput moved from 4.58M to 2.63M events/s between
  runs twenty minutes apart on the same binary and input, which CPU
  contention on 18 cores cannot explain; what moved is the memory system
  and the SSD, i.e. exactly what the dedup insert and the checkpoint
  fsyncs depend on.
- **Throughput: 4.97M / 9.36M / 18.7M / 28.0M events/s at 1 / 2 / 4 / 8
  threads (5.63x), run E,** with the three runs per cell within 2% at 1
  and 8 threads. The shape was the same in all four runs (1.83 to 1.93x
  at 2 threads, 2.8 to 3.8x at 4, 4.5 to 5.6x at 8, reader stalls falling
  from ~72K to ~2K and idle waits rising, so the workers remain the
  bottleneck up to 8 threads) while the level in runs A, C and D was 0.5
  to 0.9x of E's.
- **Dedup costs 51 ns per event, the same on clean and dirty streams**
  (run F: 1/5,035,224 - 1/6,778,647 = 51.1 ns dirty, 52.4 ns clean, 25.7%
  and 26.1% of a single thread), the morning's number reproduced to the
  nanosecond. It took six attempts to get one dedup table whose 1-thread
  cells agreed within 2%: the four earlier runs gave per-event differences
  of -71 to 263 ns, including run E's three identical runs at 4.66M,
  3.11M and 1.37M events/s seconds apart. A cache-missing hash insert per
  event is the operation most sensitive to a neighbour thrashing memory,
  so it is the first cell to go and the last to come back.
- **Checkpoints: the per-snapshot cost is the stable number.** Total
  snapshot time / 80 = 30.2, 33.4, 39.6 and 38.9 ms per snapshot across
  the four runs (slowest single snapshot 44 to 69 ms), and 10 epochs x
  that matches the wall-time increase (0.20 to 0.34 s) to within the
  overlap the reader gets by filling the queues while workers snapshot.
  The "vs none" percentages (-22% to -32%) measure the neighbour as much
  as the checkpoints: in run E the identical no-checkpoint command gave
  28.0M in the threads section and 14.2M two minutes later as the
  checkpoint baseline. At the generator's native rate the snapshot is a
  30 to 40 ms stall every 100 s, 0.03 to 0.04%.
- **Memory: identical to the morning within 1%.** 92.5 / 103.2 MiB at 1M
  / 10M events on one thread (11.5% for 10x the events, dedup set 0.99M
  to 1.19M ids, open windows ~200K in both); 87 / 119 MiB at 8 threads
  with `--queue-depth 256`; 130 / 346 MiB with the default 4096-slot
  queues once the reader has touched all 27 MB per worker. RSS does not
  care about the neighbour, which is why this section needed only one run.
- **Emit latency: p50 7.9 ms, p99 16.1 ms at 8 threads; p50 14.4 ms, p99
  25.5 ms at 1 thread (run E)**, on top of the 2 s lateness, at 200K
  events/s with 86K windows closing per second. Across the thirteen 20 s
  8-thread runs of the session the p50 stayed between 7.5 and 12.0 ms (two
  p99 outliers at 30 and 48 ms in the noisiest minutes), while the
  1-thread cells moved with the machine as the throughput cells did (p50
  14 ms in A and E, 28 ms in D): with eight workers each one's share of a
  20K-event burst is 2.5K events, so most of a sample is the producer's
  100 ms burst pattern plus the rows written before it, neither of which
  is CPU time; with one worker the whole burst is CPU time and the
  neighbour shows.
- **The decomposition reproduced.** 1K campaigns (so almost no rows) give
  a p99 of 4.6 ms at 1 thread and 1.7 ms at 8: the floor is the closing
  event waiting behind its own burst. Above the floor, (p99 - floor) /
  rows per worker per boundary is **0.24 us per row with one worker
  writing, 0.44 with four, 1.34 with eight** (morning: 0.24 / 0.33 / 1.0
  to 1.1), so the per-row cost that grows with concurrently emitting
  workers is real and the 4-thread point (p99 11.2 ms) is again the best
  for this workload. The culprit is still one of `fprintf`'s FILE lock
  plus formatter and `std::map::erase`'s allocator free; item 2 below.

### A measurement lesson, recorded so the next harness does better

Wall-clock events/s is the right headline for an idle machine and the
wrong instrument for a shared one: it cannot tell the engine from its
neighbour, and it took six harness runs over fifty minutes to get one
clean dedup table. Two things would have made the first run's dedup and
threads tables meaningful anyway. First, report CPU time per run
(`ru_utime + ru_stime` from `getrusage`, or `/usr/bin/time -l`'s user and
sys lines, which the memory section already captures): a neighbour
inflates wall time much more than it inflates the engine's own CPU
seconds, and the dedup cost per event is a CPU-time question. Second,
print the run-to-run spread as a number next to the median and refuse to
bold a cell whose spread exceeds, say, 15%, so the table itself says
which numbers are usable. Both are harness changes, not engine changes,
and are the first item below.

### What it costs

Nothing in the engine (no source under `streamkit/` changed other than
documentation). The harness: one helper, two extra words in the trap, one
extra log file, and about 4 minutes for all five sections.

### How it was verified

- Warnings: every source this track owns touched, then
  `cmake --build <dir> --target streamkit streamkit_tests adstack_tests
  oracle gen evdump` in `build-sk`, `build-sk-asan`, `build-sk-tsan`: 16
  translation units per tree, 0 warnings, 0 errors (12:30 EDT).
- Tests: `adstack_tests` + `streamkit_tests` run directly in each of the
  three trees: 16 + 63 = 79 passed, 0 sanitizer lines; `ctest -R` over
  this track's suites: 100% of 79 in each tree.
- `bench/smoke.sh build-sk`: PASS at 1/2/4/8 threads, tumbling (92,609
  windows identical) and sliding (436,985), 1,951 dups equal to the
  oracle's, 0 late (12:44).
- `bench/crash.sh build-sk 50`: 50/50 PASS, 392,549 windows identical in
  every trial, 138 kills, 47 trials killed at least twice (3 once, 21
  twice, 16 three times, 6 four, 3 five, 1 six), no leftovers (12:44 to
  12:50).
- ASan+UBSan and TSan on the paced latency path (500K events at 100K
  events/s, 4 and 8 threads, `--latency-stats --late-out`): exit 0 for
  gen and streamkit, zero sanitizer lines, 310,446 windows written and
  131,486 sampled in each of the four runs (12:51).
- `bench/throughput.sh build-sk`: six runs (A 12:31 all sections, B 12:37
  latency, C 12:42 threads/dedup/checkpoint, D 12:54 all but memory, E
  13:13 all but memory, F 13:17 dedup), plus three 10 s decomposition runs
  at 13:18; the edited harness syntax-checked (`bash -n`) and run end to
  end four times after the edit (twice through `lat_run`).

### What I would change next

1. **Make the harness measure CPU time and print the spread** (the
   lesson above). `ru_utime + ru_stime` per run from the engine's own
   summary (one `getrusage` call at exit), a "spread" column = (max -
   min) / median, and no bold median when the spread passes 15%. Then the
   dedup cost in nanoseconds per event is `(cpu_on - cpu_off) / events`,
   a quantity a neighbour barely touches, and the threads table says
   outright which cells to trust. A few lines in `main.cpp` and the
   harness; the acceptance test is rerunning today's dedup section with
   the neighbour running and getting three runs that agree.
2. **Format rows without libc `printf`, and erase closed windows in
   bulk.** Today's decomposition put the per-row cost at 0.24 us with one
   worker writing, 0.44 with four and 1.34 with eight (p99 25.5 / 11.2 /
   16.1 ms), the same contention the morning found. A hand-rolled
   unsigned-to-ASCII into a worker-owned buffer flushed with `write(2)`
   removes the FILE lock and the formatter; `open_.erase(begin, it)` after
   the loop replaces one allocator free per row with a batch. The target
   is the 1-worker per-row cost at 8 workers, which would take the p99 at
   this workload from 16 ms to about 4 ms and restore the expectation that
   more workers means lower emit latency. The first run tells which of the
   two was the culprit.
3. **A replayable-log source (Kafka) with offset commit tied to the
   checkpoint epoch.** The MANIFEST already records "input offset X"; for
   a partitioned log it records partition -> offset, captured by the
   reader when it injects the marker, and the consumer group's offsets are
   committed only after the MANIFEST rename, never before. A crash between
   snapshot and commit then replays from the last committed offsets into
   the same truncate-and-replay that resume already does, so the pipeline
   is exactly-once end to end without idempotent sinks. It needs one
   marker per source partition and new partition assignments held until
   the epoch boundary. This is the change that turns the engine from a
   file tool into a service.
4. **Incremental snapshots.** Today a snapshot is ~2.3 MB per worker and
   30 to 40 ms, half of it dedup ids that barely change between epochs
   (one bucket appended, one expired) and open windows that change mostly
   at the front. A base snapshot plus per-epoch deltas (new buckets, count
   of expired buckets, windows touched since the last epoch, part-file
   lengths), compacted into a new base every k epochs, cuts bytes per
   epoch by roughly horizon / interval. Cost: a chain to validate on
   resume (each delta hashed and bound to its predecessor) and resume time
   that grows with k. Not worth it at today's size; the right design once
   open state is a hundred times larger (long windows, many keys).
5. **Watermark alignment across partitions for ordered output.** Each
   worker publishes its watermark through one atomic; a merger pulls
   closed rows from P per-worker queues and emits them in window-start
   order up to the minimum watermark. Output becomes globally time-ordered
   (colstore could ingest it pre-sorted and prune by zone map without a
   sort) and the merger is the natural place to move the checkpoint fsync
   off the workers. Cost: the slowest partition holds every partition's
   rows back, so emit latency becomes the max over partitions, the trade
   the m3 entry chose against for the per-worker watermark. Behind a flag.
6. **Make `gen --pace` flush on every sleep** (`tools/gen.cpp`, root-owned,
   so not changed here). The 1 MiB write buffer turns a paced stream into
   100 ms bursts and puts a 1.7 to 4.6 ms floor under every latency
   sample; with per-sleep flushes the pipe would carry events as produced
   and the measurement would show the queue handoff itself.

### Files touched this session

`bench/throughput.sh` (`lat_run`, trap), `streamkit/BENCHMARKS.md`
(rewritten from this session's runs), `streamkit/README.md` (status,
harness note), this file. No engine source changed.

## 2026-10-21 13:50 EDT: integration with colstore re-verified from the current tree (no code change)

The integration (`bench/e2e.sh`, colstore's `ingest --in-csv` over this
engine's CSV, the entry above on why the boundary is CSV) was already
committed in `0dff510`; this session re-ran it from fresh configures of
`build-int`, `build-int-asan` and `build-int-tsan` and changed nothing in
`streamkit/` but this entry and one README clause. The full table and the
commands are in `colstore/NOTES.md` (same timestamp); the streamkit-side
facts:

- 5M dirty events at 4 threads with checkpoints every 1M: 49,791 dups
  (equal to the oracle), 0 late-dropped (`--lateness-s 60` against a 30 s
  maximum), 3,735,840 windows, 5 epochs committed at offset 260,000,008,
  0.79 s engine time (6.36M events/s) at load 4.9; `compare.sh` PASS,
  3,735,840 windows identical. Two Release runs, both PASS with the same
  counts; only wall-clock differed with the load (10.3 vs 4.9).
- 1M events through the ASan+UBSan and TSan trees, concurrently, with the
  sanitizers' `log_path` pointed outside the harness's work dir: E2E PASS,
  exit 0, no report file written by either.
- Exit on mismatch re-proved with a wrapper `oracle` that adds 1 to one
  row's cost: `compare` FAIL, `E2E FAIL`, exit 1.

**The boundary, restated from this side.** The CSV stays because it is
the format the checkpoint protocol already relies on (an append-only byte
stream whose length is the state: the snapshot records the output length
and `--resume` truncates to it), because `compare.sh` already proves it
against the oracle so the integration added no new way for the two
engines to disagree, and because at this scale it is not where the time
goes (colstore parses the 127 MB in 0.68 s; `compare.sh` spends 14 s
sorting the same rows). A fixed-width binary record would turn six
`from_chars` per row into a `memcpy` and save almost no bytes; an
Arrow-style stream would also carry the column names and types with the
data (today they are a convention held in `write_csv_header` here and
`aggregate_schema()` there) and would let colstore ingest record batches
as windows close rather than after the file is complete. The cost of
either is a third format plus a block-aware resume rule, which nothing
needs until an ingest must keep up with streamkit live or a column is
added on one side only.
