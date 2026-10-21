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
