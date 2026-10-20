# adstack

A miniature ads data pipeline in C++20: a streaming aggregation engine
(`streamkit`) and, later, a columnar analytics store (`colstore`), sharing
one event schema. Built to be measured: every number in this README comes
from a script in `bench/`.

```
ad events  -->  streamkit (windowed aggregates)  -->  colstore (columnar storage + queries)
```

## Build

Requires CMake 3.24+, Ninja, and a C++20 compiler (Apple clang 15+, GCC 12+,
Clang 16+). On this machine the pip-installed cmake/ninja are at
`~/.withai/runtimes/python/bin/`.

```
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build
```

Sanitizer builds: `-DADSTACK_SANITIZE=address` or `thread`.

## Quickstart

```
# 1M synthetic events with 1% duplicates and 5% arriving up to 30s late
build/bin/gen --n 1000000 --seed 1 --out events.ads
build/bin/evdump --in events.ads --n 5

# ground truth: dedup + per-campaign per-minute totals
build/bin/oracle --in events.ads --out truth.csv

# streaming: same totals, computed incrementally with a watermark
build/bin/streamkit --in events.ads --out results.csv --lateness-s 60 --late-out late.ads

bench/compare.sh truth.csv results.csv
```

Milestone 1 matches the oracle only on duplicate-free streams
(`gen --dup-frac 0`); dedup is milestone 2. `bench/smoke.sh` runs this whole
loop and is what CI runs.

## Real data

Synthetic data is for determinism (crash tests need a replayable stream).
Two adapters run the engine on real traffic:

**Criteo Attribution Modeling dataset** (real impressions, clicks and
conversions, 30 days, ~16M rows). Download from
<https://ailab.criteo.com/criteo-attribution-modeling-bidding-dataset/>, then:

```
gunzip -c criteo_attribution_dataset.tsv.gz | build/bin/criteo2events --out criteo.ads
build/bin/oracle --in criteo.ads --out criteo_truth.csv
build/bin/streamkit --in criteo.ads --out criteo_results.csv --lateness-s 3600
```

**Wikimedia EventStreams** (live, public, genuinely out-of-order, no key):

```
curl -sN https://stream.wikimedia.org/v2/stream/recentchange \
  | build/bin/wiki2events --out - \
  | build/bin/streamkit --in - --out live.csv --window-s 10 --lateness-s 5
```

The field mapping is documented at the top of each adapter. It is real
traffic shaped like the schema, not real ads.

## Layout

```
common/      event schema + binary format, aggregates, CSV, arg parsing
tools/       gen (synthetic stream), oracle (batch ground truth), evdump
streamkit/   streaming engine
adapters/    criteo2events, wiki2events
tests/       GoogleTest unit tests
bench/       compare.sh, smoke.sh, (later) throughput and crash harnesses
```

## Event format

52-byte fixed-width little-endian records after an 8-byte header. See
`common/event.h`. Fixed width keeps the reader a single `fread` and lets
colstore address rows by offset.

## Status

- [x] shared format, generator, oracle, tests, CI
- [x] streamkit m1: single-threaded tumbling windows with watermark and late-event side output
- [ ] streamkit m2: bounded dedup (time-bucketed set expiring behind the watermark)
- [ ] streamkit m3: hash-partitioned workers, sliding windows, throughput at P = 1/2/4/8
- [ ] streamkit m4: atomic checkpoints + resume; SIGKILL-50-times harness vs. oracle
- [ ] streamkit m5: benchmark table (events/sec, p99 latency, memory)
- [ ] colstore m1..m5
