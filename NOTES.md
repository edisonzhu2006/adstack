# Design notes

Running log of decisions and dead ends. This file is interview prep: every
entry should answer a "why did you do X instead of Y" question.

## 2026-10-19: foundation

**Fixed-width binary events (52 bytes) instead of JSON or protobuf.**
Reader is one `fread`, no parser, no allocation per event, and colstore can
compute a row's byte offset arithmetically. Cost: schema changes need a
version bump (`kFileVersion`) and a migration. Acceptable for a pipeline
whose schema is owned end to end. Protobuf would be the choice if other
teams produced the events.

**Events are emitted and processed in ingest order, aggregated by event
time.** This is the whole reason streaming is hard: the order we see events
in is not the order they happened in. The generator makes the two diverge on
purpose (late events, duplicates) so correctness has to be earned.

**Watermark = max(event_time) - allowed lateness.** A window closes when the
watermark passes its end. Events whose window already closed go to a side
file, never dropped silently; in ads this is money, and a counter plus a
file is the minimum audit trail. Tradeoff to measure in m3: longer lateness
means fewer late drops but more open-window state and higher emit latency.

**`std::map<WindowKey, Agg>` ordered by (window_start, campaign).** Closing
windows becomes "pop from the front while window_end <= watermark", O(log n)
per insert. A hash map would be faster per insert but would need a separate
structure to find closable windows. Revisit in m3 when per-partition state
is smaller.

**Oracle is single-threaded and dumb on purpose.** Its value is that it is
obviously correct. Never optimize it.

**Dedup is deferred to m2, not skipped.** Doing it naively (an unbounded
`unordered_set`) is trivial and wrong: memory grows forever. The right
structure expires IDs older than the watermark minus lateness, because an
event older than that would be rejected as late anyway, so its ID no longer
needs remembering.

## Open questions (answer these as you build)

- m2: time-bucketed hash set vs. a ring of bloom filters? Bloom gives false
  positives, which here means wrongly dropping a real event (lost money).
  Probably exact set with bucket expiry; measure memory at 1% dups and 60s
  lateness.
- m3: per-partition watermark or global? Per-partition closes windows
  faster when one key range stalls, but the emitted output is then
  interleaved across partitions. Does that matter for colstore ingestion?
- m4: what exactly is in a checkpoint, and how big is it at 100K open
  windows? Is fsync+rename enough on macOS (`F_FULLFSYNC`)?
