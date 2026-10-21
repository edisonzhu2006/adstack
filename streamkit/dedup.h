#pragma once
// Bounded exact deduplication of event_ids for streamkit (milestone 2).
//
// WHY THIS EXISTS. The input stream re-delivers some events (same event_id,
// same event_time, new ingest_time). Counting them twice double bills, so
// each event_id must be counted once. The oracle keeps every id it has ever
// seen in one unordered_set, which is fine for a batch job over a file and
// wrong for a stream: memory grows forever.
//
// THE IDEA. We do not need to remember an id forever, only for as long as a
// second copy of it could still be counted. Streamkit rejects an event as
// late when its window has already closed:
//
//     window_start(event_time) + window_us <= watermark
//
// Let H = watermark - window_us. For any event_time t < H:
//     window_start(t) <= t < H  =>  window_start(t) + window_us < watermark
// so the event is late no matter which window it falls in. A duplicate has
// the same event_time as its original, so once t < H every copy of that id
// will be diverted to the late output and never counted. Its id can be
// forgotten with no change to the output.
//
// Sliding windows (m3) keep the proof: the engine then rejects an event once
// its *earliest* window has closed, first_window_start(t) + window_us <=
// watermark, and first_window_start(t) <= t - window_us + slide_us, so for
// t < H that end is < watermark - window_us + slide_us <= watermark (slide
// never exceeds window). The horizon is still watermark - window_us.
//
// THE STRUCTURE. Ids live in hash sets bucketed by event_time (default 1s
// buckets). Forgetting is "drop whole buckets whose range lies below H
// rounded down to a bucket boundary", which is O(1) per bucket instead of a
// per-id sweep. Because the watermark only moves forward, the set of buckets
// is a sliding span of at most (lateness + window) / bucket + 2 buckets, so
// memory is proportional to the event rate times (lateness + window), not
// to the stream length. See streamkit/NOTES.md for the alternatives that
// were rejected (unbounded set, ring of bloom filters).
//
// USAGE (per event, in this order):
//     dedup.advance(watermark);                 // if a watermark exists
//     switch (dedup.insert(e.event_id, e.event_time_us)) { ... }
// advance() before insert() keeps the retained span bounded even when one
// event jumps the watermark far ahead: old buckets are dropped before the
// new bucket is appended.

#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <unordered_set>
#include <vector>

namespace adstack {

// Everything a Dedup needs to be rebuilt elsewhere (milestone 4 checkpoints).
// After a restart the replayed events must hit the same set, so the
// snapshot carries the ids themselves, bucket by bucket, plus the two
// numbers that place the buckets on the time axis. Ids inside a bucket are
// in no particular order (they come out of a hash set).
struct DedupState {
  std::optional<int64_t> forget_before;      // the horizon, if any advance() has happened
  int64_t front_start = 0;                   // event time where bucket 0 begins
  std::vector<std::vector<uint64_t>> buckets;

  bool operator==(const DedupState&) const = default;
};

class Dedup {
 public:
  enum class Verdict {
    kNew,        // first time this id is seen inside the horizon; it was stored
    kDuplicate,  // this id is already stored
    kExpired,    // event_time is below the horizon; not stored. The caller's
                 // late check is guaranteed to divert this event (see proof
                 // above), so forgetting it is safe.
  };

  // window_us: streamkit's tumbling window, needed to derive the horizon.
  // bucket_us: granularity of forgetting. Smaller = tighter memory, more
  //            buckets to manage. Throws std::invalid_argument if either <= 0.
  Dedup(int64_t window_us, int64_t bucket_us);

  // Raise the horizon to match a new watermark and drop every bucket that is
  // entirely below it. Watermarks that move backwards are ignored.
  void advance(int64_t watermark_us);

  // Record an id. Allocation: one hash-set node per kNew, plus an empty
  // bucket when event_time lands outside the current span.
  Verdict insert(uint64_t event_id, int64_t event_time_us);

  // The horizon as a bucket-aligned timestamp: ids with event_time < this are
  // forgotten. Empty until the first advance().
  std::optional<int64_t> forget_before() const { return forget_before_; }

  // Pure function behind advance(), exposed so tests can pin the arithmetic:
  // floor_to_bucket(watermark - window).
  static int64_t forget_before_for(int64_t watermark_us, int64_t window_us, int64_t bucket_us);

  // Checkpoint support: copy the state out, or replace this set's state with
  // a copy taken earlier (window_us and bucket_us must be the same as when
  // the state was exported; they are configuration, not state, and the
  // MANIFEST pins them). import_state() discards whatever was here.
  DedupState export_state() const;
  void import_state(const DedupState& state);

  // Introspection for the stderr summary and for tests.
  size_t ids() const { return ids_; }            // ids currently remembered
  size_t peak_ids() const { return peak_ids_; }  // high-water mark of ids()
  size_t buckets() const { return buckets_.size(); }  // retained buckets, empty ones included

 private:
  using IdSet = std::unordered_set<uint64_t>;

  // buckets_[i] covers event times [front_start_ + i * bucket_us_, + bucket_us_).
  // A deque so that both ends are cheap: we append as event time advances
  // (push_back), prepend for out-of-order events older than anything seen
  // so far (push_front), and expire from the front (pop_front).
  IdSet& bucket_for(int64_t bucket_start_us);
  IdSet make_set(size_t size_hint);
  void drop_front();

  int64_t window_us_;
  int64_t bucket_us_;
  std::deque<IdSet> buckets_;
  int64_t front_start_ = 0;  // only meaningful when !buckets_.empty()
  std::optional<int64_t> forget_before_;

  // Expired sets are cleared and reused for new buckets. Buckets are created
  // at the same rate they expire, so this keeps one or two already-sized
  // hash tables around instead of allocating a fresh table every second of
  // event time. Bounded to kMaxFree entries.
  static constexpr size_t kMaxFree = 4;
  std::vector<IdSet> free_;

  size_t ids_ = 0;
  size_t peak_ids_ = 0;
};

}  // namespace adstack
