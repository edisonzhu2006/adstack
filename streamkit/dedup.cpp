// Implementation of the time-bucketed exact dedup set. See dedup.h for the
// reasoning; this file is only mechanics.

#include "dedup.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "common/agg.h"  // window_start(): floor to a multiple, correct for negatives

namespace adstack {

namespace {

// Smallest number of ids a fresh bucket reserves space for, so the first few
// inserts into a bucket do not each trigger a rehash.
constexpr size_t kMinReserve = 16;

}  // namespace

Dedup::Dedup(int64_t window_us, int64_t bucket_us)
    : window_us_(window_us), bucket_us_(bucket_us) {
  if (window_us <= 0) throw std::invalid_argument("Dedup: window_us must be > 0");
  if (bucket_us <= 0) throw std::invalid_argument("Dedup: bucket_us must be > 0");
}

int64_t Dedup::forget_before_for(int64_t watermark_us, int64_t window_us, int64_t bucket_us) {
  // H = watermark - window is the proven-late line; rounding down to a bucket
  // boundary is conservative (we keep slightly more than we need to).
  return window_start(watermark_us - window_us, bucket_us);
}

void Dedup::advance(int64_t watermark_us) {
  const int64_t fb = forget_before_for(watermark_us, window_us_, bucket_us_);
  if (forget_before_ && fb <= *forget_before_) return;  // never move backwards
  forget_before_ = fb;
  // A bucket can go only when its whole range [start, start + bucket) is
  // below the horizon. Since fb is bucket-aligned this is "start < fb", but
  // the general form is kept so the invariant does not depend on alignment.
  while (!buckets_.empty() && front_start_ + bucket_us_ <= fb) drop_front();
}

Dedup::Verdict Dedup::insert(uint64_t event_id, int64_t event_time_us) {
  if (forget_before_ && event_time_us < *forget_before_) return Verdict::kExpired;
  IdSet& set = bucket_for(window_start(event_time_us, bucket_us_));
  if (!set.insert(event_id).second) return Verdict::kDuplicate;
  ++ids_;
  if (ids_ > peak_ids_) peak_ids_ = ids_;
  return Verdict::kNew;
}

Dedup::IdSet& Dedup::bucket_for(int64_t bucket_start_us) {
  if (buckets_.empty()) {
    front_start_ = bucket_start_us;
    buckets_.push_back(make_set(kMinReserve));
    return buckets_.front();
  }
  // Older than anything retained: grow the span backwards. Only out-of-order
  // events reach here, and never by more than (lateness + window) once a
  // watermark exists, because anything older is kExpired before this call.
  while (bucket_start_us < front_start_) {
    buckets_.push_front(make_set(buckets_.front().size()));
    front_start_ -= bucket_us_;
  }
  // Newer than anything retained: grow forwards. Gaps become empty buckets,
  // which is fine because the span is bounded by the horizon.
  const size_t idx = static_cast<size_t>((bucket_start_us - front_start_) / bucket_us_);
  while (idx >= buckets_.size()) buckets_.push_back(make_set(buckets_.back().size()));
  return buckets_[idx];
}

Dedup::IdSet Dedup::make_set(size_t size_hint) {
  // Size the new bucket like its neighbour: at a steady event rate every
  // bucket holds about the same number of ids, so this avoids the rehash
  // cascade of growing from empty without guessing the rate up front.
  IdSet set;
  if (!free_.empty()) {
    set = std::move(free_.back());
    free_.pop_back();
  }
  set.reserve(std::max(size_hint, kMinReserve));
  return set;
}

DedupState Dedup::export_state() const {
  DedupState st;
  st.forget_before = forget_before_;
  st.front_start = front_start_;
  st.buckets.reserve(buckets_.size());
  for (const IdSet& b : buckets_) st.buckets.emplace_back(b.begin(), b.end());
  return st;
}

void Dedup::import_state(const DedupState& state) {
  buckets_.clear();
  free_.clear();
  ids_ = 0;
  forget_before_ = state.forget_before;
  front_start_ = state.front_start;
  for (const std::vector<uint64_t>& ids : state.buckets) {
    IdSet set = make_set(ids.size());
    set.insert(ids.begin(), ids.end());
    ids_ += set.size();
    buckets_.push_back(std::move(set));
  }
  // The restored set is this run's high-water mark so far.
  peak_ids_ = ids_;
}

void Dedup::drop_front() {
  IdSet set = std::move(buckets_.front());
  buckets_.pop_front();
  front_start_ += bucket_us_;
  ids_ -= set.size();
  if (free_.size() < kMaxFree) {
    set.clear();  // frees the nodes, keeps the bucket array for reuse
    free_.push_back(std::move(set));
  }
}

}  // namespace adstack
