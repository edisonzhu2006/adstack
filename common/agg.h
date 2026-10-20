#pragma once
// Window keys, per-window aggregates and the CSV output format. The oracle
// and streamkit must agree on every byte of this so their outputs can be
// diffed, which is why it lives in one place.

#include <compare>
#include <cstdint>
#include <cstdio>
#include <vector>

#include "common/event.h"

namespace adstack {

struct Agg {
  uint64_t impressions = 0;
  uint64_t clicks = 0;
  uint64_t conversions = 0;
  int64_t cost_micros = 0;

  void add(const Event& e) {
    switch (static_cast<EventType>(e.event_type)) {
      case EventType::Impression: ++impressions; break;
      case EventType::Click: ++clicks; break;
      case EventType::Conversion: ++conversions; break;
    }
    cost_micros += e.cost_micros;
  }

  bool operator==(const Agg&) const = default;
};

struct WindowKey {
  int64_t window_start_us;
  uint32_t campaign_id;
  auto operator<=>(const WindowKey&) const = default;
};

// Start of the tumbling window containing t_us. Floors correctly for
// negative timestamps, which plain integer division does not.
inline int64_t window_start(int64_t t_us, int64_t window_us) {
  int64_t q = t_us / window_us;
  if ((t_us % window_us) != 0 && t_us < 0) --q;
  return q * window_us;
}

// ---- sliding windows ------------------------------------------------------
//
// A sliding window of length window_us advances by slide_us, so windows start
// at every multiple of slide_us and overlap when slide_us < window_us. An
// event at time t belongs to every window [s, s + window) with s <= t and
// s + window > t, i.e. every multiple s of slide in (t - window, t]. The
// latest such s is window_start(t, slide); the earliest is the first multiple
// of slide strictly greater than t - window. With slide == window exactly one
// window qualifies and this degenerates to the tumbling case, which is why
// "tumbling" is not a separate code path anywhere.
//
// Both the oracle and streamkit enumerate windows through these helpers so
// their outputs can never disagree on window membership.

inline int64_t first_window_start(int64_t t_us, int64_t window_us, int64_t slide_us) {
  return window_start(t_us - window_us, slide_us) + slide_us;
}

inline int64_t last_window_start(int64_t t_us, int64_t slide_us) {
  return window_start(t_us, slide_us);
}

// Calls f(start) for each window containing t_us, in ascending start order.
template <typename F>
inline void for_each_window_start(int64_t t_us, int64_t window_us, int64_t slide_us, F&& f) {
  const int64_t last = last_window_start(t_us, slide_us);
  for (int64_t s = first_window_start(t_us, window_us, slide_us); s <= last; s += slide_us) f(s);
}

// Same, materialized. For tests and for callers that do not care about the
// allocation.
inline std::vector<int64_t> window_starts(int64_t t_us, int64_t window_us, int64_t slide_us) {
  std::vector<int64_t> out;
  for_each_window_start(t_us, window_us, slide_us, [&](int64_t s) { out.push_back(s); });
  return out;
}

inline void write_csv_header(FILE* f) {
  std::fputs("window_start_us,campaign_id,impressions,clicks,conversions,cost_micros\n", f);
}

// Returns the number of bytes written (0 on error). streamkit's workers add
// it up to know how long their output file is, which is what a checkpoint
// records and a resume truncates back to.
inline size_t write_csv_row(FILE* f, const WindowKey& k, const Agg& a) {
  const int n = std::fprintf(f, "%lld,%u,%llu,%llu,%llu,%lld\n",
                             static_cast<long long>(k.window_start_us), k.campaign_id,
                             static_cast<unsigned long long>(a.impressions),
                             static_cast<unsigned long long>(a.clicks),
                             static_cast<unsigned long long>(a.conversions),
                             static_cast<long long>(a.cost_micros));
  return n < 0 ? 0 : static_cast<size_t>(n);
}

}  // namespace adstack
