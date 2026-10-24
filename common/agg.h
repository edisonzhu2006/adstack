#pragma once
// Window keys, per-window aggregates and the CSV output format. The oracle
// and streamkit must agree on every byte of this so their outputs can be
// diffed, which is why it lives in one place.

#include <compare>
#include <cstdint>
#include <cstdio>
#include <stdexcept>
#include <string>
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
    // Checked: two adversarial records of INT64_MAX micro-dollars would
    // otherwise be signed overflow (undefined behaviour) and print a
    // negative cost. One predictable branch per event; the oracle shares
    // this code, so both sides fail the same way rather than agreeing on a
    // wrapped value.
    if (__builtin_add_overflow(cost_micros, e.cost_micros, &cost_micros)) {
      throw std::overflow_error("cost_micros sum overflows int64 in one window (event_id " +
                                std::to_string(e.event_id) + ")");
    }
  }

  bool operator==(const Agg&) const = default;
};

struct WindowKey {
  int64_t window_start_us;
  uint32_t campaign_id;
  auto operator<=>(const WindowKey&) const = default;
};

// ---- supported ranges -------------------------------------------------------
//
// All window arithmetic below (t - window, start + window, the watermark
// t - lateness, the dedup horizon watermark - window) is plain int64. It
// cannot overflow as long as |event_time| <= kMaxAbsEventTimeUs and every
// duration (window, slide, lateness, dedup bucket) is <= kMaxDurationUs:
// the extremes are then about 2^62 + 3 * 2^50, well inside 2^63. Callers
// check both before any arithmetic (streamkit's Worker::process and config
// validation, the oracle's read loop): a record outside the range is
// corruption (2^62 us is about 146,000 years from 1970), not a late event,
// and is reported as an error instead of being fed to undefined behaviour.
constexpr int64_t kMaxAbsEventTimeUs = int64_t{1} << 62;
constexpr int64_t kMaxDurationUs = int64_t{1} << 50;  // about 35.7 years

inline bool event_time_supported(int64_t t_us) {
  return t_us >= -kMaxAbsEventTimeUs && t_us <= kMaxAbsEventTimeUs;
}

// Throws std::out_of_range naming the record. Shared by streamkit and the
// oracle so both reject the same records with the same message.
inline void check_event_time(const Event& e) {
  if (!event_time_supported(e.event_time_us)) {
    throw std::out_of_range("event_id " + std::to_string(e.event_id) + ": event_time_us " +
                            std::to_string(e.event_time_us) +
                            " is outside the supported range [-2^62, 2^62] (corrupt record?)");
  }
}

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
// Precondition: event_time_supported(t_us) and window_us, slide_us in
// (0, kMaxDurationUs]. The loop also never computes a start past `last`
// (it stops before adding slide_us once s > last - slide_us), so even the
// increment cannot wrap.
template <typename F>
inline void for_each_window_start(int64_t t_us, int64_t window_us, int64_t slide_us, F&& f) {
  const int64_t last = last_window_start(t_us, slide_us);
  int64_t s = first_window_start(t_us, window_us, slide_us);
  if (s > last) return;
  while (true) {
    f(s);
    if (s > last - slide_us) break;
    s += slide_us;
  }
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

// Same bytes as write_csv_row, appended to a string instead of a FILE
// (streamkit's --out - streaming path collects a batch's rows and writes them
// to stdout in one call).
inline size_t append_csv_row(std::string& out, const WindowKey& k, const Agg& a) {
  char buf[160];  // longest row: 20 + 10 + 3 * 20 + 20 digits, 5 commas, sign, newline
  const int n = std::snprintf(buf, sizeof buf, "%lld,%u,%llu,%llu,%llu,%lld\n",
                              static_cast<long long>(k.window_start_us), k.campaign_id,
                              static_cast<unsigned long long>(a.impressions),
                              static_cast<unsigned long long>(a.clicks),
                              static_cast<unsigned long long>(a.conversions),
                              static_cast<long long>(a.cost_micros));
  if (n <= 0) return 0;
  out.append(buf, static_cast<size_t>(n));
  return static_cast<size_t>(n);
}

}  // namespace adstack
