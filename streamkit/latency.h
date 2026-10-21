#pragma once
// Emit-latency bookkeeping for streamkit (milestone 5, --latency-stats).
//
// WHAT IS MEASURED. For every window the watermark closes, the wall-clock
// time between (a) the moment the bytes holding the event that pushed the
// watermark past the window's end entered the process (read(2) returned the
// block containing them) and (b) the moment the window's CSV row had been
// appended to the worker's output stream. That is the delay the engine
// itself adds on top of the configured lateness: queueing between the
// reader and the worker, the worker's position in its batch, and the
// processing of the closing event. The metric is defined precisely in
// NOTES.md ("m5"), including what it deliberately does not include (the
// allowed lateness, the upstream producer's buffering, stdio flush to disk,
// the final merge, and windows closed by end of input).
//
// WHY A SEPARATE HEADER. The arithmetic (nearest-rank percentiles over a
// bag of microsecond samples) is the part a reader will want to check and a
// test can pin without threads or files; the collection itself is three
// lines inside Worker::close_ready and Reader::append (pipeline.cpp).
//
// COST. Nothing is recorded unless Config::latency_stats is set: the reader
// then reads steady_clock once per input block, and each worker reads it
// once per emitted row and stores one uint32_t per row. Samples are kept
// exact (not binned) because a histogram would need bucket boundaries to be
// defended and the measurement runs are short; memory is 4 bytes per
// emitted window for the life of the run, which is why this is a flag.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace adstack {

struct LatencySummary {
  size_t count = 0;    // windows the watermark closed
  // min is reported next to the percentiles because windows close in bursts
  // (every campaign's window for a given second closes on the same event):
  // the first row of a burst is the pure path through queue, decode and
  // close, the later rows are charged for the rows written before them.
  uint32_t min_us = 0;
  uint32_t p50_us = 0;
  uint32_t p99_us = 0;
  uint32_t max_us = 0;
};

// Nearest-rank percentile: the smallest value such that at least q of the
// samples are <= it. For q = 0.5 over {1..100} that is 50, for q = 0.99 it
// is 99. Chosen over interpolation because the answer is always a sample
// that was actually observed, which is easier to defend in a write-up.
// `sorted` must be ascending and non-empty.
inline uint32_t percentile_nearest_rank(const std::vector<uint32_t>& sorted, double q) {
  // ceil(q * n) as an index from 1; clamp so q = 0 still returns a sample.
  const double n = static_cast<double>(sorted.size());
  size_t rank = static_cast<size_t>(q * n + 0.999999999);  // ceil without <cmath> rounding quirks
  if (rank < 1) rank = 1;
  if (rank > sorted.size()) rank = sorted.size();
  return sorted[rank - 1];
}

// Sorts the samples in place (they are not needed in arrival order) and
// returns min, p50, p99 and max. Empty input gives an all-zero summary with
// count = 0, which main.cpp reports as "no windows closed by the watermark".
inline LatencySummary summarize_latency(std::vector<uint32_t>& samples_us) {
  LatencySummary s;
  s.count = samples_us.size();
  if (samples_us.empty()) return s;
  std::sort(samples_us.begin(), samples_us.end());
  s.min_us = samples_us.front();
  s.p50_us = percentile_nearest_rank(samples_us, 0.50);
  s.p99_us = percentile_nearest_rank(samples_us, 0.99);
  s.max_us = samples_us.back();
  return s;
}

// Microseconds between two steady_clock readings taken as nanoseconds since
// the clock's epoch, clamped into uint32_t (71 minutes; anything longer is
// not an emit latency anyone needs the exact value of). A negative gap
// cannot happen with a monotonic clock, but is clamped to 0 rather than
// wrapped so a bug would show up as a suspicious pile of zeros, not as a
// 4-billion-microsecond outlier.
inline uint32_t elapsed_us(int64_t start_ns, int64_t end_ns) {
  const int64_t us = (end_ns - start_ns) / 1000;
  if (us <= 0) return 0;
  if (us > static_cast<int64_t>(UINT32_MAX)) return UINT32_MAX;
  return static_cast<uint32_t>(us);
}

}  // namespace adstack
