// Unit tests for the m5 emit-latency measurement (latency.h and the
// --latency-stats plumbing in pipeline.cpp): the percentile arithmetic on
// known inputs, and that the pipeline records exactly one sample per window
// the watermark closed (none for windows closed by end of input, none when
// the flag is off), at one and several threads.

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <string>
#include <vector>

#include "common/event.h"
#include "latency.h"
#include "pipeline.h"

using namespace adstack;

namespace {

namespace fs = std::filesystem;
constexpr int64_t kS = 1'000'000;

std::string scratch(const std::string& name) {
  const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
  return (fs::temp_directory_path() / ("streamkit_lat_" + std::to_string(stamp) + "_" + name)).string();
}

}  // namespace

// ---- arithmetic ------------------------------------------------------------------

TEST(Latency, NearestRankPercentileOnOneToHundred) {
  std::vector<uint32_t> v;
  for (uint32_t i = 1; i <= 100; ++i) v.push_back(i);
  EXPECT_EQ(percentile_nearest_rank(v, 0.50), 50u);
  EXPECT_EQ(percentile_nearest_rank(v, 0.99), 99u);
  EXPECT_EQ(percentile_nearest_rank(v, 1.00), 100u);
  EXPECT_EQ(percentile_nearest_rank(v, 0.00), 1u);   // clamped to the first sample
  EXPECT_EQ(percentile_nearest_rank(v, 0.001), 1u);  // ceil(0.1) = 1
}

TEST(Latency, PercentileIsAlwaysAnObservedSample) {
  // Odd and even sizes, and a single sample: the answer must be an element.
  for (size_t n : {1u, 2u, 3u, 7u, 1000u}) {
    std::vector<uint32_t> v;
    for (size_t i = 0; i < n; ++i) v.push_back(static_cast<uint32_t>(i * 3 + 5));
    for (double q : {0.0, 0.5, 0.9, 0.99, 1.0}) {
      const uint32_t p = percentile_nearest_rank(v, q);
      EXPECT_EQ((p - 5) % 3, 0u) << "n=" << n << " q=" << q;
      EXPECT_GE(p, v.front());
      EXPECT_LE(p, v.back());
    }
  }
}

TEST(Latency, SummaryOfEmptyAndUnsortedInput) {
  std::vector<uint32_t> none;
  const LatencySummary e = summarize_latency(none);
  EXPECT_EQ(e.count, 0u);
  EXPECT_EQ(e.p50_us, 0u);
  EXPECT_EQ(e.p99_us, 0u);
  EXPECT_EQ(e.max_us, 0u);

  std::vector<uint32_t> v = {900, 5, 300, 7, 100, 2, 50, 1, 10, 20};  // 10 samples, unsorted
  const LatencySummary s = summarize_latency(v);
  EXPECT_EQ(s.count, 10u);
  EXPECT_EQ(s.p50_us, 10u);   // sorted: 1 2 5 7 10 20 50 100 300 900; rank ceil(5) = 5th
  EXPECT_EQ(s.p99_us, 900u);  // rank ceil(9.9) = 10th
  EXPECT_EQ(s.max_us, 900u);
  EXPECT_TRUE(std::is_sorted(v.begin(), v.end()));
}

TEST(Latency, ElapsedClampsNegativeAndHuge) {
  EXPECT_EQ(elapsed_us(1'000, 1'000), 0u);
  EXPECT_EQ(elapsed_us(1'000, 3'500), 2u);          // 2.5 us truncates to 2
  EXPECT_EQ(elapsed_us(5'000, 1'000), 0u);          // clock cannot go backwards; clamp, no wrap
  EXPECT_EQ(elapsed_us(0, INT64_MAX), UINT32_MAX);  // clamp, no overflow
}

// ---- pipeline plumbing ------------------------------------------------------------

namespace {

// One campaign, one event per minute starting at t = 0, 61s, 122s, ...
// With window 60s and lateness 0 the event at 61s closes [0, 60), the one at
// 122s closes [60, 120), and so on; the last window is closed by end of
// input, not by the watermark. So n events give exactly n - 1 samples.
void write_minutes(const std::string& path, int n, uint32_t campaigns) {
  EventWriter w(path);
  for (int i = 0; i < n; ++i) {
    for (uint32_t c = 0; c < campaigns; ++c) {
      Event e;
      e.event_id = static_cast<uint64_t>(i) * campaigns + c + 1;
      e.event_time_us = static_cast<int64_t>(i) * 61 * kS;
      e.ingest_time_us = e.event_time_us;
      e.campaign_id = c;
      e.cost_micros = 1;
      w.write(e);
    }
  }
}

size_t total_windows(const PipelineResult& r) {
  size_t n = 0;
  for (const auto& w : r.workers) n += w.windows_emitted;
  return n;
}

}  // namespace

TEST(Latency, OneSamplePerWatermarkClosedWindowAndNoneAtEndOfInput) {
  const std::string in = scratch("in.ads");
  const std::string out = scratch("out.csv");
  write_minutes(in, 5, 1);

  Config cfg;
  cfg.in_path = in;
  cfg.out_path = out;
  cfg.window_us = 60 * kS;
  cfg.slide_us = 60 * kS;
  cfg.lateness_us = 0;
  cfg.threads = 1;
  cfg.latency_stats = true;

  PipelineResult r = run_pipeline(cfg);
  ASSERT_TRUE(r.error.empty()) << r.error;
  EXPECT_EQ(total_windows(r), 5u);          // all five minutes produce a row
  EXPECT_EQ(r.emit_latency_us.size(), 4u);  // but only four were closed by the watermark
  // The whole run takes milliseconds, so no sample can be more than a few
  // seconds; the bound is loose on purpose (sanitizer builds are slow).
  for (uint32_t us : r.emit_latency_us) EXPECT_LT(us, 10u * 1'000'000u);

  fs::remove(in);
  fs::remove(out);
}

TEST(Latency, OffByDefaultRecordsNothing) {
  const std::string in = scratch("in.ads");
  const std::string out = scratch("out.csv");
  write_minutes(in, 5, 1);

  Config cfg;
  cfg.in_path = in;
  cfg.out_path = out;
  cfg.window_us = 60 * kS;
  cfg.slide_us = 60 * kS;
  cfg.threads = 2;

  PipelineResult r = run_pipeline(cfg);
  ASSERT_TRUE(r.error.empty()) << r.error;
  EXPECT_EQ(total_windows(r), 5u);
  EXPECT_TRUE(r.emit_latency_us.empty());

  fs::remove(in);
  fs::remove(out);
}

TEST(Latency, SamplesAreCollectedFromEveryWorker) {
  // 64 campaigns spread over 4 workers; every worker sees every minute, so
  // each closes 4 windows per campaign it owns by watermark: 64 x 4 samples.
  const std::string in = scratch("in.ads");
  const std::string out = scratch("out.csv");
  write_minutes(in, 5, 64);

  Config cfg;
  cfg.in_path = in;
  cfg.out_path = out;
  cfg.window_us = 60 * kS;
  cfg.slide_us = 60 * kS;
  cfg.lateness_us = 0;
  cfg.threads = 4;
  cfg.batch_events = 8;  // several batches per block, so the stamp is copied per batch
  cfg.latency_stats = true;

  PipelineResult r = run_pipeline(cfg);
  ASSERT_TRUE(r.error.empty()) << r.error;
  EXPECT_EQ(total_windows(r), 5u * 64u);
  EXPECT_EQ(r.emit_latency_us.size(), 4u * 64u);

  fs::remove(in);
  fs::remove(out);
}
