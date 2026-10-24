// Unit tests for the m3 pipeline: the partition hash, the per-worker engine
// (sliding windows, late rule, dedup) driven directly without threads, and
// the whole reader + workers + merge path on a small file at several thread
// counts, checked against a brute-force recompute done in the test.

#include <initializer_list>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <climits>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include "common/agg.h"
#include "common/event.h"
#include "pipeline.h"

using namespace adstack;

namespace {

namespace fs = std::filesystem;
constexpr int64_t kS = 1'000'000;

// A unique scratch path per test so parallel ctest jobs do not collide.
std::string scratch(const std::string& name) {
  const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
  return (fs::temp_directory_path() / ("streamkit_test_" + std::to_string(stamp) + "_" + name)).string();
}

Event make(uint64_t id, int64_t t_us, uint32_t campaign, uint8_t type = 0, int64_t cost = 1) {
  Event e;
  e.event_id = id;
  e.event_time_us = t_us;
  e.ingest_time_us = t_us;
  e.campaign_id = campaign;
  e.event_type = type;
  e.cost_micros = cost;
  return e;
}

// Parses a CSV (header skipped) into (key -> agg) so comparisons are order
// insensitive, as in bench/compare.sh.
std::map<WindowKey, Agg> read_csv(const std::string& path) {
  std::map<WindowKey, Agg> out;
  std::ifstream in(path);
  std::string line;
  std::getline(in, line);  // header
  while (std::getline(in, line)) {
    std::replace(line.begin(), line.end(), ',', ' ');
    std::istringstream ss(line);
    WindowKey k{};
    Agg a;
    ss >> k.window_start_us >> k.campaign_id >> a.impressions >> a.clicks >> a.conversions >>
        a.cost_micros;
    EXPECT_TRUE(out.emplace(k, a).second) << "duplicate row " << line;
  }
  return out;
}

// The oracle's logic, inline, so this test does not depend on the oracle binary.
std::map<WindowKey, Agg> brute_force(const std::vector<Event>& events, int64_t window_us,
                                     int64_t slide_us) {
  std::unordered_set<uint64_t> seen;
  std::map<WindowKey, Agg> out;
  for (const Event& e : events) {
    if (!seen.insert(e.event_id).second) continue;
    for_each_window_start(e.event_time_us, window_us, slide_us,
                          [&](int64_t ws) { out[{ws, e.campaign_id}].add(e); });
  }
  return out;
}

Config base_config() {
  Config cfg;
  cfg.window_us = 60 * kS;
  cfg.slide_us = 60 * kS;
  cfg.lateness_us = 60 * kS;
  cfg.dedup_bucket_us = kS;
  return cfg;
}

// Drives one Worker directly (no threads) and returns its emitted rows.
std::map<WindowKey, Agg> run_worker(const Config& cfg, const std::vector<Event>& events,
                                    WorkerStats* stats_out = nullptr) {
  const std::string part = scratch("worker.csv");
  {
    Worker w(0, cfg, part, "");
    for (const Event& e : events) w.process(e);
    w.finish();
    if (stats_out) *stats_out = w.stats();
  }
  // Worker parts carry no header; prepend one so read_csv can skip it.
  const std::string with_header = part + ".h";
  {
    std::ofstream o(with_header);
    o << "header\n" << std::ifstream(part).rdbuf();
  }
  auto rows = read_csv(with_header);
  fs::remove(part);
  fs::remove(with_header);
  return rows;
}

}  // namespace

// ---- partitioning ------------------------------------------------------------

TEST(Partition, OnePartitionIsAlwaysZero) {
  for (uint32_t c : {0u, 1u, 999u, 0xFFFFFFFFu}) EXPECT_EQ(partition_of(c, 1), 0u);
}

TEST(Partition, InRangeAndDeterministic) {
  for (uint32_t p : {2u, 3u, 8u, 16u}) {
    for (uint32_t c = 0; c < 10'000; ++c) {
      const uint32_t a = partition_of(c, p);
      EXPECT_LT(a, p);
      EXPECT_EQ(a, partition_of(c, p));
    }
  }
}

TEST(Partition, SpreadsSequentialAndStructuredIds) {
  // 100K sequential ids (the generator's campaign space) and 100K ids that
  // are all multiples of 8 (which a plain `id % 8` would send to one worker)
  // must both land within 3% of an even split over 8 partitions.
  for (uint32_t stride : {1u, 8u}) {
    std::vector<uint64_t> counts(8, 0);
    for (uint32_t i = 0; i < 100'000; ++i) ++counts[partition_of(i * stride, 8)];
    for (size_t p = 0; p < 8; ++p) {
      EXPECT_NEAR(static_cast<double>(counts[p]), 12'500.0, 375.0) << "stride " << stride << " p " << p;
    }
  }
}

// ---- worker engine ------------------------------------------------------------

TEST(Worker, TumblingDedupAndLateMatchM2Semantics) {
  Config cfg = base_config();
  std::vector<Event> ev = {
      make(1, 100 * kS, 7),
      make(2, 100 * kS + 5, 7, 1, 50),   // click
      make(1, 100 * kS, 7),              // duplicate of 1
      make(3, 130 * kS, 8),
      make(4, 400 * kS, 7),              // watermark -> 340s; windows < 340s close
      make(5, 100 * kS, 7),              // late: window [60,120) closed
  };
  WorkerStats st;
  auto rows = run_worker(cfg, ev, &st);
  EXPECT_EQ(st.events, 6u);
  EXPECT_EQ(st.dups, 1u);
  EXPECT_EQ(st.late, 1u);
  EXPECT_EQ(rows, brute_force({ev[0], ev[1], ev[3], ev[4]}, cfg.window_us, cfg.slide_us));
  EXPECT_EQ(rows.at({60 * kS, 7}).impressions, 1u);
  EXPECT_EQ(rows.at({60 * kS, 7}).clicks, 1u);
  EXPECT_EQ(rows.at({60 * kS, 7}).cost_micros, 51);
}

TEST(Worker, SlidingEventCountsInWindowOverSlideWindows) {
  Config cfg = base_config();
  cfg.window_us = 300 * kS;
  cfg.slide_us = 60 * kS;
  std::vector<Event> ev = {make(1, 1000 * kS, 3), make(2, 1000 * kS, 3), make(1, 1000 * kS, 3)};
  auto rows = run_worker(cfg, ev);
  ASSERT_EQ(rows.size(), 5u);  // 300/60 windows, not 1
  for (int64_t s = 720 * kS; s <= 960 * kS; s += 60 * kS) {
    ASSERT_TRUE(rows.count({s, 3})) << "missing window starting at " << s / kS;
    EXPECT_EQ(rows.at({s, 3}).impressions, 2u);  // dup counted once in every window
  }
  EXPECT_EQ(rows, brute_force(ev, cfg.window_us, cfg.slide_us));
}

TEST(Worker, SlidingLateRuleUsesEarliestWindow) {
  // window 120s, slide 60s, lateness 0. An event at 130s is in windows
  // [60,180) and [120,240). After the watermark reaches 180s the earlier one
  // has been emitted, so a new event at 130s is late as a whole, even though
  // [120,240) is still open: partial counting would leave the output and the
  // side file disagreeing about the event.
  Config cfg = base_config();
  cfg.window_us = 120 * kS;
  cfg.slide_us = 60 * kS;
  cfg.lateness_us = 0;
  std::vector<Event> ev = {make(1, 130 * kS, 1), make(2, 180 * kS, 1), make(3, 130 * kS, 1)};
  WorkerStats st;
  auto rows = run_worker(cfg, ev, &st);
  EXPECT_EQ(st.late, 1u);
  EXPECT_EQ(rows.at({60 * kS, 1}).impressions, 1u);     // only event 1
  EXPECT_EQ(rows.at({120 * kS, 1}).impressions, 2u);    // events 1 and 2, not 3
  EXPECT_EQ(rows.at({180 * kS, 1}).impressions, 1u);    // event 2
}

TEST(Worker, WindowsCloseAsWatermarkPassesTheirEnd) {
  Config cfg = base_config();
  cfg.lateness_us = 10 * kS;
  std::vector<Event> ev;
  // One event per second for 10 minutes, all on campaign 1.
  for (int64_t t = 0; t < 600; ++t) ev.push_back(make(static_cast<uint64_t>(t + 1), t * kS, 1));
  WorkerStats st;
  auto rows = run_worker(cfg, ev, &st);
  EXPECT_EQ(rows.size(), 10u);
  EXPECT_EQ(st.windows_emitted, 10u);
  // Never more than two windows open at once: the current minute and the one
  // whose 10s grace period has not yet run out.
  EXPECT_LE(st.open_windows_peak, 2u);
  for (const auto& [k, a] : rows) EXPECT_EQ(a.impressions, 60u) << k.window_start_us;
}

// ---- end to end (threads, queues, merge) ----------------------------------------

namespace {

// Writes a small dirty stream (dups, late events, many campaigns) and returns
// the events in file order.
std::vector<Event> write_stream(const std::string& path, int n) {
  std::vector<Event> ev;
  EventWriter w(path);
  uint64_t id = 1;
  for (int i = 0; i < n; ++i) {
    const int64_t ingest = 1000 * kS + static_cast<int64_t>(i) * 10'000;  // 100 events/s
    Event e;
    if (i % 100 == 37) {
      e = ev[static_cast<size_t>(i - 1 - (i % 30))];  // a recent duplicate (index >= 6)
      e.ingest_time_us = ingest;
    } else {
      const int64_t late = (i % 20 == 3) ? (1 + i % 29) * kS : (i % 7) * 1000;
      e = make(id++, ingest - late, static_cast<uint32_t>((i * 7919) % 1000),
               static_cast<uint8_t>(i % 10 == 0 ? 1 : 0), 1 + i % 5);
    }
    w.write(e);
    ev.push_back(e);
  }
  return ev;
}

void run_end_to_end(int threads, int64_t window_s, int64_t slide_s, size_t batch, size_t depth) {
  const std::string in = scratch("in.ads");
  const std::string out = scratch("out.csv");
  const std::string late = scratch("late.ads");
  const auto events = write_stream(in, 20'000);

  Config cfg = base_config();
  cfg.in_path = in;
  cfg.out_path = out;
  cfg.late_out_path = late;
  cfg.window_us = window_s * kS;
  cfg.slide_us = slide_s * kS;
  cfg.threads = threads;
  cfg.batch_events = batch;
  cfg.queue_depth = depth;

  PipelineResult r = run_pipeline(cfg);
  ASSERT_TRUE(r.error.empty()) << r.error;
  EXPECT_EQ(r.events, events.size());
  EXPECT_EQ(r.workers.size(), static_cast<size_t>(threads));

  uint64_t total = 0, dups = 0, lates = 0;
  for (const auto& w : r.workers) { total += w.events; dups += w.dups; lates += w.late; }
  EXPECT_EQ(total, events.size());
  EXPECT_EQ(dups, 200u);   // every 100th event from index 37 is a duplicate
  EXPECT_EQ(lates, 0u);    // lateness 60s covers the max 29s delay

  EXPECT_EQ(read_csv(out), brute_force(events, cfg.window_us, cfg.slide_us));

  // The merged late file is a valid .ads file (one header) even with several
  // parts; here it is simply empty.
  EventReader lr(late);
  Event e;
  EXPECT_FALSE(lr.next(e));

  // No part files left behind.
  for (int i = 0; i < threads; ++i) {
    EXPECT_FALSE(fs::exists(out + ".part" + std::to_string(i)));
    EXPECT_FALSE(fs::exists(late + ".part" + std::to_string(i)));
  }
  fs::remove(in);
  fs::remove(out);
  fs::remove(late);
}

}  // namespace

TEST(Pipeline, MatchesBruteForceAtOneTwoFourEightThreads) {
  for (int p : {1, 2, 4, 8}) run_end_to_end(p, 60, 60, 128, 64);
}

TEST(Pipeline, SlidingMatchesBruteForceAcrossThreads) {
  for (int p : {1, 3, 8}) run_end_to_end(p, 300, 60, 128, 64);
}

TEST(Pipeline, TinyQueueAndBatchExerciseBackpressure) {
  // Two slots of one event each: the reader must wait on every event, and the
  // result must still be exact. This is the "never drop" guarantee.
  run_end_to_end(4, 60, 60, 1, 2);
}

TEST(Pipeline, LateEventsEndUpInOneMergedSideFile) {
  const std::string in = scratch("late_in.ads");
  const std::string out = scratch("late_out.csv");
  const std::string late = scratch("late_side.ads");
  std::vector<Event> ev;
  {
    EventWriter w(in);
    // Three phases over campaigns 0..7 (spread over four workers): on-time
    // events at 1000s, then events at 2000s that close the first window at
    // lateness 0 on every worker, then a new event per campaign back at
    // 1000s, which is now late. The phases must not be interleaved: a
    // worker's watermark is per worker, not per campaign, so an on-time event
    // for campaign 3 arriving after campaign 0's 2000s event would already be
    // late on the worker they share.
    for (uint32_t c = 0; c < 8; ++c) ev.push_back(make(100 + c, 1000 * kS, c));
    for (uint32_t c = 0; c < 8; ++c) ev.push_back(make(200 + c, 2000 * kS, c));
    for (uint32_t c = 0; c < 8; ++c) ev.push_back(make(300 + c, 1000 * kS + 1, c));
    for (const Event& e : ev) w.write(e);
  }
  Config cfg = base_config();
  cfg.in_path = in;
  cfg.out_path = out;
  cfg.late_out_path = late;
  cfg.lateness_us = 0;
  cfg.threads = 4;
  PipelineResult r = run_pipeline(cfg);
  ASSERT_TRUE(r.error.empty()) << r.error;

  uint64_t lates = 0;
  for (const auto& w : r.workers) lates += w.late;
  EXPECT_EQ(lates, 8u);

  // The side file is readable end to end (only one header survived the merge)
  // and holds exactly the eight late events.
  EventReader lr(late);
  Event e;
  std::vector<uint64_t> ids;
  while (lr.next(e)) ids.push_back(e.event_id);
  std::sort(ids.begin(), ids.end());
  EXPECT_EQ(ids, (std::vector<uint64_t>{300, 301, 302, 303, 304, 305, 306, 307}));

  auto rows = read_csv(out);
  EXPECT_EQ(rows.size(), 16u);
  for (uint32_t c = 0; c < 8; ++c) EXPECT_EQ(rows.at({960 * kS, c}).impressions, 1u);
  fs::remove(in);
  fs::remove(out);
  fs::remove(late);
}

TEST(Pipeline, RejectsBadConfig) {
  Config cfg = base_config();
  cfg.in_path = "/nonexistent/input.ads";
  cfg.out_path = scratch("bad.csv");
  cfg.threads = 0;
  EXPECT_THROW(run_pipeline(cfg), std::invalid_argument);
  cfg.threads = 1;
  cfg.slide_us = cfg.window_us * 2;
  EXPECT_THROW(run_pipeline(cfg), std::invalid_argument);
}

TEST(Pipeline, MissingInputThrowsBeforeAnyWorkerStarts) {
  // The input is opened before the workers exist (it decides fresh start vs
  // resume), so a missing file is a setup error: no threads, no part files.
  Config cfg = base_config();
  cfg.in_path = scratch("does_not_exist.ads");
  cfg.out_path = scratch("missing.csv");
  cfg.threads = 3;
  EXPECT_THROW(run_pipeline(cfg), std::runtime_error);
  for (int i = 0; i < 3; ++i) EXPECT_FALSE(fs::exists(cfg.out_path + ".part" + std::to_string(i)));
}

TEST(Pipeline, TornInputIsReportedNotHung) {
  // The reader fails mid-stream (a record cut in half at the end of the
  // file); the workers must still be told to stop and the error must surface
  // as a reader error rather than a hang on join().
  const std::string in = scratch("torn.ads");
  write_stream(in, 1000);
  fs::resize_file(in, fs::file_size(in) - 7);
  Config cfg = base_config();
  cfg.in_path = in;
  cfg.out_path = scratch("torn.csv");
  cfg.threads = 3;
  PipelineResult r = run_pipeline(cfg);
  EXPECT_NE(r.error.find("reader:"), std::string::npos) << r.error;
  EXPECT_NE(r.error.find("torn"), std::string::npos) << r.error;
  fs::remove(in);
  for (int i = 0; i < 3; ++i) fs::remove(cfg.out_path + ".part" + std::to_string(i));
}

// ---- review fixes (2026-10-24) -----------------------------------------------------

namespace {

void write_events(const std::string& path, const std::vector<Event>& events) {
  EventWriter w(path);
  for (const Event& e : events) w.write(e);
  w.close();
}

}  // namespace

TEST(Pipeline, ExtremeEventTimesFailLoudlyInsteadOfOverflowing) {
  // One record near INT64_MIN used to hang the engine (window_start wrapped
  // and for_each_window_start never terminated); one near INT64_MAX was
  // silently diverted as late. Both were signed overflow, i.e. undefined
  // behaviour. Now the record is rejected by name before any arithmetic.
  // Run under the ASan+UBSan build this test must also print no UBSan report.
  for (const int64_t t : std::initializer_list<int64_t>{LLONG_MIN + 5, LLONG_MAX - 5, kMaxAbsEventTimeUs + 1,
                                                        -kMaxAbsEventTimeUs - 1}) {
    for (const int64_t lateness_s : {int64_t{0}, int64_t{60}}) {
      const std::string in = scratch("extreme.ads");
      write_events(in, {make(41, t, 7), make(42, 0, 7)});
      Config cfg = base_config();
      cfg.lateness_us = lateness_s * kS;
      cfg.in_path = in;
      cfg.out_path = scratch("extreme.csv");
      cfg.threads = 2;
      const PipelineResult r = run_pipeline(cfg);  // returns: no hang
      EXPECT_NE(r.error.find("event_id 41"), std::string::npos) << t << ": " << r.error;
      EXPECT_NE(r.error.find("outside the supported range"), std::string::npos) << r.error;
      EXPECT_FALSE(fs::exists(cfg.out_path)) << "a failed run commits no output";
      for (int i = 0; i < 2; ++i) {
        // Without --ckpt-dir the parts are useless, so a failed run removes them.
        EXPECT_FALSE(fs::exists(cfg.out_path + ".part" + std::to_string(i))) << "leftover part " << i;
      }
      fs::remove(in);
    }
  }
}

TEST(Pipeline, EventTimesAtTheSupportedLimitsAreAggregatedExactly) {
  // The edge of the range must work, sliding windows included, with the
  // largest durations the config accepts (no overflow anywhere in between).
  for (const int64_t t : {-kMaxAbsEventTimeUs, kMaxAbsEventTimeUs}) {
    Config cfg = base_config();
    cfg.window_us = 300 * kS;
    cfg.slide_us = 60 * kS;
    cfg.lateness_us = kMaxDurationUs;
    const auto rows = run_worker(cfg, {make(1, t, 3), make(2, t, 3), make(2, t, 3)});
    ASSERT_EQ(rows.size(), 5u) << t;  // window / slide windows, each with 2 events (one dup)
    for (const auto& [k, a] : rows) {
      EXPECT_LE(k.window_start_us, t);
      EXPECT_GT(k.window_start_us + cfg.window_us, t);
      EXPECT_EQ(a.impressions, 2u);
    }
  }
  Config big = base_config();
  big.window_us = kMaxDurationUs + 1;
  big.slide_us = big.window_us;
  big.in_path = "/nonexistent";
  EXPECT_THROW(run_pipeline(big), std::invalid_argument);
}

TEST(Worker, ExpiredLateEventsAreCountedSeparately) {
  // Window 1 s, lateness 0: the dedup horizon is watermark - 1 s. A copy of
  // an event that arrives after the horizon passed it cannot be recognised
  // as a duplicate any more; it is diverted as late (the aggregates stay
  // exact) but counted as expired, because the late file may now hold a
  // re-delivery of an event that was already counted.
  Config cfg = base_config();
  cfg.window_us = kS;
  cfg.slide_us = kS;
  cfg.lateness_us = 0;
  WorkerStats st;
  const auto rows = run_worker(cfg,
                               {make(1, 10 * kS + 500'000, 5),    // counted in [10, 11)
                                make(2, 100 * kS, 5),             // watermark 100 s, horizon 99 s
                                make(1, 10 * kS + 500'000, 5),    // re-delivery of 1: expired + late
                                make(3, 50 * kS, 5),              // new but far too late: expired + late
                                make(4, 99 * kS + 500'000, 5),    // late, inside the horizon: remembered
                                make(4, 99 * kS + 500'000, 5)},   // ... so its copy is a dup
                               &st);
  EXPECT_EQ(st.late, 3u);
  EXPECT_EQ(st.expired, 2u);
  EXPECT_EQ(st.dups, 1u);
  EXPECT_EQ(rows.at({10 * kS, 5}).impressions, 1u) << "the re-delivery is not counted twice";
  EXPECT_EQ(rows.size(), 2u);
}

TEST(Pipeline, StdoutRowsAreVisibleBeforeEndOfInput) {
  // --out - used to buffer every row in part files and print them after the
  // last worker joined, so a consumer saw nothing until end of input while
  // --latency-stats reported milliseconds. Now each batch's rows are
  // written and flushed to stdout as soon as their windows close. Proof:
  // feed a pipe, close a window, and wait for its row to appear on stdout
  // while the pipe is still open.
  const std::string out = scratch("stdout.csv");
  int p[2];
  ASSERT_EQ(::pipe(p), 0);
  std::fflush(stdout);
  const int saved_stdout = ::dup(STDOUT_FILENO);
  const int out_fd = ::open(out.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  ASSERT_GE(out_fd, 0);
  ASSERT_GE(::dup2(out_fd, STDOUT_FILENO), 0);
  ::close(out_fd);

  auto count_rows = [&] {
    std::ifstream f(out);
    std::string line;
    size_t n = 0;
    while (std::getline(f, line)) ++n;
    return n == 0 ? 0 : n - 1;  // minus the header
  };
  bool visible_before_eof = false;
  std::thread producer([&] {
    auto send = [&](const std::vector<Event>& evs) {
      std::vector<uint8_t> buf;
      for (const Event& e : evs) {
        uint8_t rec[kEventWireSize];
        encode(e, rec);
        buf.insert(buf.end(), rec, rec + kEventWireSize);
      }
      (void)!::write(p[1], buf.data(), buf.size());
    };
    uint8_t hdr[kFileHeaderSize];
    std::memcpy(hdr, &kFileMagic, 4);
    std::memcpy(hdr + 4, &kFileVersion, 4);
    (void)!::write(p[1], hdr, sizeof hdr);
    // Window [0, 1 s) of campaigns 1..4; the event at 2.5 s closes it on
    // whichever worker owns each campaign.
    std::vector<Event> first;
    for (uint32_t c = 1; c <= 4; ++c) first.push_back(make(c, 500'000, c));
    for (uint32_t c = 1; c <= 4; ++c) first.push_back(make(100 + c, 2'500'000, c));
    send(first);
    for (int i = 0; i < 500 && !visible_before_eof; ++i) {  // up to 5 s
      if (count_rows() >= 4) visible_before_eof = true;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    send({make(200, 2'700'000, 1)});  // same window as 101: closes nothing new
    ::close(p[1]);
  });

  Config cfg = base_config();
  cfg.window_us = kS;
  cfg.slide_us = kS;
  cfg.lateness_us = 0;
  cfg.in_path = "/dev/fd/" + std::to_string(p[0]);
  cfg.out_path = "-";
  cfg.threads = 2;
  cfg.latency_stats = true;
  PipelineResult r = run_pipeline(cfg);
  producer.join();
  std::fflush(stdout);
  ::dup2(saved_stdout, STDOUT_FILENO);
  ::close(saved_stdout);
  ::close(p[0]);

  ASSERT_TRUE(r.error.empty()) << r.error;
  EXPECT_TRUE(visible_before_eof) << "rows only appeared at end of input";
  EXPECT_EQ(r.emit_latency_us.size(), 4u) << "one sample per watermark-closed window";
  auto rows = read_csv(out);
  EXPECT_EQ(rows.size(), 8u);  // [0,1) and [2,3) for campaigns 1..4
  for (uint32_t c = 1; c <= 4; ++c) EXPECT_EQ(rows.at({0, c}).impressions, 1u);
  EXPECT_EQ(rows.at({2 * kS, 1}).impressions, 2u);
  fs::remove(out);
}
