// Unit tests for m4 checkpoints and resume: the snapshot codec (round trip,
// truncation, corruption), the manifest, resume preparation (partial and
// stale epochs), dedup export/import, truncate-on-resume at the worker
// level, and the whole pipeline failing after a checkpoint and resuming to
// byte-identical output.

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
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
#include <sys/stat.h>
#include <unistd.h>

#include "checkpoint.h"
#include "common/agg.h"
#include "common/event.h"
#include "dedup.h"
#include "pipeline.h"

using namespace adstack;

namespace {

namespace fs = std::filesystem;
constexpr int64_t kS = 1'000'000;

// A unique scratch directory per test so parallel ctest jobs do not collide.
fs::path scratch_dir(const std::string& name) {
  const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
  const fs::path d = fs::temp_directory_path() / ("streamkit_ckpt_" + std::to_string(stamp) + "_" + name);
  fs::create_directories(d);
  return d;
}

std::string slurp(const fs::path& p) {
  std::ifstream in(p, std::ios::binary);
  std::stringstream ss;
  ss << in.rdbuf();
  return ss.str();
}

void spit(const fs::path& p, const std::string& s) {
  std::ofstream out(p, std::ios::binary);
  out << s;
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

WorkerSnapshot sample_snapshot() {
  WorkerSnapshot s;
  s.epoch = 7;
  s.worker = 3;
  s.partitions = 8;
  s.input_offset = 8 + 52 * 7'000'000ULL;
  s.out_bytes = 123'456;
  s.late_bytes = 8 + 52 * 9;
  s.max_event_time_us = 1'767'225'600LL * kS + 5 * kS;
  s.watermark_us = s.max_event_time_us - 60 * kS;
  s.events = 875'001;
  s.dups = 8'750;
  s.late = 12;
  s.expired = 5;
  s.windows_emitted = 40'000;
  for (uint32_t c = 0; c < 50; ++c) {
    Agg a;
    a.impressions = c * 3;
    a.clicks = c;
    a.conversions = c % 2;
    a.cost_micros = static_cast<int64_t>(c) * 1000 - 7;
    s.open_windows.emplace_back(WindowKey{s.watermark_us - static_cast<int64_t>(c % 3) * 60 * kS, c}, a);
  }
  s.dedup.forget_before = s.watermark_us - 60 * kS;
  s.dedup.front_start = *s.dedup.forget_before;
  for (int b = 0; b < 5; ++b) {
    std::vector<uint64_t> ids;
    for (uint64_t i = 0; i < 100; ++i) ids.push_back(static_cast<uint64_t>(b) * 1000 + i * 7);
    s.dedup.buckets.push_back(ids);
  }
  s.dedup.buckets.push_back({});  // an empty bucket (a gap in event time) survives too
  return s;
}

// Byte layout of a snapshot, from the format comment in checkpoint.h: magic +
// version (8), the fixed fields up to and including n_windows (104), 44 per
// open window, the dedup header (25), 8 per bucket count, 8 per id, 8 of
// checksum. Pinned here so a silent change to the format fails a test.
constexpr size_t kSnapHeader = 8;
constexpr size_t kSnapFixed = 104;
constexpr size_t kSnapPerWindow = 44;
constexpr size_t kSnapDedupHeader = 25;

Manifest sample_manifest() {
  Manifest m;
  m.epoch = 12;
  m.input_offset = 8 + 52 * 12'000'000ULL;
  m.partitions = 8;
  m.window_us = 60 * kS;
  m.slide_us = 60 * kS;
  m.lateness_us = 60 * kS;
  m.dedup_bucket_us = kS;
  m.dedup = true;
  m.checkpoint_every_events = 1'000'000;
  m.late_out = true;
  // Hashes above INT64_MAX must survive the text round trip.
  m.input = InputFingerprint{8 + 52 * 20'000'000ULL, 0xF00DFACE12345678ULL, 0x0123456789ABCDEFULL};
  return m;
}

}  // namespace

// ---- snapshot codec -------------------------------------------------------------

TEST(Snapshot, RoundTripIsExact) {
  const WorkerSnapshot s = sample_snapshot();
  const std::vector<uint8_t> bytes = encode_snapshot(s);
  EXPECT_EQ(decode_snapshot(bytes), s);
  // Nothing is padded or duplicated: 50 windows, 6 bucket counts, 500 ids.
  EXPECT_EQ(bytes.size(), kSnapHeader + kSnapFixed + 50 * kSnapPerWindow + kSnapDedupHeader +
                              6 * 8 + 500 * 8 + 8);
}

TEST(Snapshot, EmptyStateRoundTrips) {
  WorkerSnapshot s;
  s.worker = 0;
  s.partitions = 1;
  EXPECT_EQ(decode_snapshot(encode_snapshot(s)), s);
}

TEST(Snapshot, TruncationAndCorruptionAreLoudErrors) {
  const std::vector<uint8_t> bytes = encode_snapshot(sample_snapshot());
  // Every prefix must be rejected: the checksum covers the whole file, and a
  // prefix that happens to end on a field boundary must not decode either.
  for (size_t cut : {size_t{0}, size_t{7}, size_t{12}, bytes.size() / 2, bytes.size() - 1}) {
    std::vector<uint8_t> cutb(bytes.begin(), bytes.begin() + static_cast<long>(cut));
    EXPECT_THROW(decode_snapshot(cutb), std::runtime_error) << "cut at " << cut;
  }
  // One flipped bit anywhere fails the checksum.
  for (size_t at : {size_t{0}, size_t{40}, bytes.size() - 9, bytes.size() - 1}) {
    std::vector<uint8_t> bad = bytes;
    bad[at] ^= 0x01;
    EXPECT_THROW(decode_snapshot(bad), std::runtime_error) << "flip at " << at;
  }
  // A bogus window count with a valid checksum cannot make us allocate
  // terabytes: re-sign a doctored body and expect a clean "truncated".
  std::vector<uint8_t> doctored = bytes;
  const uint64_t huge = ~0ULL;
  std::memcpy(doctored.data() + kSnapHeader + kSnapFixed - 8, &huge, 8);  // n_windows field
  const uint64_t sum = fnv1a64(doctored.data(), doctored.size() - 8);
  std::memcpy(doctored.data() + doctored.size() - 8, &sum, 8);
  EXPECT_THROW(decode_snapshot(doctored), std::runtime_error);
}

TEST(Snapshot, AtomicWriteLeavesNoTempAndReadsBack) {
  const fs::path d = scratch_dir("atomic");
  const WorkerSnapshot s = sample_snapshot();
  const fs::path p = d / "worker-3.snap";
  atomic_write_file(p, encode_snapshot(s));
  EXPECT_TRUE(fs::exists(p));
  EXPECT_FALSE(fs::exists(d / "worker-3.snap.tmp"));
  EXPECT_EQ(read_snapshot_file(p), s);
  // A second write replaces the content atomically (rename over).
  WorkerSnapshot s2 = s;
  s2.epoch = 8;
  atomic_write_file(p, encode_snapshot(s2));
  EXPECT_EQ(read_snapshot_file(p).epoch, 8u);
  fs::remove_all(d);
}

// ---- manifest ----------------------------------------------------------------------

TEST(Manifest, RoundTripAndHumanReadable) {
  const Manifest m = sample_manifest();
  const std::string text = format_manifest(m);
  EXPECT_NE(text.find("epoch 12\n"), std::string::npos);
  EXPECT_NE(text.find("input_offset 624000008\n"), std::string::npos);
  EXPECT_EQ(parse_manifest(text), m);
  Manifest nd = m;
  nd.dedup = false;
  EXPECT_EQ(parse_manifest(format_manifest(nd)), nd);
}

TEST(Manifest, RejectsMalformedInput) {
  const std::string good = format_manifest(sample_manifest());
  EXPECT_THROW(parse_manifest(""), std::runtime_error);
  EXPECT_THROW(parse_manifest("not a manifest\nepoch 1\n"), std::runtime_error);
  // Missing key.
  std::string missing = good;
  missing.erase(missing.find("partitions"), std::string("partitions 8\n").size());
  EXPECT_THROW(parse_manifest(missing), std::runtime_error);
  // Non-numeric value.
  std::string bad = good;
  bad.replace(bad.find("epoch 12"), 8, "epoch tw");
  EXPECT_THROW(parse_manifest(bad), std::runtime_error);
  // Trailing junk on a number.
  std::string junk = good;
  junk.replace(junk.find("epoch 12"), 8, "epoch 12x");
  EXPECT_THROW(parse_manifest(junk), std::runtime_error);
  // Zero partitions is meaningless.
  std::string zero = good;
  zero.replace(zero.find("partitions 8"), 12, "partitions 0");
  EXPECT_THROW(parse_manifest(zero), std::runtime_error);
}

// ---- resume preparation ---------------------------------------------------------------

namespace {

void write_fake_snapshot(const fs::path& ckpt, uint64_t epoch, uint32_t worker, uint32_t partitions) {
  WorkerSnapshot s;
  s.epoch = epoch;
  s.worker = worker;
  s.partitions = partitions;
  fs::create_directories(epoch_dir(ckpt, epoch));
  atomic_write_file(snapshot_path(ckpt, epoch, worker), encode_snapshot(s));
}

}  // namespace

TEST(Resume, EmptyOrMissingDirHasNoStateAndNothingToResume) {
  const fs::path d = scratch_dir("empty");
  EXPECT_FALSE(checkpoint_dir_has_state(d));
  EXPECT_FALSE(prepare_resume(d).has_value());
  EXPECT_FALSE(checkpoint_dir_has_state(d / "does-not-exist"));
  EXPECT_FALSE(prepare_resume(d / "does-not-exist").has_value());
  fs::remove_all(d);
}

TEST(Resume, NoManifestMeansStartOverAndPartialEpochIsDeleted) {
  // Crash between the first snapshot writes: epoch-1 has one of two
  // snapshots and nothing was ever committed.
  const fs::path d = scratch_dir("partial_only");
  write_fake_snapshot(d, 1, 0, 2);
  spit(d / "manifest.tmp", "half written");
  EXPECT_TRUE(checkpoint_dir_has_state(d));
  EXPECT_FALSE(prepare_resume(d).has_value());
  EXPECT_FALSE(fs::exists(epoch_dir(d, 1)));
  EXPECT_FALSE(fs::exists(d / "manifest.tmp"));
  EXPECT_FALSE(checkpoint_dir_has_state(d));
  fs::remove_all(d);
}

TEST(Resume, CommittedEpochIsKeptAndEveryOtherEpochIsRemoved) {
  // MANIFEST names epoch 5. epoch-4 is the previous epoch whose delete was
  // interrupted (crash after rename, before remove); epoch-6 is the next one,
  // caught with one of two snapshots written; epoch-7 is complete but was
  // never committed (crash after the last snapshot rename, before the
  // manifest rename). Only epoch-5 may survive.
  const fs::path d = scratch_dir("mixed");
  Manifest m = sample_manifest();
  m.epoch = 5;
  m.partitions = 2;
  for (uint32_t w = 0; w < 2; ++w) write_fake_snapshot(d, 4, w, 2);
  for (uint32_t w = 0; w < 2; ++w) write_fake_snapshot(d, 5, w, 2);
  write_fake_snapshot(d, 6, 1, 2);
  for (uint32_t w = 0; w < 2; ++w) write_fake_snapshot(d, 7, w, 2);
  atomic_write_file(manifest_path(d), format_manifest(m));

  const auto got = prepare_resume(d);
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, m);
  EXPECT_TRUE(fs::exists(snapshot_path(d, 5, 0)));
  EXPECT_TRUE(fs::exists(snapshot_path(d, 5, 1)));
  EXPECT_FALSE(fs::exists(epoch_dir(d, 4)));
  EXPECT_FALSE(fs::exists(epoch_dir(d, 6)));
  EXPECT_FALSE(fs::exists(epoch_dir(d, 7)));
  // Idempotent: preparing again changes nothing.
  EXPECT_EQ(*prepare_resume(d), m);
  fs::remove_all(d);
}

TEST(Resume, CommittedEpochMissingASnapshotIsCorruption) {
  const fs::path d = scratch_dir("corrupt");
  Manifest m = sample_manifest();
  m.epoch = 2;
  m.partitions = 3;
  write_fake_snapshot(d, 2, 0, 3);
  write_fake_snapshot(d, 2, 2, 3);  // worker 1 missing
  atomic_write_file(manifest_path(d), format_manifest(m));
  EXPECT_THROW(prepare_resume(d), std::runtime_error);
  fs::remove_all(d);
}

// ---- coordinator -------------------------------------------------------------------------

TEST(Coordinator, LastArriverCommitsAndDeletesPreviousEpoch) {
  const fs::path d = scratch_dir("coord");
  Manifest t = sample_manifest();
  t.partitions = 3;
  CheckpointCoordinator c(d, t);
  for (uint64_t epoch = 1; epoch <= 3; ++epoch) {
    for (uint32_t w = 0; w < 3; ++w) {
      c.ensure_epoch_dir(epoch);
      atomic_write_file(snapshot_path(d, epoch, w), encode_snapshot(WorkerSnapshot{}));
      if (w < 2) {
        c.snapshot_written(epoch, 1000 * epoch, 10);
        EXPECT_EQ(c.stats().committed, epoch - 1) << "committed too early";
      }
    }
    c.snapshot_written(epoch, 1000 * epoch, 10);
    EXPECT_EQ(c.stats().committed, epoch);
    EXPECT_EQ(c.stats().last_epoch, epoch);
    EXPECT_EQ(c.stats().last_input_offset, 1000 * epoch);
    EXPECT_EQ(c.stats().last_snapshot_bytes, 30u);
    EXPECT_EQ(parse_manifest(slurp(manifest_path(d))).epoch, epoch);
    if (epoch > 1) EXPECT_FALSE(fs::exists(epoch_dir(d, epoch - 1)));
    EXPECT_TRUE(fs::exists(epoch_dir(d, epoch)));
  }
  EXPECT_FALSE(fs::exists(d / "manifest.tmp"));
  fs::remove_all(d);
}

// ---- dedup state -----------------------------------------------------------------------------

TEST(DedupState, ExportImportPreservesIdsHorizonAndExpiry) {
  // Watermark 1000s puts the horizon at 940s; every id below sits at or
  // above it (950..1049s) so all 500 are actually stored.
  Dedup a(60 * kS, kS);
  a.advance(1000 * kS);
  for (uint64_t id = 1; id <= 500; ++id) a.insert(id, (950 + static_cast<int64_t>(id % 100)) * kS);
  const DedupState st = a.export_state();
  EXPECT_EQ(st.forget_before, a.forget_before());
  size_t total = 0;
  for (const auto& b : st.buckets) total += b.size();
  EXPECT_EQ(total, a.ids());

  Dedup b(60 * kS, kS);
  b.import_state(st);
  EXPECT_EQ(b.ids(), a.ids());
  EXPECT_EQ(b.buckets(), a.buckets());
  EXPECT_EQ(b.forget_before(), a.forget_before());
  // Every remembered id is still a duplicate in the copy, and a new id is not.
  for (uint64_t id = 1; id <= 500; ++id) {
    EXPECT_EQ(b.insert(id, (950 + static_cast<int64_t>(id % 100)) * kS), Dedup::Verdict::kDuplicate);
  }
  EXPECT_EQ(b.insert(501, 1000 * kS), Dedup::Verdict::kNew);
  // Expiry behaves identically afterwards.
  a.insert(501, 1000 * kS);
  a.advance(1100 * kS);
  b.advance(1100 * kS);
  EXPECT_EQ(a.ids(), b.ids());
  EXPECT_EQ(a.buckets(), b.buckets());
  EXPECT_EQ(a.export_state().front_start, b.export_state().front_start);
}

TEST(DedupState, ImportOfFreshStateIsAFreshSet) {
  Dedup a(60 * kS, kS);
  a.advance(10 * kS);
  a.insert(1, 10 * kS);
  a.import_state(DedupState{});
  EXPECT_EQ(a.ids(), 0u);
  EXPECT_EQ(a.buckets(), 0u);
  EXPECT_FALSE(a.forget_before().has_value());
  EXPECT_EQ(a.insert(1, 10 * kS), Dedup::Verdict::kNew);
}

// ---- worker: truncate on resume ------------------------------------------------------------------

TEST(WorkerResume, TruncatesPartFilesAndReplaysToIdenticalBytes) {
  // A worker emits one row, checkpoints, emits two more rows and a late
  // event, finishes. A second worker built from the snapshot must cut the
  // part files back to the checkpoint and, after replaying the same
  // post-checkpoint events, end with byte-identical files. Row 2 also proves
  // the dedup state came back: event 4 is a duplicate of event 3 and must
  // not be counted after the resume either.
  const fs::path d = scratch_dir("worker_resume");
  Config cfg;
  cfg.window_us = 60 * kS;
  cfg.slide_us = 60 * kS;
  cfg.lateness_us = 60 * kS;
  cfg.dedup_bucket_us = kS;
  cfg.threads = 1;
  cfg.ckpt_dir = (d / "ckpt").string();
  const std::string out = (d / "out.csv.part0").string();
  const std::string late = (d / "late.ads.part0").string();

  const std::vector<Event> before = {make(1, 100 * kS, 1), make(2, 110 * kS, 1), make(3, 200 * kS, 1)};
  const std::vector<Event> after = {make(3, 200 * kS, 1), make(5, 320 * kS, 1), make(6, 100 * kS, 1)};

  Manifest t;
  t.partitions = 1;
  t.window_us = cfg.window_us;
  t.slide_us = cfg.slide_us;
  t.lateness_us = cfg.lateness_us;
  t.dedup_bucket_us = cfg.dedup_bucket_us;
  CheckpointCoordinator coord(cfg.ckpt_dir, t);

  uint64_t bytes_at_ckpt = 0;
  {
    Worker w(0, cfg, out, late, &coord);
    for (const Event& e : before) w.process(e);
    EXPECT_EQ(w.stats().windows_emitted, 1u);  // [60,120) closed by the 200s event
    w.checkpoint(1, 8 + 3 * 52);
    bytes_at_ckpt = w.out_bytes();
    EXPECT_GT(bytes_at_ckpt, 0u);
    EXPECT_EQ(fs::file_size(out), bytes_at_ckpt);  // flushed and on disk at the marker
    for (const Event& e : after) w.process(e);
    w.finish();
    EXPECT_EQ(w.stats().dups, 1u);
    EXPECT_EQ(w.stats().late, 1u);
    EXPECT_EQ(w.stats().windows_emitted, 3u);
  }
  const std::string fresh_out = slurp(out);
  const std::string fresh_late = slurp(late);
  EXPECT_GT(fresh_out.size(), bytes_at_ckpt);
  EXPECT_EQ(fresh_late.size(), kFileHeaderSize + kEventWireSize);

  // The epoch was committed (one partition, so the one snapshot completes it).
  const auto m = prepare_resume(cfg.ckpt_dir);
  ASSERT_TRUE(m.has_value());
  EXPECT_EQ(m->epoch, 1u);
  const WorkerSnapshot snap = read_snapshot_file(snapshot_path(cfg.ckpt_dir, 1, 0));
  EXPECT_EQ(snap.out_bytes, bytes_at_ckpt);
  EXPECT_EQ(snap.late_bytes, kFileHeaderSize);  // no late event before the marker
  EXPECT_EQ(snap.open_windows.size(), 1u);       // [180,240) with event 3
  EXPECT_EQ(snap.events, 3u);

  {
    Worker w(0, cfg, out, late, &coord, &snap);
    EXPECT_EQ(fs::file_size(out), bytes_at_ckpt);         // truncated
    EXPECT_EQ(fs::file_size(late), kFileHeaderSize);      // truncated
    EXPECT_EQ(w.out_bytes(), bytes_at_ckpt);
    EXPECT_EQ(w.stats().events, 3u);                       // counters are cumulative
    for (const Event& e : after) w.process(e);
    w.finish();
    EXPECT_EQ(w.stats().dups, 1u);
    EXPECT_EQ(w.stats().late, 1u);
    EXPECT_EQ(w.stats().windows_emitted, 3u);
  }
  EXPECT_EQ(slurp(out), fresh_out);
  EXPECT_EQ(slurp(late), fresh_late);
  fs::remove_all(d);
}

TEST(WorkerResume, PartFileShorterThanSnapshotIsAnError) {
  const fs::path d = scratch_dir("short_part");
  Config cfg;
  cfg.threads = 1;
  const std::string out = (d / "out.csv.part0").string();
  spit(out, "12 bytes....");
  WorkerSnapshot snap;
  snap.worker = 0;
  snap.partitions = 1;
  snap.out_bytes = 100;  // claims more than exists
  EXPECT_THROW(Worker(0, cfg, out, "", nullptr, &snap), std::runtime_error);
  snap.out_bytes = 5;
  { Worker w(0, cfg, out, "", nullptr, &snap); }
  EXPECT_EQ(fs::file_size(out), 5u);
  fs::remove_all(d);
}

// ---- pipeline: checkpoint, fail, resume -------------------------------------------------------------

namespace {

// A dirty stream: duplicates, late events (some beyond lateness so the late
// side file is exercised too), 1000 campaigns.
std::vector<Event> write_stream(const std::string& path, int n) {
  std::vector<Event> ev;
  EventWriter w(path);
  uint64_t id = 1;
  for (int i = 0; i < n; ++i) {
    const int64_t ingest = 1000 * kS + static_cast<int64_t>(i) * 10'000;  // 100 events/s
    Event e;
    if (i % 100 == 37) {
      e = ev[static_cast<size_t>(i - 1 - (i % 30))];
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

Config pipeline_config(const fs::path& d, const std::string& tag, int threads) {
  Config cfg;
  cfg.window_us = 60 * kS;
  cfg.slide_us = 60 * kS;
  cfg.lateness_us = 5 * kS;  // smaller than the stream's max delay, so late events exist
  cfg.dedup_bucket_us = kS;
  cfg.threads = threads;
  cfg.queue_depth = 64;
  cfg.in_path = (d / "in.ads").string();
  cfg.out_path = (d / (tag + ".csv")).string();
  cfg.late_out_path = (d / (tag + ".ads")).string();
  return cfg;
}

uint64_t sum_events(const PipelineResult& r) {
  uint64_t n = 0;
  for (const auto& w : r.workers) n += w.events;
  return n;
}

}  // namespace

TEST(PipelineCheckpoint, CheckpointsCommitAndDirectoryIsRemovedOnSuccess) {
  const fs::path d = scratch_dir("ckpt_ok");
  write_stream((d / "in.ads").string(), 20'000);
  Config plain = pipeline_config(d, "plain", 4);
  Config ck = pipeline_config(d, "ck", 4);
  ck.ckpt_dir = (d / "ckpt").string();
  ck.checkpoint_every_events = 1000;

  PipelineResult rp = run_pipeline(plain);
  ASSERT_TRUE(rp.error.empty()) << rp.error;
  PipelineResult rc = run_pipeline(ck);
  ASSERT_TRUE(rc.error.empty()) << rc.error;
  EXPECT_EQ(rc.checkpoints.committed, 20u);  // markers at 1000, 2000, ..., 20000
  EXPECT_EQ(rc.checkpoints.last_input_offset, kFileHeaderSize + 20'000 * kEventWireSize);
  EXPECT_FALSE(fs::exists(ck.ckpt_dir)) << "checkpoint dir must go once the output is committed";
  for (int i = 0; i < 4; ++i) EXPECT_FALSE(fs::exists(ck.out_path + ".part" + std::to_string(i)));
  EXPECT_FALSE(fs::exists(ck.out_path + ".tmp"));
  // Checkpointing does not change a single output byte.
  EXPECT_EQ(slurp(ck.out_path), slurp(plain.out_path));
  EXPECT_EQ(slurp(ck.late_out_path), slurp(plain.late_out_path));
  fs::remove_all(d);
}

TEST(PipelineCheckpoint, FailAfterCheckpointThenResumeIsByteIdentical) {
  for (int threads : {1, 4}) {
    const fs::path d = scratch_dir("ckpt_resume_" + std::to_string(threads));
    write_stream((d / "in.ads").string(), 20'000);
    Config plain = pipeline_config(d, "plain", threads);
    PipelineResult rp = run_pipeline(plain);
    ASSERT_TRUE(rp.error.empty()) << rp.error;

    // Run 1: dies right after the reader injected marker 7. The workers still
    // drain their queues (marker 7 included, so epoch 7 commits) and then
    // emit every open window, i.e. they write rows past the checkpoint.
    Config ck = pipeline_config(d, "ck", threads);
    ck.ckpt_dir = (d / "ckpt").string();
    ck.checkpoint_every_events = 1000;
    ck.fail_after_checkpoint = 7;
    PipelineResult r1 = run_pipeline(ck);
    EXPECT_NE(r1.error.find("fail_after_checkpoint"), std::string::npos) << r1.error;
    EXPECT_EQ(r1.checkpoints.committed, 7u);
    EXPECT_TRUE(fs::exists(manifest_path(ck.ckpt_dir)));
    EXPECT_FALSE(fs::exists(ck.out_path));  // nothing was merged
    for (int i = 0; i < threads; ++i) {
      const std::string part = ck.out_path + ".part" + std::to_string(i);
      ASSERT_TRUE(fs::exists(part));
      const WorkerSnapshot s = read_snapshot_file(snapshot_path(ck.ckpt_dir, 7, static_cast<uint32_t>(i)));
      EXPECT_GT(fs::file_size(part), s.out_bytes) << "finish() wrote rows past the checkpoint";
    }

    // Without --resume the stale directory is an error, and nothing is touched.
    Config no_resume = ck;
    no_resume.fail_after_checkpoint = 0;
    EXPECT_THROW(run_pipeline(no_resume), std::runtime_error);
    EXPECT_TRUE(fs::exists(manifest_path(ck.ckpt_dir)));

    // A different thread count cannot resume these snapshots.
    Config wrong_p = no_resume;
    wrong_p.resume = true;
    wrong_p.threads = threads + 1;
    EXPECT_THROW(run_pipeline(wrong_p), std::runtime_error);

    // Run 2: resume. Truncate, seek, replay, merge.
    Config resume = no_resume;
    resume.resume = true;
    PipelineResult r2 = run_pipeline(resume);
    ASSERT_TRUE(r2.error.empty()) << r2.error;
    EXPECT_TRUE(r2.resumed);
    EXPECT_EQ(r2.resumed_epoch, 7u);
    EXPECT_EQ(r2.resumed_offset, kFileHeaderSize + 7000 * kEventWireSize);
    EXPECT_EQ(r2.events, 13'000u);           // read in this run
    EXPECT_EQ(sum_events(r2), 20'000u);      // processed over both runs
    EXPECT_EQ(r2.checkpoints.committed, 13u);  // epochs 8..20
    EXPECT_FALSE(fs::exists(ck.ckpt_dir));
    EXPECT_EQ(slurp(ck.out_path), slurp(plain.out_path));
    EXPECT_EQ(slurp(ck.late_out_path), slurp(plain.late_out_path));
    fs::remove_all(d);
  }
}

TEST(PipelineCheckpoint, ResumeTwiceInARowStillMatches) {
  // Crash, resume, crash again before the next epoch commits, resume again:
  // the second resume starts from the same epoch and truncates again.
  const fs::path d = scratch_dir("ckpt_twice");
  write_stream((d / "in.ads").string(), 20'000);
  Config plain = pipeline_config(d, "plain", 3);
  ASSERT_TRUE(run_pipeline(plain).error.empty());

  Config ck = pipeline_config(d, "ck", 3);
  ck.ckpt_dir = (d / "ckpt").string();
  ck.checkpoint_every_events = 2500;
  ck.fail_after_checkpoint = 3;  // epoch 3 committed, dies
  EXPECT_FALSE(run_pipeline(ck).error.empty());

  Config again = ck;
  again.resume = true;
  again.fail_after_checkpoint = 1;  // resumes at 3, commits epoch 4, dies
  PipelineResult r = run_pipeline(again);
  EXPECT_FALSE(r.error.empty());
  EXPECT_EQ(r.resumed_epoch, 3u);
  EXPECT_EQ(parse_manifest(slurp(manifest_path(ck.ckpt_dir))).epoch, 4u);

  Config last = ck;
  last.resume = true;
  last.fail_after_checkpoint = 0;
  PipelineResult r3 = run_pipeline(last);
  ASSERT_TRUE(r3.error.empty()) << r3.error;
  EXPECT_EQ(r3.resumed_epoch, 4u);
  EXPECT_EQ(sum_events(r3), 20'000u);
  EXPECT_EQ(slurp(ck.out_path), slurp(plain.out_path));
  EXPECT_EQ(slurp(ck.late_out_path), slurp(plain.late_out_path));
  fs::remove_all(d);
}

TEST(PipelineCheckpoint, ResumeWithNothingCommittedStartsOver) {
  // Killed before the first epoch committed: --resume must behave like a
  // fresh start, and the partial epoch must be cleaned up.
  const fs::path d = scratch_dir("ckpt_nothing");
  write_stream((d / "in.ads").string(), 5'000);
  Config plain = pipeline_config(d, "plain", 2);
  ASSERT_TRUE(run_pipeline(plain).error.empty());

  Config ck = pipeline_config(d, "ck", 2);
  ck.ckpt_dir = (d / "ckpt").string();
  ck.checkpoint_every_events = 1000;
  ck.resume = true;
  write_fake_snapshot(ck.ckpt_dir, 1, 0, 2);  // worker 1 never got there
  spit(ck.out_path + ".part0", "stale rows from the dead run\n");
  PipelineResult r = run_pipeline(ck);
  ASSERT_TRUE(r.error.empty()) << r.error;
  EXPECT_FALSE(r.resumed);
  EXPECT_EQ(r.resumed_offset, kFileHeaderSize);
  EXPECT_EQ(slurp(ck.out_path), slurp(plain.out_path));
  fs::remove_all(d);
}

TEST(PipelineCheckpoint, PipeInputCannotBeCheckpointed) {
  // A FIFO is not seekable, so a resume could never rewind it. The error has
  // to come before any event is processed and say why.
  const fs::path d = scratch_dir("fifo");
  const std::string fifo = (d / "in.fifo").string();
  ASSERT_EQ(::mkfifo(fifo.c_str(), 0600), 0);
  // open(O_RDONLY) on a FIFO blocks until a writer shows up; feed it a header
  // from another thread and then close, which the reader sees as EOF.
  std::thread writer([&] {
    const int fd = ::open(fifo.c_str(), O_WRONLY);
    uint8_t hdr[kFileHeaderSize];
    std::memcpy(hdr, &kFileMagic, 4);
    std::memcpy(hdr + 4, &kFileVersion, 4);
    (void)!::write(fd, hdr, sizeof hdr);
    ::close(fd);
  });
  Config cfg = pipeline_config(d, "fifo", 2);
  cfg.in_path = fifo;
  cfg.ckpt_dir = (d / "ckpt").string();
  try {
    run_pipeline(cfg);
    ADD_FAILURE() << "expected a throw";
  } catch (const std::runtime_error& ex) {
    EXPECT_NE(std::string(ex.what()).find("seekable"), std::string::npos) << ex.what();
  }
  writer.join();
  fs::remove_all(d);
}

// ---- review fixes (2026-10-24): what a resume must refuse, and cleanup order ----------------

namespace {

// Run 1 of a resume scenario: checkpoint every 1000 events, die after the
// 5th marker, leave MANIFEST naming epoch 5.
Config died_after_epoch_5(const fs::path& d, int threads) {
  Config ck = pipeline_config(d, "ck", threads);
  ck.ckpt_dir = (d / "ckpt").string();
  ck.checkpoint_every_events = 1000;
  ck.fail_after_checkpoint = 5;
  EXPECT_NE(run_pipeline(ck).error.find("fail_after_checkpoint"), std::string::npos);
  EXPECT_EQ(parse_manifest(slurp(manifest_path(ck.ckpt_dir))).epoch, 5u);
  ck.fail_after_checkpoint = 0;
  ck.resume = true;
  return ck;
}

// Expects run_pipeline(cfg) to refuse with a message containing `what`,
// and the checkpoint to be left exactly as it was (still resumable).
void expect_refused(const Config& cfg, const std::string& what) {
  const std::string before = slurp(manifest_path(cfg.ckpt_dir));
  try {
    run_pipeline(cfg);
    ADD_FAILURE() << "expected a refusal mentioning: " << what;
  } catch (const std::runtime_error& ex) {
    EXPECT_NE(std::string(ex.what()).find(what), std::string::npos) << ex.what();
  }
  EXPECT_EQ(slurp(manifest_path(cfg.ckpt_dir)), before);
  EXPECT_TRUE(fs::exists(snapshot_path(cfg.ckpt_dir, 5, 0)));
}

}  // namespace

TEST(PipelineCheckpoint, ResumeAgainstADifferentInputIsRefused) {
  // Before the fix this resumed silently and produced output matching
  // neither input. Same size, same record count, different content.
  const fs::path d = scratch_dir("ckpt_other_input");
  write_stream((d / "in.ads").string(), 20'000);
  Config plain = pipeline_config(d, "plain", 3);
  ASSERT_TRUE(run_pipeline(plain).error.empty());
  Config ck = died_after_epoch_5(d, 3);

  // Same length, one record changed near the end (the tail block differs).
  const std::string other = (d / "other.ads").string();
  fs::copy_file(ck.in_path, other);
  {
    const int fd = ::open(other.c_str(), O_WRONLY);
    ASSERT_GE(fd, 0);
    const uint8_t junk[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    ASSERT_EQ(::pwrite(fd, junk, sizeof junk, static_cast<off_t>(fs::file_size(other) - 52)), 8);
    ::close(fd);
  }
  Config swapped = ck;
  swapped.in_path = other;
  expect_refused(swapped, "input file is not the one");

  // A grown input (appended to after the checkpoint) is a different input too.
  const std::string grown = (d / "grown.ads").string();
  fs::copy_file(ck.in_path, grown);
  {
    EventWriter w(grown, EventWriter::Mode::kAppend);
    w.write(make(999'999, 1000 * kS, 1));
    w.close();
  }
  Config longer = ck;
  longer.in_path = grown;
  expect_refused(longer, "input file is not the one");

  // The original input still resumes to the uninterrupted output.
  PipelineResult r = run_pipeline(ck);
  ASSERT_TRUE(r.error.empty()) << r.error;
  EXPECT_EQ(slurp(ck.out_path), slurp(plain.out_path));
  fs::remove_all(d);
}

TEST(PipelineCheckpoint, ResumeWithADifferentIntervalIsRefused) {
  // Epoch k means "the first k*N events"; a new N would number the next
  // epoch onto the committed epoch's directory (k = 5 at N = 1000 and at
  // N = 1100 for the marker at 5500) and overwrite its snapshots in place.
  const fs::path d = scratch_dir("ckpt_interval");
  write_stream((d / "in.ads").string(), 20'000);
  Config ck = died_after_epoch_5(d, 2);
  Config other = ck;
  other.checkpoint_every_events = 1100;
  expect_refused(other, "--checkpoint-every-events differs");
  fs::remove_all(d);
}

TEST(PipelineCheckpoint, ResumeThatAddsOrDropsLateOutIsRefused) {
  // Before the fix, dropping --late-out exited 0, never produced the late
  // file and left its parts behind; adding it failed on a missing part.
  const fs::path d = scratch_dir("ckpt_late_out");
  write_stream((d / "in.ads").string(), 20'000);
  Config ck = died_after_epoch_5(d, 2);
  Config dropped = ck;
  dropped.late_out_path.clear();
  expect_refused(dropped, "wrote --late-out and this one does not");
  fs::remove_all(d);

  const fs::path d2 = scratch_dir("ckpt_late_out_added");
  write_stream((d2 / "in.ads").string(), 20'000);
  Config start = pipeline_config(d2, "ck", 2);
  start.late_out_path.clear();
  start.ckpt_dir = (d2 / "ckpt").string();
  start.checkpoint_every_events = 1000;
  start.fail_after_checkpoint = 5;
  EXPECT_FALSE(run_pipeline(start).error.empty());
  Config added = start;
  added.fail_after_checkpoint = 0;
  added.resume = true;
  added.late_out_path = (d2 / "late.ads").string();
  expect_refused(added, "adds --late-out");
  fs::remove_all(d2);
}

TEST(PipelineCheckpoint, CrashDuringCleanupNeverLeavesAManifestWithoutItsEpoch) {
  // Cleanup after the output rename used to be one fs::remove_all, which on
  // APFS can delete epoch-k before MANIFEST. A crash in between left
  // "MANIFEST names epoch k, epoch-k missing", and --resume hard-failed
  // although the output was complete. remove_checkpoint_dir() now deletes
  // MANIFEST first, so the only intermediate state is "no MANIFEST, epoch
  // directory still there", which resumes by starting over.
  const fs::path d = scratch_dir("ckpt_cleanup");
  write_stream((d / "in.ads").string(), 20'000);
  Config plain = pipeline_config(d, "plain", 3);
  ASSERT_TRUE(run_pipeline(plain).error.empty());
  Config ck = died_after_epoch_5(d, 3);

  // The state the old order could produce is still (rightly) corruption.
  {
    const fs::path copy = d / "ckpt_copy";
    fs::copy(ck.ckpt_dir, copy, fs::copy_options::recursive);
    fs::remove_all(epoch_dir(copy, 5));
    EXPECT_THROW(prepare_resume(copy), std::runtime_error);
    fs::remove_all(copy);
  }

  // The state the new order produces: MANIFEST gone, epoch-5 still there,
  // part files from the dead run. Resume starts over and matches.
  fs::remove(manifest_path(ck.ckpt_dir));
  ASSERT_TRUE(fs::exists(epoch_dir(ck.ckpt_dir, 5)));
  PipelineResult r = run_pipeline(ck);
  ASSERT_TRUE(r.error.empty()) << r.error;
  EXPECT_FALSE(r.resumed);
  EXPECT_EQ(slurp(ck.out_path), slurp(plain.out_path));
  EXPECT_EQ(slurp(ck.late_out_path), slurp(plain.late_out_path));
  EXPECT_FALSE(fs::exists(ck.ckpt_dir));

  // remove_checkpoint_dir itself: everything goes, MANIFEST included.
  const fs::path c = d / "ckpt2";
  write_fake_snapshot(c, 3, 0, 1);
  Manifest m = sample_manifest();
  m.epoch = 3;
  m.partitions = 1;
  atomic_write_file(manifest_path(c), format_manifest(m));
  remove_checkpoint_dir(c, true);
  EXPECT_FALSE(fs::exists(c));
  remove_checkpoint_dir(c, true);  // a missing directory is not an error
  fs::remove_all(d);
}

TEST(InputFingerprint, CoversSizeHeadAndTail) {
  const fs::path d = scratch_dir("fingerprint");
  const fs::path p = d / "f.bin";
  auto fp = [&] {
    const int fd = ::open(p.c_str(), O_RDONLY);
    EXPECT_GE(fd, 0);
    const InputFingerprint f = fingerprint_input(fd);
    EXPECT_EQ(::lseek(fd, 0, SEEK_CUR), 0) << "pread must not move the offset";
    ::close(fd);
    return f;
  };
  spit(p, std::string(3 << 20, 'a'));  // 3 MiB: head and tail blocks do not overlap
  const InputFingerprint a = fp();
  EXPECT_EQ(a.size, 3u << 20);
  std::string s(3 << 20, 'a');
  s[(3 << 20) - 1] = 'b';
  spit(p, s);
  const InputFingerprint tail_changed = fp();
  EXPECT_EQ(tail_changed.head_fnv, a.head_fnv);
  EXPECT_NE(tail_changed.tail_fnv, a.tail_fnv);
  s[(3 << 20) - 1] = 'a';
  s[0] = 'b';
  spit(p, s);
  EXPECT_NE(fp().head_fnv, a.head_fnv);
  spit(p, "");  // empty file: both hashes are of zero bytes
  EXPECT_EQ(fp().size, 0u);
  fs::remove_all(d);
}

TEST(Snapshot, ExpiredCounterSurvivesResume) {
  WorkerSnapshot s = sample_snapshot();
  s.expired = 1234;
  EXPECT_EQ(decode_snapshot(encode_snapshot(s)).expired, 1234u);
  // A version-1 snapshot (no expired field) is refused, not misread.
  std::vector<uint8_t> v1 = encode_snapshot(s);
  const uint32_t one = 1;
  std::memcpy(v1.data() + 4, &one, 4);
  const uint64_t sum = fnv1a64(v1.data(), v1.size() - 8);
  std::memcpy(v1.data() + v1.size() - 8, &sum, 8);
  EXPECT_THROW(decode_snapshot(v1), std::runtime_error);
}
