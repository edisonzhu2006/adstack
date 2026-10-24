// streamkit: streaming aggregation engine for ad events.
//
// MILESTONE 5 (this file): flags and the stderr summary. The engine itself is
// in pipeline.h: one reader thread hash-partitions events by campaign_id
// into P bounded SPSC queues; P worker threads each own a dedup set, an
// open-window map, a watermark and an output part file; the parts are merged
// at the end. Windows are sliding (--slide-s), tumbling by default. With
// --ckpt-dir the reader injects a checkpoint marker every N events, each
// worker snapshots atomically, and --resume continues after a crash with
// byte-identical output (checkpoint.h). Matches the oracle on the
// generator's default dirty stream at any thread count, with or without
// SIGKILLs in between (bench/crash.sh). --latency-stats (m5) measures the
// engine's own emit delay per window (latency.h); bench/throughput.sh is the
// benchmark driver and streamkit/BENCHMARKS.md the write-up.
//
//   streamkit --in events.ads --out results.csv [--window-s 60] [--slide-s WINDOW]
//             [--lateness-s 0] [--late-out late.ads] [--threads P] [--queue-depth 4096]
//             [--batch 128] [--dedup-bucket-s 1] [--no-dedup] [--reader-decode]
//             [--ckpt-dir DIR] [--checkpoint-every-events 1000000] [--resume]
//             [--throttle-eps N] [--no-fsync] [--latency-stats] [--fail-after-checkpoint K]
//   gen --n 1000000 --pace --out - | streamkit --in - --out live.csv --latency-stats

#include <algorithm>
#include <cstdio>
#include <exception>
#include <string>
#include <thread>
#include <vector>

#include "common/agg.h"
#include "common/args.h"
#include "pipeline.h"

using namespace adstack;

namespace {

void usage() {
  std::fprintf(stderr,
    "streamkit --in FILE --out FILE [--window-s 60] [--slide-s WINDOW] [--lateness-s 0]\n"
    "          [--late-out FILE] [--threads P] [--queue-depth 4096] [--batch 128]\n"
    "          [--dedup-bucket-s 1] [--no-dedup] [--reader-decode]\n"
    "          [--ckpt-dir DIR] [--checkpoint-every-events 1000000] [--resume] [--throttle-eps N]\n"
    "          [--no-fsync] [--latency-stats] [--fail-after-checkpoint K]\n"
    "  --slide-s        window hop; < --window-s gives overlapping (sliding) windows,\n"
    "                   default = window (tumbling)\n"
    "  --lateness-s     how long after a window's end we keep accepting events for it;\n"
    "                   watermark = max(event_time) - lateness, per worker. 0 = close immediately.\n"
    "  --late-out       where events older than the watermark are written (never dropped silently)\n"
    "  --threads        worker threads; events are hash-partitioned by campaign_id\n"
    "                   (default: hardware threads, at most 8)\n"
    "  --queue-depth    batches buffered per worker before the reader waits (backpressure)\n"
    "  --batch          events per batch handed from the reader to a worker\n"
    "  --dedup-bucket-s width of the event-time buckets the dedup set expires by (integer seconds)\n"
    "  --no-dedup       count duplicates twice (for measuring dedup overhead only)\n"
    "  --reader-decode  decode records in the reader instead of the workers (for measurement only)\n"
    "  --ckpt-dir       write a checkpoint (per-worker snapshots + MANIFEST) here every\n"
    "                   --checkpoint-every-events events; input must be a seekable file\n"
    "  --resume         continue from --ckpt-dir's MANIFEST if there is one (otherwise start over);\n"
    "                   without it a checkpoint directory with state in it is an error\n"
    "  --throttle-eps   cap the reader at N events/s (for crash tests; 0 = unlimited)\n"
    "  --no-fsync       skip fsync in the checkpoint path (for measuring its cost only; a\n"
    "                   checkpoint then survives SIGKILL but not power loss)\n"
    "  --latency-stats  print min/p50/p99/max emit latency at exit: per window closed by the\n"
    "                   watermark, wall time from the closing event's block being read to\n"
    "                   the row being written to stdout (--out -) or appended to the part\n"
    "                   file's buffer (--out FILE, visible only at end of input; zero cost when off)\n"
    "  --fail-after-checkpoint K\n"
    "                   TEST HOOK: the reader aborts the run (exit 1) right after injecting\n"
    "                   checkpoint marker K, leaving a checkpoint to --resume from. 0 = off\n"
    "  --out -          rows are written to stdout as windows close; with --out FILE they\n"
    "                   appear only when the run commits at end of input\n"
    "Unknown flags and malformed numbers are errors (exit 2).\n");
}

int default_threads() {
  const unsigned hw = std::thread::hardware_concurrency();  // 0 when unknown
  return static_cast<int>(std::clamp(hw, 1u, 8u));
}

}  // namespace

// Durations are given in whole seconds and kept in microseconds; the bound
// keeps both the multiplication and all later window arithmetic inside
// int64 (common/agg.h, kMaxDurationUs).
constexpr int64_t kMaxSeconds = kMaxDurationUs / 1'000'000;

int main(int argc, char** argv) {
  Args args(argc, argv);
  if (args.has("--help")) {
    usage();
    return 0;
  }
  const std::vector<std::string> bad = args.unknown(
      {"--in", "--out", "--late-out", "--window-s", "--slide-s", "--lateness-s", "--dedup-bucket-s",
       "--no-dedup", "--threads", "--queue-depth", "--batch", "--reader-decode", "--ckpt-dir",
       "--checkpoint-every-events", "--resume", "--throttle-eps", "--fail-after-checkpoint", "--no-fsync",
       "--latency-stats"});
  if (!bad.empty()) {
    std::fprintf(stderr, "streamkit: unknown argument '%s' (see --help)\n", bad.front().c_str());
    return 2;
  }
  if (!args.has("--in") || !args.has("--out")) {
    usage();
    return 2;
  }

  Config cfg;
  int64_t window_s = 0, slide_s = 0, lateness_s = 0, bucket_s = 0, threads = 0, depth = 0, batch = 0,
          every = 0, fail_after = 0;
  try {
    window_s = args.i64("--window-s", 60);
    slide_s = args.i64("--slide-s", window_s);
    lateness_s = args.i64("--lateness-s", 0);
    bucket_s = args.i64("--dedup-bucket-s", 1);
    threads = args.i64("--threads", default_threads());
    depth = args.i64("--queue-depth", 4096);
    batch = args.i64("--batch", 128);
    every = args.i64("--checkpoint-every-events", 1'000'000);
    fail_after = args.i64("--fail-after-checkpoint", 0);
    cfg.throttle_eps = args.dbl("--throttle-eps", 0);
  } catch (const std::exception& ex) {
    std::fprintf(stderr, "streamkit: %s\n", ex.what());
    return 2;
  }
  if (window_s < 1 || bucket_s < 1 || lateness_s < 0 || window_s > kMaxSeconds || bucket_s > kMaxSeconds ||
      lateness_s > kMaxSeconds) {
    std::fprintf(stderr, "streamkit: --window-s and --dedup-bucket-s must be in [1, %lld], --lateness-s in [0, %lld]\n",
                 static_cast<long long>(kMaxSeconds), static_cast<long long>(kMaxSeconds));
    return 2;
  }
  if (slide_s < 1 || slide_s > window_s) {
    std::fprintf(stderr, "streamkit: need 1 <= --slide-s <= --window-s\n");
    return 2;
  }
  if (threads < 1 || threads > 1024 || depth < 2 || batch < 1) {
    std::fprintf(stderr, "streamkit: --threads in [1, 1024], --queue-depth >= 2, --batch >= 1\n");
    return 2;
  }
  if (every < 1 || fail_after < 0 || cfg.throttle_eps < 0) {
    std::fprintf(stderr, "streamkit: --checkpoint-every-events >= 1, --fail-after-checkpoint >= 0, --throttle-eps >= 0\n");
    return 2;
  }
  cfg.in_path = args.str("--in", "");
  cfg.out_path = args.str("--out", "-");
  cfg.late_out_path = args.str("--late-out", "");
  cfg.window_us = window_s * 1'000'000;
  cfg.slide_us = slide_s * 1'000'000;
  cfg.lateness_us = lateness_s * 1'000'000;
  cfg.dedup_bucket_us = bucket_s * 1'000'000;
  cfg.dedup = !args.has("--no-dedup");
  cfg.threads = static_cast<int>(threads);
  cfg.queue_depth = static_cast<size_t>(depth);
  cfg.batch_events = static_cast<size_t>(batch);
  cfg.reader_decode = args.has("--reader-decode");
  cfg.ckpt_dir = args.str("--ckpt-dir", "");
  cfg.checkpoint_every_events = static_cast<uint64_t>(every);
  cfg.resume = args.has("--resume");
  cfg.fail_after_checkpoint = static_cast<uint64_t>(fail_after);
  cfg.fsync = !args.has("--no-fsync");
  cfg.latency_stats = args.has("--latency-stats");
  if (cfg.resume && cfg.ckpt_dir.empty()) {
    std::fprintf(stderr, "streamkit: --resume needs --ckpt-dir\n");
    return 2;
  }

  PipelineResult r;
  try {
    r = run_pipeline(cfg);
  } catch (const std::exception& ex) {
    std::fprintf(stderr, "streamkit: %s\n", ex.what());
    return 1;
  }
  if (!r.error.empty()) {
    std::fprintf(stderr, "streamkit: %s\n", r.error.c_str());
    return 1;
  }

  // Totals across workers, plus the numbers that explain scaling: how evenly
  // the hash spread the events, how often the reader had to wait for a full
  // queue (workers are the bottleneck) and how often workers found their
  // queue empty (the reader is).
  WorkerStats total;
  uint64_t min_events = UINT64_MAX, max_events = 0;
  double min_busy = 1e300, max_busy = 0;
  for (const WorkerStats& w : r.workers) {
    min_busy = std::min(min_busy, w.busy_secs);
    max_busy = std::max(max_busy, w.busy_secs);
    total.events += w.events;
    total.dups += w.dups;
    total.late += w.late;
    total.expired += w.expired;
    total.windows_emitted += w.windows_emitted;
    total.batches += w.batches;
    total.idle_waits += w.idle_waits;
    total.dedup_peak_ids += w.dedup_peak_ids;
    total.open_windows_peak += w.open_windows_peak;
    min_events = std::min(min_events, w.events);
    max_events = std::max(max_events, w.events);
  }
  // events/s is over the events this run read (r.events), not the workers'
  // cumulative counts, which a resumed run inherits from its snapshots.
  const double eps = r.total_secs > 0 ? static_cast<double>(r.events) / r.total_secs : 0.0;
  std::fprintf(stderr,
               "streamkit: %llu events, %llu dups, %llu late (%llu past the dedup horizon), %llu windows, "
               "%.2fs, %.0f events/s, %d threads\n",
               static_cast<unsigned long long>(total.events),
               static_cast<unsigned long long>(total.dups),
               static_cast<unsigned long long>(total.late),
               static_cast<unsigned long long>(total.expired),
               static_cast<unsigned long long>(total.windows_emitted), r.total_secs, eps,
               cfg.threads);
  std::fprintf(stderr,
               "streamkit: per-worker events min %llu max %llu, busy %.2fs..%.2fs, %llu batches, "
               "reader stalls %llu, worker idle waits %llu, merge %.3fs\n",
               static_cast<unsigned long long>(min_events),
               static_cast<unsigned long long>(max_events), min_busy, max_busy,
               static_cast<unsigned long long>(total.batches),
               static_cast<unsigned long long>(r.reader_stalls),
               static_cast<unsigned long long>(total.idle_waits), r.total_secs - r.read_secs);
  std::fprintf(stderr,
               "streamkit: window %llds slide %llds lateness %llds, dedup %s (peak ids %zu, summed "
               "over workers), peak open windows %zu (summed)\n",
               static_cast<long long>(cfg.window_us / 1'000'000),
               static_cast<long long>(cfg.slide_us / 1'000'000),
               static_cast<long long>(cfg.lateness_us / 1'000'000), cfg.dedup ? "on" : "off",
               total.dedup_peak_ids, total.open_windows_peak);
  if (!cfg.ckpt_dir.empty()) {
    // Checkpoint cost, so NOTES.md can quote it: how many epochs committed,
    // how big the last one was (all workers' snapshots added up) and how long
    // the slowest single snapshot took, fsyncs included.
    uint64_t snap_bytes = 0, snapshots = 0;
    size_t snap_windows = 0;
    double snap_max = 0, snap_total = 0;
    for (const WorkerStats& w : r.workers) {
      snap_bytes += w.last_snapshot_bytes;
      snap_windows += w.last_snapshot_windows;
      snapshots += w.snapshots;
      snap_total += w.snapshot_secs_total;
      snap_max = std::max(snap_max, w.snapshot_secs_max);
    }
    std::fprintf(stderr,
                 "streamkit: checkpoints: %s epoch %llu offset %llu, %llu epochs committed (last epoch %llu at offset "
                 "%llu), %llu snapshots, last snapshot %llu bytes / %zu open windows (summed), slowest "
                 "snapshot %.4fs, snapshot time total %.3fs, manifest time %.4fs\n",
                 r.resumed ? "resumed from" : "started fresh at",
                 static_cast<unsigned long long>(r.resumed_epoch),
                 static_cast<unsigned long long>(r.resumed_offset),
                 static_cast<unsigned long long>(r.checkpoints.committed),
                 static_cast<unsigned long long>(r.checkpoints.last_epoch),
                 static_cast<unsigned long long>(r.checkpoints.last_input_offset),
                 static_cast<unsigned long long>(snapshots),
                 static_cast<unsigned long long>(snap_bytes), snap_windows, snap_max, snap_total,
                 r.checkpoints.manifest_secs);
  }
  if (cfg.latency_stats) {
    // Emit latency (m5). The precise definition is in latency.h and
    // NOTES.md; in one line: per window the watermark closed, wall time from
    // read(2) returning the block holding the event that closed it to that
    // window's row being appended to the output stream. Windows closed by
    // end of input are not counted; the configured lateness is not included.
    const LatencySummary lat = summarize_latency(r.emit_latency_us);
    if (lat.count == 0) {
      std::fprintf(stderr, "streamkit: emit latency: no windows were closed by the watermark\n");
    } else {
      // input reads says how finely the arrival stamps were taken: on a pipe
      // fed by gen --pace it is the producer's write granularity.
      const uint64_t reads = r.input_reads > 0 ? r.input_reads : 1;
      // The end point differs by output mode, and the label says which: with
      // --out - the row is in stdout (visible to the consumer); with a file
      // it is in the part file's buffer and nobody sees it before the run
      // commits at end of input.
      const char* end_point = cfg.out_path == "-" ? "block read -> row written to stdout"
                                                  : "block read -> row appended to part buffer, not yet visible";
      std::fprintf(stderr,
                   "streamkit: emit latency over %zu watermark-closed windows: min %u us, p50 %u us, "
                   "p99 %u us, max %u us (%s; excludes lateness); "
                   "input reads %llu (mean %llu events/read)\n",
                   lat.count, lat.min_us, lat.p50_us, lat.p99_us, lat.max_us, end_point,
                   static_cast<unsigned long long>(r.input_reads),
                   static_cast<unsigned long long>(r.events / reads));
    }
  }
  return 0;
}
