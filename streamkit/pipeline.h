#pragma once
// streamkit pipeline (milestone 3): one reader thread, P hash-partitioned
// worker threads, bounded SPSC queues between them.
//
// WHY THIS SHAPE. m2 was one thread doing everything at ~5M events/s. The
// expensive work per event is the dedup hash insert and the open-window map
// update, both of which touch state keyed by the event. If two threads shared
// that state they would need a lock per event (or a concurrent map), which is
// slower than one thread. Instead every campaign_id is assigned to exactly
// one worker by a hash, so each worker owns its keys outright:
//
//   reader ---> queue[0] ---> worker 0 (dedup set, open windows, watermark, out.part0)
//          \--> queue[1] ---> worker 1 (dedup set, open windows, watermark, out.part1)
//           \-> ...
//
// No mutable state of ours is shared between threads on the hot path: the
// only cross-thread traffic we create is the queue itself (spsc_queue.h),
// and the reader publishes batches, not single events, so even that is one
// cache-line handoff per Config::batch_events events. The process-wide
// allocator and libc are still shared: every new dedup id and every new
// window allocates a node, every closed window frees one and formats a row
// through fprintf. NOTES.md (m5) measures a per-row cost that grows with
// the worker count and attributes it to that shared machinery.
//
// The exception is --out - (stdout): workers then write each batch's rows
// straight to stdout under one mutex so a consumer sees a row as soon as
// its window closes (StreamSink below), one lock per batch that emitted
// rows, not per event.
//
// Partitioning by campaign_id is also what keeps dedup exact with no shared
// set: a duplicate is a byte-for-byte copy of its original, so it carries the
// same campaign_id and lands on the same worker, which remembers the id.
//
// EACH WORKER HAS ITS OWN WATERMARK. A worker closes windows based only on
// the event times it has seen. If one partition's traffic stalls, only that
// worker's windows stay open; the others keep emitting. The cost is that the
// output is interleaved across partitions (compare.sh sorts, so correctness
// is unaffected). See NOTES.md for the comparison with a global watermark.
//
// EACH WORKER HAS ITS OWN OUTPUT FILE (<out>.part<i>) when --out is a file.
// Rows then become visible only at the end of the run, when the parts are
// merged and renamed into place (that rename is the run's commit, which is
// what makes checkpoint/resume exactly-once). Appending to one
// shared file would need a lock per row. More importantly, m4's checkpoint
// records "this worker's output is N bytes long" and truncates back to N on
// resume; that is only well defined when the worker alone writes the file.
// The main thread concatenates the parts, in partition order, after the
// workers finish.
//
// CHECKPOINTS (m4) ride the same queues. Every --checkpoint-every-events
// events the reader flushes its partial batches and publishes a marker
// batch (epoch k, input offset X) to every worker. A worker that dequeues
// the marker has seen exactly its share of the events before X, so it
// snapshots its state (checkpoint.h) and records how long its part file is.
// --resume reloads the snapshots, truncates the part files back to those
// lengths and rewinds the input to X. See checkpoint.h for the protocol.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "checkpoint.h"
#include "common/agg.h"
#include "common/event.h"
#include "dedup.h"
#include "latency.h"
#include "spsc_queue.h"

namespace adstack {

struct Config {
  std::string in_path;         // ".ads" file or "-" for stdin
  std::string out_path;        // CSV, or "-" for stdout
  std::string late_out_path;   // ".ads" side file; empty = count only
  int64_t window_us = 60'000'000;
  int64_t slide_us = 60'000'000;   // == window_us means tumbling
  int64_t lateness_us = 0;
  int64_t dedup_bucket_us = 1'000'000;
  bool dedup = true;
  int threads = 1;
  size_t queue_depth = 4096;       // slots (batches) per worker queue
  size_t batch_events = 128;       // events per batch
  // Measurement knob only: decode in the reader instead of the workers, to
  // show where the decode belongs (NOTES.md, "where to decode").
  bool reader_decode = false;

  // ---- checkpointing (m4) ----
  std::string ckpt_dir;                        // empty = no checkpoints
  uint64_t checkpoint_every_events = 1'000'000;
  bool resume = false;                         // continue from ckpt_dir's MANIFEST if present
  // Reader pacing in events/s (0 = unlimited). Exists so a test run lasts
  // long enough to be killed mid-flight (bench/crash.sh).
  double throttle_eps = 0;
  // Test hook: the reader throws right after injecting marker number k, so
  // an in-process test can exercise resume without SIGKILL. 0 = off.
  uint64_t fail_after_checkpoint = 0;
  // Measurement knob only (--no-fsync): skip every fsync in the checkpoint
  // path to see what durability costs. Still correct against SIGKILL (the
  // kernel has the bytes), not against power loss.
  bool fsync = true;

  // ---- emit latency (m5) ----
  // --latency-stats: stamp every input block with steady_clock on arrival
  // and, for every window the watermark closes, record how long after that
  // stamp its row was appended (latency.h has the exact definition). Off by
  // default: when false no clock is read and nothing is stored.
  bool latency_stats = false;
};

// --out - only: the shared destination workers write their rows to as
// windows close. One lock per batch that emitted rows; each write is
// followed by fflush, so the bytes are in the pipe (or file) behind stdout
// when the lock is released. No checkpoints in this mode (plan_start
// refuses --ckpt-dir with --out -), so nothing ever needs truncating.
struct StreamSink {
  std::mutex mu;
  FILE* f = nullptr;
};

// Which worker owns a campaign. Deterministic across runs and machines (m4's
// resume depends on a campaign landing on the same part file after a
// restart). Not plain `campaign_id % partitions`: the mixing step makes the
// split depend on every bit of the id, so structured ids (all multiples of
// 8, say) still spread evenly.
uint32_t partition_of(uint32_t campaign_id, uint32_t partitions);

// One unit of reader -> worker handoff. Exactly one of raw/events is used:
// raw holds n undecoded wire records (the default), events holds decoded
// records (--reader-decode). Batches live inside the queue slots and are
// reused: raw is sized once to the batch capacity and then only memcpy'd
// into (vector::insert of 52 bytes at a time measured slower than the decode
// it was meant to avoid), and clear() keeps every buffer's capacity.
struct Batch {
  std::vector<uint8_t> raw;
  std::vector<Event> events;
  size_t n = 0;       // records in raw
  bool eof = false;   // the reader's last message to a worker: drain and finish
  // A checkpoint marker (m4): carries no events. The worker snapshots when it
  // dequeues it, because at that point it has processed exactly the events
  // the reader handed out before `input_offset`.
  bool marker = false;
  uint64_t epoch = 0;
  uint64_t input_offset = 0;
  // --latency-stats (m5): steady_clock reading, in ns, of when read(2)
  // returned the input block holding this batch's first record. 0 when the
  // flag is off. A batch can straddle two blocks, in which case the stamp is
  // the earlier one, so a measured latency is never understated.
  int64_t arrival_ns = 0;

  size_t size() const { return events.empty() ? n : events.size(); }
  void clear() {
    n = 0; events.clear(); eof = false; marker = false; epoch = 0; input_offset = 0; arrival_ns = 0;
  }

  void append_raw(const uint8_t* rec, size_t capacity_records) {
    if (raw.size() < capacity_records * kEventWireSize) raw.resize(capacity_records * kEventWireSize);
    std::memcpy(raw.data() + n * kEventWireSize, rec, kEventWireSize);
    ++n;
  }
};

struct WorkerStats {
  uint64_t events = 0;
  uint64_t dups = 0;
  uint64_t late = 0;
  // Of `late`, the events that were also past the dedup horizon (Dedup's
  // kExpired). The engine no longer remembers their ids, so it cannot tell
  // a genuinely late first copy from a re-delivery of an event it already
  // counted (or already wrote to the late file). Aggregates are unaffected
  // (dedup.h has the proof); the late side file is not deduplicated for
  // these, so a consumer that re-adds late events must dedup them by
  // event_id against the output first. Zero whenever the dedup horizon
  // (window + lateness) covers the stream's re-delivery delay.
  uint64_t expired = 0;
  uint64_t windows_emitted = 0;
  uint64_t batches = 0;
  uint64_t idle_waits = 0;   // times the worker found its queue empty
  double busy_secs = 0;      // time spent inside process_batch (not waiting)
  size_t dedup_peak_ids = 0;
  size_t open_windows_peak = 0;
  // m4: checkpoint cost as seen by this worker.
  uint64_t snapshots = 0;
  uint64_t last_snapshot_bytes = 0;
  size_t last_snapshot_windows = 0;   // open windows in the last snapshot
  double snapshot_secs_max = 0;       // slowest single snapshot (fsync included)
  double snapshot_secs_total = 0;
};

// The m2 engine, owned by one thread, over one partition. process() is the
// per-event logic; run() is the thread body that feeds it from the queue.
class Worker {
 public:
  // coord: where to report snapshots (nullptr = checkpoints off).
  // resume: state to start from (nullptr = fresh). With a resume snapshot the
  // part files are truncated to the recorded lengths and continued; without
  // one they are created empty.
  // sink: --out - streaming destination (nullptr = write out_part_path).
  Worker(int id, const Config& cfg, std::string out_part_path, std::string late_part_path,
         CheckpointCoordinator* coord = nullptr, const WorkerSnapshot* resume = nullptr,
         StreamSink* sink = nullptr);
  ~Worker();
  Worker(const Worker&) = delete;
  Worker& operator=(const Worker&) = delete;

  // One event through dedup, late check, window assignment and window closing.
  void process(const Event& e);
  // End of stream: every open window is final. Flushes and closes the files.
  void finish();
  // Thread body: pop batches until the eof batch, then finish(). Any
  // exception is captured in error() instead of terminating the process; the
  // worker keeps draining so the reader never blocks on a dead queue.
  void run();
  // Checkpoint: make the part files durable, write this worker's snapshot
  // for `epoch` atomically, tell the coordinator. Public so tests can drive
  // it without threads.
  void checkpoint(uint64_t epoch, uint64_t input_offset);
  // The state a checkpoint of this worker would record right now.
  WorkerSnapshot snapshot(uint64_t epoch, uint64_t input_offset) const;

  SpscQueue<Batch>& queue() { return queue_; }
  const WorkerStats& stats() const { return stats_; }
  // --latency-stats: one sample (microseconds) per window this worker's
  // watermark closed, in emission order. Empty when the flag is off.
  const std::vector<uint32_t>& emit_latencies_us() const { return emit_latency_us_; }
  const std::string& error() const { return error_; }
  int64_t watermark() const { return watermark_; }
  int id() const { return id_; }
  uint64_t out_bytes() const { return out_bytes_; }

 private:
  void advance_watermark(int64_t event_time_us);
  // record_latency: true when called because the watermark moved (process),
  // false at end of input (finish), whose closings are not emit latency.
  void close_ready(bool record_latency);
  void process_batch(const Batch& b);
  // --out -: write the rows collected since the last call to the sink.
  void flush_stream();
  void open_outputs_fresh();
  void open_outputs_resumed(const WorkerSnapshot& s);
  void restore(const WorkerSnapshot& s);

  const int id_;
  const Config cfg_;
  SpscQueue<Batch> queue_;
  Dedup dedup_;
  // Ordered by (window_start, campaign) so closing is "pop from the front
  // while window_end <= watermark" (see root NOTES.md).
  std::map<WindowKey, Agg> open_;
  int64_t max_event_time_;
  int64_t watermark_;
  const std::string out_path_;
  const std::string late_path_;
  FILE* out_ = nullptr;
  // Bytes this worker has written to out_, counted from fprintf's return
  // values rather than asked of the OS: it is exact, it is what the snapshot
  // records, and on resume it starts at the recorded length.
  uint64_t out_bytes_ = 0;
  std::unique_ptr<EventWriter> late_;
  CheckpointCoordinator* coord_ = nullptr;
  // --out -: rows of the current batch, and how many of them close_ready
  // sampled for --latency-stats (their samples are taken after the write).
  StreamSink* sink_ = nullptr;
  std::string pending_rows_;
  size_t pending_samples_ = 0;
  WorkerStats stats_;
  std::string error_;
  // --latency-stats: arrival stamp of the batch being processed (0 = none)
  // and the samples collected so far.
  int64_t batch_arrival_ns_ = 0;
  std::vector<uint32_t> emit_latency_us_;
};

struct PipelineResult {
  // Records the reader handed out in this run. On a resumed run this is
  // less than the workers' cumulative `events` (restored from the
  // snapshots), and it is what events/s is computed from.
  uint64_t events = 0;
  uint64_t reader_stalls = 0;    // times the reader found a queue full (backpressure)
  // read(2) calls that returned data. On a file that is about one per MiB;
  // on a pipe it is how often the producer's bytes arrived, which is the
  // granularity of the --latency-stats arrival stamps (m5), so the summary
  // prints it next to the percentiles. Counted always; it is one increment
  // per block.
  uint64_t input_reads = 0;
  double read_secs = 0;          // reader start to last worker joined
  double total_secs = 0;         // including concatenation of the parts
  std::vector<WorkerStats> workers;
  std::vector<int64_t> watermarks;
  std::string error;             // first worker error, empty on success
  // m4
  bool resumed = false;
  uint64_t resumed_epoch = 0;
  uint64_t resumed_offset = 0;   // input byte offset the run started from
  CheckpointStats checkpoints;
  // m5: every worker's emit-latency samples, concatenated (unsorted). Empty
  // unless Config::latency_stats; main.cpp summarizes them.
  std::vector<uint32_t> emit_latency_us;
};

// Runs the whole thing on the calling thread (which becomes the reader) plus
// cfg.threads worker threads, then merges the per-worker files into
// cfg.out_path (and cfg.late_out_path). Throws std::runtime_error on I/O
// setup failures.
PipelineResult run_pipeline(const Config& cfg);

}  // namespace adstack
