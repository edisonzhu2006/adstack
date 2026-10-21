// Implementation of the streamkit pipeline: the reader loop, the worker
// engine, checkpoints and resume, and the final merge of per-worker files.
// See pipeline.h for the design and checkpoint.h for the checkpoint
// protocol; this file is mechanics plus the few decisions that only make
// sense next to the code.

#include "pipeline.h"

#include <cerrno>
#include <chrono>
#include <climits>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <thread>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace adstack {

namespace {

namespace fs = std::filesystem;

// ---- waiting --------------------------------------------------------------
//
// Both the reader (queue full) and a worker (queue empty) wait the same way:
// re-check a few times, then give the core away with yield(), and if the
// other side still has not moved, sleep briefly. Spinning first keeps the
// steady-state handoff latency at nanoseconds; sleeping eventually keeps an
// idle worker off the CPU on a slow live stream (a paced 10K events/s input
// would otherwise burn eight cores doing nothing). The 50us cap bounds the
// latency the sleep can add. Nothing is ever dropped: the caller loops until
// its operation succeeds.
constexpr int kSpinRounds = 64;
constexpr int kYieldRounds = 1024;

// steady_clock as a plain integer (ns), for the --latency-stats stamps. One
// call per input block in the reader and one per emitted row in a worker,
// and only when the flag is on.
int64_t now_ns() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

void backoff(int& round) {
  if (round < kSpinRounds) {
    // Tight re-check; the caller's try_* call is the spin.
  } else if (round < kYieldRounds) {
    std::this_thread::yield();
  } else {
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }
  ++round;
}

// ---- input -------------------------------------------------------------------
//
// Reads the input in large blocks of whole records. Uses read(2) rather than
// fread: fread blocks until the *entire* request is satisfied, which on a
// live pipe (gen --pace, the Wikimedia adapter) would hold events back until
// a megabyte had accumulated. read() returns whatever is available, so on a
// file we get full blocks and on a pipe we get what is there right now.
class BlockReader {
 public:
  explicit BlockReader(const std::string& path) {
    if (path == "-") {
      fd_ = STDIN_FILENO;
      owns_ = false;
    } else {
      fd_ = ::open(path.c_str(), O_RDONLY);
      if (fd_ < 0) throw std::runtime_error("cannot open for read: " + path);
    }
    buf_.resize(kBlockBytes);
    read_header(path);
  }

  ~BlockReader() {
    if (owns_ && fd_ >= 0) ::close(fd_);
  }
  BlockReader(const BlockReader&) = delete;
  BlockReader& operator=(const BlockReader&) = delete;

  // A pipe or terminal cannot be rewound, so a checkpoint's "input offset X"
  // would be meaningless for it. Checked once at startup so the failure is
  // a clear message, not a wrong result after a resume.
  bool seekable() const { return ::lseek(fd_, 0, SEEK_CUR) >= 0; }

  // Total size in bytes (seekable inputs only).
  uint64_t size() const {
    struct stat st {};
    if (::fstat(fd_, &st) != 0) throw std::runtime_error("fstat failed");
    return static_cast<uint64_t>(st.st_size);
  }

  // Resume: continue reading at an absolute byte offset.
  void seek_to(uint64_t offset) {
    if (::lseek(fd_, static_cast<off_t>(offset), SEEK_SET) < 0) {
      throw std::runtime_error(std::string("lseek: ") + std::strerror(errno));
    }
    carry_ = 0;
  }

  // Fills `records` with a pointer to n whole records (n * kEventWireSize
  // bytes, valid until the next call) and returns n; 0 at end of input.
  // `short_read` is set when the OS returned less than asked for, which on a
  // pipe means "that is all there is right now" (the caller flushes partial
  // batches so a live stream stays live).
  size_t next_block(const uint8_t*& records, bool& short_read) {
    // Bytes of a torn record left over from the previous block move to the
    // front; the next read continues after them.
    if (carry_ > 0) std::memmove(buf_.data(), buf_.data() + carry_from_, carry_);
    size_t filled = carry_;
    const size_t want = buf_.size() - filled;
    const ssize_t n = read_some(buf_.data() + filled, want);
    short_read = static_cast<size_t>(n) < want;
    if (n == 0) {
      if (filled != 0) throw std::runtime_error("torn record at end of input");
      records = nullptr;
      return 0;
    }
    filled += static_cast<size_t>(n);
    const size_t whole = filled / kEventWireSize;
    carry_ = filled - whole * kEventWireSize;
    carry_from_ = whole * kEventWireSize;
    records = buf_.data();
    return whole;
  }

 private:
  static constexpr size_t kBlockBytes = 1 << 20;  // 1 MiB: ~20K records per read on a file

  // read() with EINTR retried; returns 0 only at end of input.
  ssize_t read_some(uint8_t* dst, size_t len) {
    while (true) {
      const ssize_t n = ::read(fd_, dst, len);
      if (n >= 0) return n;
      if (errno == EINTR) continue;
      throw std::runtime_error(std::string("read: ") + std::strerror(errno));
    }
  }

  void read_header(const std::string& path) {
    uint8_t hdr[kFileHeaderSize];
    size_t got = 0;
    while (got < kFileHeaderSize) {
      const ssize_t n = read_some(hdr + got, kFileHeaderSize - got);
      if (n == 0) throw std::runtime_error("missing header: " + path);
      got += static_cast<size_t>(n);
    }
    uint32_t magic = 0, version = 0;
    std::memcpy(&magic, hdr, 4);
    std::memcpy(&version, hdr + 4, 4);
    if (magic != kFileMagic) throw std::runtime_error("bad magic: " + path);
    if (version != kFileVersion) throw std::runtime_error("bad version: " + path);
  }

  int fd_ = -1;
  bool owns_ = true;
  std::vector<uint8_t> buf_;
  size_t carry_ = 0;       // bytes of a partial record at the end of the last block
  size_t carry_from_ = 0;  // where in buf_ they start
};

// ---- output merge -------------------------------------------------------------

// Appends each part to `out` in order, skipping the first `skip_after_first`
// bytes of every part except the first (used to drop the .ads header that
// each per-worker late file carries). The parts are left in place; the
// caller removes them once the merged file is committed (see run_pipeline).
void concat_parts(const std::vector<std::string>& parts, FILE* out, size_t skip_after_first) {
  std::vector<uint8_t> buf(1 << 20);
  for (size_t i = 0; i < parts.size(); ++i) {
    FILE* f = std::fopen(parts[i].c_str(), "rb");
    if (!f) throw std::runtime_error("cannot reopen part: " + parts[i]);
    if (i > 0 && skip_after_first > 0) {
      std::fseek(f, static_cast<long>(skip_after_first), SEEK_SET);
    }
    while (true) {
      const size_t n = std::fread(buf.data(), 1, buf.size(), f);
      if (n == 0) break;
      if (std::fwrite(buf.data(), 1, n, out) != n) {
        std::fclose(f);
        throw std::runtime_error("short write while merging parts");
      }
    }
    std::fclose(f);
  }
}

// Merges parts into `final_path` atomically: write <final>.tmp, fsync,
// rename. A crash during the merge therefore leaves either no output file
// or the previous complete one, never a half-merged one; and the resumed run
// simply merges again. With csv_header the CSV header line goes first; the
// late file instead keeps the .ads header of its first part.
void merge_atomic(const std::vector<std::string>& parts, const std::string& final_path,
                  bool csv_header, size_t skip_after_first) {
  const std::string tmp = final_path + ".tmp";
  FILE* out = std::fopen(tmp.c_str(), "wb");
  if (!out) throw std::runtime_error("cannot open output: " + tmp);
  try {
    if (csv_header) write_csv_header(out);
    concat_parts(parts, out, skip_after_first);
    if (std::fflush(out) != 0) throw std::runtime_error("flush output failed");
    durable_sync(::fileno(out));
  } catch (...) {
    std::fclose(out);
    throw;
  }
  if (std::fclose(out) != 0) throw std::runtime_error("close output failed");
  if (::rename(tmp.c_str(), final_path.c_str()) != 0) {
    throw std::runtime_error(std::string("rename output: ") + std::strerror(errno));
  }
}

// Part files sit next to the output. "-" (stdout) has no "next to", so they
// go to a fresh directory under the system temp dir instead.
std::string part_dir_for(const std::string& out_path) {
  if (out_path != "-") return "";
  const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
  const fs::path dir = fs::temp_directory_path() / ("streamkit-" + std::to_string(stamp));
  fs::create_directories(dir);
  return dir.string();
}

std::string part_path(const std::string& dir, const std::string& base, int i) {
  const std::string name = (base == "-" ? "stdout" : base) + ".part" + std::to_string(i);
  return dir.empty() ? name : (fs::path(dir) / fs::path(name).filename()).string();
}

// Cuts a part file back to `len` bytes. The file must exist and be at least
// that long: shorter means the bytes a snapshot vouched for are gone, which
// is corruption, not something to paper over.
void truncate_part(const std::string& path, uint64_t len) {
  struct stat st {};
  if (::stat(path.c_str(), &st) != 0) {
    throw std::runtime_error("resume: part file missing: " + path);
  }
  if (static_cast<uint64_t>(st.st_size) < len) {
    throw std::runtime_error("resume: part file shorter than its checkpoint: " + path);
  }
  if (::truncate(path.c_str(), static_cast<off_t>(len)) != 0) {
    throw std::runtime_error(std::string("truncate ") + path + ": " + std::strerror(errno));
  }
}

}  // namespace

// ---- partitioning -------------------------------------------------------------

uint32_t partition_of(uint32_t campaign_id, uint32_t partitions) {
  // splitmix64 finalizer: a well-known bijective mixer, so distinct ids map
  // to well-spread 64-bit values and any slice of the bits is usable.
  uint64_t x = campaign_id;
  x += 0x9E3779B97F4A7C15ULL;
  x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
  x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
  x ^= x >> 31;
  // Range reduction by multiply-and-shift instead of `% partitions`: the top
  // 32 bits of the hash, scaled into [0, partitions). Same distribution as
  // modulo for a well-mixed hash, and a multiply instead of a 64-bit
  // division, which mattered once the reader became the bottleneck.
  return static_cast<uint32_t>(((x >> 32) * partitions) >> 32);
}

// ---- worker -------------------------------------------------------------------

Worker::Worker(int id, const Config& cfg, std::string out_part_path, std::string late_part_path,
               CheckpointCoordinator* coord, const WorkerSnapshot* resume)
    : id_(id),
      cfg_(cfg),
      queue_(cfg.queue_depth),
      dedup_(cfg.window_us, cfg.dedup_bucket_us),
      max_event_time_(LLONG_MIN),
      watermark_(LLONG_MIN),
      out_path_(std::move(out_part_path)),
      late_path_(std::move(late_part_path)),
      coord_(coord) {
  if (resume) {
    restore(*resume);
    open_outputs_resumed(*resume);
  } else {
    open_outputs_fresh();
  }
}

Worker::~Worker() {
  if (out_) std::fclose(out_);
}

void Worker::open_outputs_fresh() {
  out_ = std::fopen(out_path_.c_str(), "w");
  if (!out_) throw std::runtime_error("cannot open output part: " + out_path_);
  // One row is ~40 bytes; a 1 MiB stdio buffer turns ~25K rows into one write().
  std::setvbuf(out_, nullptr, _IOFBF, 1 << 20);
  out_bytes_ = 0;
  if (!late_path_.empty()) late_ = std::make_unique<EventWriter>(late_path_);
}

void Worker::open_outputs_resumed(const WorkerSnapshot& s) {
  // This is what makes the output exactly-once: rows written after the
  // checkpoint and before the crash are cut off here, and the replayed
  // events will write them again.
  truncate_part(out_path_, s.out_bytes);
  out_ = std::fopen(out_path_.c_str(), "a");
  if (!out_) throw std::runtime_error("cannot reopen output part: " + out_path_);
  std::setvbuf(out_, nullptr, _IOFBF, 1 << 20);
  out_bytes_ = s.out_bytes;
  if (!late_path_.empty()) {
    truncate_part(late_path_, s.late_bytes);
    late_ = std::make_unique<EventWriter>(late_path_, EventWriter::Mode::kAppend);
    if (late_->bytes() != s.late_bytes) throw std::runtime_error("late part length mismatch after truncate");
  }
}

void Worker::restore(const WorkerSnapshot& s) {
  if (s.worker != static_cast<uint32_t>(id_)) throw std::runtime_error("snapshot belongs to another worker");
  max_event_time_ = s.max_event_time_us;
  watermark_ = s.watermark_us;
  stats_.events = s.events;
  stats_.dups = s.dups;
  stats_.late = s.late;
  stats_.windows_emitted = s.windows_emitted;
  open_.clear();
  // The snapshot is in map order, so inserting with a hint at the end is
  // linear instead of n log n.
  for (const auto& [k, a] : s.open_windows) open_.emplace_hint(open_.end(), k, a);
  stats_.open_windows_peak = open_.size();
  dedup_.import_state(s.dedup);
}

WorkerSnapshot Worker::snapshot(uint64_t epoch, uint64_t input_offset) const {
  WorkerSnapshot s;
  s.epoch = epoch;
  s.worker = static_cast<uint32_t>(id_);
  s.partitions = static_cast<uint32_t>(cfg_.threads);
  s.input_offset = input_offset;
  s.out_bytes = out_bytes_;
  s.late_bytes = late_ ? late_->bytes() : 0;
  s.max_event_time_us = max_event_time_;
  s.watermark_us = watermark_;
  s.events = stats_.events;
  s.dups = stats_.dups;
  s.late = stats_.late;
  s.windows_emitted = stats_.windows_emitted;
  s.open_windows.assign(open_.begin(), open_.end());
  s.dedup = dedup_.export_state();
  return s;
}

void Worker::checkpoint(uint64_t epoch, uint64_t input_offset) {
  if (!coord_) throw std::logic_error("checkpoint without a coordinator");
  const auto t0 = std::chrono::steady_clock::now();

  // 1. The part files must be on disk before a snapshot says how long they
  //    are; otherwise a power loss could leave the file shorter than the
  //    recorded length and the resume would have nothing to truncate to.
  if (std::fflush(out_) != 0) throw std::runtime_error("flush output part failed");
  if (cfg_.fsync) {
    durable_sync(::fileno(out_));
    if (late_) late_->sync();
  } else if (late_) {
    late_->flush();
  }

  // 2. Serialize the state as of this instant and write it atomically next to
  //    the other workers' snapshots for this epoch.
  const WorkerSnapshot snap = snapshot(epoch, input_offset);
  const std::vector<uint8_t> bytes = encode_snapshot(snap);
  coord_->ensure_epoch_dir(epoch);
  atomic_write_file(snapshot_path(coord_->dir(), epoch, snap.worker), bytes, cfg_.fsync);

  // 3. Report. The last worker to report commits the epoch (checkpoint.h).
  coord_->snapshot_written(epoch, input_offset, bytes.size());

  ++stats_.snapshots;
  stats_.last_snapshot_bytes = bytes.size();
  stats_.last_snapshot_windows = snap.open_windows.size();
  const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  stats_.snapshot_secs_total += secs;
  if (secs > stats_.snapshot_secs_max) stats_.snapshot_secs_max = secs;
}

void Worker::advance_watermark(int64_t event_time_us) {
  max_event_time_ = event_time_us;
  watermark_ = max_event_time_ - cfg_.lateness_us;
  // Advance dedup before inserting the current event so stale buckets go
  // before a new one is appended (keeps the retained span bounded; m2 notes).
  dedup_.advance(watermark_);
}

void Worker::close_ready(bool record_latency) {
  // Windows are keyed by start and all have the same length, so the oldest
  // start is also the earliest end: pop from the front until one is still
  // open. This holds for sliding windows too, which is why the map key did
  // not need to change for m3.
  //
  // Latency (m5): every window closed here was closed by the event being
  // processed right now, which arrived with the current batch; the watermark
  // first passed the window's end in this very call (close_ready runs on
  // every advance). So "row appended" minus "batch arrived" is the engine's
  // whole delay for that window. The clock is read after each row, so a row
  // behind others in the same call is charged for them too.
  const bool record = record_latency && cfg_.latency_stats && batch_arrival_ns_ != 0;
  while (!open_.empty()) {
    auto it = open_.begin();
    if (it->first.window_start_us + cfg_.window_us > watermark_) break;
    out_bytes_ += write_csv_row(out_, it->first, it->second);
    ++stats_.windows_emitted;
    open_.erase(it);
    if (record) emit_latency_us_.push_back(elapsed_us(batch_arrival_ns_, now_ns()));
  }
}

void Worker::process(const Event& e) {
  ++stats_.events;

  // 1. Watermark. The event that raises it can never be late under the
  //    watermark it just set (its windows end after its own time).
  const bool advanced = e.event_time_us > max_event_time_;
  if (advanced) advance_watermark(e.event_time_us);

  // 2. Dedup. kExpired passes through: the late check below is guaranteed to
  //    divert it (proof in dedup.h, which also covers sliding windows).
  if (cfg_.dedup && dedup_.insert(e.event_id, e.event_time_us) == Dedup::Verdict::kDuplicate) {
    ++stats_.dups;
    return;
  }

  // 3. Late check. With sliding windows an event belongs to several windows;
  //    it is late as soon as the *earliest* of them has already been emitted.
  //    Then the whole event goes to the side file rather than being counted
  //    in the windows still open, so the output and the side file never both
  //    claim part of one event. (With lateness >= the stream's maximum
  //    delay, which is how the oracle comparison is run, no event ever
  //    reaches this partially-closed state.)
  const int64_t first_start = first_window_start(e.event_time_us, cfg_.window_us, cfg_.slide_us);
  if (watermark_ != LLONG_MIN && first_start + cfg_.window_us <= watermark_) {
    ++stats_.late;
    if (late_) late_->write(e);
    return;
  }

  // 4. Aggregate into every window containing the event (one for tumbling),
  //    then close whatever the new watermark finished.
  for_each_window_start(e.event_time_us, cfg_.window_us, cfg_.slide_us, [&](int64_t ws) {
    open_[{ws, e.campaign_id}].add(e);
  });
  if (open_.size() > stats_.open_windows_peak) stats_.open_windows_peak = open_.size();
  if (advanced) close_ready(/*record_latency=*/true);
}

void Worker::finish() {
  // End of input closes everything left; that is not watermark latency, so
  // these closings are not sampled.
  watermark_ = LLONG_MAX;
  close_ready(/*record_latency=*/false);
  stats_.dedup_peak_ids = dedup_.peak_ids();
  if (out_) {
    if (std::fclose(out_) != 0) {
      out_ = nullptr;
      throw std::runtime_error("close output part failed");
    }
    out_ = nullptr;
  }
  late_.reset();  // flushes and closes the late part
}

void Worker::process_batch(const Batch& b) {
  if (b.marker) {
    checkpoint(b.epoch, b.input_offset);
    return;
  }
  batch_arrival_ns_ = b.arrival_ns;  // 0 unless --latency-stats
  if (!b.events.empty()) {
    for (const Event& e : b.events) process(e);
    return;
  }
  const uint8_t* p = b.raw.data();
  const uint8_t* end = p + b.n * kEventWireSize;
  for (; p < end; p += kEventWireSize) process(decode(p));
}

void Worker::run() {
  bool done = false;
  int round = 0;
  while (!done) {
    Batch* b = queue_.try_front();
    if (!b) {
      if (round == 0) ++stats_.idle_waits;
      backoff(round);
      continue;
    }
    round = 0;
    ++stats_.batches;
    done = b->eof;
    if (error_.empty()) {
      // After a failure we keep popping (and ignoring) batches so the reader
      // never blocks forever on a full queue; the error is reported at the end.
      // Busy time is clocked per batch (two clock reads per 128 events) so the
      // summary can show whether workers ran at the same speed; see NOTES.md.
      const auto t0 = std::chrono::steady_clock::now();
      try {
        process_batch(*b);
      } catch (const std::exception& ex) {
        error_ = ex.what();
      }
      stats_.busy_secs += std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    }
    queue_.pop();
  }
  if (error_.empty()) {
    try {
      finish();
    } catch (const std::exception& ex) {
      error_ = ex.what();
    }
  }
}

// ---- reader + orchestration ----------------------------------------------------

namespace {

// Blocks until the queue has a free slot. This is the backpressure: a slow
// worker slows the reader down, nothing is ever dropped.
Batch* claim_blocking(SpscQueue<Batch>& q, uint64_t& stalls) {
  int round = 0;
  while (true) {
    if (Batch* b = q.try_claim()) return b;
    if (round == 0) ++stalls;
    backoff(round);
  }
}

class Reader {
 public:
  Reader(const Config& cfg, std::vector<std::unique_ptr<Worker>>& workers, uint64_t start_offset)
      : cfg_(cfg),
        workers_(workers),
        current_(workers.size(), nullptr),
        handed_((start_offset - kFileHeaderSize) / kEventWireSize) {
    if (cfg_.ckpt_dir.empty() || cfg_.checkpoint_every_events == 0) {
      next_marker_at_ = UINT64_MAX;
    } else {
      // Markers fall on multiples of N counted from the start of the file,
      // so epoch k always means "the first k*N events", whichever run wrote it.
      next_marker_at_ = (handed_ / cfg_.checkpoint_every_events + 1) * cfg_.checkpoint_every_events;
    }
  }

  // Reads everything, partitions it, and ends every queue with an eof batch.
  void run(BlockReader& in, PipelineResult& result) {
    const uint32_t partitions = static_cast<uint32_t>(workers_.size());
    const uint8_t* records = nullptr;
    bool short_read = false;
    size_t n = 0;
    const auto t0 = std::chrono::steady_clock::now();
    uint64_t handed_at_start = handed_;
    while ((n = in.next_block(records, short_read)) > 0) {
      // --latency-stats: this is the instant the block's bytes entered the
      // process; every batch started from this block carries it.
      if (cfg_.latency_stats) block_arrival_ns_ = now_ns();
      for (size_t i = 0; i < n; ++i) {
        const uint8_t* rec = records + i * kEventWireSize;
        // Peek campaign_id at its fixed offset; the rest of the record is
        // decoded by the worker (see NOTES.md, "where to decode").
        uint32_t campaign = 0;
        std::memcpy(&campaign, rec + kWireOffsetCampaignId, sizeof(campaign));
        append(partition_of(campaign, partitions), rec);
        ++handed_;
        if (handed_ == next_marker_at_) inject_marker();
        // Throttle: hold the reader to --throttle-eps events/s, checked every
        // 1024 events so the sleep granularity stays coarse.
        if (cfg_.throttle_eps > 0 && (handed_ & 1023) == 0) {
          const double due = static_cast<double>(handed_ - handed_at_start) / cfg_.throttle_eps;
          std::this_thread::sleep_until(t0 + std::chrono::duration<double>(due));
        }
      }
      result.events += n;
      // On a live pipe a short read means "nothing more right now": ship the
      // partial batches so the workers do not sit on stale events.
      if (short_read) flush_all();
    }
    flush_all();
    for (size_t p = 0; p < workers_.size(); ++p) {
      Batch* b = claim_blocking(workers_[p]->queue(), stalls_);
      b->clear();
      b->eof = true;
      workers_[p]->queue().publish();
    }
    result.reader_stalls = stalls_;
  }

 private:
  void append(uint32_t p, const uint8_t* rec) {
    Batch*& b = current_[p];
    if (!b) {
      b = claim_blocking(workers_[p]->queue(), stalls_);
      b->clear();
      b->arrival_ns = block_arrival_ns_;  // stays 0 when the flag is off
    }
    if (cfg_.reader_decode) {
      b->events.push_back(decode(rec));
    } else {
      b->append_raw(rec, cfg_.batch_events);
    }
    if (b->size() >= cfg_.batch_events) {
      workers_[p]->queue().publish();
      b = nullptr;
    }
  }

  void flush_all() {
    for (size_t p = 0; p < current_.size(); ++p) {
      if (current_[p] && current_[p]->size() > 0) {
        workers_[p]->queue().publish();
        current_[p] = nullptr;
      }
    }
  }

  // The checkpoint barrier. Partial batches go first so that every event
  // before offset X is ahead of the marker in its queue; then one marker per
  // worker. Nothing waits: the reader continues reading the next block while
  // the workers snapshot whenever they get to it.
  void inject_marker() {
    flush_all();
    const uint64_t epoch = handed_ / cfg_.checkpoint_every_events;
    const uint64_t offset = kFileHeaderSize + handed_ * kEventWireSize;
    for (size_t p = 0; p < workers_.size(); ++p) {
      Batch* b = claim_blocking(workers_[p]->queue(), stalls_);
      b->clear();
      b->marker = true;
      b->epoch = epoch;
      b->input_offset = offset;
      workers_[p]->queue().publish();
    }
    next_marker_at_ += cfg_.checkpoint_every_events;
    ++markers_;
    if (cfg_.fail_after_checkpoint != 0 && markers_ == cfg_.fail_after_checkpoint) {
      throw std::runtime_error("fail_after_checkpoint test hook fired");
    }
  }

  const Config& cfg_;
  std::vector<std::unique_ptr<Worker>>& workers_;
  std::vector<Batch*> current_;  // the claimed-but-unpublished slot per partition
  uint64_t stalls_ = 0;
  uint64_t handed_;              // records handed out since the start of the file
  uint64_t next_marker_at_;
  uint64_t markers_ = 0;
  int64_t block_arrival_ns_ = 0;  // --latency-stats: when the current block was read
};

Manifest manifest_for(const Config& cfg) {
  Manifest m;
  m.partitions = static_cast<uint32_t>(cfg.threads);
  m.window_us = cfg.window_us;
  m.slide_us = cfg.slide_us;
  m.lateness_us = cfg.lateness_us;
  m.dedup_bucket_us = cfg.dedup_bucket_us;
  m.dedup = cfg.dedup;
  return m;
}

// Decides between a fresh start and a resume, and validates the input and
// the checkpoint directory against each other. Returns the manifest to
// resume from, or nullopt for a fresh start.
std::optional<Manifest> plan_start(const Config& cfg, BlockReader& in) {
  if (cfg.ckpt_dir.empty()) return std::nullopt;
  if (!in.seekable()) {
    // A resume needs to rewind the input to offset X; a pipe cannot be rewound.
    throw std::runtime_error(
        "--ckpt-dir needs a seekable input file; stdin or a pipe cannot be rewound on resume");
  }
  if (cfg.out_path == "-") throw std::runtime_error("--ckpt-dir needs --out to be a file (its parts are truncated on resume)");
  if (!cfg.resume) {
    if (checkpoint_dir_has_state(cfg.ckpt_dir)) {
      throw std::runtime_error("checkpoint directory " + cfg.ckpt_dir +
                               " holds a previous run; pass --resume to continue it or remove it");
    }
    return std::nullopt;
  }
  std::optional<Manifest> m = prepare_resume(cfg.ckpt_dir);
  if (!m) return std::nullopt;  // nothing committed yet: start over from byte 0
  // Everything but the epoch and offset must match this run's flags: a
  // different thread count would route campaigns to other workers' state,
  // and different window parameters would make the snapshots meaningless.
  Manifest expected = manifest_for(cfg);
  expected.epoch = m->epoch;
  expected.input_offset = m->input_offset;
  if (*m != expected) {
    throw std::runtime_error(
        "resume: flags differ from the checkpoint's (threads, window, slide, lateness, dedup must match MANIFEST)");
  }
  const uint64_t size = in.size();
  if (m->input_offset > size || (m->input_offset - kFileHeaderSize) % kEventWireSize != 0) {
    throw std::runtime_error("resume: MANIFEST input_offset does not fit the input file");
  }
  return m;
}

}  // namespace

PipelineResult run_pipeline(const Config& cfg) {
  if (cfg.threads < 1) throw std::invalid_argument("threads must be >= 1");
  if (cfg.batch_events < 1) throw std::invalid_argument("batch_events must be >= 1");
  if (cfg.slide_us <= 0 || cfg.slide_us > cfg.window_us) {
    throw std::invalid_argument("need 0 < slide <= window");
  }

  const auto t0 = std::chrono::steady_clock::now();
  PipelineResult result;

  // Open the input first: it decides whether a checkpointed run is possible.
  BlockReader in(cfg.in_path);
  const std::optional<Manifest> resume_from = plan_start(cfg, in);
  uint64_t start_offset = kFileHeaderSize;
  std::unique_ptr<CheckpointCoordinator> coord;
  if (!cfg.ckpt_dir.empty()) {
    coord = std::make_unique<CheckpointCoordinator>(cfg.ckpt_dir, manifest_for(cfg), cfg.fsync);
  }
  if (resume_from) {
    start_offset = resume_from->input_offset;
    in.seek_to(start_offset);
    result.resumed = true;
    result.resumed_epoch = resume_from->epoch;
  }
  result.resumed_offset = start_offset;

  const std::string dir = part_dir_for(cfg.out_path);
  std::vector<std::string> out_parts, late_parts;
  std::vector<std::unique_ptr<Worker>> workers;
  for (int i = 0; i < cfg.threads; ++i) {
    out_parts.push_back(part_path(dir, cfg.out_path, i));
    std::string late;
    if (!cfg.late_out_path.empty()) {
      late = part_path(dir, cfg.late_out_path, i);
      late_parts.push_back(late);
    }
    std::optional<WorkerSnapshot> snap;
    if (resume_from) {
      snap = read_snapshot_file(snapshot_path(cfg.ckpt_dir, resume_from->epoch, static_cast<uint32_t>(i)));
      if (snap->input_offset != resume_from->input_offset || snap->epoch != resume_from->epoch) {
        throw std::runtime_error("resume: snapshot and MANIFEST disagree for worker " + std::to_string(i));
      }
    }
    workers.push_back(std::make_unique<Worker>(i, cfg, out_parts.back(), late, coord.get(),
                                               snap ? &*snap : nullptr));
  }

  std::vector<std::thread> threads;
  threads.reserve(workers.size());
  for (auto& w : workers) threads.emplace_back([&w] { w->run(); });

  // The calling thread is the reader. If it throws (torn input, say) the
  // workers must still be told to stop, or join() below would hang.
  std::string reader_error;
  try {
    Reader(cfg, workers, start_offset).run(in, result);
  } catch (const std::exception& ex) {
    reader_error = ex.what();
    for (auto& w : workers) {
      Batch* b = claim_blocking(w->queue(), result.reader_stalls);
      b->clear();
      b->eof = true;
      w->queue().publish();
    }
  }
  for (auto& t : threads) t.join();
  result.read_secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();

  for (auto& w : workers) {
    result.workers.push_back(w->stats());
    result.watermarks.push_back(w->watermark());
    const auto& lat = w->emit_latencies_us();
    result.emit_latency_us.insert(result.emit_latency_us.end(), lat.begin(), lat.end());
    if (result.error.empty() && !w->error().empty()) {
      result.error = "worker " + std::to_string(w->id()) + ": " + w->error();
    }
  }
  if (!reader_error.empty()) result.error = "reader: " + reader_error;
  if (coord) result.checkpoints = coord->stats();
  workers.clear();  // closes any file a failed worker still holds
  if (!result.error.empty()) return result;

  // Merge, in an order that survives a crash at any point (checkpoint.h):
  //   1. late file: tmp + rename        (idempotent: a resume redoes it)
  //   2. output:    tmp + rename        (the commit of the whole run)
  //   3. delete the checkpoint directory (a later --resume starts over from
  //      byte 0 and rewrites the same bytes; without it a stale MANIFEST
  //      would point at part files we are about to delete)
  //   4. delete the part files
  if (!late_parts.empty()) {
    // Every part starts with the 8-byte .ads header; keep only the first.
    merge_atomic(late_parts, cfg.late_out_path, false, kFileHeaderSize);
  }
  if (cfg.out_path == "-") {
    write_csv_header(stdout);
    concat_parts(out_parts, stdout, 0);
    std::fflush(stdout);
  } else {
    merge_atomic(out_parts, cfg.out_path, true, 0);
  }
  std::error_code ec;
  if (coord) fs::remove_all(cfg.ckpt_dir, ec);
  for (const auto& p : out_parts) fs::remove(p, ec);   // best effort; a leftover part is not an error
  for (const auto& p : late_parts) fs::remove(p, ec);
  if (!dir.empty()) fs::remove(dir, ec);

  result.total_secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  return result;
}

}  // namespace adstack
