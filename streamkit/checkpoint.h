#pragma once
// Checkpointing and exactly-once resume for streamkit (milestone 4).
//
// WHY THIS EXISTS. A streaming job runs for days. If the process dies (crash,
// SIGKILL, machine reboot) it must come back and continue without counting
// any event twice or losing any, and the output it had already written must
// end up byte-identical to what a run that never died would have produced.
//
// THE IDEA, in plain language (Chandy-Lamport style, with markers). Every N
// input events the reader notes "I have handed out every byte before offset
// X of the input" and drops a marker into every worker's queue, in the same
// queue the events travel in, right after the last event before X. Because a
// queue is FIFO, when worker i dequeues the marker it has processed exactly
// the events before X that belong to it, and none after. It writes its state
// at that instant to disk. When all P workers have done so, the epoch is
// committed by renaming a MANIFEST file into place. Resume = read MANIFEST,
// reload every worker's state, rewind the input to X, go. No thread ever
// stops another: the marker rides in the data stream, so each worker
// snapshots at its own pace and the reader keeps reading ahead.
//
// WHY MARKERS AND NOT "STOP THE WORLD". Stopping all workers, snapshotting,
// and restarting needs a barrier on the hot path and idles P-1 cores while
// the slowest worker drains. The marker costs one extra queue slot per
// worker per epoch and nothing else; a worker that is behind simply reaches
// the marker later. Both give the same consistency ("state = everything
// before X") because the only communication between threads is the queue
// itself, so there are no in-flight messages to capture.
//
// WHAT MAKES OUTPUT EXACTLY-ONCE. Each worker owns one output part file, and
// its snapshot records how long that file was at the marker. Rows written
// after the snapshot and before a crash are simply truncated away on resume;
// the replayed events regenerate them. This is why per-worker files exist
// (pipeline.h) and why the alternative, de-duplicating output rows, is not
// needed: truncation needs no memory of what was emitted.
//
// ON-DISK LAYOUT
//   <ckpt_dir>/MANIFEST                   text, see Manifest; its presence
//                                         commits the epoch it names
//   <ckpt_dir>/epoch-<k>/worker-<i>.snap  binary, see snapshot format below
//   <ckpt_dir>/epoch-<k>/worker-<i>.tmp   in progress; never read
//   <ckpt_dir>/manifest.tmp               in progress; never read
//
// COMMIT PROTOCOL. Every file is written to a .tmp name, fsync'ed, then
// rename(2)'d to its final name, then the directory is fsync'ed. rename is
// atomic on POSIX: a reader sees either the old name's file or the new one,
// never a half-written file under the final name. fsync on the file makes
// the bytes durable; fsync on the directory makes the *name* durable (the
// directory entry is data of the directory, and a crash can otherwise lose
// the rename while keeping the file). On macOS fsync only pushes data to the
// drive's cache; fcntl(F_FULLFSYNC) asks the drive to flush that cache too,
// so durable_sync() and durable_sync_dir() both use it when the file system
// supports it (plain fsync otherwise). Snapshots are written before the
// manifest, so the manifest's rename is the single commit point: either
// MANIFEST names epoch k and all P snapshots of k exist, or it still names
// k-1 and epoch k is garbage to be deleted on resume.
//
// SNAPSHOT FORMAT (little-endian, fixed-width fields, no padding):
//   u32 magic "SKS1"       u32 version (2)
//   u64 epoch              u32 worker   u32 partitions   u64 input_offset
//   u64 out_bytes          u64 late_bytes
//   i64 max_event_time_us  i64 watermark_us
//   u64 events  u64 dups  u64 late  u64 expired  u64 windows_emitted
//                                                        (cumulative counters)
//   u64 n_windows, then n_windows x { i64 window_start_us, u32 campaign_id,
//                                     u64 impressions, u64 clicks, u64 conversions,
//                                     i64 cost_micros }
//   u8  dedup_has_horizon  i64 dedup_forget_before  i64 dedup_front_start
//   u64 n_buckets, then n_buckets x { u64 n_ids, n_ids x u64 event_id }
//   u64 checksum           FNV-1a over every byte above
// The checksum is belt and braces: rename already guarantees a .snap is
// complete, but a cheap 64-bit hash turns any bug or disk fault into a loud
// error instead of a silently wrong resume.

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/agg.h"
#include "dedup.h"

namespace adstack {

// ---- durable file primitives ------------------------------------------------

// fsync the file, using F_FULLFSYNC on Apple platforms when the file system
// supports it (falls back to fsync when it does not). Throws on failure.
void durable_sync(int fd);
// fsync a directory so renames, creations and deletions inside it are
// durable. F_FULLFSYNC on Apple too, for the same reason as durable_sync():
// by the reasoning above a plain fsync would leave the directory entry in
// the drive's cache, so the rename it is meant to make durable would not
// survive a power loss. Falls back to fsync where F_FULLFSYNC is refused.
void durable_sync_dir(const std::filesystem::path& dir);
// Write bytes to `<final>.tmp`, fsync, rename over `final`, fsync the parent
// directory. After it returns, `final` either has the new content or (if the
// machine died first) the old content; never a mix. durable=false skips both
// fsyncs; it exists only to measure what they cost (--no-fsync) and makes
// the result safe against a process crash but not against a power loss.
void atomic_write_file(const std::filesystem::path& final_path, const std::vector<uint8_t>& bytes,
                       bool durable = true);
// Same, for text.
void atomic_write_file(const std::filesystem::path& final_path, const std::string& text,
                       bool durable = true);

// FNV-1a, 64-bit. Not cryptographic; it only has to catch accidents.
uint64_t fnv1a64(const uint8_t* data, size_t n);

// ---- snapshot ---------------------------------------------------------------

struct WorkerSnapshot {
  uint64_t epoch = 0;
  uint32_t worker = 0;
  uint32_t partitions = 0;
  uint64_t input_offset = 0;   // the X the marker carried; cross-checked against MANIFEST
  uint64_t out_bytes = 0;      // length of <out>.part<worker> at the marker
  uint64_t late_bytes = 0;     // length of <late>.part<worker> at the marker (0 if none)
  int64_t max_event_time_us = 0;
  int64_t watermark_us = 0;
  uint64_t events = 0;
  uint64_t dups = 0;
  uint64_t late = 0;
  uint64_t expired = 0;        // of `late`, how many were past the dedup horizon
  uint64_t windows_emitted = 0;
  std::vector<std::pair<WindowKey, Agg>> open_windows;  // in map order
  DedupState dedup;

  bool operator==(const WorkerSnapshot&) const = default;
};

std::vector<uint8_t> encode_snapshot(const WorkerSnapshot& s);
// Throws std::runtime_error on bad magic, truncation or checksum mismatch.
WorkerSnapshot decode_snapshot(const std::vector<uint8_t>& bytes);

// Convenience: read the whole file and decode it.
WorkerSnapshot read_snapshot_file(const std::filesystem::path& path);

// ---- manifest -----------------------------------------------------------------

// Identity of an input file, cheap enough to take at every start: its size
// and FNV-1a over its first and last min(size, 1 MiB) bytes. A resume
// against a different file (another seed of the same size, say) is refused
// instead of splicing two streams together. The input must stay unchanged
// for the life of the run, restarts included; a file that is appended to
// between restarts has a new identity and cannot be resumed. Not a proof of
// identity (two files could share size, head and tail), but it turns the
// mistakes that actually happen into a loud error.
struct InputFingerprint {
  uint64_t size = 0;
  uint64_t head_fnv = 0;
  uint64_t tail_fnv = 0;

  bool operator==(const InputFingerprint&) const = default;
};
// Reads the two blocks with pread(2), so the file offset is not moved.
InputFingerprint fingerprint_input(int fd);

// What the resumed run needs to know, and what it checks its own flags
// against: a different partition count would put campaigns on the wrong
// workers; different window parameters would make the snapshots
// meaningless; a different checkpoint interval would number new epochs so
// that one can land on the committed epoch's directory and overwrite its
// snapshots in place; adding or dropping --late-out would orphan or miss the
// late part files; a different input would be spliced onto the old one.
// Text, one `key value` per line, so `cat MANIFEST` works.
struct Manifest {
  uint64_t epoch = 0;
  uint64_t input_offset = 0;
  uint32_t partitions = 0;
  int64_t window_us = 0;
  int64_t slide_us = 0;
  int64_t lateness_us = 0;
  int64_t dedup_bucket_us = 0;
  bool dedup = true;
  uint64_t checkpoint_every_events = 1'000'000;  // Config's default
  bool late_out = false;            // whether the run writes a late side file
  InputFingerprint input;

  bool operator==(const Manifest&) const = default;
};

std::string format_manifest(const Manifest& m);
// Throws std::runtime_error on a malformed or incomplete manifest.
Manifest parse_manifest(const std::string& text);

// ---- directory layout and resume -----------------------------------------------

std::filesystem::path manifest_path(const std::filesystem::path& ckpt_dir);
std::filesystem::path epoch_dir(const std::filesystem::path& ckpt_dir, uint64_t epoch);
std::filesystem::path snapshot_path(const std::filesystem::path& ckpt_dir, uint64_t epoch, uint32_t worker);

// True if the directory holds anything a previous run left behind (MANIFEST,
// epoch-* directories or temp files). Without --resume this is an error:
// silently starting over on top of a checkpoint would hide a bug.
bool checkpoint_dir_has_state(const std::filesystem::path& ckpt_dir);

// Decides where to resume from and cleans up the leftovers of a crash:
//   - no MANIFEST: nothing committed, returns nullopt, deletes any partial
//     epoch directories (a crash between the first snapshot writes);
//   - MANIFEST names epoch k: every other epoch-* directory goes (k-1 if the
//     crash hit after the rename but before the delete; k+1 if it hit while
//     k+1's snapshots were being written; a complete-but-uncommitted epoch
//     counts as partial, since only the rename commits), and all P snapshots
//     of k must exist or the checkpoint is corrupt (runtime_error).
// Also removes a stray manifest.tmp.
std::optional<Manifest> prepare_resume(const std::filesystem::path& ckpt_dir);

// Deletes a checkpoint directory once the run's output is committed.
// MANIFEST goes first (and, when durable, the directory is synced), then
// everything else. std::filesystem::remove_all deletes in directory order,
// which on APFS can be epoch-k before MANIFEST; a crash in between used to
// leave a MANIFEST naming a missing epoch, which the next --resume reports
// as corruption. With MANIFEST gone first, every crash point leaves either a
// complete checkpoint or no MANIFEST (start over, rewrite the same output).
void remove_checkpoint_dir(const std::filesystem::path& ckpt_dir, bool durable);

// ---- coordinator ----------------------------------------------------------------

struct CheckpointStats {
  uint64_t committed = 0;          // epochs whose MANIFEST was renamed into place
  uint64_t last_epoch = 0;
  uint64_t last_input_offset = 0;
  uint64_t last_snapshot_bytes = 0;  // sum over workers for the last committed epoch
  double manifest_secs = 0;          // time spent writing manifests (all epochs)
};

// Shared by all workers. Each worker, after renaming its .snap into place,
// calls snapshot_written(); the call that completes the set for an epoch
// writes the MANIFEST and deletes the previous epoch. One mutex, taken once
// per worker per epoch (every million events by default), so it is nowhere
// near the hot path. The "last arriver commits" shape avoids a coordinator
// thread and a condition variable; it is the same idea as a barrier.
class CheckpointCoordinator {
 public:
  // durable=false is the --no-fsync measurement mode (see atomic_write_file).
  CheckpointCoordinator(std::filesystem::path ckpt_dir, Manifest manifest_template, bool durable = true);

  // Creates <ckpt_dir>/epoch-<k> once (under the lock, so P workers racing
  // to create the same directory cannot trip over each other).
  std::filesystem::path ensure_epoch_dir(uint64_t epoch);

  // Called by a worker after its .snap is durable and renamed. Throws (into
  // the calling worker, which records it as its error) if the commit fails.
  void snapshot_written(uint64_t epoch, uint64_t input_offset, uint64_t snapshot_bytes);

  CheckpointStats stats() const;
  const std::filesystem::path& dir() const { return dir_; }

 private:
  void commit_locked(uint64_t epoch, uint64_t input_offset);

  const std::filesystem::path dir_;
  const Manifest template_;
  const bool durable_;
  mutable std::mutex mu_;
  struct Pending { uint32_t arrived = 0; uint64_t bytes = 0; };
  std::map<uint64_t, Pending> pending_;  // epoch -> how many workers have arrived
  CheckpointStats stats_;
};

}  // namespace adstack
