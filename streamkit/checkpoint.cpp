// Implementation of streamkit checkpoints: durable writes, the snapshot
// codec, the manifest, resume preparation and the epoch coordinator. The
// reasoning is in checkpoint.h; this file is mechanics plus the few
// decisions that only make sense next to the code.

#include "checkpoint.h"

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <system_error>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace adstack {

namespace fs = std::filesystem;

namespace {

std::string errno_text(const char* what) {
  return std::string(what) + ": " + std::strerror(errno);
}

// Writes all of `n` bytes, retrying on short writes and EINTR.
void write_all(int fd, const uint8_t* data, size_t n) {
  while (n > 0) {
    const ssize_t w = ::write(fd, data, n);
    if (w < 0) {
      if (errno == EINTR) continue;
      throw std::runtime_error(errno_text("write"));
    }
    data += w;
    n -= static_cast<size_t>(w);
  }
}

// ---- binary codec helpers ---------------------------------------------------

class ByteWriter {
 public:
  template <typename T>
  void put(T v) {
    static_assert(std::is_trivially_copyable_v<T>);
    const size_t at = buf_.size();
    buf_.resize(at + sizeof(T));
    std::memcpy(buf_.data() + at, &v, sizeof(T));
  }
  std::vector<uint8_t> take() { return std::move(buf_); }
  size_t size() const { return buf_.size(); }
  const uint8_t* data() const { return buf_.data(); }

 private:
  std::vector<uint8_t> buf_;
};

// Every read is bounds-checked: a truncated or garbage snapshot must throw,
// never read past the buffer.
class ByteReader {
 public:
  ByteReader(const uint8_t* p, size_t n) : p_(p), end_(p + n) {}

  template <typename T>
  T get() {
    static_assert(std::is_trivially_copyable_v<T>);
    if (remaining() < sizeof(T)) throw std::runtime_error("snapshot truncated");
    T v;
    std::memcpy(&v, p_, sizeof(T));
    p_ += sizeof(T);
    return v;
  }
  size_t remaining() const { return static_cast<size_t>(end_ - p_); }

 private:
  const uint8_t* p_;
  const uint8_t* end_;
};

constexpr uint32_t kSnapshotMagic = 0x31534B53;  // "SKS1" read little-endian
constexpr uint32_t kSnapshotVersion = 1;
constexpr const char* kManifestHeader = "streamkit-checkpoint-manifest v1";

}  // namespace

// ---- durable file primitives --------------------------------------------------

void durable_sync(int fd) {
#if defined(__APPLE__)
  // fsync(2) on macOS only guarantees the data reached the drive, not that
  // the drive wrote it out of its own cache; F_FULLFSYNC asks for that too.
  // Some file systems (network mounts, some external drives) return ENOTSUP,
  // in which case plain fsync is the best available.
  if (::fcntl(fd, F_FULLFSYNC) == 0) return;
#endif
  if (::fsync(fd) != 0) throw std::runtime_error(errno_text("fsync"));
}

void durable_sync_dir(const fs::path& dir) {
  const int fd = ::open(dir.c_str(), O_RDONLY);
  if (fd < 0) throw std::runtime_error(errno_text("open dir for fsync"));
  // Plain fsync for directories: the directory's data is its entries, and
  // those are tiny metadata writes the file system journals itself.
  const int rc = ::fsync(fd);
  const int saved = errno;
  ::close(fd);
  if (rc != 0) {
    errno = saved;
    throw std::runtime_error(errno_text("fsync dir"));
  }
}

void atomic_write_file(const fs::path& final_path, const std::vector<uint8_t>& bytes, bool durable) {
  const fs::path tmp = final_path.string() + ".tmp";
  const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) throw std::runtime_error(errno_text(("open " + tmp.string()).c_str()));
  try {
    write_all(fd, bytes.data(), bytes.size());
    if (durable) durable_sync(fd);
  } catch (...) {
    ::close(fd);
    throw;
  }
  if (::close(fd) != 0) throw std::runtime_error(errno_text("close"));
  // The commit point. rename(2) replaces the name atomically: any later
  // open() of final_path sees either the previous file or this one.
  if (::rename(tmp.c_str(), final_path.c_str()) != 0) throw std::runtime_error(errno_text("rename"));
  if (durable) durable_sync_dir(final_path.parent_path().empty() ? fs::path(".") : final_path.parent_path());
}

void atomic_write_file(const fs::path& final_path, const std::string& text, bool durable) {
  atomic_write_file(final_path, std::vector<uint8_t>(text.begin(), text.end()), durable);
}

uint64_t fnv1a64(const uint8_t* data, size_t n) {
  uint64_t h = 1469598103934665603ULL;
  for (size_t i = 0; i < n; ++i) {
    h ^= data[i];
    h *= 1099511628211ULL;
  }
  return h;
}

// ---- snapshot codec -------------------------------------------------------------

std::vector<uint8_t> encode_snapshot(const WorkerSnapshot& s) {
  ByteWriter w;
  w.put(kSnapshotMagic);
  w.put(kSnapshotVersion);
  w.put(s.epoch);
  w.put(s.worker);
  w.put(s.partitions);
  w.put(s.input_offset);
  w.put(s.out_bytes);
  w.put(s.late_bytes);
  w.put(s.max_event_time_us);
  w.put(s.watermark_us);
  w.put(s.events);
  w.put(s.dups);
  w.put(s.late);
  w.put(s.windows_emitted);
  w.put(static_cast<uint64_t>(s.open_windows.size()));
  for (const auto& [k, a] : s.open_windows) {
    w.put(k.window_start_us);
    w.put(k.campaign_id);
    w.put(a.impressions);
    w.put(a.clicks);
    w.put(a.conversions);
    w.put(a.cost_micros);
  }
  w.put(static_cast<uint8_t>(s.dedup.forget_before.has_value() ? 1 : 0));
  w.put(s.dedup.forget_before.value_or(0));
  w.put(s.dedup.front_start);
  w.put(static_cast<uint64_t>(s.dedup.buckets.size()));
  for (const auto& bucket : s.dedup.buckets) {
    w.put(static_cast<uint64_t>(bucket.size()));
    for (uint64_t id : bucket) w.put(id);
  }
  w.put(fnv1a64(w.data(), w.size()));
  return w.take();
}

WorkerSnapshot decode_snapshot(const std::vector<uint8_t>& bytes) {
  if (bytes.size() < sizeof(uint64_t)) throw std::runtime_error("snapshot truncated");
  // Check the checksum first so every other error message can be trusted.
  const size_t body = bytes.size() - sizeof(uint64_t);
  uint64_t stored = 0;
  std::memcpy(&stored, bytes.data() + body, sizeof(stored));
  if (fnv1a64(bytes.data(), body) != stored) throw std::runtime_error("snapshot checksum mismatch");

  ByteReader r(bytes.data(), body);
  if (r.get<uint32_t>() != kSnapshotMagic) throw std::runtime_error("snapshot bad magic");
  if (r.get<uint32_t>() != kSnapshotVersion) throw std::runtime_error("snapshot bad version");
  WorkerSnapshot s;
  s.epoch = r.get<uint64_t>();
  s.worker = r.get<uint32_t>();
  s.partitions = r.get<uint32_t>();
  s.input_offset = r.get<uint64_t>();
  s.out_bytes = r.get<uint64_t>();
  s.late_bytes = r.get<uint64_t>();
  s.max_event_time_us = r.get<int64_t>();
  s.watermark_us = r.get<int64_t>();
  s.events = r.get<uint64_t>();
  s.dups = r.get<uint64_t>();
  s.late = r.get<uint64_t>();
  s.windows_emitted = r.get<uint64_t>();
  const uint64_t n_windows = r.get<uint64_t>();
  // Each window takes 44 bytes; refuse a count the buffer cannot hold before
  // reserving, so a corrupt count cannot ask for terabytes.
  if (n_windows > r.remaining() / 44) throw std::runtime_error("snapshot truncated");
  s.open_windows.reserve(static_cast<size_t>(n_windows));
  for (uint64_t i = 0; i < n_windows; ++i) {
    WindowKey k{};
    Agg a;
    k.window_start_us = r.get<int64_t>();
    k.campaign_id = r.get<uint32_t>();
    a.impressions = r.get<uint64_t>();
    a.clicks = r.get<uint64_t>();
    a.conversions = r.get<uint64_t>();
    a.cost_micros = r.get<int64_t>();
    s.open_windows.emplace_back(k, a);
  }
  const bool has_horizon = r.get<uint8_t>() != 0;
  const int64_t forget_before = r.get<int64_t>();
  if (has_horizon) s.dedup.forget_before = forget_before;
  s.dedup.front_start = r.get<int64_t>();
  const uint64_t n_buckets = r.get<uint64_t>();
  if (n_buckets > r.remaining() / sizeof(uint64_t)) throw std::runtime_error("snapshot truncated");
  s.dedup.buckets.reserve(static_cast<size_t>(n_buckets));
  for (uint64_t b = 0; b < n_buckets; ++b) {
    const uint64_t n_ids = r.get<uint64_t>();
    if (n_ids > r.remaining() / sizeof(uint64_t)) throw std::runtime_error("snapshot truncated");
    std::vector<uint64_t> ids;
    ids.reserve(static_cast<size_t>(n_ids));
    for (uint64_t i = 0; i < n_ids; ++i) ids.push_back(r.get<uint64_t>());
    s.dedup.buckets.push_back(std::move(ids));
  }
  if (r.remaining() != 0) throw std::runtime_error("snapshot has trailing bytes");
  return s;
}

WorkerSnapshot read_snapshot_file(const fs::path& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) throw std::runtime_error("cannot open snapshot: " + path.string());
  std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  try {
    return decode_snapshot(bytes);
  } catch (const std::exception& ex) {
    throw std::runtime_error(path.string() + ": " + ex.what());
  }
}

// ---- manifest ------------------------------------------------------------------

std::string format_manifest(const Manifest& m) {
  std::ostringstream o;
  o << kManifestHeader << "\n"
    << "epoch " << m.epoch << "\n"
    << "input_offset " << m.input_offset << "\n"
    << "partitions " << m.partitions << "\n"
    << "window_us " << m.window_us << "\n"
    << "slide_us " << m.slide_us << "\n"
    << "lateness_us " << m.lateness_us << "\n"
    << "dedup_bucket_us " << m.dedup_bucket_us << "\n"
    << "dedup " << (m.dedup ? 1 : 0) << "\n";
  return o.str();
}

Manifest parse_manifest(const std::string& text) {
  std::istringstream in(text);
  std::string line;
  if (!std::getline(in, line) || line != kManifestHeader) {
    throw std::runtime_error("manifest: bad header");
  }
  std::map<std::string, std::string> kv;
  while (std::getline(in, line)) {
    if (line.empty()) continue;
    const size_t sp = line.find(' ');
    if (sp == std::string::npos) throw std::runtime_error("manifest: malformed line: " + line);
    kv[line.substr(0, sp)] = line.substr(sp + 1);
  }
  auto need = [&](const char* key) -> long long {
    auto it = kv.find(key);
    if (it == kv.end()) throw std::runtime_error(std::string("manifest: missing ") + key);
    char* end = nullptr;
    errno = 0;
    const long long v = std::strtoll(it->second.c_str(), &end, 10);
    if (errno != 0 || end == it->second.c_str() || *end != '\0') {
      throw std::runtime_error(std::string("manifest: bad number for ") + key);
    }
    return v;
  };
  Manifest m;
  m.epoch = static_cast<uint64_t>(need("epoch"));
  m.input_offset = static_cast<uint64_t>(need("input_offset"));
  m.partitions = static_cast<uint32_t>(need("partitions"));
  m.window_us = need("window_us");
  m.slide_us = need("slide_us");
  m.lateness_us = need("lateness_us");
  m.dedup_bucket_us = need("dedup_bucket_us");
  m.dedup = need("dedup") != 0;
  if (m.partitions == 0) throw std::runtime_error("manifest: partitions must be >= 1");
  return m;
}

// ---- layout and resume ------------------------------------------------------------

fs::path manifest_path(const fs::path& ckpt_dir) { return ckpt_dir / "MANIFEST"; }

fs::path epoch_dir(const fs::path& ckpt_dir, uint64_t epoch) {
  return ckpt_dir / ("epoch-" + std::to_string(epoch));
}

fs::path snapshot_path(const fs::path& ckpt_dir, uint64_t epoch, uint32_t worker) {
  return epoch_dir(ckpt_dir, epoch) / ("worker-" + std::to_string(worker) + ".snap");
}

namespace {

// "epoch-<k>" -> k, or nullopt for anything else.
std::optional<uint64_t> epoch_of_dir_name(const std::string& name) {
  const std::string prefix = "epoch-";
  if (name.rfind(prefix, 0) != 0 || name.size() == prefix.size()) return std::nullopt;
  uint64_t k = 0;
  for (size_t i = prefix.size(); i < name.size(); ++i) {
    if (name[i] < '0' || name[i] > '9') return std::nullopt;
    k = k * 10 + static_cast<uint64_t>(name[i] - '0');
  }
  return k;
}

}  // namespace

bool checkpoint_dir_has_state(const fs::path& ckpt_dir) {
  std::error_code ec;
  if (!fs::is_directory(ckpt_dir, ec)) return false;
  for (const auto& entry : fs::directory_iterator(ckpt_dir, ec)) {
    const std::string name = entry.path().filename().string();
    if (name == "MANIFEST" || name == "manifest.tmp" || epoch_of_dir_name(name)) return true;
  }
  return false;
}

std::optional<Manifest> prepare_resume(const fs::path& ckpt_dir) {
  std::error_code ec;
  if (!fs::is_directory(ckpt_dir, ec)) return std::nullopt;

  std::optional<Manifest> committed;
  const fs::path mp = manifest_path(ckpt_dir);
  if (fs::exists(mp)) {
    std::ifstream in(mp);
    std::stringstream text;
    text << in.rdbuf();
    committed = parse_manifest(text.str());
  }

  // Everything that is not the committed epoch is a leftover of some crash.
  for (const auto& entry : fs::directory_iterator(ckpt_dir)) {
    const std::string name = entry.path().filename().string();
    if (name == "manifest.tmp") {
      fs::remove(entry.path());
      continue;
    }
    const auto k = epoch_of_dir_name(name);
    if (!k) continue;
    if (!committed || *k != committed->epoch) fs::remove_all(entry.path());
  }

  if (committed) {
    for (uint32_t i = 0; i < committed->partitions; ++i) {
      const fs::path sp = snapshot_path(ckpt_dir, committed->epoch, i);
      if (!fs::exists(sp)) {
        throw std::runtime_error("checkpoint corrupt: MANIFEST names epoch " +
                                 std::to_string(committed->epoch) + " but " + sp.string() +
                                 " is missing");
      }
    }
  }
  return committed;
}

// ---- coordinator -----------------------------------------------------------------

CheckpointCoordinator::CheckpointCoordinator(fs::path ckpt_dir, Manifest manifest_template, bool durable)
    : dir_(std::move(ckpt_dir)), template_(manifest_template), durable_(durable) {
  fs::create_directories(dir_);
}

fs::path CheckpointCoordinator::ensure_epoch_dir(uint64_t epoch) {
  std::lock_guard<std::mutex> lock(mu_);
  const fs::path d = epoch_dir(dir_, epoch);
  fs::create_directories(d);
  return d;
}

void CheckpointCoordinator::snapshot_written(uint64_t epoch, uint64_t input_offset,
                                             uint64_t snapshot_bytes) {
  std::lock_guard<std::mutex> lock(mu_);
  Pending& p = pending_[epoch];
  ++p.arrived;
  p.bytes += snapshot_bytes;
  if (p.arrived < template_.partitions) return;
  // Last arriver: every worker's .snap for this epoch is renamed and durable
  // (each one did that before calling here), so the epoch can be committed.
  const uint64_t bytes = p.bytes;
  pending_.erase(epoch);
  commit_locked(epoch, input_offset);
  stats_.last_snapshot_bytes = bytes;
}

void CheckpointCoordinator::commit_locked(uint64_t epoch, uint64_t input_offset) {
  const auto t0 = std::chrono::steady_clock::now();
  Manifest m = template_;
  m.epoch = epoch;
  m.input_offset = input_offset;
  // The directory holding the epoch's snapshots was fsync'ed by each worker
  // after its rename, so by now the names are durable; this rename is the
  // commit point for the whole epoch.
  atomic_write_file(manifest_path(dir_), format_manifest(m), durable_);
  ++stats_.committed;
  stats_.last_epoch = epoch;
  stats_.last_input_offset = input_offset;
  stats_.manifest_secs +=
      std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  // The previous epoch is now unreachable (resume only ever reads MANIFEST's
  // epoch), so delete it. If we die before this line, prepare_resume()
  // removes it next time; nothing depends on it going away now.
  if (epoch > 0) {
    std::error_code ec;
    fs::remove_all(epoch_dir(dir_, epoch - 1), ec);
  }
}

CheckpointStats CheckpointCoordinator::stats() const {
  std::lock_guard<std::mutex> lock(mu_);
  return stats_;
}

}  // namespace adstack
