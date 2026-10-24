#include "common/event.h"

#include <cstring>
#include <stdexcept>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace adstack {

namespace {

template <typename T>
inline void put(uint8_t*& p, T v) {
  static_assert(std::is_trivially_copyable_v<T>);
  std::memcpy(p, &v, sizeof(T));
  p += sizeof(T);
}

template <typename T>
inline T get(const uint8_t*& p) {
  T v;
  std::memcpy(&v, p, sizeof(T));
  p += sizeof(T);
  return v;
}

constexpr size_t kWriteBuf = 1 << 20;  // 1 MiB

}  // namespace

static_assert(kEventWireSize == 52, "wire size drifted; bump kFileVersion");

void encode(const Event& e, uint8_t* out) {
  uint8_t* p = out;
  put(p, e.event_id);
  put(p, e.event_time_us);
  put(p, e.ingest_time_us);
  put(p, e.event_type);
  put(p, e.advertiser_id);
  put(p, e.campaign_id);
  put(p, e.video_id);
  put(p, e.country);
  put(p, e.device);
  put(p, e.cost_micros);
}

Event decode(const uint8_t* in) {
  const uint8_t* p = in;
  Event e;
  e.event_id = get<uint64_t>(p);
  e.event_time_us = get<int64_t>(p);
  e.ingest_time_us = get<int64_t>(p);
  e.event_type = get<uint8_t>(p);
  e.advertiser_id = get<uint32_t>(p);
  e.campaign_id = get<uint32_t>(p);
  e.video_id = get<uint64_t>(p);
  e.country = get<uint16_t>(p);
  e.device = get<uint8_t>(p);
  e.cost_micros = get<int64_t>(p);
  return e;
}

// ---- writer -------------------------------------------------------------

EventWriter::EventWriter(const std::string& path, Mode mode) {
  uint64_t existing = 0;
  if (path == "-") {
    f_ = stdout;
    owns_ = false;
  } else {
    if (mode == Mode::kAppend) {
      struct stat st {};
      if (::stat(path.c_str(), &st) == 0) existing = static_cast<uint64_t>(st.st_size);
    }
    f_ = std::fopen(path.c_str(), mode == Mode::kAppend ? "ab" : "wb");
    if (!f_) throw std::runtime_error("cannot open for write: " + path);
  }
  buf_.reserve(kWriteBuf);
  // bytes_ counts what has reached the file: `existing` now, and whatever
  // flush() writes later (the header below is in buf_, so flush() counts it).
  if (existing == 0) {
    uint8_t hdr[kFileHeaderSize];
    uint8_t* p = hdr;
    put(p, kFileMagic);
    put(p, kFileVersion);
    buf_.insert(buf_.end(), hdr, hdr + kFileHeaderSize);
  } else {
    // A file being continued must hold a header and whole records; a torn
    // tail means someone truncated it wrong, and appending would corrupt it.
    if (existing < kFileHeaderSize || (existing - kFileHeaderSize) % kEventWireSize != 0) {
      std::fclose(f_);
      f_ = nullptr;
      throw std::runtime_error("cannot append to a torn event file: " + path);
    }
    bytes_ = existing;
  }
}

EventWriter::~EventWriter() {
  if (!f_) return;
  try {
    flush();
  } catch (...) {
    // Swallowed on purpose; see the header. close() is the checked path.
  }
  if (owns_) std::fclose(f_);
}

void EventWriter::close() {
  if (!f_) return;
  flush();
  FILE* f = f_;
  f_ = nullptr;
  if (owns_) {
    if (std::fclose(f) != 0) throw std::runtime_error("close of event file failed");
  } else if (std::fflush(f) != 0) {
    throw std::runtime_error("flush of event stream failed");
  }
}

void EventWriter::write(const Event& e) {
  if (!f_) throw std::logic_error("EventWriter::write after close");
  uint8_t tmp[kEventWireSize];
  encode(e, tmp);
  buf_.insert(buf_.end(), tmp, tmp + kEventWireSize);
  ++count_;
  if (buf_.size() >= kWriteBuf) flush();
}

void EventWriter::flush() {
  if (buf_.empty() || !f_) return;
  if (std::fwrite(buf_.data(), 1, buf_.size(), f_) != buf_.size()) {
    throw std::runtime_error("short write to event file");
  }
  bytes_ += buf_.size();
  buf_.clear();
  if (std::fflush(f_) != 0) throw std::runtime_error("flush of event file failed");
}

void EventWriter::sync() {
  if (!f_) throw std::logic_error("EventWriter::sync after close");
  flush();
  const int fd = ::fileno(f_);
#if defined(__APPLE__)
  // fsync on macOS only pushes to the drive's cache; F_FULLFSYNC flushes the
  // cache too. It is unsupported on some file systems, hence the fallback.
  if (::fcntl(fd, F_FULLFSYNC) == 0) return;
#endif
  if (::fsync(fd) != 0) throw std::runtime_error("fsync failed");
}

// ---- reader -------------------------------------------------------------

EventReader::EventReader(const std::string& path) {
  if (path == "-") {
    f_ = stdin;
    owns_ = false;
  } else {
    f_ = std::fopen(path.c_str(), "rb");
    if (!f_) throw std::runtime_error("cannot open for read: " + path);
  }
  uint8_t hdr[kFileHeaderSize];
  if (std::fread(hdr, 1, kFileHeaderSize, f_) != kFileHeaderSize) {
    throw std::runtime_error("missing header: " + path);
  }
  const uint8_t* p = hdr;
  if (get<uint32_t>(p) != kFileMagic) throw std::runtime_error("bad magic: " + path);
  if (get<uint32_t>(p) != kFileVersion) throw std::runtime_error("bad version: " + path);
}

EventReader::~EventReader() {
  if (owns_ && f_) std::fclose(f_);
}

bool EventReader::next(Event& out) {
  uint8_t tmp[kEventWireSize];
  size_t n = std::fread(tmp, 1, kEventWireSize, f_);
  if (n == 0) return false;
  if (n != kEventWireSize) throw std::runtime_error("torn record at end of file");
  out = decode(tmp);
  ++count_;
  return true;
}

const char* event_type_name(uint8_t t) {
  switch (static_cast<EventType>(t)) {
    case EventType::Impression: return "impression";
    case EventType::Click: return "click";
    case EventType::Conversion: return "conversion";
  }
  return "unknown";
}

}  // namespace adstack
