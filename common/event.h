#pragma once
// Shared ad-event schema and the binary file format used by every tool in
// adstack. Keep this header small and stable: streamkit, colstore, the
// generator, the oracle and the real-data adapters all speak this format.

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

namespace adstack {

enum class EventType : uint8_t { Impression = 0, Click = 1, Conversion = 2 };
enum class Device : uint8_t { Mobile = 0, Desktop = 1, TV = 2 };

struct Event {
  uint64_t event_id = 0;        // unique per logical event; a duplicate reuses it
  int64_t event_time_us = 0;    // when the event happened (microseconds since epoch)
  int64_t ingest_time_us = 0;   // when it reached us; always >= event_time_us
  uint8_t event_type = 0;       // EventType
  uint32_t advertiser_id = 0;
  uint32_t campaign_id = 0;
  uint64_t video_id = 0;
  uint16_t country = 0;         // ISO 3166-1 numeric, 0 = unknown
  uint8_t device = 0;           // Device
  int64_t cost_micros = 0;      // micro-dollars, 0 when not billable

  bool operator==(const Event&) const = default;
};

// Wire layout: fixed 52 bytes, little-endian, fields in declaration order,
// no padding. Fixed width keeps the reader trivial (fread of 52 bytes) and
// lets colstore compute row offsets without an index.
constexpr size_t kEventWireSize = 8 + 8 + 8 + 1 + 4 + 4 + 8 + 2 + 1 + 8;

// Byte offset of campaign_id inside a wire record (after event_id,
// event_time_us, ingest_time_us, event_type, advertiser_id). streamkit's
// reader thread partitions on campaign_id and peeks it here instead of
// decoding the whole record; tests/event_test.cpp pins it against decode().
constexpr size_t kWireOffsetCampaignId = 8 + 8 + 8 + 1 + 4;

// File = 8-byte header (magic "ADS1" + uint32 version) + N wire events.
constexpr uint32_t kFileMagic = 0x31534441;  // "ADS1" read as little-endian
constexpr uint32_t kFileVersion = 1;
constexpr size_t kFileHeaderSize = 8;

void encode(const Event& e, uint8_t* out);   // writes kEventWireSize bytes
Event decode(const uint8_t* in);             // reads kEventWireSize bytes

// Buffered sequential writer. Not thread-safe.
class EventWriter {
 public:
  enum class Mode {
    kTruncate,  // start a new file: write the header
    kAppend,    // continue an existing file (streamkit resume): keep its
                // header and records, write the header only if it is empty
  };
  explicit EventWriter(const std::string& path, Mode mode = Mode::kTruncate);  // "-" = stdout
  ~EventWriter();
  EventWriter(const EventWriter&) = delete;
  EventWriter& operator=(const EventWriter&) = delete;

  void write(const Event& e);
  void flush();
  // flush() plus fsync (F_FULLFSYNC on Apple), so the records are on disk
  // before a checkpoint claims they are. Throws on failure.
  void sync();
  // Records written through this writer this session (append mode does not
  // count the records that were already in the file).
  uint64_t count() const { return count_; }
  // Length of the file in bytes once flush() has run: header plus every
  // record, including those already present in append mode.
  uint64_t bytes() const { return bytes_; }

 private:
  FILE* f_ = nullptr;
  bool owns_ = true;
  std::vector<uint8_t> buf_;
  uint64_t count_ = 0;
  uint64_t bytes_ = 0;
};

// Buffered sequential reader. Not thread-safe.
class EventReader {
 public:
  explicit EventReader(const std::string& path);  // "-" = stdin
  ~EventReader();
  EventReader(const EventReader&) = delete;
  EventReader& operator=(const EventReader&) = delete;

  // Returns false at end of file. Throws std::runtime_error on a torn record.
  bool next(Event& out);
  uint64_t count() const { return count_; }

 private:
  FILE* f_ = nullptr;
  bool owns_ = true;
  uint64_t count_ = 0;
};

const char* event_type_name(uint8_t t);

}  // namespace adstack
