#pragma once
// ColWriter: turns a stream of adstack::Event into a .col file.
//
// Rows are buffered in ten typed vectors (one per column). When the buffer
// reaches rows_per_group, each vector is encoded into one chunk (encoding
// chosen per chunk by encode_smallest(), or forced PLAIN) and appended to
// the file, and the chunk's offset/length/encoding/min/max are recorded in
// the in-memory footer. finish() flushes the last partial group and writes
// the footer + trailer. Single pass, no seeking.
//
// Not thread-safe. Call finish() exactly once; without it the file has no
// trailer and readers will (correctly) reject it.

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

#include "common/event.h"
#include "src/format.h"

namespace colstore {

// Which encodings the writer may use. PlainOnly exists so the PLAIN
// baseline in every size table is produced by the same binary, and so
// tests can address chunk bytes arithmetically.
enum class EncodingPolicy { Auto, PlainOnly };

class ColWriter {
 public:
  explicit ColWriter(const std::string& path, uint32_t rows_per_group = kDefaultRowsPerGroup,
                     EncodingPolicy policy = EncodingPolicy::Auto);
  ~ColWriter();
  ColWriter(const ColWriter&) = delete;
  ColWriter& operator=(const ColWriter&) = delete;

  void append(const adstack::Event& e);
  void finish();

  uint64_t rows_written() const { return footer_.total_rows + static_cast<uint64_t>(batch_rows()); }
  uint64_t bytes_written() const { return offset_; }
  const Footer& footer() const { return footer_; }  // complete only after finish()

 private:
  // One row group's worth of values, column by column.
  struct Batch {
    std::vector<uint64_t> event_id;
    std::vector<int64_t> event_time_us;
    std::vector<int64_t> ingest_time_us;
    std::vector<uint8_t> event_type;
    std::vector<uint32_t> advertiser_id;
    std::vector<uint32_t> campaign_id;
    std::vector<uint64_t> video_id;
    std::vector<uint16_t> country;
    std::vector<uint8_t> device;
    std::vector<int64_t> cost_micros;

    void reserve(size_t n);
    void clear();
  };

  size_t batch_rows() const { return batch_.event_id.size(); }
  void flush_row_group();
  template <typename T>
  ChunkMeta write_chunk(ColumnId col, const std::vector<T>& values);
  void write_bytes(const void* data, size_t len);

  FILE* f_ = nullptr;
  std::string path_;
  uint64_t offset_ = 0;  // bytes written so far == next chunk's offset
  Batch batch_;
  std::vector<uint8_t> scratch_;  // reused encode buffer
  std::vector<uint64_t> wide_;    // reused 64-bit view of the column being encoded
  Footer footer_;
  EncodingPolicy policy_;
  bool finished_ = false;
};

}  // namespace colstore
