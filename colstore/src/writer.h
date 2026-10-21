#pragma once
// ColWriter: turns a stream of rows into a .col file.
//
// A row is one 64-bit pattern per schema column (unsigned values
// zero-extended, i64 sign-extended, as encoding.h's widen() produces).
// Rows are buffered column by column at the column's natural width, so a
// column buffer IS the PLAIN chunk for the row group. When the buffer
// reaches rows_per_group, each column is encoded into one chunk (encoding
// chosen per chunk by encode_smallest(), or forced PLAIN) and appended to
// the file, and the chunk's offset/length/encoding/min/max are recorded in
// the in-memory footer. finish() flushes the last partial group and writes
// the footer + trailer. Single pass, no seeking.
//
// The writer takes a Schema (format.h) so the same code produces the
// 10-column event table and the 6-column aggregate table; append(Event) is
// a convenience for the former that widens the fields and calls
// append_row().
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
  // The event table (event_schema()).
  explicit ColWriter(const std::string& path, uint32_t rows_per_group = kDefaultRowsPerGroup,
                     EncodingPolicy policy = EncodingPolicy::Auto);
  // Any table. Throws std::invalid_argument on an invalid schema.
  ColWriter(const std::string& path, Schema schema, uint32_t rows_per_group, EncodingPolicy policy);
  ~ColWriter();
  ColWriter(const ColWriter&) = delete;
  ColWriter& operator=(const ColWriter&) = delete;

  const Schema& schema() const { return footer_.columns; }

  // One row: bits[c] is column c's widened value. Throws std::invalid_argument
  // if a value does not fit its column's width (a CSV "300" for a u8).
  void append_row(const uint64_t* bits);
  void append_row(const std::vector<uint64_t>& bits);
  // Only valid when schema() is the event schema; throws std::logic_error otherwise.
  void append(const adstack::Event& e);
  void finish();

  uint64_t rows_written() const { return footer_.total_rows + batch_rows_; }
  uint64_t bytes_written() const { return offset_; }
  const Footer& footer() const { return footer_; }  // complete only after finish()

 private:
  // One column's buffered values for the current row group, at natural
  // width, little-endian: exactly the bytes a PLAIN chunk holds. `bytes`
  // is sized once for a full row group and `used` is the fill cursor, so
  // appending a value is one fixed-size memcpy with no capacity check.
  struct ColumnBuffer {
    ColumnType type = ColumnType::U64;
    size_t width = 8;
    std::vector<uint8_t> bytes;
    size_t used = 0;
  };

  void flush_row_group();
  ChunkMeta write_chunk(const ColumnBuffer& col);
  void write_bytes(const void* data, size_t len);

  FILE* f_ = nullptr;
  std::string path_;
  uint64_t offset_ = 0;  // bytes written so far == next chunk's offset
  std::vector<ColumnBuffer> cols_;
  size_t batch_rows_ = 0;         // rows buffered in cols_
  std::vector<uint8_t> scratch_;  // reused encode buffer
  std::vector<uint64_t> wide_;    // reused 64-bit view of the column being encoded
  std::vector<uint64_t> row_;     // reused by append(Event)
  Footer footer_;
  EncodingPolicy policy_;
  bool is_event_schema_ = false;
  bool finished_ = false;
};

}  // namespace colstore
