#pragma once
// ColReader: opens a .col file, loads the footer, and reads column chunks on
// demand. This is the API every later milestone (scan, filter, aggregate)
// builds on, so it stays small: "give me row group i, column c, as a vector".
//
// I/O is fseek + fread at the chunk's recorded offset. Chunks are large and
// contiguous (64K rows * 8 bytes = 512 KiB for a wide column), so one read
// syscall per chunk is already a rounding error next to decoding; mmap would
// buy nothing here and would complicate error handling (SIGBUS on a truncated
// file instead of a clean exception). Revisit if m3 needs random point reads.

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

#include "common/event.h"
#include "src/encoding.h"
#include "src/format.h"

namespace colstore {

class ColReader {
 public:
  // Throws std::runtime_error if the file is missing, truncated, or not a .col.
  explicit ColReader(const std::string& path);
  ~ColReader();
  ColReader(const ColReader&) = delete;
  ColReader& operator=(const ColReader&) = delete;

  const Footer& footer() const { return footer_; }
  size_t num_row_groups() const { return footer_.row_groups.size(); }
  uint64_t total_rows() const { return footer_.total_rows; }
  uint64_t file_size() const { return file_size_; }
  uint64_t footer_bytes() const { return footer_len_; }

  const ChunkMeta& chunk(size_t row_group, ColumnId col) const;

  // Raw encoded bytes of one chunk, exactly chunk(...).length of them.
  std::vector<uint8_t> read_chunk_bytes(size_t row_group, ColumnId col) const;

  // Decoded values of one chunk. T must have the column's width (u8 column
  // -> uint8_t, i64 column -> int64_t, ...); anything else throws.
  template <typename T>
  std::vector<T> read_column(size_t row_group, ColumnId col) const {
    const ChunkMeta& m = chunk(row_group, col);
    const ColumnType type = column_spec(col).type;
    if (sizeof(T) != value_width(type)) {
      throw std::runtime_error(std::string("read_column: wrong value type for ") +
                               column_spec(col).name);
    }
    const std::vector<uint8_t> bytes = read_chunk_bytes(row_group, col);
    // PLAIN is one memcpy straight into the typed vector; everything else
    // decodes to 64-bit patterns and is narrowed (with a width check).
    if (m.encoding == static_cast<uint32_t>(Encoding::Plain)) {
      return decode_plain<T>(bytes.data(), bytes.size(), m.rows);
    }
    return narrow_all<T>(decode_chunk(checked_encoding(m), bytes.data(), bytes.size(), m.rows, type));
  }

  // Decoded values of one chunk as 64-bit patterns (unsigned columns
  // zero-extended, i64 sign-extended), for callers that pick the column at
  // run time (dump). Format with as_signed() when is_signed(column type).
  std::vector<uint64_t> read_column_bits(size_t row_group, ColumnId col) const;

  // Reassembles whole Events for a row group (all ten columns). Used by
  // `verify`; a real query never does this, that is the point of columnar.
  std::vector<adstack::Event> read_events(size_t row_group) const;

 private:
  void read_at(uint64_t offset, void* dst, size_t len) const;
  static Encoding checked_encoding(const ChunkMeta& m);  // throws on an unknown id

  FILE* f_ = nullptr;
  std::string path_;
  uint64_t file_size_ = 0;
  uint64_t footer_len_ = 0;
  Footer footer_;
};

}  // namespace colstore
