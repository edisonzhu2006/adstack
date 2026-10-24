// ColWriter implementation. See writer.h.

#include "src/writer.h"

#include <cstring>
#include <stdexcept>

#include "src/crc32c.h"
#include "src/encoding.h"
#include "src/predicate.h"  // event_row_bits(): the Event -> widened-row view

namespace colstore {

namespace {
constexpr size_t kStdioBuf = 1 << 20;  // 1 MiB stdio buffer: fewer write syscalls
}

ColWriter::ColWriter(const std::string& path, uint32_t rows_per_group, EncodingPolicy policy)
    : ColWriter(path, event_schema(), rows_per_group, policy) {}

ColWriter::ColWriter(const std::string& path, Schema schema, uint32_t rows_per_group, EncodingPolicy policy)
    : path_(path), policy_(policy) {
  if (rows_per_group == 0) throw std::invalid_argument("rows_per_group must be > 0");
  validate_schema(schema);
  is_event_schema_ = schema == event_schema();
  footer_.columns = std::move(schema);
  footer_.rows_per_group = rows_per_group;
  for (const ColumnSpec& c : footer_.columns) {
    ColumnBuffer buf;
    buf.type = c.type;
    buf.width = value_width(c.type);
    buf.bytes.resize(static_cast<size_t>(rows_per_group) * buf.width);
    cols_.push_back(std::move(buf));
  }
  row_.resize(cols_.size());

  f_ = std::fopen(path.c_str(), "wb");
  if (!f_) throw std::runtime_error("cannot open for write: " + path);
  std::setvbuf(f_, nullptr, _IOFBF, kStdioBuf);

  uint8_t hdr[kHeaderSize];
  uint8_t* p = hdr;
  put_le(p, kColMagic);
  put_le(p, kColVersion);
  write_bytes(hdr, kHeaderSize);
}

ColWriter::~ColWriter() {
  if (f_) std::fclose(f_);
}

void ColWriter::append_row(const uint64_t* bits) {
  if (finished_) throw std::logic_error("append after finish");
  for (size_t c = 0; c < cols_.size(); ++c) {
    ColumnBuffer& col = cols_[c];
    const uint64_t v = bits[c];
    // A widened value of a narrow unsigned column has zero high bits; one
    // that does not came from outside (a CSV field) and must not be
    // silently truncated. 8-byte columns use every bit.
    if (col.width < 8 && (v >> (8 * col.width)) != 0) {
      throw std::invalid_argument("value " + std::to_string(v) + " does not fit column " + footer_.columns[c].name +
                                  " (" + column_type_name(col.type) + ")");
    }
    // Little-endian host (format.h): the low `width` bytes of the pattern
    // are the value at its natural width. The switch gives memcpy a
    // compile-time size, so each case is a single store.
    uint8_t* dst = col.bytes.data() + col.used;
    switch (col.width) {
      case 1: std::memcpy(dst, &v, 1); break;
      case 2: std::memcpy(dst, &v, 2); break;
      case 4: std::memcpy(dst, &v, 4); break;
      default: std::memcpy(dst, &v, 8); break;
    }
    col.used += col.width;
  }
  if (++batch_rows_ == footer_.rows_per_group) flush_row_group();
}

void ColWriter::append_row(const std::vector<uint64_t>& bits) {
  if (bits.size() != cols_.size()) throw std::invalid_argument("append_row: wrong number of values");
  append_row(bits.data());
}

void ColWriter::append(const adstack::Event& e) {
  if (!is_event_schema_) throw std::logic_error("append(Event) on a writer whose schema is not the event table");
  event_row_bits(e, row_.data());
  append_row(row_.data());
}

void ColWriter::flush_row_group() {
  if (batch_rows_ == 0) return;
  RowGroupMeta rg;
  rg.rows = static_cast<uint32_t>(batch_rows_);
  // Chunk order within a row group == schema order.
  for (ColumnBuffer& col : cols_) {
    rg.chunks.push_back(write_chunk(col));
    col.used = 0;
  }
  footer_.total_rows += rg.rows;
  footer_.row_groups.push_back(std::move(rg));
  batch_rows_ = 0;
}

ChunkMeta ColWriter::write_chunk(const ColumnBuffer& col) {
  ChunkMeta m;
  m.offset = offset_;
  m.rows = static_cast<uint32_t>(batch_rows_);
  // Zone map for every column, straight from the typed bytes (one
  // std::minmax_element pass at the column's own width).
  min_max_bytes(col.type, col.bytes.data(), batch_rows_, m.min_bits, m.max_bits);

  // Try the compact codecs on the 64-bit view of the column and keep the
  // smallest; if nothing beats PLAIN (or the policy forbids trying), the
  // chunk is the buffer itself and the view is never built. See
  // encode_smallest() for the rule.
  Encoding enc = Encoding::Plain;
  scratch_.clear();
  if (policy_ == EncodingPolicy::Auto) {
    widen_bytes(col.type, col.bytes.data(), batch_rows_, wide_);
    enc = encode_smallest(wide_.data(), wide_.size(), col.type, scratch_);
  }

  m.encoding = static_cast<uint32_t>(enc);
  // The checksum covers exactly the bytes written, so a reader can tell a
  // flipped bit anywhere in the chunk from a valid value (crc32c.h).
  const uint8_t* data = enc == Encoding::Plain ? col.bytes.data() : scratch_.data();
  m.length = enc == Encoding::Plain ? col.used : scratch_.size();
  m.crc32c = crc32c(data, static_cast<size_t>(m.length));
  write_bytes(data, static_cast<size_t>(m.length));
  return m;
}

void ColWriter::finish() {
  if (finished_) return;
  flush_row_group();

  const std::vector<uint8_t> body = encode_footer(footer_);
  write_bytes(body.data(), body.size());

  uint8_t trailer[kTrailerSize];
  uint8_t* p = trailer;
  put_le(p, static_cast<uint64_t>(body.size()));
  put_le(p, kColMagic);
  write_bytes(trailer, kTrailerSize);

  if (std::fflush(f_) != 0) throw std::runtime_error("flush failed: " + path_);
  std::fclose(f_);
  f_ = nullptr;
  finished_ = true;
}

void ColWriter::write_bytes(const void* data, size_t len) {
  if (len == 0) return;
  if (std::fwrite(data, 1, len, f_) != len) throw std::runtime_error("short write: " + path_);
  offset_ += len;
}

}  // namespace colstore
