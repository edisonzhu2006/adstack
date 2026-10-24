// ColReader implementation. See reader.h.

#include "src/reader.h"

#include <stdexcept>

#include "src/crc32c.h"

namespace colstore {

ColReader::ColReader(const std::string& path) : path_(path) {
  f_ = std::fopen(path.c_str(), "rb");
  if (!f_) throw std::runtime_error("cannot open for read: " + path);

  // File size via seek-to-end; the trailer lives in the last 12 bytes.
  if (std::fseek(f_, 0, SEEK_END) != 0) throw std::runtime_error("seek failed: " + path);
  const long end = std::ftell(f_);
  if (end < 0) throw std::runtime_error("ftell failed: " + path);
  file_size_ = static_cast<uint64_t>(end);
  if (file_size_ < kHeaderSize + kTrailerSize) throw std::runtime_error("not a .col file (too small): " + path);

  uint8_t hdr[kHeaderSize];
  read_at(0, hdr, kHeaderSize);
  const uint8_t* p = hdr;
  if (get_le<uint32_t>(p) != kColMagic) throw std::runtime_error("bad header magic: " + path);
  const uint32_t version = get_le<uint32_t>(p);
  if (version != kColVersion) {
    throw std::runtime_error("unsupported .col version " + std::to_string(version) + ": " + path);
  }

  uint8_t trailer[kTrailerSize];
  read_at(file_size_ - kTrailerSize, trailer, kTrailerSize);
  p = trailer;
  footer_len_ = get_le<uint64_t>(p);
  if (get_le<uint32_t>(p) != kColMagic) {
    throw std::runtime_error("bad trailer magic (truncated or unfinished file): " + path);
  }
  if (footer_len_ > file_size_ - kHeaderSize - kTrailerSize) {
    throw std::runtime_error("footer length exceeds file: " + path);
  }

  std::vector<uint8_t> body(static_cast<size_t>(footer_len_));
  const uint64_t footer_offset = file_size_ - kTrailerSize - footer_len_;
  read_at(footer_offset, body.data(), body.size());
  footer_ = decode_footer(body.data(), body.size());

  // Every chunk must lie inside the data region, before the footer.
  for (const auto& rg : footer_.row_groups) {
    for (const auto& c : rg.chunks) {
      if (c.offset < kHeaderSize || c.offset + c.length > footer_offset) {
        throw std::runtime_error("chunk extends outside data region: " + path);
      }
    }
  }
}

ColReader::~ColReader() {
  if (f_) std::fclose(f_);
}

const ChunkMeta& ColReader::chunk(size_t row_group, ColumnId col) const {
  if (row_group >= footer_.row_groups.size()) throw std::out_of_range("row group index");
  if (column_index(col) >= footer_.columns.size()) throw std::out_of_range("column index");
  return footer_.row_groups[row_group].chunks[column_index(col)];
}

std::vector<uint8_t> ColReader::read_chunk_bytes(size_t row_group, ColumnId col) const {
  const ChunkMeta& m = chunk(row_group, col);
  std::vector<uint8_t> bytes(static_cast<size_t>(m.length));
  read_at(m.offset, bytes.data(), bytes.size());
  // Every chunk is checked on every read, before any decoder sees it: the
  // decoders catch structural damage, the checksum catches the rest (a
  // flipped bit that still decodes to some value).
  if (crc32c(bytes.data(), bytes.size()) != m.crc32c) {
    throw std::runtime_error("chunk checksum mismatch (row group " + std::to_string(row_group) + ", column " +
                             spec_of(footer_.columns, col).name + "): " + path_ + " is corrupt");
  }
  return bytes;
}

Encoding ColReader::checked_encoding(const ChunkMeta& m) {
  if (m.encoding >= kNumEncodings) {
    throw std::runtime_error("unsupported encoding id " + std::to_string(m.encoding));
  }
  return static_cast<Encoding>(m.encoding);
}

std::vector<uint64_t> ColReader::read_column_bits(size_t row_group, ColumnId col) const {
  const ChunkMeta& m = chunk(row_group, col);
  const std::vector<uint8_t> bytes = read_chunk_bytes(row_group, col);
  return decode_chunk(checked_encoding(m), bytes.data(), bytes.size(), m.rows, spec_of(footer_.columns, col).type);
}

std::vector<adstack::Event> ColReader::read_events(size_t row_group) const {
  if (!is_event_table()) throw std::logic_error("read_events: this file is not an event table");
  const auto event_id = read_column<uint64_t>(row_group, ColumnId::EventId);
  const auto event_time_us = read_column<int64_t>(row_group, ColumnId::EventTimeUs);
  const auto ingest_time_us = read_column<int64_t>(row_group, ColumnId::IngestTimeUs);
  const auto event_type = read_column<uint8_t>(row_group, ColumnId::EventType);
  const auto advertiser_id = read_column<uint32_t>(row_group, ColumnId::AdvertiserId);
  const auto campaign_id = read_column<uint32_t>(row_group, ColumnId::CampaignId);
  const auto video_id = read_column<uint64_t>(row_group, ColumnId::VideoId);
  const auto country = read_column<uint16_t>(row_group, ColumnId::Country);
  const auto device = read_column<uint8_t>(row_group, ColumnId::Device);
  const auto cost_micros = read_column<int64_t>(row_group, ColumnId::CostMicros);

  const size_t n = event_id.size();
  std::vector<adstack::Event> out(n);
  for (size_t i = 0; i < n; ++i) {
    adstack::Event& e = out[i];
    e.event_id = event_id[i];
    e.event_time_us = event_time_us[i];
    e.ingest_time_us = ingest_time_us[i];
    e.event_type = event_type[i];
    e.advertiser_id = advertiser_id[i];
    e.campaign_id = campaign_id[i];
    e.video_id = video_id[i];
    e.country = country[i];
    e.device = device[i];
    e.cost_micros = cost_micros[i];
  }
  return out;
}

void ColReader::read_at(uint64_t offset, void* dst, size_t len) const {
  if (len == 0) return;
  if (offset + len > file_size_) throw std::runtime_error("read past end of file: " + path_);
  if (std::fseek(f_, static_cast<long>(offset), SEEK_SET) != 0) {
    throw std::runtime_error("seek failed: " + path_);
  }
  if (std::fread(dst, 1, len, f_) != len) throw std::runtime_error("short read: " + path_);
}

}  // namespace colstore
