// ColWriter implementation. See writer.h.

#include "src/writer.h"

#include <stdexcept>

#include "src/encoding.h"

namespace colstore {

namespace {
constexpr size_t kStdioBuf = 1 << 20;  // 1 MiB stdio buffer: fewer write syscalls
}

void ColWriter::Batch::reserve(size_t n) {
  event_id.reserve(n);
  event_time_us.reserve(n);
  ingest_time_us.reserve(n);
  event_type.reserve(n);
  advertiser_id.reserve(n);
  campaign_id.reserve(n);
  video_id.reserve(n);
  country.reserve(n);
  device.reserve(n);
  cost_micros.reserve(n);
}

void ColWriter::Batch::clear() {
  event_id.clear();
  event_time_us.clear();
  ingest_time_us.clear();
  event_type.clear();
  advertiser_id.clear();
  campaign_id.clear();
  video_id.clear();
  country.clear();
  device.clear();
  cost_micros.clear();
}

ColWriter::ColWriter(const std::string& path, uint32_t rows_per_group, EncodingPolicy policy)
    : path_(path), policy_(policy) {
  if (rows_per_group == 0) throw std::invalid_argument("rows_per_group must be > 0");
  f_ = std::fopen(path.c_str(), "wb");
  if (!f_) throw std::runtime_error("cannot open for write: " + path);
  std::setvbuf(f_, nullptr, _IOFBF, kStdioBuf);

  footer_.rows_per_group = rows_per_group;
  batch_.reserve(rows_per_group);

  uint8_t hdr[kHeaderSize];
  uint8_t* p = hdr;
  put_le(p, kColMagic);
  put_le(p, kColVersion);
  write_bytes(hdr, kHeaderSize);
}

ColWriter::~ColWriter() {
  if (f_) std::fclose(f_);
}

void ColWriter::append(const adstack::Event& e) {
  if (finished_) throw std::logic_error("append after finish");
  batch_.event_id.push_back(e.event_id);
  batch_.event_time_us.push_back(e.event_time_us);
  batch_.ingest_time_us.push_back(e.ingest_time_us);
  batch_.event_type.push_back(e.event_type);
  batch_.advertiser_id.push_back(e.advertiser_id);
  batch_.campaign_id.push_back(e.campaign_id);
  batch_.video_id.push_back(e.video_id);
  batch_.country.push_back(e.country);
  batch_.device.push_back(e.device);
  batch_.cost_micros.push_back(e.cost_micros);
  if (batch_rows() == footer_.rows_per_group) flush_row_group();
}

void ColWriter::flush_row_group() {
  if (batch_rows() == 0) return;
  RowGroupMeta rg;
  rg.rows = static_cast<uint32_t>(batch_rows());
  // Chunk order within a row group == ColumnId order == Event field order.
  rg.chunks[static_cast<size_t>(ColumnId::EventId)] = write_chunk(ColumnId::EventId, batch_.event_id);
  rg.chunks[static_cast<size_t>(ColumnId::EventTimeUs)] = write_chunk(ColumnId::EventTimeUs, batch_.event_time_us);
  rg.chunks[static_cast<size_t>(ColumnId::IngestTimeUs)] = write_chunk(ColumnId::IngestTimeUs, batch_.ingest_time_us);
  rg.chunks[static_cast<size_t>(ColumnId::EventType)] = write_chunk(ColumnId::EventType, batch_.event_type);
  rg.chunks[static_cast<size_t>(ColumnId::AdvertiserId)] = write_chunk(ColumnId::AdvertiserId, batch_.advertiser_id);
  rg.chunks[static_cast<size_t>(ColumnId::CampaignId)] = write_chunk(ColumnId::CampaignId, batch_.campaign_id);
  rg.chunks[static_cast<size_t>(ColumnId::VideoId)] = write_chunk(ColumnId::VideoId, batch_.video_id);
  rg.chunks[static_cast<size_t>(ColumnId::Country)] = write_chunk(ColumnId::Country, batch_.country);
  rg.chunks[static_cast<size_t>(ColumnId::Device)] = write_chunk(ColumnId::Device, batch_.device);
  rg.chunks[static_cast<size_t>(ColumnId::CostMicros)] = write_chunk(ColumnId::CostMicros, batch_.cost_micros);
  footer_.total_rows += rg.rows;
  footer_.row_groups.push_back(rg);
  batch_.clear();
}

template <typename T>
ChunkMeta ColWriter::write_chunk(ColumnId col, const std::vector<T>& values) {
  const ColumnType type = column_spec(col).type;
  if (sizeof(T) != value_width(type)) throw std::logic_error("write_chunk: value type does not match column");

  ChunkMeta m;
  m.offset = offset_;
  m.rows = static_cast<uint32_t>(values.size());
  min_max_bits(values, m.min_bits, m.max_bits);  // zone map for m3, every integer column

  // Try the compact codecs on the 64-bit view of the column and keep the
  // smallest; if nothing beats PLAIN (or the policy forbids trying), write
  // the typed memcpy form. See encode_smallest() for the rule.
  Encoding enc = Encoding::Plain;
  scratch_.clear();
  if (policy_ == EncodingPolicy::Auto) {
    widen_all(values, wide_);
    enc = encode_smallest(wide_.data(), wide_.size(), type, scratch_);
  }
  if (enc == Encoding::Plain) encode_plain(values, scratch_);

  m.encoding = static_cast<uint32_t>(enc);
  m.length = scratch_.size();
  write_bytes(scratch_.data(), scratch_.size());
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
